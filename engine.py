"""Bounded listener state machine and killable network child, independent of framework types."""

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import time
from . import logic


class NetworkProcess:
    """One disposable client; reconnection creates a fresh process and never replays its queue."""

    def __init__(self, config, auth):
        self.process = subprocess.Popen([sys.executable, "-B", str(Path(__file__).with_name("worker.py"))],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={key: value for key, value in os.environ.items() if key in
                 {"PATH", "LANG", "LC_ALL", "SYSTEMROOT"}}, close_fds=True)
        os.set_blocking(self.process.stdout.fileno(), False)
        os.set_blocking(self.process.stdin.fileno(), False)
        self.outgoing = bytearray(logic.encoded({"config": config, "auth": auth}) + b"\n")
        self.incoming = b""

    def send(self, value):
        line = logic.encoded(value) + b"\n"
        if len(self.outgoing) + len(line) > logic.MAX_LINE * 2:
            raise logic.MqttError("Network command buffer is full.")
        self.outgoing.extend(line)

    def poll(self):
        if self.outgoing:
            try:
                count = os.write(self.process.stdin.fileno(), self.outgoing)
                del self.outgoing[:count]
            except BlockingIOError:
                pass
        items = []
        # Limit work per listener tick; Stop and control commands stay responsive.
        for _ in range(16):
            try:
                chunk = os.read(self.process.stdout.fileno(), 65536)
            except BlockingIOError:
                break
            if not chunk:
                break
            self.incoming += chunk
            while b"\n" in self.incoming:
                line, self.incoming = self.incoming.split(b"\n", 1)
                items.append(logic.loads(line))
            if len(self.incoming) > logic.MAX_LINE:
                raise logic.MqttError("Oversized network response.")
        if not items and self.process.poll() is not None:
            raise logic.MqttError("Network process ended.")
        return items

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=0.2)
        self.process.stdin.close()
        self.process.stdout.close()


class Connection:
    """Only this Run owns IDs, pending requests and retry budget. No silent business retry."""

    def __init__(self, config, auth, emit, factory=NetworkProcess):
        self.config, self.auth, self.emit, self.factory = config, auth, emit, factory
        self.child, self.pending, self.seen = None, {}, {}
        self.state = "inactive" if not config["broker_host"] else "connecting"
        self.retry_count, self.next_attempt = 0, 0
        self.started, self.activity = 0, 0
        self.report(self.state)

    def report(self, code, **data):
        self.emit({"status": {"code": code, "connection": self.state, **data}})

    def close_child(self):
        if self.child:
            self.child.close()
            self.child = None
        for request_id in self.pending:
            self.report("publish_uncertain", request_id=request_id)
        self.pending.clear()

    def fail(self, code, now):
        self.close_child()
        self.state = "retry_wait" if self.retry_count < self.config["reconnect_attempts"] else "disconnected"
        self.retry_count += 1
        self.next_attempt = now + self.config["reconnect_delay_sec"]
        self.report(code, retry_scheduled=self.state == "retry_wait")

    def command(self, item):
        if item.get("type") == "command":
            action = item["action"]
            if action == "pause":
                self.state = "paused"
                self.close_child()
            elif action == "resume" and self.state in {"paused", "disconnected"}:
                self.state = "connecting" if self.config["broker_host"] else "inactive"
                self.retry_count, self.next_attempt = 0, 0
            self.report(self.state, **({"request_id": item["request_id"]} if "request_id" in item else {}))
            return
        request_id = item["request_id"]
        digest = hashlib.sha256(logic.encoded(item)).hexdigest()
        if request_id in self.seen:
            self.report("duplicate_ignored" if self.seen[request_id] == digest else "request_id_conflict", request_id=request_id)
        elif self.state != "connected":
            self.report("publish_not_connected", request_id=request_id)
        elif len(self.seen) >= 10000:
            self.report("run_request_limit", request_id=request_id)
        elif len(self.pending) >= self.config["max_queue"]:
            self.report("publish_queue_full", request_id=request_id)
        else:
            self.seen[request_id] = digest
            self.pending[request_id] = time.monotonic()
            try:
                self.child.send(item)
            except (OSError, logic.MqttError):
                self.fail("publish_transport_failed", time.monotonic())

    def tick(self, now):
        if self.state in {"paused", "inactive", "disconnected"}:
            return
        if self.child is None and now >= self.next_attempt:
            try:
                self.child = self.factory(self.config, self.auth)
                self.started = self.activity = now
                self.state = "connecting"
                self.report(self.state)
            except (OSError, ValueError):
                self.fail("network_process_unavailable", now)
                return
        if self.child is None:
            return
        try:
            items = self.child.poll()
        except (OSError, ValueError):
            self.fail("connection_lost", now)
            return
        for item in items:
            self.activity = now
            kind = item.get("type")
            if kind == "error":
                self.fail(item["code"], now)
                return
            if kind == "ready":
                self.state = "connected"
                self.report("connected")
            elif kind == "message":
                self.emit({"message": item["message"]})
            elif kind == "ack" and item.get("request_id") in self.pending:
                self.pending.pop(item["request_id"])
                self.report("published", request_id=item["request_id"], assurance=item["assurance"])
        if self.state == "connecting" and now - self.started > self.config["connect_timeout_sec"]:
            self.fail("connection_timeout", now)
        elif now - self.activity > 5:
            self.fail("connection_unresponsive", now)
        elif any(now - started > 30 for started in self.pending.values()):
            self.fail("publish_ack_timeout", now)

    def close(self):
        # Stop is not an occasion to publish graph events; active close reports no result.
        if self.child:
            self.child.close()
            self.child = None
        self.pending.clear()
