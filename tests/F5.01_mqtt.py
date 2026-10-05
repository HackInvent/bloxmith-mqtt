"""FB1/FB2/FB3: pure admission rules, real Paho/broker transport and bounded reconnects."""

from pathlib import Path
import socket
import sys
import time
from types import MappingProxyType, SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests"), str(Path(__file__).parent)]
from blocs.mqtt import logic, engine
from broker_fixture import broker, until
from paho.mqtt.client import topic_matches_sub


def refused(action):
    try:
        action()
    except logic.MqttError:
        return
    raise AssertionError("Invalid request was accepted")


def main():
    config = logic.configuration({"publish_topics": ["allowed/#"]})
    assert logic.configuration(MappingProxyType({"subscriptions": ("allowed/#",)}))["subscriptions"] == ["allowed/#"]
    publish = {"request_id": "p1", "topic": "allowed/item", "payload": {"temperature": 21.5}}
    for raw in ({"password": "not-config"}, {"tls": "false"}, {"broker_host": "https://host/path"},
        {"qos": 2}, {"max_queue": 0}, {"subscriptions": ["a/#/b"]}, {"publish_topics": ["a", "a"]},
        {"secret_ref": "secret://workspace/mqtt", "tls": False}, {"clean_session": False}, {"payload_format": {}}):
        refused(lambda: logic.configuration(raw))
    for raw in ({**publish, "topic": "other/x"}, {**publish, "retain": True}, {**publish, "broker_host": "evil"},
        {**publish, "request_id": "../id"}, {**publish, "payload": float("nan")}, {**publish, "topic": "allowed/\ud800"},
        {**publish, "payload": "\ud800"}, '{"request_id":"a","request_id":"b"}'):
        refused(lambda: logic.message(raw, "publish", config))
    refused(lambda: logic.message({"action": []}, "command", config))
    for pattern in ("#", "a/#", "a/+", "$SYS/#", "/+", "+/x", "a", "a//+"):
        for value in ("a", "a/", "a/b", "$SYS/x", "/x", "a//b", "a/x", "ab"):
            assert logic.matches(pattern, value) == topic_matches_sub(pattern, value), (pattern, value)
    wallet = SimpleNamespace(services={"resolve_secret": lambda _: '{"username":"fixture","password":"never-log-this"}'})
    auth_config = logic.configuration({"secret_ref": "secret://workspace/mqtt"})
    assert logic.credentials(wallet, auth_config)["username"] == "fixture"
    refused(lambda: logic.credentials(SimpleNamespace(services={}), auth_config))
    wallet.services["resolve_secret"] = lambda _: {"username": "secret-value-must-not-leak"}
    try:
        logic.credentials(wallet, auth_config)
    except logic.MqttError as error:
        assert "secret-value-must-not-leak" not in str(error)

    for tls in (False, True):
        with broker(tls=tls, authenticated=tls) as endpoint:
            config = logic.configuration({**endpoint, "subscriptions": ["allowed/#"], "publish_topics": ["allowed/#"], "reconnect_attempts": 0})
            results = []
            auth = {"username": "fixture", "password": "local-test-password"} if tls else None
            connection = engine.Connection(config, auth, results.append)
            try:
                until(connection, lambda: connection.state == "connected")
                child = connection.child.process
                assert "password" not in str(child.args)
                item = logic.message(publish, "publish", config)
                connection.command(item)
                until(connection, lambda: any(row.get("status", {}).get("code") == "published" for row in results)
                      and any("message" in row for row in results))
                received = next(row["message"] for row in results if "message" in row)
                assert received["topic"] == publish["topic"] and received["payload"] == publish["payload"]
                assert received["qos"] == 1 and received["retained"] is False
                connection.command(item)
                assert results[-1]["status"]["code"] == "duplicate_ignored"
                connection.command({**item, "payload": "changed"})
                assert results[-1]["status"]["code"] == "request_id_conflict"
                connection.command({"type": "command", "action": "pause"})
                assert connection.state == "paused" and child.poll() is not None
                connection.command({**item, "request_id": "paused"})
                assert results[-1]["status"]["code"] == "publish_not_connected"
                connection.command({"type": "command", "action": "resume"})
                until(connection, lambda: connection.state == "connected")
                connection.command(item)
                assert results[-1]["status"]["code"] == "duplicate_ignored"
            finally:
                started = time.monotonic()
                process = connection.child.process if connection.child else None
                connection.close()
                assert time.monotonic() - started < 0.8
                assert process is None or process.poll() is not None
            if tls:
                bad = engine.Connection({**config, "ca_file": ""}, auth, results.append)
                try:
                    until(bad, lambda: bad.state == "disconnected")
                    assert bad.state != "connected", "Untrusted TLS certificates must never be accepted"
                finally:
                    bad.close()
                wrong = engine.Connection(config, {"username": "fixture", "password": "wrong-fixture-password"}, results.append)
                try:
                    until(wrong, lambda: wrong.state == "disconnected")
                    assert "wrong-fixture-password" not in str(results)
                finally:
                    wrong.close()
    # Real protocol variants: QoS 0, retained replay filters, malformed and excessive ingress.
    with broker() as endpoint:
        base = logic.configuration({**endpoint, "payload_format": "text", "qos": 0, "publish_topics": ["allowed/#"],
            "subscriptions": ["allowed/#"], "allow_retained": True, "reconnect_attempts": 0})
        rows = []
        sender = engine.Connection(base, None, rows.append)
        try:
            until(sender, lambda: sender.state == "connected")
            sender.command(logic.message({**publish, "payload": "retained fixture", "retain": True}, "publish", base))
            until(sender, lambda: any(row.get("status", {}).get("assurance") == "socket_sent" for row in rows))
            for allowed in (False, True):
                values = []
                receiver = engine.Connection({**base, "allow_retained": allowed}, None, values.append)
                try:
                    until(receiver, lambda: receiver.state == "connected")
                    deadline = time.monotonic() + 0.25
                    while time.monotonic() < deadline:
                        receiver.tick(time.monotonic()); time.sleep(0.01)
                    messages = [row["message"] for row in values if "message" in row]
                    assert bool(messages) == allowed, values
                    if allowed:
                        assert messages[0]["retained"] is True and messages[0]["payload"] == "retained fixture"
                finally:
                    receiver.close()
            values = []
            receiver = engine.Connection({**base, "payload_format": "json"}, None, values.append)
            try:
                until(receiver, lambda: receiver.state == "disconnected")
                assert values[-1]["status"]["code"] == "invalid_incoming_payload"
            finally:
                receiver.close()
            values = []
            receiver = engine.Connection({**base, "max_payload_bytes": 1}, None, values.append)
            try:
                until(receiver, lambda: receiver.state == "disconnected")
                assert values[-1]["status"]["code"] == "invalid_incoming_payload"
            finally:
                receiver.close()
            # aMQTT 0.11.3 replays retained topics from all subscriptions on new
            # connections (broker._handle_client_session), even without a matching
            # subscription. Verify our refusal; do not weaken the topic allowlist.
            values = []
            receiver = engine.Connection({**base, "subscriptions": []}, None, values.append)
            try:
                until(receiver, lambda: receiver.state == "disconnected")
                assert values[-1]["status"]["code"] == "invalid_incoming_payload", values
                assert not any("message" in row for row in values)
            finally:
                receiver.close()
        finally:
            sender.close()
    # A separate real broker makes the rate fixture independent of retained replay.
    with broker() as endpoint:
        base = logic.configuration({**endpoint, "payload_format": "text", "qos": 0,
            "publish_topics": ["burst/#"], "reconnect_attempts": 0})
        values, sender_values = [], []
        receiver = engine.Connection({**base, "subscriptions": ["burst/#"],
            "max_messages_per_sec": 1}, None, values.append)
        sender = engine.Connection(base, None, sender_values.append)
        try:
            until(receiver, lambda: receiver.state in {"connected", "disconnected"})
            assert receiver.state == "connected", values
            until(sender, lambda: sender.state in {"connected", "disconnected"})
            assert sender.state == "connected", sender_values
            for number in range(3):
                sender.command(logic.message({**publish, "topic": "burst/item",
                    "request_id": "burst-"+str(number), "payload": "burst"}, "publish", base))
            deadline = time.monotonic() + 5
            while receiver.state != "disconnected" and time.monotonic() < deadline:
                sender.tick(time.monotonic()); receiver.tick(time.monotonic()); time.sleep(0.01)
            assert receiver.state == "disconnected", values
            assert values[-1]["status"]["code"] == "incoming_rate_exceeded"
            assert len([row for row in values if "message" in row]) <= 1
        finally:
            receiver.close()
            sender.close()
    # An accepting TCP socket that never replies to CONNECT must remain immediately stoppable.
    with socket.socket() as stalled:
        stalled.bind(("127.0.0.1", 0)); stalled.listen(2)
        results = []
        connection = engine.Connection(logic.configuration({"broker_host": "127.0.0.1", "broker_port": stalled.getsockname()[1],
            "tls": False, "connect_timeout_sec": 1, "reconnect_attempts": 0}), None, results.append)
        connection.tick(time.monotonic())
        process = connection.child.process
        until(connection, lambda: connection.state == "disconnected", seconds=3)
        assert process.poll() is not None
        assert results[-1]["status"]["code"] == "connection_timeout"
        connection.close()
    # A lost ACK is uncertain, not a successful delivery and not a reconnect replay.
    class Fake:
        def __init__(self, *_): self.items, self.closed = [], False
        def poll(self): return self.items
        def send(self, item): pass
        def close(self): self.closed = True
    rows = []
    connection = engine.Connection(logic.configuration({"broker_host": "test.invalid", "publish_topics": ["allowed/#"],
        "reconnect_attempts": 1, "max_queue": 1}), None, rows.append, factory=Fake)
    connection.tick(1); connection.child.items = [{"type": "ready"}]; connection.tick(2)
    connection.child.items = []
    item = logic.message(publish, "publish", connection.config)
    connection.command(item)
    connection.command({**item, "request_id": "overflow"})
    assert rows[-1]["status"]["code"] == "publish_queue_full"
    connection.fail("connection_lost", 3)
    assert any(row.get("status", {}).get("code") == "publish_uncertain" for row in rows)
    connection.tick(5); connection.child.items = [{"type": "ready"}]; connection.tick(6)
    connection.command(item)
    assert rows[-1]["status"]["code"] == "duplicate_ignored" and not connection.pending
    connection.fail("connection_lost", 7)
    assert connection.state == "disconnected"
    connection.close()
    print("[ok] MQTT validation, real TLS/anonymous exchange, duplicates, bounded shutdown and uncertain delivery")


if __name__ == "__main__":
    main()
