"""Supervised, single-connection Paho adapter; stdin/stdout are private bounded JSON pipes.

No framework imports, graph publishing, reconnect thread or credentials in arguments.
The owning listener terminates this process on timeout, pause and Run Stop.
"""

import collections
from datetime import datetime, timezone
import os
import resource
import select
import ssl
import sys
import time

from logic import MAX_LINE, MqttError, configuration, encoded, loads, matches, message, payload_bytes, topic


def emit(value):
    """A blocked pipe remains killable by the parent; never log provider exceptions."""
    raw = encoded(value) + b"\n"
    while raw:
        count = os.write(1, raw)
        raw = raw[count:]


def run():
    # Bound packet allocation before Paho sees an untrusted broker's payload length.
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    import paho.mqtt.client as mqtt

    first = sys.stdin.buffer.readline(MAX_LINE + 2)
    if not first.endswith(b"\n") or len(first) > MAX_LINE + 1:
        return 2
    settings = loads(first)
    config = configuration(settings["config"])
    auth = settings.get("auth")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=config["client_id"],
                         protocol=mqtt.MQTTv311, clean_session=config["clean_session"], reconnect_on_failure=False)
    client.connect_timeout = config["connect_timeout_sec"]
    client.max_inflight_messages_set(config["max_queue"])
    client.max_queued_messages_set(config["max_queue"])
    if config["tls"]:
        client.tls_set_context(ssl.create_default_context(cafile=config["ca_file"] or None))
    if auth:
        client.username_pw_set(auth["username"], auth["password"])
    ready, failure, pending, rates = False, None, {}, collections.deque()
    subscription_mid = None

    def connected(_client, _userdata, _flags, reason, _properties):
        nonlocal ready, failure, subscription_mid
        if reason.is_failure:
            failure = "connection_refused"
        elif config["subscriptions"]:
            code, subscription_mid = client.subscribe([(value, config["qos"]) for value in config["subscriptions"]])
            if code != mqtt.MQTT_ERR_SUCCESS:
                failure = "subscription_refused"
        else:
            ready = True
            emit({"type": "ready"})

    def subscribed(_client, _userdata, mid, codes, _properties):
        nonlocal ready, failure
        if mid != subscription_mid or not codes or any(code.is_failure for code in codes):
            failure = "subscription_refused"
        else:
            ready = True
            emit({"type": "ready"})

    def disconnected(*_args):
        nonlocal failure
        failure = "connection_lost"

    def received(_client, _userdata, packet):
        nonlocal failure
        if failure:
            return
        now = time.monotonic()
        while rates and now - rates[0] >= 1:
            rates.popleft()
        rates.append(now)
        if len(rates) > config["max_messages_per_sec"]:
            failure = "incoming_rate_exceeded"
            return
        if packet.retain and not config["allow_retained"]:
            return
        try:
            name = topic(packet.topic)
            if not any(matches(pattern, name) for pattern in config["subscriptions"]):
                raise MqttError("unsolicited topic")
            if len(packet.payload) > config["max_payload_bytes"]:
                raise MqttError("oversized")
            value = loads(packet.payload, config["max_payload_bytes"]) if config["payload_format"] == "json" else packet.payload.decode("utf-8", errors="strict")
            emit({"type": "message", "message": {"topic": name, "payload": value, "qos": packet.qos,
                "retained": bool(packet.retain), "duplicate": bool(packet.dup),
                "received_at": datetime.now(timezone.utc).isoformat()}})
        except (MqttError, UnicodeError):
            # A hostile or misconfigured source cannot flood the status output either.
            failure = "invalid_incoming_payload"

    def published(_client, _userdata, mid, reason, _properties):
        item = pending.pop(mid, None)
        if item:
            emit({"type": "ack", "request_id": item,
                  "assurance": "socket_sent" if config["qos"] == 0 else "broker_ack"})

    client.on_connect, client.on_subscribe, client.on_disconnect = connected, subscribed, disconnected
    client.on_message, client.on_publish = received, published
    client.connect(config["broker_host"], config["broker_port"], config["keepalive_sec"])
    os.set_blocking(0, False)
    buffer, heartbeat = b"", time.monotonic()
    while not failure:
        # This is the sole network loop; no SDK background thread or reconnect replay.
        result = client.loop(timeout=0.04)
        if result != mqtt.MQTT_ERR_SUCCESS:
            failure = "connection_lost"
            break
        if select.select([0], [], [], 0)[0]:
            chunk = os.read(0, 65536)
            if not chunk:
                return 0
            buffer += chunk
            if len(buffer) > MAX_LINE:
                raise MqttError("Oversized helper command.")
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                item = loads(line)
                item.pop("type", None)
                item = message(item, "publish", config)
                if not ready or len(pending) >= config["max_queue"]:
                    raise MqttError("Unexpected helper publication.")
                # Paho may call on_publish synchronously for QoS 0. Track that case explicitly.
                info = client.publish(item["topic"], payload_bytes(item["payload"], config),
                                      qos=config["qos"], retain=item["retain"])
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    failure = "publish_failed"
                    break
                if info.is_published():
                    emit({"type": "ack", "request_id": item["request_id"], "assurance": "socket_sent"})
                else:
                    pending[info.mid] = item["request_id"]
        if time.monotonic() - heartbeat >= 1:
            emit({"type": "heartbeat"})
            heartbeat = time.monotonic()
    emit({"type": "error", "code": failure})
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except (Exception, MemoryError):
        try:
            emit({"type": "error", "code": "connection_adapter_failed"})
        except Exception:
            pass
        raise SystemExit(2) from None
