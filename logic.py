"""Bounded MQTT 3.1.1 settings and message contracts; no network or runtime imports."""

import ipaddress
import json
import re
from collections.abc import Mapping

DEFAULTS = {
    "broker_host": "", "broker_port": 8883, "tls": True, "secret_ref": "", "ca_file": "",
    "subscriptions": [], "publish_topics": [], "payload_format": "json", "qos": 1,
    "allow_retained": False, "clean_session": True, "client_id": "", "keepalive_sec": 30,
    "connect_timeout_sec": 10, "reconnect_attempts": 3, "reconnect_delay_sec": 2,
    "max_payload_bytes": 16384, "max_queue": 32, "max_messages_per_sec": 50,
}
SECRET_REF = re.compile(r"secret://(?:workspace/[A-Za-z0-9_.-]{1,80}|project/[A-Za-z0-9_.-]{1,80}/[A-Za-z0-9_.-]{1,80})\Z")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
MAX_LINE = 900000  # Fits under the runtime mailbox's ASCII JSON limit with envelope overhead.


class MqttError(ValueError):
    """A safe configuration/message error with no provider credential details."""


def encoded(value, limit=MAX_LINE):
    """Only finite shallow-enough JSON crosses a process or graph boundary."""
    stack = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if depth > 24:
            raise MqttError("JSON nesting exceeds 24 levels.")
        if isinstance(item, dict):
            if any(type(key) is not str for key in item):
                raise MqttError("JSON keys must be strings.")
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
        elif type(item) not in (str, int, float, bool, type(None)):
            raise MqttError("Payload must be JSON-compatible.")
    try:
        result = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(result) > limit:
            raise MqttError("Serialized message exceeds its byte limit.")
        return result
    except (ValueError, UnicodeError, OverflowError, TypeError) as exc:
        raise MqttError("Invalid or oversized JSON value.") from exc


def loads(value, limit=MAX_LINE):
    """Reject duplicate keys, non-finite values and excessive input before decoding."""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise MqttError("Payload must be valid UTF-8.") from exc
    if isinstance(value, str):
        def unique(pairs):
            result = {}
            for key, item in pairs:
                if key in result:
                    raise MqttError("Duplicate JSON keys are not accepted.")
                result[key] = item
            return result
        try:
            if len(value.encode("utf-8")) > limit:
                raise MqttError("Input exceeds its byte limit.")
            value = json.loads(value, object_pairs_hook=unique,
                parse_constant=lambda _: (_ for _ in ()).throw(MqttError("Non-finite JSON.")))
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise MqttError("Malformed or oversized JSON.") from exc
    encoded(value)
    return value


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise MqttError("request_id requires 1–128 bounded ASCII key characters.")
    return value


def topic(value, *, pattern=False):
    """Support standard +/# level filters, not arbitrary regex or shared subscriptions."""
    if not isinstance(value, str) or not value:
        raise MqttError("A topic requires 1–256 UTF-8 bytes.")
    if any(ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF for char in value) or value.startswith("$share/"):
        raise MqttError("Control characters and shared-subscription syntax are not supported.")
    if len(value.encode("utf-8")) > 256:
        raise MqttError("A topic requires 1–256 UTF-8 bytes.")
    levels = value.split("/")
    for index, level in enumerate(levels):
        if ("+" in level or "#" in level) and (not pattern or level not in {"+", "#"} or (level == "#" and index != len(levels) - 1)):
            raise MqttError("Wildcards must occupy a complete filter level; # must be last.")
    return value


def matches(pattern, value):
    """MQTT level matching, including the special leading-$ namespace rule."""
    topic(pattern, pattern=True)
    topic(value)
    if value.startswith("$") and pattern[0] in "+#":
        return False
    left, right = pattern.split("/"), value.split("/")
    for index, part in enumerate(left):
        if part == "#":
            return True
        if index >= len(right) or (part != "+" and part != right[index]):
            return False
    return len(left) == len(right)


def configuration(raw):
    if not isinstance(raw, Mapping) or set(raw) - set(DEFAULTS) - {"execution", "runtime_path", "runtime_path_label"}:
        raise MqttError("Unknown setting; credentials belong in the wallet.")
    config = {**DEFAULTS, **{key: value for key, value in raw.items() if key in DEFAULTS}}
    for name in ("broker_host", "secret_ref", "ca_file", "client_id"):
        value = config[name]
        if not isinstance(value, str) or len(value) > 1024 or any(ord(char) < 32 for char in value):
            raise MqttError(f"Invalid {name}.")
        config[name] = value.strip()
    host = config["broker_host"]
    if host:
        try:
            config["broker_host"] = str(ipaddress.ip_address(host))
            if "%" in host:
                raise ValueError()
        except ValueError:
            try:
                host = host.encode("idna").decode("ascii")
            except UnicodeError as exc:
                raise MqttError("Invalid broker hostname.") from exc
            if len(host) > 253 or any(not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label) for label in host.split(".")):
                raise MqttError("Broker must be an explicit hostname or IP, not a URL/path.")
            config["broker_host"] = host
    for name in ("tls", "allow_retained", "clean_session"):
        if type(config[name]) is not bool:
            raise MqttError(f"{name} must be a boolean.")
    if config["secret_ref"] and (not SECRET_REF.fullmatch(config["secret_ref"]) or not config["tls"]):
        raise MqttError("Authentication requires TLS and a complete wallet secret reference.")
    if config["ca_file"] and not config["tls"]:
        raise MqttError("A CA file is meaningful only with TLS enabled.")
    if config["client_id"] and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", config["client_id"]):
        raise MqttError("client_id requires 1–64 ASCII key characters.")
    if not config["clean_session"] and not config["client_id"]:
        raise MqttError("A persistent broker session requires an explicit unique client_id.")
    for name, minimum, maximum in (("broker_port", 1, 65535), ("qos", 0, 1), ("keepalive_sec", 10, 120),
        ("connect_timeout_sec", 1, 30), ("reconnect_attempts", 0, 10), ("reconnect_delay_sec", 1, 60),
        ("max_payload_bytes", 1, 65536), ("max_queue", 1, 64), ("max_messages_per_sec", 1, 200)):
        if type(config[name]) is not int or not minimum <= config[name] <= maximum:
            raise MqttError(f"{name} must be an integer between {minimum} and {maximum}.")
    for name in ("subscriptions", "publish_topics"):
        values = config[name]
        if not isinstance(values, (list, tuple)) or len(values) > 16:
            raise MqttError(f"{name} requires an array of at most 16 filters.")
        for value in values:
            topic(value, pattern=True)
        if len(set(values)) != len(values):
            raise MqttError("Topic filters must be distinct.")
        config[name] = list(values)
    if config["payload_format"] not in ("json", "text"):
        raise MqttError("Select the explicit json or text payload format.")
    return config


def payload_bytes(value, config):
    """Payload wire size is UTF-8, independently of the ASCII process-envelope budget."""
    try:
        if config["payload_format"] == "text":
            if not isinstance(value, str):
                raise MqttError("Text mode requires a string payload.")
            raw = value.encode("utf-8", errors="strict")
        else:
            encoded(value)
            raw = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except UnicodeError:
        raise MqttError("Payload must be valid UTF-8.") from None
    if len(raw) > config["max_payload_bytes"]:
        raise MqttError("Payload exceeds max_payload_bytes.")
    return raw


def message(value, port, config):
    """Canonical per-event command; source data cannot override broker, secrets or policy."""
    value = loads(value)
    if not isinstance(value, dict):
        raise MqttError("A single JSON object is required.")
    if port == "command":
        if set(value) - {"action", "request_id"} or value.get("action") not in ("pause", "resume", "status"):
            raise MqttError("Command action must be pause, resume or status.")
        if "request_id" in value:
            identifier(value["request_id"])
        return {"type": "command", **value}
    if port != "publish" or set(value) - {"request_id", "topic", "payload", "retain"} or "payload" not in value:
        raise MqttError("Publish requires request_id, topic, payload and optional retain only.")
    identifier(value.get("request_id"))
    selected = topic(value.get("topic"))
    if not any(matches(pattern, selected) for pattern in config["publish_topics"]):
        raise MqttError("Publishing to this topic is not permitted.")
    retain = value.get("retain", False)
    if type(retain) is not bool or (retain and not config["allow_retained"]):
        raise MqttError("Retained publishing is disabled or retain is not a boolean.")
    payload_bytes(value["payload"], config)
    result = {"type": "publish", **value, "retain": retain}
    encoded(result)
    return result


def credentials(context, config):
    """Resolve only the configured secret; errors never expose wallet/provider contents."""
    if not config["secret_ref"]:
        return None
    resolver = context.services.get("resolve_secret")
    if not callable(resolver):
        raise MqttError("The wallet resolver is unavailable.")
    try:
        value = resolver(config["secret_ref"])
        result = loads(value, 8192)
        if not isinstance(result, dict) or set(result) != {"username", "password"}:
            raise ValueError()
        for key in ("username", "password"):
            if not isinstance(result[key], str) or not 1 <= len(result[key]) <= 2048 or any(ord(char) < 32 for char in result[key]):
                raise ValueError()
        return result
    except Exception:
        raise MqttError("Unlock the wallet and supply a JSON secret with nonempty username and password.") from None
