"""Disposable loopback aMQTT broker, used only by package tests."""

import asyncio
from contextlib import contextmanager
from pathlib import Path
import socket
import subprocess
import threading
from tempfile import TemporaryDirectory
import time


@contextmanager
def broker(*, tls=False, authenticated=False):
    from amqtt.broker import Broker
    with TemporaryDirectory(prefix="mqtt-broker-") as temporary:
        root = Path(temporary)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        listener = {"type": "tcp", "bind": f"127.0.0.1:{port}", "max_connections": 16}
        plugins = {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}}
        if authenticated:
            from passlib.hash import sha512_crypt
            (root / "passwords").write_text("fixture:" + sha512_crypt.hash("local-test-password") + "\n", encoding="utf-8")
            plugins = {"amqtt.plugins.authentication.FileAuthPlugin": {"password_file": str(root / "passwords")}}
        if tls:
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-keyout", str(root / "key.pem"), "-out", str(root / "cert.pem"), "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            listener.update(ssl=True, certfile=str(root / "cert.pem"), keyfile=str(root / "key.pem"))
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        async def start():
            instance = Broker({"listeners": {"default": listener}, "plugins": plugins})
            await instance.start()
            return instance
        instance = asyncio.run_coroutine_threadsafe(start(), loop).result(timeout=10)
        try:
            yield {"broker_host": "127.0.0.1", "broker_port": port, "tls": tls,
                   "ca_file": str(root / "cert.pem") if tls else ""}
        finally:
            asyncio.run_coroutine_threadsafe(instance.shutdown(), loop).result(timeout=10)
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()


def until(connection, predicate, *, seconds=8):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        connection.tick(time.monotonic())
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("MQTT fixture condition not reached; state=" + connection.state)
