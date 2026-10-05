"""FB4/FB5: actual supervised Run listeners across every package origin and mode."""

import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests"), str(Path(__file__).parent)]
from blocs.mqtt.block import MqttBlock
from broker_fixture import broker
from block_test_packages import install_test_package, surface_payload, prepare_release_run
from ui_smoke_common import (create_project_api, create_run_api, data_edge, graph_payload,
    http_json, isolated_server, stop_run_api, text_node, wait_for_run_predicate, wait_for_run_terminal)


def main():
    block = MqttBlock()
    # The delivered event is authoritative even if the other port has a stale cached value.
    admitted = []
    context = SimpleNamespace(config={"publish_topics": ["test/#"]}, runtime_mode="zeromq_active",
        input_ports=[SimpleNamespace(id=1, name="publish"), SimpleNamespace(id=2, name="command")],
        output_ports=[SimpleNamespace(id=1, name="message"), SimpleNamespace(id=2, name="status")],
        input_events=[SimpleNamespace(input_port_id=2, value='{"action":"pause"}')],
        services={"runtime_listener": SimpleNamespace(send=admitted.append)},
        input_value=lambda *_: (_ for _ in ()).throw(AssertionError("Do not replay cached publications")))
    assert block.execute_runtime(context).status == "success"
    assert admitted == [{"type": "command", "action": "pause"}]
    context.input_events *= 2
    assert json.loads(block.execute_runtime(context).outputs[0].value)["code"] == "command_rejected"
    assert len(admitted) == 1

    with broker() as endpoint:
        for mode in ("centralized", "zeromq_active"):
            for origin in (None, "managed", "linked"):
                with isolated_server() as server:
                    node = block.build_node_payload(node_id="mqtt", position={"x": 390, "y": 180},
                        config_overrides={**endpoint, "subscriptions": ["test/#"], "publish_topics": ["test/#"], "reconnect_attempts": 0})
                    if origin:
                        model = install_test_package(server, "mqtt", origin=origin)
                        node["block_version"] = model["version"]
                        for surface in ("modal", "inspector_panel", "node_card"):
                            html = surface_payload(server, model, node, surface=surface)["html"]
                            assert "{{" not in html
                            if surface != "node_card":
                                assert 'data-block-config-field="broker_host"' in html
                    graph = graph_payload("MQTT isolated loopback", [
                        text_node("publisher", "Publish request", '{"request_id":"manual","topic":"test/manual","payload":1}', 40, 100),
                        text_node("control", "Command", '{"action":"status"}', 40, 320), node], [
                        data_edge("pub", "publisher", 1, "mqtt", 1), data_edge("cmd", "control", 1, "mqtt", 2)])
                    project = create_project_api(server, document=graph)["project"]
                    # Active listeners connect on preparation; do not auto-Play the
                    # upstream fixtures while injecting explicit events below.
                    created = (prepare_release_run(server, project["graph_id"], graph)
                        if mode == "zeromq_active" else
                        create_run_api(server, graph, project_id=project["graph_id"], runtime_mode=mode))
                    rid = created["run_id"]
                    if mode == "centralized":
                        result = wait_for_run_terminal(server, rid, timeout_sec=30)
                        assert result["status"] == "success", result.get("logs")
                        assert json.loads(result["output_values"]["mqtt:2"]["value"])["code"] == "preview"
                        assert not result["output_values"].get("mqtt:1")
                        continue
                    def output(data, port=2):
                        raw = data.get("output_values", {}).get("mqtt:"+str(port), {}).get("value")
                        return json.loads(raw) if raw else {}
                    def wait(predicate):
                        result = wait_for_run_predicate(server, rid, lambda data: data.get("status") == "failed" or predicate(data),
                            "MQTT runtime condition missing", timeout_sec=20)
                        assert result.get("status") != "failed", result.get("logs")
                        return result
                    def send(node_id, value):
                        http_json(server.base_url, f"/api/runs/{rid}/active/control", method="POST", payload={
                            "action": "publish_output", "node_id": node_id, "port_id": 1,
                            "value": json.dumps(value), "content_type": "application/json"})
                    try:
                        prepared = wait(lambda data: output(data).get("code") == "connected")
                        assert not prepared["output_values"].get("mqtt:1"), "Preparation cannot run the upstream Text Source"
                        publish = {"request_id": "live-1", "topic": "test/runtime", "payload": {"source": origin or "bundled"}}
                        send("publisher", publish)
                        result = wait(lambda data: output(data).get("code") == "published" and output(data, 1).get("payload") == publish["payload"])
                        assert output(result)["assurance"] == "broker_ack"
                        send("publisher", publish)
                        wait(lambda data: output(data).get("code") == "duplicate_ignored")
                        send("control", {"action": "pause"})
                        wait(lambda data: output(data).get("code") == "paused")
                        send("publisher", {**publish, "request_id": "paused"})
                        wait(lambda data: output(data).get("code") == "publish_not_connected")
                        send("control", {"action": "resume"})
                        wait(lambda data: output(data).get("code") == "connected")
                        send("publisher", {**publish, "request_id": "live-2", "payload": "resumed"})
                        wait(lambda data: output(data).get("request_id") == "live-2" and output(data, 1).get("payload") == "resumed")
                    finally:
                        stop_run_api(server, rid)
                        terminal = wait_for_run_terminal(server, rid, timeout_sec=10)
                        assert "runtime_listeners.shutdown_timeout" not in str(terminal.get("logs"))
                print("[ok] MQTT framework " + mode + " " + str(origin), flush=True)


if __name__ == "__main__":
    main()
