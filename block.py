"""External MQTT messages through the public Run listener and explicit data ports."""

import json
from pathlib import Path
import time
from bloxsmith_app.block_api import APPLICATION_JSON, BlockDefinition, BlockRuntimeOutput, BlockRuntimePreparation, BlockRuntimeResult
from . import engine, logic, ui


# FB1 - Validate explicit broker/TLS/wallet settings and allowlisted JSON/text publications.
# FB2 - Subscribe on Run, preserve topic/retained/QoS metadata and bound ingress/queues.
# FB3 - Pause/resume, report uncertain delivery, never automatically replay publications.
# FB4 - Bound network-process lifetime and support every package origin without host patches.
# FB5 - Own translated responsive modal/inspector, safe drafts and release-scoped assets.
class MqttBlock(BlockDefinition):
    kind = "mqtt"

    def ui_assets(self, surface="modal"):
        return list(self.model.get("ui_assets", {}).get(surface, []))

    def prepare_runtime(self, context):
        logic.configuration(context.config)
        return BlockRuntimePreparation(listen_on_run=context.runtime_mode == "zeromq_active")

    def result(self, context, values):
        outputs = [BlockRuntimeOutput(port_id=int(port.id), port_name=port.name,
                   value=logic.encoded(values[port.name]).decode(), content_type=APPLICATION_JSON)
                   for port in context.output_ports if port.name in values]
        return BlockRuntimeResult(status="success", outputs=outputs)

    def execute_runtime(self, context):
        try:
            config = logic.configuration(context.config)
            if context.runtime_mode != "zeromq_active":
                return self.result(context, {"status": {"code": "preview", "live": False,
                    "detail": "Use Active Runtime. One Shot does not connect or publish."}})
            names = {int(port.id): port.name for port in context.input_ports}
            if context.input_events:
                items = [(names.get(event.input_port_id), event.value) for event in context.input_events]
            else:
                items = [(name, context.input_value(name, str(ident))) for ident, name in names.items()
                         if context.has_input_value(name, str(ident))]
            if not items:
                return BlockRuntimeResult(status="skipped", outputs=[])
            if len(items) != 1:
                raise logic.MqttError("Send one publish or command per activation, not a batch.")
            name, value = items[0]
            item = logic.message(value, name, config)
            client = context.services.get("runtime_listener")
            if client is None:
                raise logic.MqttError("The listener command bridge is unavailable.")
            client.send(item)
            return BlockRuntimeResult(status="success", outputs=[])
        except (ValueError, TypeError, RuntimeError, OSError) as exc:
            detail = str(exc) if isinstance(exc, logic.MqttError) else "The listener command could not be admitted."
            return self.result(context, {"status": {"code": "command_rejected", "detail": detail}})

    def listen_runtime(self, context):
        config = logic.configuration(context.config)
        def emit(values):
            if not context.stop_requested():
                context.emit_result(self.result(context, values))
        connection = None
        try:
            if config["ca_file"]:
                path = Path(config["ca_file"]).expanduser()
                config["ca_file"] = str(path if path.is_absolute() else Path(context.root_dir) / path)
            auth = logic.credentials(context, config) if config["broker_host"] else None
            connection = engine.Connection(config, auth, emit)
            while not context.stop_requested():
                received = context.receive_command(timeout_sec=0.02)
                if received is not None:
                    try:
                        item = dict(received.payload)
                        kind = item.pop("type", None)
                        item = logic.message(item, "command" if kind == "command" else "publish", config)
                        connection.command(item)
                    except (ValueError, TypeError):
                        emit({"status": {"code": "command_rejected", "detail": "Invalid listener message."}})
                connection.tick(time.monotonic())
        except logic.MqttError as exc:
            emit({"status": {"code": "configuration_error", "detail": str(exc)}})
            # A business configuration failure remains recoverable after settings + next Run.
            while not context.stop_requested():
                context.receive_command(timeout_sec=0.1)
        finally:
            if connection:
                connection.close()

    def handle_ui_action(self, *, node, action, values, payload=None):
        if action in {"save_settings", "modal_update_fields", "inspector_update_fields"}:
            patch = (values or {}).get("node_patch") or {}
            if "config" in patch:
                try:
                    logic.configuration({**self.default_config(), **(node.get("config") or {}), **patch["config"]})
                except (ValueError, TypeError) as exc:
                    return {"error": self.translate("block.mqtt.error", {"detail": str(exc)}, fallback="MQTT operation failed: {detail}")}
        return ui.replace_config(super().handle_ui_action(node=node,
            action="modal_update_fields" if action == "save_settings" else action, values=values, payload=payload), node)

    def render_modal(self, *, node, payload=None):
        return ui.modal(self, node, payload)

    def render_inspector_panel(self, *, node, payload=None):
        return ui.inspector(self, node, payload)

    def render_node_card(self, *, node, payload=None):
        config = {**self.default_config(), **(node.get("config") or {})}
        preview = config["broker_host"] or self.translate("block.mqtt.inactive", fallback="Configure a broker")
        return ui.card(self, node, preview)
