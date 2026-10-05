# MQTT

![MQTT](media/thumbnail.webp)

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->

Connect a BloxSmith graph to an explicitly configured **external MQTT 3.1.1 broker**. Receive JSON or UTF-8 text messages on Run and publish only to allowed topics. This block is separate from the framework's internal Topic Broadcast service and from audio transport.

## Setup

Install `requirements.txt` into the Python environment used by BloxSmith. The initial implementation requires Linux/POSIX and Eclipse Paho MQTT 2.1.0. It does not install or host a broker.

1. Configure a broker hostname/IP and port. An empty hostname leaves the block inactive.
2. Keep TLS enabled. System CA certificates are used unless an explicit PEM CA file is supplied; relative CA paths resolve against the application root. Certificate and hostname verification cannot be disabled. Anonymous plaintext is available for trusted local development only.
3. For authentication, configure a wallet reference such as `secret://workspace/mqtt`. Its value must be a JSON object with `username` and `password`. Credentials require TLS and never belong in the graph, command line or logs.
4. Set subscribed topic filters and allowed publish filters, for example `["factory/temperature/+"]`. An empty publish list disables publishing. `+` matches one topic level; a final `#` matches remaining levels. Shared subscriptions are not supported.
5. Choose JSON or text, save settings, then start Active Runtime. One Shot returns a preview status without opening a connection or publishing.

The modal and inspector edit a draft. Apply saves it; Cancel discards it. Connection changes take effect on the next Run, not on a loaded connection. Neither opening the modal nor Apply publishes a command.

## Inputs

| Port | Message | Behavior |
| --- | --- | --- |
| `publish` | `{"request_id":"reading-42","topic":"factory/temperature/a","payload":{"value":21.5},"retain":false}` | Publish one bounded payload using configured QoS. Broker, credentials, subscriptions and allowlists cannot be overridden by inputs. |
| `command` | `{"action":"pause"}` | Disconnect and stop receiving. Outstanding publications become uncertain. |
| `command` | `{"action":"resume"}` | Start a fresh connection from paused/disconnected state; reset the connection retry budget. |
| `command` | `{"action":"status","request_id":"check-1"}` | Emit current connection status without publishing to MQTT. |

Only the newly delivered input event is handled; another port's cached value is not replayed. Ambiguous batches are refused. JSON requires unique keys and finite values; text requires a string. Input messages are data, never executable instructions.

## Outputs and delivery semantics

`message` emits `{topic, payload, qos, retained, duplicate, received_at}`. The timestamp is UTC. Retained reception and retained publication are disabled by default. Enabling them is explicit. Incoming QoS 1 can duplicate messages: consumers requiring business idempotency must use their own stable IDs. This block is not a durable inbox and a broker acknowledgement does not prove downstream graph delivery.

`status` reports connection transitions, rejected commands and publication outcomes. `published` includes `request_id` and `assurance`: `socket_sent` for QoS 0 or `broker_ack` for QoS 1. Neither means the recipient completed a business action. No periodic heartbeat is emitted into the graph.

On connection loss, pause, queue transport failure or a 30-second missing-ACK deadline, outstanding requests produce **`publish_uncertain`**. They may already have reached the broker. They are **not automatically retried**. An identical admitted `request_id` is ignored for the rest of that Run; a different payload under that ID is refused. This memory is capped at 10,000 IDs, is not evicted automatically and does not survive a new Run. Reusing a new ID after an uncertain delivery can duplicate the external effect.

Disconnected/paused publications and full-queue rejections are not buffered. Reconnection creates a fresh client, preserving the Run's seen IDs but not reusing the previous client's outgoing queue. Additional reconnect attempts are bounded across the Run until explicit resume; a successfully re-established connection does not silently reset that budget.

## Boundaries

- MQTT 3.1.1 over TCP/TLS, QoS 0 or 1. No MQTT 5 properties, WebSockets, binary/audio transport, QoS 2, Last Will or client-certificate authentication in this version.
- Clean broker sessions are the default. A persistent session requires an explicit stable, unique client ID; never share it between concurrently running nodes. Broker-side retention is not a durable outgoing queue or a graph receipt store.
- At most 16 subscription and 16 publication filters, 256 UTF-8 bytes per topic, 64 KiB maximum payload, 64 outstanding publications, JSON depth 24. Defaults are smaller. The child-process command buffer and message envelopes are bounded independently.
- Incoming rate overflow or invalid/oversized payload disconnects visibly rather than growing an unbounded queue. Connection retry limits still apply. A slow or saturated framework output path remains subject to the framework's own queue policy.
- Network work runs in a supervised, memory-limited subprocess. DNS/TLS/CONNECT/SUBACK have a whole-attempt deadline. Pause and Stop terminate the child, including a blocked connection. No detached network thread survives the listener.
- Limits mitigate malformed peers; this is not an OS security sandbox or a multi-tenant isolation boundary. Broker ACLs and trusted graph administration remain necessary.

## Tests

Package suites cover validation and event identity, real Paho exchanges with a disposable loopback aMQTT broker (including TLS), uncertain delivery and bounded shutdown, actual Run listeners in bundled/managed/linked origins, One Shot preview and responsive translated UI. No user broker, wallet, application instance or paid provider is contacted. Test-only requirements also include aMQTT, OpenSSL and the private framework/browser harness. Public sources do not redistribute that framework.

Protocol implementation reference: [Eclipse Paho Python client](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html). Broker fixture: [aMQTT](https://amqtt.readthedocs.io/en/latest/references/broker_config/).

## License

Apache-2.0. Dependencies retain their respective licenses; their sources are not vendored here.
