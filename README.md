# lucid-orchestrator

MQTT-facing control service for LUCID.

Owns:
- MQTT subscriptions and command publishing
- agent/component state APIs
- user provisioning APIs
- auth log and schema APIs
- the shared WebSocket event stream

Internal APIs:
- `POST /api/internal/command` publishes a command and can wait for its MQTT result
- `POST /api/internal/broadcast` fans out non-MQTT events to UI WebSocket clients
