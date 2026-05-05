# lucid-orchestrator — Implementation

> **Package:** `lucid-orchestrator` | **Container:** `lucid-orchestrator` | **Internal port:** 5000

## Overview

The orchestrator is the core backend service of Central Command. It maintains a persistent MQTT connection to the EMQX broker, subscribes to all LUCID agent topics, and exposes a FastAPI REST API and WebSocket endpoint for the dashboard, AI service, and automation service to consume. It is the single point of truth for: command dispatch, experiment execution, topic link management, MQTT user synchronization, and live event broadcasting.

## Key Modules and Responsibilities

| Module | Responsibility |
|--------|----------------|
| `main.py` | FastAPI app creation, lifespan management, dependency wiring, template seeding, interrupted run recovery |
| `mqtt_bridge.py` | Paho MQTT v5 client wrapper; subscribes to 18 wildcard patterns; parses topics; enqueues `MqttEvent` objects; resolves request-response futures |
| `db.py` | Postgres schema initialization (orchestrator-owned tables); CRUD for agents, components, MQTT users, topic links, experiments, sync state |
| `broadcaster.py` | Async task that drains the MQTT event queue via `run_in_executor` and fans out to WebSocket clients. DB persistence is handled by EMQX Rule Engine, not this module. |
| `ws_manager.py` | Thread-safe WebSocket client set with `asyncio.Lock`; parallel broadcast via `asyncio.gather`; dead client cleanup |
| `command_dispatch.py` | Builds MQTT command topics and payloads; publishes via bridge; optionally waits for response via `RequestResponseManager` |
| `command_catalog.py` | Static catalog of agent commands; dynamic catalog for component commands derived from `capabilities` in Postgres; payload templates for known actions |
| `auth_service.py` | HTTP client for the `lucid-auth` service (create/delete agents, CC, observers; fetch MQTT state) |
| `sync.py` | Periodic sync loops: `sync_mqtt_users` snapshots auth state into Postgres shadow tables; `sync_topic_links` reconciles EMQX rules with local DB; `sync_forever` runs both every N seconds |
| `experiments/engine.py` | `ExperimentEngine`: executes template-defined experiment runs with step types: command, delay, parallel, topic_link, approval, wait_for_condition. Supports retries, cancellation, and approval gates. |
| `experiments/request_response.py` | `RequestResponseManager`: maps outgoing `request_id` to `asyncio.Future`; resolves from the paho thread via `call_soon_threadsafe`; fails all pending on MQTT disconnect |
| `experiments/models.py` | Pydantic models: `TemplateDef`, `StepDef`, `ParameterSchema` with per-type validation |
| `experiments/parser.py` | YAML/JSON template loading; `${param}` substitution preserving types; `${steps.<name>.result.<path>}` cross-step references; seed template loading from `templates/` directory |
| `routes/api.py` | REST API: agents CRUD, command dispatch, logs, command history, command catalog, topic tree, MQTT user management, auth log, topic links, schema introspection, WebSocket endpoint |
| `routes/experiments.py` | REST API: experiment template CRUD, run lifecycle (start/cancel/approve), run and step queries |
| `topic_links/manager.py` | `TopicLinkManager`: creates EMQX Rule Engine republish rules via the EMQX REST API; JWT auth with automatic refresh |
| `topic_links/service.py` | Business logic for topic link CRUD; bridges `TopicLinkManager` (EMQX) with Postgres; enforces experiment-run ownership locks |

## Important Implementation Details

### MQTT Bridge Threading Model

The `MqttBridge` runs paho-mqtt's network loop in a daemon thread. When a message arrives:

1. `_on_message` parses the topic via `parse_topic()` into `(agent_id, component_id, topic_type)`.
2. JSON-decodes the payload (falls back to string on decode error).
3. If the topic is `evt/*` and an RRM is wired, resolves the matching future via `loop.call_soon_threadsafe` (the only safe way to touch asyncio from paho's thread).
4. Notifies any registered telemetry watchers (used by `wait_for_condition` experiment steps).
5. Constructs an `MqttEvent` and places it on a bounded queue (10,000 max). If the queue is full, the event is dropped silently (back-pressure).

### Broadcaster Architecture

The `Broadcaster` runs as a single `asyncio.create_task`. It calls `queue.Queue.get` with a 0.1s timeout wrapped in `run_in_executor`, so the asyncio event loop is never blocked. All database persistence is handled by EMQX Rule Engine (`setup_rules.py`) -- the broadcaster exists solely to push live events to browser WebSocket clients.

### Sync Loop

`sync_forever` runs every 5 seconds (configurable via `ORCHESTRATOR_SYNC_INTERVAL_S`):

1. **MQTT user sync** — Fetches the full auth state from `lucid-auth` (`/mqtt-state`), replaces the local `mqtt_users` and `mqtt_acl_rules` shadow tables, and ensures agent entries exist in the `agents` table.
2. **Topic link sync** — Fetches all EMQX rules, parses those that match the topic link format (single republish action with SQL `FROM "topic"`), reconciles with the local `topic_links` table (create/update/delete).

### Experiment Engine

The engine supports six step types:

| Step Type | Behavior |
|-----------|----------|
| `command` | Publishes an MQTT command via `send_command(wait=True)` and awaits the response |
| `delay` | `asyncio.sleep(duration_s)` |
| `parallel` | Runs sub-steps concurrently via `asyncio.gather` |
| `topic_link` | Creates/activates/deactivates/deletes EMQX republish rules |
| `approval` | Pauses execution until a human approves via REST API or the AI agent |
| `wait_for_condition` | Registers a telemetry watcher on the MQTT bridge and waits until a condition (equals/in/not_equals on a dot-path field) is met or timeout |

Each step supports retries with exponential backoff (`2^attempt` seconds). Failed steps can either abort the run or continue based on `on_failure`. All step state (start, complete, fail) is persisted to `experiment_steps` and broadcast via WebSocket.

### Request-Response Correlation

The `RequestResponseManager` enables synchronous command-response patterns over async MQTT:

1. `send_and_wait()` injects a `request_id` UUID into the payload, registers an `asyncio.Future`, publishes via the bridge, and awaits with timeout.
2. When `MqttBridge._on_message` sees an `evt/*` topic with a matching `request_id`, it calls `rrm.resolve_threadsafe()` which uses `loop.call_soon_threadsafe` to safely resolve the future from the paho thread.
3. On MQTT disconnect, `fail_all_pending()` fails all outstanding futures with a `ConnectionError`.

## How It Connects to Other Services

- **EMQX** — Direct MQTT connection for subscribing and publishing; REST API for rule management and auth
- **PostgreSQL** — Direct psycopg2 connections for all data persistence
- **lucid-auth** — HTTP client (`AuthService`) for user/ACL management
- **lucid-ui** — Serves as the upstream API; UI proxies all `/api/*` requests here
- **lucid-ai** — Receives commands via the internal `/api/internal/command` endpoint
- **Edge agents** — Communicates exclusively via MQTT
