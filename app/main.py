import asyncio
import logging
import os
import queue
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg2.extras
from fastapi import FastAPI

from app.auth_service import AuthService
from app import db as DB
from app.broadcaster import Broadcaster
from app.experiments.engine import ExperimentEngine
from app.experiments.parser import load_seed_templates
from app.experiments.request_response import RequestResponseManager
from app.mqtt_bridge import MqttBridge
from app.routes.api import router as api_router
from app.routes.experiments import router as experiments_router
from app.sync import sync_forever, sync_mqtt_users, sync_topic_links
from app.topic_links.manager import TopicLinkManager
from app.topic_links import service as topic_link_service

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def _seed_templates() -> None:
    try:
        templates = load_seed_templates()
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not load seed templates: %s", exc)
        return

    if not templates:
        return

    with DB.connect() as conn:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            for tpl in templates:
                cur.execute(
                    """
                    INSERT INTO experiment_templates (
                        id, name, version, description, parameters_schema,
                        definition, tags, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        version = EXCLUDED.version,
                        description = EXCLUDED.description,
                        parameters_schema = EXCLUDED.parameters_schema,
                        definition = EXCLUDED.definition,
                        tags = EXCLUDED.tags
                    """,
                    (
                        tpl.id,
                        tpl.name,
                        tpl.version,
                        tpl.description,
                        psycopg2.extras.Json(tpl.parameters_schema_dict()),
                        psycopg2.extras.Json(tpl.to_definition_dict()),
                        tpl.tags,
                        now,
                    ),
                )
        conn.commit()

    log.info("Seeded %d experiment template(s)", len(templates))


def _recover_interrupted_runs(app) -> None:
    ended_at = datetime.now(timezone.utc)
    with DB.connect() as conn:
        run_ids = DB.mark_active_experiment_runs_failed(
            conn,
            ended_at=ended_at,
            error="orchestrator restarted during run",
        )
        conn.commit()
    for run_id in run_ids:
        try:
            topic_link_service.delete_owned_topic_links(app, "experiment-run", run_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not clean up topic links for recovered run %s: %s", run_id, exc)
    if run_ids:
        log.warning("Marked %d interrupted experiment run(s) as failed", len(run_ids))


@asynccontextmanager
async def lifespan(app: FastAPI):
    DB.init_schema()
    _seed_templates()

    emqx_api_url = os.environ["EMQX_API_URL"]
    emqx_api_user = os.environ["EMQX_API_USERNAME"]
    emqx_api_pass = os.environ["EMQX_API_PASSWORD"]
    cc_username = os.environ["LUCID_MQTT_USERNAME"]
    tlm = TopicLinkManager(emqx_api_url, emqx_api_user, emqx_api_pass)
    auth = AuthService()
    sync_interval_s = float(os.environ.get("ORCHESTRATOR_SYNC_INTERVAL_S", "5"))

    event_queue: queue.Queue = queue.Queue(maxsize=10_000)
    ws_clients: set = set()
    rrm = RequestResponseManager()

    bridge = MqttBridge(event_queue, rrm)
    bridge.start()

    broadcaster = Broadcaster(event_queue, ws_clients)
    bc_task = asyncio.create_task(broadcaster.run())

    app.state.bridge = bridge
    app.state.ws_clients = ws_clients
    app.state.rrm = rrm
    app.state.tlm = tlm
    app.state.auth = auth
    app.state.cc_username = cc_username
    app.state.experiment_engine = ExperimentEngine(app)

    _recover_interrupted_runs(app)

    sync_mqtt_users(app, strict=False)
    sync_topic_links(app, strict=False)
    sync_task = asyncio.create_task(sync_forever(app, interval_s=sync_interval_s))

    log.info("lucid-orchestrator started")
    yield

    sync_task.cancel()
    broadcaster.stop()
    bc_task.cancel()
    bridge.stop()
    log.info("lucid-orchestrator stopped")


app = FastAPI(title="LUCID Orchestrator", lifespan=lifespan)
app.include_router(api_router, prefix="/api")
app.include_router(experiments_router, prefix="/api/experiments")


@app.get("/health")
def health():
    return {"status": "ok"}
