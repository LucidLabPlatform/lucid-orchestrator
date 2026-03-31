import asyncio
import logging
import os
import queue
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.auth_service import AuthService
from app import db as DB
from app.broadcaster import Broadcaster
from app.experiments.request_response import RequestResponseManager
from app.mqtt_bridge import MqttBridge
from app.routes.api import router as api_router
from app.sync import sync_forever, sync_mqtt_users, sync_topic_links
from app.topic_links.manager import TopicLinkManager

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    DB.init_schema()

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


@app.get("/health")
def health():
    return {"status": "ok"}
