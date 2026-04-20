import logging
import asyncio
import os
import threading
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import settings
from app.schemas.ready import HealthResponse, WSMessage
from app.websocket.manager import websocket_manager
from app.utils.pi_bridge import is_ready_input_enabled, set_ready_input_enabled

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────── #

def _terminal_ready_loop(loop: asyncio.AbstractEventLoop) -> None:
    room_id = os.getenv("ROOM_ID", "arena")

    print("\n=== READY INPUT (backend terminal) ===")
    print("Type y/n for each team. Example: y n")
    print("Press Ctrl+C to stop.\n")

    while True:
        if not is_ready_input_enabled():
            # Wait until the round ends before asking again
            try:
                asyncio.run_coroutine_threadsafe(asyncio.sleep(0.5), loop).result()
            except Exception:
                # If loop is not ready yet, just sleep locally
                import time
                time.sleep(0.5)
            continue

        try:
            raw = input("Teams ready? (attackers defenders): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n[READY] Input stopped.")
            break

        parts = raw.split()
        if len(parts) != 2 or parts[0] not in ("y", "n") or parts[1] not in ("y", "n"):
            print("[READY] Please enter: y n (e.g. y n, n y, y y, n n)")
            continue

        attackers_ready = parts[0] == "y"
        defenders_ready = parts[1] == "y"

        def _send(event: str, payload: dict | None = None) -> None:
            msg = {
                "event": event,
                "payload": payload or {},
                "room_id": room_id,
            }
            asyncio.run_coroutine_threadsafe(
                websocket_manager.broadcast_all(msg, room_id), loop
            )
            logger.info("[READY] sent event=%s room=%s payload=%s", event, room_id, msg["payload"])

        _send("attackers_ready" if attackers_ready else "attackers_not_ready")
        _send("defenders_ready" if defenders_ready else "defenders_not_ready")

        if attackers_ready and defenders_ready:
            _send("both_teams_ready")
            _send(
                "teams_ready",
                {"attackersReady": True, "defendersReady": True},
            )
            set_ready_input_enabled(False)
        else:
            if (not attackers_ready) and (not defenders_ready):
                _send("no_team_ready")
            _send(
                "teams_ready",
                {"attackersReady": attackers_ready, "defendersReady": defenders_ready},
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 %s starting up | env=%s", settings.APP_NAME, settings.ENVIRONMENT)

    loop = asyncio.get_running_loop()
    t = threading.Thread(target=_terminal_ready_loop, args=(loop,), daemon=True)
    t.start()

    yield
    logger.info("🛑 %s shutting down", settings.APP_NAME)


# ── App ───────────────────────────────────────────────────────────────────── #

app = FastAPI(
    title=settings.APP_NAME,
    version="2.0.0",
    description="Real-time multiplayer backend — WebSocket sync across multiple PCs",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────── #
# Allow all origins so ngrok tunnels and different PCs can connect freely.
# Tighten this in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── REST router ───────────────────────────────────────────────────────────── #
app.include_router(api_router, prefix="/api/v1")


# ── REST endpoints ────────────────────────────────────────────────────────── #

@app.get("/")
async def root():
    return {
        "message": f"{settings.APP_NAME} Backend is running",
        "environment": settings.ENVIRONMENT,
        "websocket_endpoint": "/ws/{room_id}",
        "docs": "/docs",
    }


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="healthy",
        app_name=settings.APP_NAME,
        environment=settings.ENVIRONMENT,
        websocket_connections=websocket_manager.get_connection_count(),
        rooms=websocket_manager.get_rooms(),
    )


# ── 🔥 CORE WEBSOCKET ENDPOINT ────────────────────────────────────────────── #

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    """
    Real-time sync endpoint.

    Connect from any machine:
        ws://localhost:8000/ws/arena          (local)
        wss://<ngrok-url>/ws/arena            (remote)

    Flow:
        1. Client connects      → registered in room
        2. Client sends JSON    → broadcast to ALL clients in same room
        3. Client disconnects   → cleanly removed, others unaffected

    Message format (send this from any client):
        {
            "event": "arena.update",
            "payload": { "score": 42 },
            "room_id": "arena"
        }
    """
    await websocket_manager.connect(websocket, room_id)

    # Notify everyone in the room that a new client joined
    await websocket_manager.broadcast_all(
        {
            "event": "system.client_joined",
            "payload": {
                "room_id": room_id,
                "total_connections": websocket_manager.get_connection_count(room_id),
            },
            "room_id": room_id,
        },
        room_id,
    )

    try:
        while True:
            # ----------------------------------------------------------------
            # Receive a message from THIS client
            # ----------------------------------------------------------------
            try:
                data = await websocket.receive_json()
            except ValueError:
                # Client sent invalid JSON — send error back, keep connection
                await websocket_manager.send_personal(
                    {
                        "event": "system.error",
                        "payload": {"detail": "Invalid JSON — message ignored"},
                        "room_id": room_id,
                    },
                    websocket,
                )
                continue

            # ----------------------------------------------------------------
            # Validate and normalise message
            # ----------------------------------------------------------------
            try:
                msg = WSMessage(**data)
            except Exception:
                # Accept the raw dict even if it doesn't match the full schema
                msg = WSMessage(
                    event=data.get("event", "unknown"),
                    payload=data.get("payload"),
                    room_id=room_id,
                )

            # Force room_id to match the URL parameter — clients cannot
            # accidentally broadcast into a room they didn't connect to
            msg.room_id = room_id

            logger.info(
                "MSG received | room=%s | event=%s", room_id, msg.event
            )

            # ----------------------------------------------------------------
            # 🔥 Broadcast to ALL clients in this room
            # ----------------------------------------------------------------
            await websocket_manager.broadcast_all(msg.model_dump(mode="json"), room_id)

    except WebSocketDisconnect:
        await websocket_manager.disconnect(websocket, room_id)
        # Notify remaining clients
        await websocket_manager.broadcast_all(
            {
                "event": "system.client_left",
                "payload": {
                    "room_id": room_id,
                    "total_connections": websocket_manager.get_connection_count(room_id),
                },
                "room_id": room_id,
            },
            room_id,
        )

    except Exception as exc:
        logger.error("Unexpected WS error in room=%s: %s", room_id, exc)
        await websocket_manager.disconnect(websocket, room_id)
