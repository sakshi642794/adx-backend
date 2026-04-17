import logging
import asyncio
import os
import threading
import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import settings
from app.schemas.ready import HealthResponse, WSMessage
from app.websocket.manager import websocket_manager
from app.utils.pi_bridge import is_ready_input_enabled, set_ready_input_enabled
from app.utils.timer_speed import timer_speed_manager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _build_timer_speed_announcement(snapshot: dict, reason: str) -> str:
    mode = snapshot.get("effectiveMode", "normal")
    fast_count = snapshot.get("fastCount", 0)
    slow_count = snapshot.get("slowCount", 0)

    if reason == "activated":
        activated = snapshot.get("activatedMode", "fast").upper()
        if mode == "fast":
            return f"TIME SHIFT: {activated} MODE ACTIVE - ROUND AND SPIKE TIMERS NOW 2X"
        if mode == "slow":
            return f"TIME SHIFT: {activated} MODE ACTIVE - ROUND AND SPIKE TIMERS NOW 0.5X"
        return f"TIME SHIFT: {activated} MODE COUNTERED - ROUND AND SPIKE TIMERS BACK TO NORMAL"

    if reason == "expired":
        if mode == "fast":
            return f"TIME SHIFT UPDATED - FAST MODE STILL ACTIVE ({fast_count} FAST / {slow_count} SLOW)"
        if mode == "slow":
            return f"TIME SHIFT UPDATED - SLOW MODE STILL ACTIVE ({fast_count} FAST / {slow_count} SLOW)"
        return "TIME SHIFT EXPIRED - ROUND AND SPIKE TIMERS BACK TO NORMAL"

    return "TIME SHIFT RESET - ROUND AND SPIKE TIMERS BACK TO NORMAL"


async def _broadcast_timer_speed_update(room_id: str, snapshot: dict, reason: str) -> None:
    payload = {
        **snapshot,
        "announcement": _build_timer_speed_announcement(snapshot, reason),
        "reason": reason,
    }
    await websocket_manager.broadcast_all(
        {
            "event": "timer_speed_update",
            "payload": payload,
            "room_id": room_id,
        },
        room_id,
    )


async def _schedule_timer_speed_expiry(room_id: str, effect_id: str, expires_at_ms: int) -> None:
    delay_seconds = max(0, (expires_at_ms - int(time.time() * 1000)) / 1000)
    await asyncio.sleep(delay_seconds)

    snapshot = await timer_speed_manager.expire_effect(room_id, effect_id)
    if snapshot is not None:
        await _broadcast_timer_speed_update(room_id, snapshot, "expired")


# ── Lifespan ──────────────────────────────────────────────────────────────── #

def _terminal_ready_loop(loop: asyncio.AbstractEventLoop) -> None:
    room_id = os.getenv("ROOM_ID", "arena")

    print("\n=== BACKEND TERMINAL CONTROLS ===")
    print("Ready input (only when enabled):")
    print("  - y n            (attackers defenders)")
    print("  - ready y n")
    print("CO commands (anytime):")
    print("  - fast           (activate 2x time for round/spike for 60s)")
    print("  - slow           (activate 0.5x time for round/spike for 60s)")
    print("  - kill A1        (broadcast A1-killed)")
    print("  - revive A1      (broadcast revive-A1)")
    print("  - help\n")

    async def _activate_timer_mode(mode: str) -> None:
        snapshot = await timer_speed_manager.activate(room_id, mode)  # type: ignore[arg-type]
        await _broadcast_timer_speed_update(room_id, snapshot, "activated")
        asyncio.create_task(
            _schedule_timer_speed_expiry(
                room_id,
                snapshot["activatedEffectId"],
                snapshot["activatedUntil"],
            )
        )

    async def _send_event(event: str, payload: dict | None = None) -> None:
        await websocket_manager.broadcast_all(
            {"event": event, "payload": payload or {}, "room_id": room_id},
            room_id,
        )

    while True:
        try:
            raw = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[READY] Input stopped.")
            break

        if not raw:
            continue

        s = raw.strip()
        lower = s.lower()

        if lower in ("help", "?"):
            print("Commands:")
            print("  y n / ready y n")
            print("  fast | slow")
            print("  kill A1..A5 | kill D1..D5")
            print("  revive A1..A5 | revive D1..D5")
            continue

        if lower == "fast":
            asyncio.run_coroutine_threadsafe(_activate_timer_mode("fast"), loop)
            continue

        if lower == "slow":
            asyncio.run_coroutine_threadsafe(_activate_timer_mode("slow"), loop)
            continue

        if lower.startswith("kill "):
            pid = s.split(maxsplit=1)[1].strip().upper()
            asyncio.run_coroutine_threadsafe(_send_event(f"{pid}-killed"), loop)
            continue

        if lower.startswith("revive "):
            pid = s.split(maxsplit=1)[1].strip().upper()
            asyncio.run_coroutine_threadsafe(_send_event(f"revive-{pid}"), loop)
            continue

        # Ready input parsing
        parts = lower.split()
        if parts and parts[0] == "ready":
            parts = parts[1:]

        if len(parts) == 2 and parts[0] in ("y", "n") and parts[1] in ("y", "n"):
            if not is_ready_input_enabled():
                print("[READY] Ready input currently disabled (wait for round end).")
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
            continue

        print("[CTRL] Unknown command. Type 'help'.")


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

            if msg.event in {"activate_fast_mode", "activate_slow_mode"}:
                mode = "fast" if msg.event == "activate_fast_mode" else "slow"
                await websocket_manager.broadcast_all(msg.model_dump(mode="json"), room_id)
                snapshot = await timer_speed_manager.activate(room_id, mode)
                await _broadcast_timer_speed_update(room_id, snapshot, "activated")
                asyncio.create_task(
                    _schedule_timer_speed_expiry(
                        room_id,
                        snapshot["activatedEffectId"],
                        snapshot["activatedUntil"],
                    )
                )
                continue

            if msg.event in {"round_end", "attackers_win", "defenders_win", "defuse_success", "reset_game"}:
                snapshot = await timer_speed_manager.reset_room(room_id)
                if snapshot.get("hadActiveEffects"):
                    await _broadcast_timer_speed_update(room_id, snapshot, "reset")

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
