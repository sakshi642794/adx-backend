import logging
import threading
from app.websocket.manager import websocket_manager

logger = logging.getLogger(__name__)

_ready_lock = threading.Lock()
_ready_input_enabled = True

def set_ready_input_enabled(enabled: bool) -> None:
    global _ready_input_enabled
    with _ready_lock:
        _ready_input_enabled = enabled

def is_ready_input_enabled() -> bool:
    with _ready_lock:
        return _ready_input_enabled

async def handle_pi_event(data: dict, room_id: str = "arena") -> None:
    print("[PI] Incoming Data:", data)
    logger.info("[PI] Incoming event: %s", data.get("event", "unknown"))

    # Pi already sends { event, payload, timestamp } — pass through as-is
    message = {
        "event": data.get("event", "pi.event"),
        "payload": data.get("payload", {}),
        "room_id": room_id,
        "timestamp": data.get("timestamp", None),
    }

    await websocket_manager.broadcast_all(message, room_id)

    # Toggle terminal ready prompt based on round lifecycle
    event = data.get("event")
    if event in ("round_started",):
        set_ready_input_enabled(False)
    if event in ("round_end", "attackers_win", "defenders_win", "defuse_success"):
        set_ready_input_enabled(True)
