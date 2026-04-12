import logging
from app.websocket.manager import websocket_manager

logger = logging.getLogger(__name__)

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