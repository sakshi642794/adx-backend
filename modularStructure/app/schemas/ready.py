from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime, timezone


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── WebSocket schemas ─────────────────────────────────────────────────────── #

class WSMessage(BaseModel):
    """
    Standard envelope for every WebSocket message in the system.

    Clients send:
        { "event": "arena.update", "payload": {...}, "room_id": "arena" }

    Server echoes back the same shape with a server-assigned timestamp.
    """
    event: str
    payload: Any = None
    room_id: str = "global"
    timestamp: datetime = Field(default_factory=_utcnow)


class WSAck(BaseModel):
    """Acknowledgement returned after a WebSocket action is processed."""
    success: bool
    event: str
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=_utcnow)


# ── REST schemas ──────────────────────────────────────────────────────────── #

class HealthResponse(BaseModel):
    status: str
    app_name: str
    environment: str
    websocket_connections: int
    rooms: dict
    timestamp: datetime = Field(default_factory=_utcnow)


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int
    timestamp: datetime = Field(default_factory=_utcnow)
