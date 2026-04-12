import asyncio
import logging
from typing import Dict, List
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Central WebSocket connection manager.

    Maintains a room-based pool:
        { room_id: [WebSocket, WebSocket, ...] }

    Any message received by ONE client is immediately broadcast
    to ALL clients in the same room — including across different
    machines connected via ngrok or any network.

    Dead connections are detected and silently removed so they
    never block or crash the broadcast loop.
    """

    def __init__(self) -> None:
        # room_id → list of active WebSocket connections
        self._rooms: Dict[str, List[WebSocket]] = {}
        # Per-room asyncio locks to prevent race conditions during
        # concurrent connect / disconnect / broadcast calls
        self._locks: Dict[str, asyncio.Lock] = {}

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_lock(self, room_id: str) -> asyncio.Lock:
        if room_id not in self._locks:
            self._locks[room_id] = asyncio.Lock()
        return self._locks[room_id]

    def _ensure_room(self, room_id: str) -> None:
        if room_id not in self._rooms:
            self._rooms[room_id] = []

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    async def connect(self, websocket: WebSocket, room_id: str = "global") -> None:
        """
        Accept the WebSocket handshake and register the client in a room.
        Called once per connection, before the receive loop starts.
        """
        await websocket.accept()
        async with self._get_lock(room_id):
            self._ensure_room(room_id)
            self._rooms[room_id].append(websocket)

        count = self.get_connection_count(room_id)
        logger.info(
            "Client connected | room=%s | total_in_room=%d", room_id, count
        )

    async def disconnect(self, websocket: WebSocket, room_id: str = "global") -> None:
        """
        Remove a client from its room. Safe to call even if the socket
        was never registered or has already been removed.
        """
        async with self._get_lock(room_id):
            if room_id in self._rooms:
                try:
                    self._rooms[room_id].remove(websocket)
                except ValueError:
                    pass  # already gone — no problem
                if not self._rooms[room_id]:
                    # Clean up empty rooms entirely
                    del self._rooms[room_id]
                    if room_id in self._locks:
                        del self._locks[room_id]

        logger.info(
            "Client disconnected | room=%s | remaining=%d",
            room_id,
            self.get_connection_count(room_id),
        )

    async def broadcast_all(
        self, message: dict, room_id: str = "global"
    ) -> None:
        """
        🔥 CORE METHOD — send `message` to EVERY client in `room_id`.

        Guarantees:
        - Non-blocking: each send is awaited individually
        - Dead connections are collected and removed after the loop
        - A single broken socket never interrupts other deliveries
        - Works across localhost, LAN, and ngrok tunnels identically
        """
        if room_id not in self._rooms:
            return

        # Snapshot the list so we don't hold the lock during I/O
        async with self._get_lock(room_id):
            connections = list(self._rooms.get(room_id, []))

        dead: List[WebSocket] = []

        for ws in connections:
            try:
                await ws.send_json(message)
            except Exception as exc:
                # Socket is broken — mark for removal, keep going
                logger.warning(
                    "Failed to send to client in room=%s: %s", room_id, exc
                )
                dead.append(ws)

        # Remove dead connections
        for ws in dead:
            await self.disconnect(ws, room_id)

        if dead:
            logger.info(
                "Cleaned %d dead connection(s) from room=%s", len(dead), room_id
            )

    async def send_personal(
        self, message: dict, websocket: WebSocket
    ) -> None:
        """
        Send a message to ONE specific client only.
        If the socket is dead the error is swallowed gracefully.
        """
        try:
            await websocket.send_json(message)
        except Exception as exc:
            logger.warning("send_personal failed: %s", exc)
            # Find which room this socket belongs to and clean it up
            for room_id in list(self._rooms.keys()):
                if websocket in self._rooms.get(room_id, []):
                    await self.disconnect(websocket, room_id)
                    break

    async def broadcast_to_all_rooms(self, message: dict) -> None:
        """
        Broadcast a message to EVERY client in EVERY room.
        Useful for server-wide announcements.
        """
        for room_id in list(self._rooms.keys()):
            await self.broadcast_all(message, room_id)

    def get_connection_count(self, room_id: str = "global") -> int:
        """
        Return number of connections.
        - If room_id == "global" → total across all rooms
        - Otherwise → count for the specific room
        """
        if room_id == "global":
            return sum(len(conns) for conns in self._rooms.values())
        return len(self._rooms.get(room_id, []))

    def get_rooms(self) -> Dict[str, int]:
        """Return a dict of { room_id: connection_count } for monitoring."""
        return {room_id: len(conns) for room_id, conns in self._rooms.items()}


# ── Singleton ────────────────────────────────────────────────────────────── #
# Import this object everywhere — never instantiate a second manager.
websocket_manager = WebSocketManager()
