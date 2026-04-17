"""
Raspberry Pi WebSocket relay.

Goal:
- Connect to the CO/backend WebSocket (upstream)
- Re-broadcast every upstream message to local HUD clients (downstream)

This lets the Pi "listen" to CO actions (timer alter, kill, revive, etc.)
and fan them out locally so PCs can just connect to the Pi on LAN.

Env:
- BACKEND_WS_URL: ws://<backend-host>:8000/ws/arena
- LOCAL_WS_HOST: 0.0.0.0
- LOCAL_WS_PORT: 8080
"""

import asyncio
import json
import os
import signal
from typing import Set, Optional, Any

import websockets

try:
    # Newer websockets versions
    from websockets.server import WebSocketServerProtocol  # type: ignore
    from websockets.client import WebSocketClientProtocol  # type: ignore
except Exception:
    # Fall back to loose typing for older versions
    WebSocketServerProtocol = Any  # type: ignore
    WebSocketClientProtocol = Any  # type: ignore


BACKEND_WS_URL = os.getenv("BACKEND_WS_URL", "ws://localhost:8000/ws/arena")
LOCAL_WS_HOST = os.getenv("LOCAL_WS_HOST", "0.0.0.0")
LOCAL_WS_PORT = int(os.getenv("LOCAL_WS_PORT", "8080"))


class Relay:
    def __init__(self) -> None:
        self._local_clients: Set[WebSocketServerProtocol] = set()
        self._upstream: Optional[WebSocketClientProtocol] = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()

    async def stop(self) -> None:
        self._stop.set()
        async with self._lock:
            if self._upstream is not None:
                try:
                    await self._upstream.close()
                except Exception:
                    pass
                self._upstream = None

        # Close locals
        for ws in list(self._local_clients):
            try:
                await ws.close()
            except Exception:
                pass

    async def _broadcast_local(self, raw: str) -> None:
        if not self._local_clients:
            return

        dead = []
        for ws in list(self._local_clients):
            try:
                await ws.send(raw)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._local_clients.discard(ws)

    async def local_handler(self, ws: WebSocketServerProtocol) -> None:
        self._local_clients.add(ws)
        try:
            async for message in ws:
                # Optional: allow local tools to send messages upstream.
                async with self._lock:
                    upstream = self._upstream
                if upstream is None:
                    continue
                try:
                    # Ensure we forward valid JSON strings (backend expects JSON).
                    if isinstance(message, str):
                        json.loads(message)
                        await upstream.send(message)
                except Exception:
                    # Ignore malformed local messages.
                    continue
        finally:
            self._local_clients.discard(ws)

    async def upstream_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(BACKEND_WS_URL, ping_interval=20, ping_timeout=20) as upstream:
                    async with self._lock:
                        self._upstream = upstream
                    backoff = 1.0

                    async for raw in upstream:
                        if not isinstance(raw, str):
                            continue
                        await self._broadcast_local(raw)
            except Exception:
                async with self._lock:
                    self._upstream = None
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 1.7)


async def main() -> None:
    relay = Relay()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(relay.stop()))
        except NotImplementedError:
            # Windows / some runtimes
            pass

    upstream_task = asyncio.create_task(relay.upstream_loop())

    async with websockets.serve(relay.local_handler, LOCAL_WS_HOST, LOCAL_WS_PORT):
        await relay._stop.wait()

    upstream_task.cancel()
    try:
        await upstream_task
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
