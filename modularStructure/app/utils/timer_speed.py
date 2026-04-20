import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Literal


TimerMode = Literal["fast", "slow"]


@dataclass
class TimerSpeedEffect:
    effect_id: str
    mode: TimerMode
    expires_at_ms: int


class TimerSpeedManager:
    def __init__(self) -> None:
        self._effects_by_room: Dict[str, list[TimerSpeedEffect]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _lock_for(self, room_id: str) -> asyncio.Lock:
        if room_id not in self._locks:
            self._locks[room_id] = asyncio.Lock()
        return self._locks[room_id]

    def _cleanup_expired_unlocked(self, room_id: str, now_ms: int) -> None:
        active = [
            effect
            for effect in self._effects_by_room.get(room_id, [])
            if effect.expires_at_ms > now_ms
        ]
        if active:
            self._effects_by_room[room_id] = active
        else:
            self._effects_by_room.pop(room_id, None)

    def _snapshot_unlocked(self, room_id: str, now_ms: int) -> dict:
        self._cleanup_expired_unlocked(room_id, now_ms)
        effects = self._effects_by_room.get(room_id, [])

        fast_count = sum(1 for effect in effects if effect.mode == "fast")
        slow_count = sum(1 for effect in effects if effect.mode == "slow")
        net = max(-1, min(1, fast_count - slow_count))

        if net > 0:
            effective_mode = "fast"
            speed_multiplier = 2.0
        elif net < 0:
            effective_mode = "slow"
            speed_multiplier = 0.5
        else:
            effective_mode = "normal"
            speed_multiplier = 1.0

        next_expiry_at = min((effect.expires_at_ms for effect in effects), default=None)

        return {
            "fastCount": fast_count,
            "slowCount": slow_count,
            "activeCount": len(effects),
            "effectiveMode": effective_mode,
            "speedMultiplier": speed_multiplier,
            "nextExpiryAt": next_expiry_at,
            "durationSeconds": 60,
        }

    async def activate(self, room_id: str, mode: TimerMode) -> dict:
        now_ms = self._now_ms()
        effect = TimerSpeedEffect(
            effect_id=uuid.uuid4().hex,
            mode=mode,
            expires_at_ms=now_ms + 60_000,
        )

        async with self._lock_for(room_id):
            self._cleanup_expired_unlocked(room_id, now_ms)
            self._effects_by_room.setdefault(room_id, []).append(effect)
            snapshot = self._snapshot_unlocked(room_id, now_ms)

        snapshot["activatedMode"] = mode
        snapshot["activatedEffectId"] = effect.effect_id
        snapshot["activatedUntil"] = effect.expires_at_ms
        return snapshot

    async def expire_effect(self, room_id: str, effect_id: str) -> dict | None:
        now_ms = self._now_ms()

        async with self._lock_for(room_id):
            effects = self._effects_by_room.get(room_id, [])
            before = len(effects)
            remaining = [effect for effect in effects if effect.effect_id != effect_id]

            if before == len(remaining):
                self._cleanup_expired_unlocked(room_id, now_ms)
                return None

            if remaining:
                self._effects_by_room[room_id] = remaining
            else:
                self._effects_by_room.pop(room_id, None)

            snapshot = self._snapshot_unlocked(room_id, now_ms)

        snapshot["expiredEffectId"] = effect_id
        return snapshot

    async def reset_room(self, room_id: str) -> dict:
        now_ms = self._now_ms()
        async with self._lock_for(room_id):
            had_active = bool(self._effects_by_room.get(room_id))
            self._effects_by_room.pop(room_id, None)
            snapshot = self._snapshot_unlocked(room_id, now_ms)

        snapshot["hadActiveEffects"] = had_active
        return snapshot


timer_speed_manager = TimerSpeedManager()
