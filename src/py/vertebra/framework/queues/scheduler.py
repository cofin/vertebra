"""Distributed cron schedule expression parser and lease-elected cron scheduler."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import msgspec

if TYPE_CHECKING:
    from vertebra.framework.queues.base import QueueService, TaskEnvelope

__all__ = (
    "CronExpression",
    "CronScheduler",
)


def _parse_field(
    raw: str,
    min_val: int,
    max_val: int,
    *,
    is_dow: bool = False,
) -> frozenset[int]:
    """Parse a single cron field expression into a set of allowed integer values."""
    values: set[int] = set()
    parts = raw.strip().split(",")
    for part in parts:
        step = 1
        if "/" in part:
            subparts = part.split("/", 1)
            step = int(subparts[1])
            if step <= 0:
                msg = f"Step must be positive in cron field: {raw!r}"
                raise ValueError(msg)
            range_expr = subparts[0]
        else:
            range_expr = part

        if range_expr == "*":
            start_val, end_val = min_val, max_val
        elif "-" in range_expr:
            start_s, end_s = range_expr.split("-", 1)
            start_val = int(start_s)
            end_val = int(end_s)
        else:
            val = int(range_expr)
            if is_dow and val == 7:
                val = 0
            if val < min_val or val > max_val:
                msg = f"Value {val} out of bounds [{min_val}, {max_val}] in cron field: {raw!r}"
                raise ValueError(msg)
            values.add(val)
            continue

        if start_val < min_val or end_val > max_val or start_val > end_val:
            msg = f"Range {start_val}-{end_val} out of bounds in cron field: {raw!r}"
            raise ValueError(msg)

        for v in range(start_val, end_val + 1, step):
            if is_dow and v == 7:
                values.add(0)
            else:
                values.add(v)

    return frozenset(values)


class CronExpression(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Parsed 5-field cron expression representation."""

    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]

    @classmethod
    def parse(cls, expression: str) -> CronExpression:
        """Parse standard 5-field cron expression (minute, hour, dom, month, dow)."""
        fields = expression.strip().split()
        if len(fields) != 5:
            msg = f"Expected exactly 5 fields in cron expression, got {len(fields)}: {expression!r}"
            raise ValueError(msg)

        mins = _parse_field(fields[0], 0, 59)
        hrs = _parse_field(fields[1], 0, 23)
        dom = _parse_field(fields[2], 1, 31)
        mon = _parse_field(fields[3], 1, 12)
        dow = _parse_field(fields[4], 0, 7, is_dow=True)

        return cls(
            minutes=mins,
            hours=hrs,
            days_of_month=dom,
            months=mon,
            days_of_week=dow,
        )

    def matches(self, dt: datetime, tz_name: str = "UTC") -> bool:
        """Evaluate if the provided datetime matches this cron expression in the target timezone."""
        localized = dt.astimezone(ZoneInfo(tz_name)) if dt.tzinfo else dt.replace(tzinfo=ZoneInfo("UTC"))
        dow = localized.isoweekday() % 7
        return (
            localized.minute in self.minutes
            and localized.hour in self.hours
            and localized.day in self.days_of_month
            and localized.month in self.months
            and dow in self.days_of_week
        )


class CronScheduler:
    """Distributed lease-elected cron job scheduler dispatching due tasks into QueueService."""

    __slots__ = (
        "_check_interval",
        "_last_dispatched_slot",
        "_lease_key",
        "_lease_ttl",
        "_node_id",
        "_queue_service",
    )

    def __init__(
        self,
        queue_service: QueueService,
        *,
        node_id: str | None = None,
        lease_key: str = "cron:leader",
        lease_ttl_seconds: float = 30.0,
    ) -> None:
        self._queue_service = queue_service
        self._node_id = node_id or uuid.uuid4().hex
        self._lease_key = lease_key
        self._lease_ttl = lease_ttl_seconds
        self._last_dispatched_slot: str | None = None

    @property
    def node_id(self) -> str:
        """Return the unique scheduler node identifier."""
        return self._node_id

    async def tick(self, now: datetime | None = None) -> list[TaskEnvelope]:
        """Attempt lease-based slot election and enqueue tasks matching the current minute tick."""
        current_dt = now or datetime.now(UTC)
        slot_key = current_dt.strftime("%Y-%m-%dT%H:%M")

        backend = self._queue_service.backend
        lease = await backend.acquire_lease(
            self._lease_key,
            self._node_id,
            self._lease_ttl,
            slot=slot_key,
        )
        if lease is None:
            return []

        if lease.last_slot == slot_key and self._last_dispatched_slot == slot_key:
            return []

        enqueued: list[TaskEnvelope] = []
        for tdef in self._queue_service.registered_tasks.values():
            if tdef.cron is None:
                continue
            expr = CronExpression.parse(tdef.cron.expression)
            if expr.matches(current_dt, tdef.cron.timezone):
                idempotency_key = f"cron:{tdef.name}:{slot_key}"
                env = await self._queue_service.enqueue(
                    tdef.name,
                    lane=tdef.lane,
                    idempotency_key=idempotency_key,
                )
                enqueued.append(env)

        self._last_dispatched_slot = slot_key
        await backend.renew_lease(
            self._lease_key,
            self._node_id,
            self._lease_ttl,
            slot=slot_key,
        )
        return enqueued

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        check_interval_seconds: float = 1.0,
    ) -> None:
        """Execute scheduled background loop until stop_event is signaled."""
        while not stop_event.is_set():
            await self._queue_service.backend.promote_scheduled()
            await self.tick()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=check_interval_seconds)
