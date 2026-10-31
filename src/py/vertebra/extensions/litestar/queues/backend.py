"""Vertebra shared-memory queue and execution backends for litestar-queues."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_queues.backends.memory import InMemoryQueueBackend
    from litestar_queues.execution.base import BaseExecutionBackend
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-queues",
        install_package="litestar-queues",
        extra="queues",
    ) from exc

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.records import QueuedTaskRecord
    from litestar_queues.service import QueueService


class SHMQueueBackend(InMemoryQueueBackend):
    """Vertebra shared-memory queue backend with adaptive wakeup clamping."""

    __slots__ = (
        "_adaptive_clamping",
        "_max_wakeup_timeout",
        "_min_wakeup_timeout",
    )

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        min_wakeup_timeout: float = 0.005,
        max_wakeup_timeout: float = 30.0,
        adaptive_clamping: bool = True,
    ) -> None:
        """Initialize the shared-memory queue backend with adaptive wakeup clamping bounds."""
        ensure_module(
            "litestar_queues",
            package="litestar-queues is required for litestar queue integration",
            extra="queues",
        )
        super().__init__(config=config)
        self._min_wakeup_timeout = max(0.001, min_wakeup_timeout)
        self._max_wakeup_timeout = max(self._min_wakeup_timeout, max_wakeup_timeout)
        self._adaptive_clamping = adaptive_clamping

    @property
    def min_wakeup_timeout(self) -> float:
        """Return the minimum clamped wakeup timeout in seconds."""
        return self._min_wakeup_timeout

    @property
    def max_wakeup_timeout(self) -> float:
        """Return the maximum clamped wakeup timeout in seconds."""
        return self._max_wakeup_timeout

    async def wait_for_wakeups(self, timeout: float | None = None) -> bool:
        """Wait for new task notifications with adaptive timeout clamping."""
        if not self._adaptive_clamping:
            return bool(await super().wait_for_wakeups(timeout))

        requested = self._max_wakeup_timeout if timeout is None else timeout
        effective_timeout = min(max(requested, self._min_wakeup_timeout), self._max_wakeup_timeout)
        try:
            await asyncio.wait_for(self._notification_event.wait(), timeout=effective_timeout)
            self._notification_event.clear()
            return True
        except TimeoutError:
            return False


class SHMExecutionBackend(BaseExecutionBackend):
    """Vertebra shared-memory execution backend with zero-cold-start execution."""

    __slots__ = ("_warm_contexts",)

    def __init__(self) -> None:
        """Initialize the shared-memory execution backend."""
        ensure_module(
            "litestar_queues",
            package="litestar-queues is required for litestar queue integration",
            extra="queues",
        )
        super().__init__()
        self._warm_contexts: dict[str, Any] = {}

    async def execute(
        self,
        service: QueueService,
        record: QueuedTaskRecord,
        *,
        worker_id: str | None = None,
    ) -> QueuedTaskRecord:
        """Execute one claimed task record directly in-process with zero cold start."""
        return await service.execute_record(record, worker_id=worker_id)
