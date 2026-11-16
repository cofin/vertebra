"""Vertebra shared-memory queue and execution backends for litestar-queues."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ClassVar

import msgspec

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_queues.backends.factory import queue_backend
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
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = (
    "SHMExecutionBackend",
    "SHMQueueBackend",
    "SHMQueueBackendConfig",
)


class SHMQueueBackendConfig(msgspec.Struct, frozen=True):
    """Configuration for vertebra_shm queue backend."""

    backend_name: ClassVar[str] = "vertebra_shm"
    segment_name: str = "vertebra_queue"
    """POSIX shared-memory segment name used for cross-worker wakeups."""

    min_wakeup_timeout: float = 0.005
    """Minimum adaptive wakeup interval in seconds."""

    max_wakeup_timeout: float = 30.0
    """Maximum fallback wakeup interval in seconds."""

    adaptive_clamping: bool = True
    """Whether worker poll timeouts clamp dynamically to upcoming scheduled tasks."""

    terminal_ttl: timedelta | None = None
    """Optional retention duration for completed or failed task records."""

    def create_backend(self, config: QueueConfig | None = None) -> SHMQueueBackend:
        """Create a configured SHMQueueBackend instance."""
        return SHMQueueBackend(
            config=config,
            backend_config=self,
            min_wakeup_timeout=self.min_wakeup_timeout,
            max_wakeup_timeout=self.max_wakeup_timeout,
            adaptive_clamping=self.adaptive_clamping,
            segment_name=self.segment_name,
            terminal_ttl=self.terminal_ttl,
        )


@queue_backend("vertebra_shm")
class SHMQueueBackend(InMemoryQueueBackend):
    """Vertebra shared-memory queue backend with adaptive wakeup clamping."""

    __slots__ = (
        "_adaptive_clamping",
        "_max_wakeup_timeout",
        "_min_wakeup_timeout",
        "_ring",
        "_segment_name",
        "_terminal_ttl",
    )

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        backend_config: SHMQueueBackendConfig | None = None,
        min_wakeup_timeout: float = 0.005,
        max_wakeup_timeout: float = 30.0,
        adaptive_clamping: bool = True,
        segment_name: str = "vertebra_queue",
        terminal_ttl: timedelta | None = None,
    ) -> None:
        """Initialize the shared-memory queue backend with adaptive wakeup clamping bounds."""
        ensure_module(
            "litestar_queues",
            package="litestar-queues",
            extra="queues",
        )
        super().__init__(config=config)
        if backend_config is not None:
            min_wakeup_timeout = backend_config.min_wakeup_timeout
            max_wakeup_timeout = backend_config.max_wakeup_timeout
            adaptive_clamping = backend_config.adaptive_clamping
            segment_name = backend_config.segment_name
            terminal_ttl = backend_config.terminal_ttl
        self._min_wakeup_timeout = max(0.001, min_wakeup_timeout)
        self._max_wakeup_timeout = max(self._min_wakeup_timeout, max_wakeup_timeout)
        self._adaptive_clamping = adaptive_clamping
        self._segment_name = segment_name
        self._terminal_ttl = terminal_ttl
        self._ring: Any = None

    @property
    def min_wakeup_timeout(self) -> float:
        """Return the minimum clamped wakeup timeout in seconds."""
        return self._min_wakeup_timeout

    @property
    def max_wakeup_timeout(self) -> float:
        """Return the maximum clamped wakeup timeout in seconds."""
        return self._max_wakeup_timeout

    @property
    def segment_name(self) -> str:
        """Return the shared memory segment name."""
        return self._segment_name

    @property
    def terminal_ttl(self) -> timedelta | None:
        """Return the terminal retention duration."""
        return self._terminal_ttl

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
            package="litestar-queues",
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
        """Execute the task record directly within the current process."""
        return await service.execute_record(record, worker_id=worker_id)
