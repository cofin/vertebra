"""Adapter-owned PostgreSQL logical replication change stream and durable checkpoints."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from vertebra.db.adapters.postgres.types import PostgresReplicationIdentity, PostgresReplicationRecord
from vertebra.db.cdc import CDCConfig, ChangeEvent, ChangeFeed
from vertebra.db.driver import (
    AsyncChangeSource,
    OperationalError,
    SerializationFailure,
)
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.serializers import encode_json_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._postgres import AsyncPostgresReplication
    from vertebra.db.adapters.postgres.events import PostgresReplicationDriver

__all__ = (
    "MemoryPostgresCheckpointStore",
    "PostgresChangeConfig",
    "PostgresChangeSource",
    "PostgresCheckpoint",
    "PostgresCheckpointConflictError",
    "PostgresCheckpointStore",
    "PostgresReplicationGapError",
)

type IdentityKey = tuple[str, str, str, str]


class PostgresCheckpointConflictError(SerializationFailure):
    """Raised when optimistic CAS checkpoint persistence detects a stale writer."""

    def __init__(
        self,
        message: str = "PostgreSQL CDC checkpoint compare-and-swap conflict",
        *,
        identity_key: IdentityKey | None = None,
        expected_version: int | None = None,
        actual_version: int | None = None,
    ) -> None:
        """Initialize a checkpoint CAS conflict error carrying SQLSTATE 40001."""
        super().__init__(message)
        self.identity_key = identity_key
        self.expected_version = expected_version
        self.actual_version = actual_version


class PostgresReplicationGapError(OperationalError):
    """Raised fail-closed when replication resume integrity cannot be guaranteed."""

    def __init__(
        self,
        message: str,
        *,
        reason: str,
        identity_key: IdentityKey | None = None,
    ) -> None:
        """Initialize a fail-closed replication gap or uncertainty error."""
        super().__init__(message)
        self.reason = reason
        self.identity_key = identity_key


class PostgresChangeConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for PostgreSQL logical replication CDC streaming."""

    slot: str
    """Logical replication slot identifier."""

    publication: str
    """PostgreSQL publication name to subscribe to."""

    start_lsn: int | None = None
    """Optional starting LSN override when no durable checkpoint exists."""

    reconnect: bool = True
    """Whether to reconnect across retryable stream drops."""

    max_reconnect_attempts: int = 5
    """Maximum consecutive reconnect attempts before failing closed."""

    initial_backoff_seconds: float = 0.05
    """Initial delay before the first reconnect attempt in seconds."""

    max_backoff_seconds: float = 1.0
    """Maximum exponential reconnect backoff delay in seconds."""

    checkpoint_timeout_seconds: float = 5.0
    """Bounded timeout in seconds for checkpoint store loads, saves, and close."""

    queue_capacity: int = 1024
    """Bounded native replication delivery queue capacity."""

    cdc_config: CDCConfig | None = None
    """Optional ChangeFeed spill and replay configuration."""

    def __post_init__(self) -> None:
        """Validate identifiers, LSN bounds, and numeric lifecycle parameters."""
        validate_identifier(self.slot, "slot", allow_dots=False)
        validate_identifier(self.publication, "publication", allow_dots=False)
        if self.start_lsn is not None and self.start_lsn < 0:
            raise ImproperConfigurationError("start_lsn must be a non-negative integer")
        if self.max_reconnect_attempts < 0:
            raise ImproperConfigurationError("max_reconnect_attempts must be >= 0")
        if self.initial_backoff_seconds < 0.0 or self.max_backoff_seconds < 0.0:
            raise ImproperConfigurationError("reconnect backoff bounds must be >= 0")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ImproperConfigurationError("max_backoff_seconds must be >= initial_backoff_seconds")
        if self.checkpoint_timeout_seconds <= 0.0:
            raise ImproperConfigurationError("checkpoint_timeout_seconds must be > 0")
        if self.queue_capacity <= 0:
            raise ImproperConfigurationError("queue_capacity must be > 0")


class PostgresCheckpoint(FrozenBaseStruct, frozen=True, gc=False):
    """Durable CAS-versioned replication cursor paired with ChangeFeed position."""

    identity_key: IdentityKey
    """Four-tuple identity ``(system_identifier, database, slot, publication_fingerprint)``."""

    commit_end_lsn: int
    """Monotonic durably acknowledged WAL commit end LSN."""

    next_event_id: int
    """Monotonic next ChangeFeed event sequence identifier."""

    version: int = 1
    """Monotonic optimistic CAS version counter."""

    def __post_init__(self) -> None:
        """Validate 4-part replication identity key and monotone counters."""
        if len(self.identity_key) != 4 or any(not isinstance(part, str) or not part for part in self.identity_key):
            raise ValueError(
                "identity_key must be a 4-tuple of non-empty strings "
                "(system_identifier, database, slot, publication_fingerprint)"
            )
        validate_identifier(self.identity_key[2], "slot", allow_dots=False)
        if self.commit_end_lsn < 0:
            raise ValueError("commit_end_lsn must be non-negative")
        if self.next_event_id < 1:
            raise ValueError("next_event_id must be >= 1")
        if self.version < 1:
            raise ValueError("version must be >= 1")

    @property
    def system_identifier(self) -> str:
        """Return the PostgreSQL cluster system_identifier component."""
        return self.identity_key[0]

    @property
    def database(self) -> str:
        """Return the PostgreSQL database component."""
        return self.identity_key[1]

    @property
    def slot(self) -> str:
        """Return the logical replication slot name component."""
        return self.identity_key[2]

    @property
    def publication_fingerprint(self) -> str:
        """Return the canonical publication fingerprint component."""
        return self.identity_key[3]

    def advance(self, *, commit_end_lsn: int, next_event_id: int) -> PostgresCheckpoint:
        """Return the next monotone checkpoint increment with version + 1."""
        if commit_end_lsn < self.commit_end_lsn:
            raise ValueError(f"commit_end_lsn regression: {commit_end_lsn} < {self.commit_end_lsn}")
        if next_event_id < self.next_event_id:
            raise ValueError(f"next_event_id regression: {next_event_id} < {self.next_event_id}")
        return PostgresCheckpoint(
            identity_key=self.identity_key,
            commit_end_lsn=commit_end_lsn,
            next_event_id=next_event_id,
            version=self.version + 1,
        )


@runtime_checkable
class PostgresCheckpointStore(Protocol):
    """Protocol for durable compare-and-swap PostgreSQL CDC checkpoint stores."""

    async def load(self, identity_key: IdentityKey) -> PostgresCheckpoint | None:
        """Load the latest persisted checkpoint for ``identity_key`` or ``None``."""
        ...

    async def save(
        self,
        checkpoint: PostgresCheckpoint,
        *,
        expected: PostgresCheckpoint | None,
    ) -> PostgresCheckpoint:
        """Persist ``checkpoint`` atomically only if stored cursor matches ``expected``."""
        ...


class MemoryPostgresCheckpointStore:
    """In-memory optimistic CAS checkpoint store enforcing single-writer fencing."""

    __slots__ = ("_checkpoints", "_lock")

    def __init__(self) -> None:
        """Initialize an empty in-memory checkpoint table."""
        self._checkpoints: dict[IdentityKey, PostgresCheckpoint] = {}
        self._lock = asyncio.Lock()

    async def load(self, identity_key: IdentityKey) -> PostgresCheckpoint | None:
        """Return the persisted checkpoint for ``identity_key`` if present."""
        async with self._lock:
            return self._checkpoints.get(identity_key)

    async def save(
        self,
        checkpoint: PostgresCheckpoint,
        *,
        expected: PostgresCheckpoint | None,
    ) -> PostgresCheckpoint:
        """Persist ``checkpoint`` using strict expected-version and monotonicity checks."""
        async with self._lock:
            current = self._checkpoints.get(checkpoint.identity_key)
            if expected is None:
                if current is not None:
                    raise PostgresCheckpointConflictError(
                        "Checkpoint already exists for identity_key",
                        identity_key=checkpoint.identity_key,
                        expected_version=None,
                        actual_version=current.version,
                    )
                if checkpoint.version != 1:
                    raise PostgresCheckpointConflictError(
                        "Initial checkpoint save requires version == 1",
                        identity_key=checkpoint.identity_key,
                        expected_version=1,
                        actual_version=checkpoint.version,
                    )
            else:
                if current is None or current.version != expected.version:
                    raise PostgresCheckpointConflictError(
                        "Stale checkpoint writer detected during CAS save",
                        identity_key=checkpoint.identity_key,
                        expected_version=expected.version,
                        actual_version=current.version if current is not None else None,
                    )
                if checkpoint.version != expected.version + 1:
                    raise PostgresCheckpointConflictError(
                        "Checkpoint CAS version must increment by 1",
                        identity_key=checkpoint.identity_key,
                        expected_version=expected.version + 1,
                        actual_version=checkpoint.version,
                    )
                if checkpoint.commit_end_lsn < current.commit_end_lsn:
                    raise PostgresCheckpointConflictError(
                        "Checkpoint commit_end_lsn cannot regress",
                        identity_key=checkpoint.identity_key,
                        expected_version=expected.version,
                        actual_version=current.version,
                    )
                if checkpoint.next_event_id < current.next_event_id:
                    raise PostgresCheckpointConflictError(
                        "Checkpoint next_event_id cannot regress",
                        identity_key=checkpoint.identity_key,
                        expected_version=expected.version,
                        actual_version=current.version,
                    )
            self._checkpoints[checkpoint.identity_key] = checkpoint
            return checkpoint


class PostgresChangeSource(AsyncChangeSource):
    """Durable PostgreSQL logical replication change source backed by ChangeFeed.

    Guarantees save-before-wire-ack semantics using single-flight shielded
    compare-and-swap checkpoint updates, reconciles uncertain commits against
    the durable checkpoint store before resuming yields, and exposes a bounded
    fail-closed ``aclose()``.
    """

    _RETRYABLE_RECONNECT_REASONS = frozenset({"connection_closed", "disconnected", "transport", "transient"})
    _FAIL_CLOSED_REASONS = frozenset({"wal_unavailable", "identity_mismatch"})

    __slots__ = (
        "_ack_lock",
        "_ack_task",
        "_checkpoint",
        "_closed",
        "_config",
        "_driver",
        "_feed",
        "_identity",
        "_init_lock",
        "_initialized",
        "_lsn_to_event_id",
        "_needs_reconcile",
        "_op_lock",
        "_pending_record",
        "_reconcile_lock",
        "_stop_event",
        "_store",
        "_stream",
        "_terminated",
        "_yield_gate",
    )

    def __init__(
        self,
        driver: PostgresReplicationDriver,
        config: PostgresChangeConfig,
        checkpoint_store: PostgresCheckpointStore | None = None,
        *,
        feed: ChangeFeed | None = None,
    ) -> None:
        """Initialize the PostgreSQL CDC source without opening network streams yet."""
        self._driver = driver
        self._config = config
        self._store: PostgresCheckpointStore = (
            checkpoint_store if checkpoint_store is not None else MemoryPostgresCheckpointStore()
        )
        self._feed = (
            feed
            if feed is not None
            else ChangeFeed(config=config.cdc_config if config.cdc_config is not None else CDCConfig())
        )
        self._identity: PostgresReplicationIdentity | None = None
        self._initialized = False
        self._checkpoint: PostgresCheckpoint | None = None
        self._stream: AsyncPostgresReplication | None = None
        self._closed = False
        self._terminated = False
        self._needs_reconcile = False
        self._yield_gate = asyncio.Event()
        self._yield_gate.set()
        self._stop_event = asyncio.Event()
        self._init_lock = asyncio.Lock()
        self._op_lock = asyncio.Lock()
        self._ack_lock = asyncio.Lock()
        self._reconcile_lock = asyncio.Lock()
        self._ack_task: asyncio.Task[PostgresCheckpoint] | None = None
        self._pending_record: PostgresReplicationRecord | None = None
        self._lsn_to_event_id: dict[int, int] = {}

    @property
    def identity(self) -> PostgresReplicationIdentity | None:
        """Return the resolved PostgreSQL replication identity once initialized."""
        return self._identity

    @property
    def checkpoint(self) -> PostgresCheckpoint | None:
        """Return the latest durable checkpoint known to this source."""
        return self._checkpoint

    @property
    def feed(self) -> ChangeFeed:
        """Return the backing ChangeFeed managing in-memory window and spill/replay."""
        return self._feed

    @property
    def closed(self) -> bool:
        """Return whether this change source has been closed."""
        return self._closed

    async def __anext__(self) -> ChangeEvent:
        """Yield the next durable ChangeEvent when the yield gate permits."""
        while True:
            self._check_lifecycle()
            await self._yield_gate.wait()
            self._check_lifecycle()
            pending_ack = self._ack_task
            if pending_ack is not None:
                if not pending_ack.done():
                    await self._await_pending_ack(pending_ack)
                    continue
                self._ack_task = None
            async with self._op_lock:
                self._check_lifecycle()
                if self._needs_reconcile:
                    await self._reconcile_uncertainty()
                if not self._yield_gate.is_set():
                    continue
                self._check_lifecycle()
                record = await self._next_native_record()
                self._check_lifecycle()
                if self._needs_reconcile:
                    continue
                if not self._yield_gate.is_set():
                    self._pending_record = record
                    continue
                return self._publish_record(record)

    def __aiter__(self) -> AsyncIterator[ChangeEvent]:
        """Return self as an asynchronous iterator of ChangeEvent objects."""
        return self

    def stream(self) -> AsyncIterator[ChangeEvent]:
        """Yield ``ChangeEvent`` items asynchronously as database changes arrive."""
        return self

    def replay_since(self, last_event_id: int) -> list[ChangeEvent]:
        """Return buffered ``ChangeEvent`` items emitted after ``last_event_id``."""
        return self._feed.replay_since(last_event_id)

    async def acknowledge(self, commit_end_lsn: int) -> PostgresCheckpoint:
        """Durably persist checkpoint via CAS before acknowledging ``commit_end_lsn`` to Postgres.

        Protected with ``asyncio.shield`` so external task cancellation cannot
        abort an in-flight CAS save mid-commit; concurrent callers wait on the
        active single-flight acknowledgement task in order.
        """
        if commit_end_lsn < 0:
            raise ValueError("commit_end_lsn must be non-negative")
        self._check_ack_lifecycle()
        await self._ensure_initialized()
        while True:
            active_task = self._ack_task
            if active_task is not None and not active_task.done():
                await asyncio.shield(active_task)
                continue
            async with self._ack_lock:
                active_task = self._ack_task
                if active_task is not None and not active_task.done():
                    continue
                self._check_ack_lifecycle()
                if self._needs_reconcile:
                    await self._reconcile_uncertainty()
                    self._check_ack_lifecycle()
                if self._checkpoint is not None and commit_end_lsn <= self._checkpoint.commit_end_lsn:
                    return self._checkpoint
                self._yield_gate.clear()
                task = asyncio.create_task(self._run_ack_transaction(commit_end_lsn))
                task.add_done_callback(self._consume_task_exception)
                self._ack_task = task
                break
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._ack_task is task:
                self._ack_task = None

    async def aclose(self) -> None:
        """Close the change source within bounded timeout, reconciling uncertain state."""
        self._closed = True
        self._stop_event.set()
        deadline = asyncio.get_running_loop().time() + self._config.checkpoint_timeout_seconds
        pending_task = self._ack_task
        primary_error: BaseException | None = None
        try:
            if pending_task is not None and not pending_task.done():
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0.0:
                    raise PostgresReplicationGapError(
                        "Timed out waiting for in-flight checkpoint acknowledgement during aclose",
                        reason="checkpoint_uncertain",
                        identity_key=self._identity.identity_key if self._identity else None,
                    )
                try:
                    await asyncio.wait_for(asyncio.shield(pending_task), timeout=remaining)
                except TimeoutError as exc:
                    raise PostgresReplicationGapError(
                        "Timed out waiting for in-flight checkpoint acknowledgement during aclose",
                        reason="checkpoint_uncertain",
                        identity_key=self._identity.identity_key if self._identity else None,
                    ) from exc
                except asyncio.CancelledError:
                    current_task = asyncio.current_task()
                    if not pending_task.cancelled() or (current_task is not None and current_task.cancelling() > 0):
                        raise
                except PostgresCheckpointConflictError:
                    pass
                except PostgresReplicationGapError:
                    pass
            if self._needs_reconcile:
                await self._reconcile_under_lock(
                    deadline=deadline,
                    clear_pending_and_stream=False,
                    timeout_message="Timed out reconciling uncertain checkpoint during aclose",
                )
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            self._pending_record = None
            self._yield_gate.set()
            await self._close_active_stream(suppress_error=primary_error is not None)

    async def __aenter__(self) -> PostgresChangeSource:
        """Enter async context manager returning the change source."""
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context manager by calling ``aclose()``."""
        await self.aclose()

    def _check_lifecycle(self) -> None:
        """Raise termination conflict or StopAsyncIteration when closed or fenced."""
        if self._terminated:
            raise PostgresCheckpointConflictError(
                "CDC ownership terminated after checkpoint conflict",
                identity_key=self._identity.identity_key if self._identity else None,
            )
        if self._closed:
            raise StopAsyncIteration

    def _check_ack_lifecycle(self) -> None:
        """Raise closed-gap or termination conflict before checkpoint acknowledgement."""
        if self._closed:
            raise PostgresReplicationGapError(
                "CDC change source is closed",
                reason="closed",
                identity_key=self._identity.identity_key if self._identity else None,
            )
        if self._terminated:
            raise PostgresCheckpointConflictError(
                "CDC ownership terminated after checkpoint conflict",
                identity_key=self._identity.identity_key if self._identity else None,
            )

    async def _wait_backoff(self, seconds: float) -> None:
        """Wait up to ``seconds`` for reconnect backoff unless closed or fenced."""
        if seconds <= 0.0:
            self._check_lifecycle()
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        self._check_lifecycle()

    async def _await_pending_ack(self, pending_ack: asyncio.Task[PostgresCheckpoint]) -> None:
        """Wait for an active single-flight acknowledgement or immediate stop without cancelling it."""
        stop_waiter = asyncio.create_task(self._stop_event.wait())
        try:
            await asyncio.wait(
                {pending_ack, stop_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            if not stop_waiter.done():
                stop_waiter.cancel()

    async def _ensure_initialized(self) -> None:
        """Resolve replication identity and load initial checkpoint under ``_init_lock``."""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            try:
                identity = await self._driver._replication_identity(
                    self._config.slot,
                    self._config.publication,
                )
            except PostgresReplicationGapError:
                raise
            except Exception as exc:
                gap_reason = self._classify_gap_reason(exc)
                if gap_reason is not None:
                    raise PostgresReplicationGapError(
                        str(exc),
                        reason=gap_reason,
                    ) from exc
                raise
            loaded = await asyncio.wait_for(
                self._store.load(identity.identity_key),
                timeout=self._config.checkpoint_timeout_seconds,
            )
            if loaded is not None:
                self._validate_identity_match(identity, loaded)
                self._checkpoint = loaded
                self._sync_feed_cursor(loaded.next_event_id)
            self._identity = identity
            self._initialized = True

    def _sync_feed_cursor(self, next_event_id: int) -> None:
        """Monotonically advance the backing ChangeFeed event sequence counter."""
        self._feed._next_id = max(self._feed._next_id, 1, int(next_event_id))

    async def _close_active_stream(self, *, suppress_error: bool = False) -> None:
        """Close any currently attached native stream generation."""
        stream = self._stream
        self._stream = None
        if stream is not None:
            if suppress_error:
                with contextlib.suppress(Exception):
                    await stream.aclose()
            else:
                await stream.aclose()

    def _effective_start_lsn(self) -> int | None:
        """Compute the resume LSN from persisted checkpoint or initial configuration."""
        if self._checkpoint is not None:
            return self._checkpoint.commit_end_lsn
        return self._config.start_lsn

    async def _advance_reconnect_backoff(
        self,
        attempts: int,
        backoff: float,
        cause: BaseException | None = None,
    ) -> tuple[int, float]:
        """Increment reconnect attempts, enforce cap, wait backoff, and advance delay."""
        attempts += 1
        if attempts > self._config.max_reconnect_attempts:
            error = PostgresReplicationGapError(
                f"Exhausted {self._config.max_reconnect_attempts} replication reconnect attempts",
                reason="retries_exhausted",
                identity_key=self._identity.identity_key if self._identity else None,
            )
            if cause is not None:
                raise error from cause
            raise error from None
        await self._wait_backoff(backoff)
        if backoff > 0.0:
            backoff = min(self._config.max_backoff_seconds, backoff * 2.0)
        return attempts, backoff

    async def _open_fresh_stream(self) -> AsyncPostgresReplication:
        """Open a single native replication stream generation with reconnect=False."""
        await self._ensure_initialized()
        assert self._identity is not None
        attempts = 0
        backoff = self._config.initial_backoff_seconds
        while True:
            self._check_lifecycle()
            if self._needs_reconcile:
                await self._reconcile_uncertainty()
                continue
            try:
                stream = await self._driver._open_replication_stream(
                    self._config,
                    start_lsn=self._effective_start_lsn(),
                    reconnect=False,
                )
            except PostgresReplicationGapError:
                self._check_lifecycle()
                raise
            except Exception as exc:
                self._check_lifecycle()
                if self._needs_reconcile:
                    await self._reconcile_uncertainty()
                    continue
                gap_reason = self._classify_gap_reason(exc)
                if gap_reason is not None:
                    raise PostgresReplicationGapError(
                        str(exc),
                        reason=gap_reason,
                        identity_key=self._identity.identity_key,
                    ) from exc
                if not self._config.reconnect or not self._is_retryable_stream_error(exc):
                    raise
                attempts, backoff = await self._advance_reconnect_backoff(attempts, backoff, cause=exc)
                continue
            if self._closed or self._terminated or self._needs_reconcile:
                with contextlib.suppress(Exception):
                    await stream.aclose()
                self._check_lifecycle()
                await self._reconcile_uncertainty()
                continue
            self._stream = stream
            return stream

    def _apply_reconciled_checkpoint(self, loaded: PostgresCheckpoint | None) -> None:
        """Validate and monotonically apply a durable checkpoint snapshot."""
        if loaded is None:
            return
        assert self._identity is not None
        self._validate_identity_match(self._identity, loaded)
        if self._checkpoint is None or (loaded.version, loaded.commit_end_lsn) >= (
            self._checkpoint.version,
            self._checkpoint.commit_end_lsn,
        ):
            self._checkpoint = loaded
            self._sync_feed_cursor(loaded.next_event_id)
        else:
            self._sync_feed_cursor(self._checkpoint.next_event_id)
        self._prune_acked_lsns(self._checkpoint.commit_end_lsn)

    async def _reconcile_under_lock(
        self,
        *,
        deadline: float | None = None,
        clear_pending_and_stream: bool,
        timeout_message: str | None,
    ) -> None:
        """Load the latest durable checkpoint under ``_reconcile_lock`` within optional ``deadline``."""
        effective_deadline = (
            deadline
            if deadline is not None
            else asyncio.get_running_loop().time() + self._config.checkpoint_timeout_seconds
        )
        if effective_deadline - asyncio.get_running_loop().time() <= 0.0:
            raise PostgresReplicationGapError(
                timeout_message or "Checkpoint state remains uncertain during bounded aclose",
                reason="checkpoint_uncertain",
                identity_key=self._identity.identity_key if self._identity else None,
            )
        try:
            async with asyncio.timeout_at(effective_deadline):
                async with self._reconcile_lock:
                    if not self._needs_reconcile:
                        return
                    if clear_pending_and_stream:
                        await self._ensure_initialized()
                        self._pending_record = None
                        await self._close_active_stream(suppress_error=True)
                        self._lsn_to_event_id.clear()
                    if self._identity is None:
                        self._needs_reconcile = False
                        return
                    loaded = await self._store.load(self._identity.identity_key)
                    self._apply_reconciled_checkpoint(loaded)
                    self._needs_reconcile = False
        except TimeoutError as exc:
            if timeout_message is not None:
                raise PostgresReplicationGapError(
                    timeout_message,
                    reason="checkpoint_uncertain",
                    identity_key=self._identity.identity_key if self._identity else None,
                ) from exc
            raise

    async def _reconcile_uncertainty(self) -> None:
        """Reload durable checkpoint and reset pending state after uncertain commit."""
        await self._reconcile_under_lock(
            clear_pending_and_stream=True,
            timeout_message=None,
        )

    def _prune_acked_lsns(self, commit_end_lsn: int) -> None:
        """Drop LSN-to-event mappings at or below a durably acknowledged commit_end_lsn."""
        stale = [lsn for lsn in self._lsn_to_event_id if lsn <= commit_end_lsn]
        for lsn in stale:
            del self._lsn_to_event_id[lsn]

    async def _next_native_record(self) -> PostgresReplicationRecord:
        """Pull the next raw replication record, driving adapter-owned reconnects cleanly."""
        attempts = 0
        backoff = self._config.initial_backoff_seconds
        while True:
            self._check_lifecycle()
            if self._pending_record is not None:
                record = self._pending_record
                self._pending_record = None
                return record
            stream = self._stream
            if stream is None:
                stream = await self._open_fresh_stream()
            try:
                next_record = await stream.next()
            except PostgresReplicationGapError:
                await self._close_active_stream(suppress_error=True)
                self._check_lifecycle()
                raise
            except StopAsyncIteration:
                self._check_lifecycle()
                raise
            except Exception as exc:
                await self._close_active_stream(suppress_error=True)
                self._check_lifecycle()
                if self._needs_reconcile:
                    await self._reconcile_uncertainty()
                    continue
                gap_reason = self._classify_gap_reason(exc)
                if gap_reason is not None:
                    raise PostgresReplicationGapError(
                        str(exc),
                        reason=gap_reason,
                        identity_key=self._identity.identity_key if self._identity else None,
                    ) from exc
                if not self._config.reconnect or not self._is_retryable_stream_error(exc):
                    raise
                attempts, backoff = await self._advance_reconnect_backoff(attempts, backoff, cause=exc)
                continue
            if next_record is None:
                await self._close_active_stream(suppress_error=True)
                self._check_lifecycle()
                if self._needs_reconcile:
                    await self._reconcile_uncertainty()
                    continue
                if not self._config.reconnect:
                    raise StopAsyncIteration
                attempts, backoff = await self._advance_reconnect_backoff(attempts, backoff)
                continue
            self._check_lifecycle()
            return next_record

    def _publish_record(self, record: PostgresReplicationRecord) -> ChangeEvent:
        """Append a native record into ChangeFeed and track its LSN boundary."""
        payload = self._record_to_payload(record)
        _, event = self._feed.publish_event(record.kind, payload)
        if record.commit_end_lsn is not None and record.commit_end_lsn >= 0:
            self._lsn_to_event_id[record.commit_end_lsn] = self._feed._next_id
        return event

    def _build_next_checkpoint(self, commit_end_lsn: int) -> PostgresCheckpoint:
        """Construct the target monotonic checkpoint for ``commit_end_lsn``."""
        assert self._identity is not None
        next_event_id = self._lsn_to_event_id.get(commit_end_lsn, self._feed._next_id)
        if self._checkpoint is None:
            return PostgresCheckpoint(
                identity_key=self._identity.identity_key,
                commit_end_lsn=commit_end_lsn,
                next_event_id=max(1, next_event_id),
                version=1,
            )
        return self._checkpoint.advance(
            commit_end_lsn=commit_end_lsn,
            next_event_id=max(self._checkpoint.next_event_id, next_event_id),
        )

    async def _run_ack_transaction(self, commit_end_lsn: int) -> PostgresCheckpoint:
        """Execute CAS save before native wire acknowledge under single-flight gating."""
        await self._ensure_initialized()
        assert self._identity is not None
        try:
            target = self._build_next_checkpoint(commit_end_lsn)
            saved = await asyncio.wait_for(
                self._store.save(target, expected=self._checkpoint),
                timeout=self._config.checkpoint_timeout_seconds,
            )
            self._checkpoint = saved
            self._sync_feed_cursor(saved.next_event_id)
            self._prune_acked_lsns(saved.commit_end_lsn)
            stream = self._stream
            if stream is not None:
                stream.acknowledge(commit_end_lsn)
            self._needs_reconcile = False
            return saved
        except PostgresCheckpointConflictError:
            self._terminated = True
            self._stop_event.set()
            self._pending_record = None
            await self._close_active_stream(suppress_error=True)
            raise
        except SerializationFailure as exc:
            self._terminated = True
            self._stop_event.set()
            self._pending_record = None
            await self._close_active_stream(suppress_error=True)
            raise PostgresCheckpointConflictError(
                str(exc),
                identity_key=self._identity.identity_key if self._identity else None,
            ) from exc
        except Exception as exc:
            self._needs_reconcile = True
            self._pending_record = None
            await self._close_active_stream(suppress_error=True)
            raise PostgresReplicationGapError(
                f"Checkpoint acknowledgement failed for lsn {commit_end_lsn}: {exc}",
                reason="checkpoint_uncertain",
                identity_key=self._identity.identity_key if self._identity else None,
            ) from exc
        except BaseException:
            self._needs_reconcile = True
            self._pending_record = None
            raise
        finally:
            self._yield_gate.set()

    @staticmethod
    def _consume_task_exception(task: asyncio.Task[PostgresCheckpoint]) -> None:
        """Retrieve task exceptions when detached after cancellation to avoid GC noise."""
        if not task.cancelled():
            task.exception()

    @staticmethod
    def _validate_identity_match(
        expected: PostgresReplicationIdentity,
        stored: PostgresCheckpoint,
    ) -> None:
        """Verify stored checkpoint matches the live server/slot replication identity."""
        if stored.identity_key != expected.identity_key:
            raise PostgresReplicationGapError(
                "PostgreSQL replication identity mismatch against persisted checkpoint",
                reason="identity_mismatch",
                identity_key=expected.identity_key,
            )

    @classmethod
    def _classify_gap_reason(cls, exc: BaseException) -> str | None:
        """Extract an explicit fail-closed replication gap reason when present."""
        reason = getattr(exc, "reason", None)
        if isinstance(reason, str) and reason in cls._FAIL_CLOSED_REASONS:
            return reason
        code = getattr(exc, "code", None)
        if isinstance(code, str) and code in cls._FAIL_CLOSED_REASONS:
            return code
        text = str(exc).lower()
        if "wal_unavailable" in text or ("wal segment" in text and "removed" in text):
            return "wal_unavailable"
        if "identity_mismatch" in text or ("system_identifier" in text and "mismatch" in text):
            return "identity_mismatch"
        return None

    @classmethod
    def _is_retryable_stream_error(cls, exc: BaseException) -> bool:
        """Determine whether a stream failure is a retryable disconnect vs fail-closed error."""
        if cls._classify_gap_reason(exc) is not None:
            return False
        reason = getattr(exc, "reason", None)
        if isinstance(reason, str) and reason in cls._RETRYABLE_RECONNECT_REASONS:
            return True
        return isinstance(exc, (ConnectionError, TimeoutError, EOFError, OperationalError))

    @staticmethod
    def _record_to_payload(record: PostgresReplicationRecord) -> dict[str, Any]:
        """Convert a native logical replication record to a serializable ChangeFeed payload."""
        payload: dict[str, Any] = {
            "kind": record.kind,
            "commit_end_lsn": record.commit_end_lsn,
            "generation": record.generation,
            **record.payload,
        }
        encode_json_bytes(payload)
        return payload
