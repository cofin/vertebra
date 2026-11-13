"""Single-instance EmbeddingBroker with token-budgeted length-sorted batching and 3.14t/3.15t subprocess quarantine."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import threading
import uuid
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any, cast

import msgspec

from vertebra._vertebra import VectorBatchView, VectorView
from vertebra.server._gil_audit import is_free_threaded
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "BackendFactory",
    "BackendInferenceCallable",
    "EmbeddingBroker",
    "EmbeddingBrokerConfig",
)

BackendInferenceCallable = Callable[[list[str], int], list[list[float]]]


class EmbeddingBrokerConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for the `EmbeddingBroker` micro-batcher and subprocess quarantine."""

    max_batch_requests: int = 64
    max_batch_tokens: int = 16_384
    max_pending_requests: int = 1024
    coalesce_window_ms: float = 1.0
    bulk_chunk_size: int = 128
    isolate_gil_subprocess: bool = True
    outlier_length_ratio: float = 4.0


class BackendFactory(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Importable factory that builds an embedding inference callable.

    The quarantined sidecar receives this value over its JSON pipe and imports ``target`` itself,
    so the factory must be importable in a fresh interpreter.
    """

    target: str
    """``"package.module:qualname"`` of a callable returning a ``BackendInferenceCallable``."""
    options: dict[str, Any] = msgspec.field(default_factory=dict)
    """Keyword arguments passed to the target callable."""

    def build(self) -> BackendInferenceCallable:
        """Import ``target`` and call it with ``options``.

        Raises:
            ImproperConfigurationError: When ``target`` is not ``module:qualname``.
            MissingDependencyError: When the target module cannot be imported.
        """
        module_name, _, qualname = self.target.partition(":")
        if not module_name or not qualname:
            msg = f"BackendFactory target must be 'module:qualname', got {self.target!r}"
            raise ImproperConfigurationError(msg)
        resolved: Any = ensure_module(module_name)
        for attr in qualname.split("."):
            resolved = getattr(resolved, attr)
        return cast("BackendInferenceCallable", resolved(**self.options))


class _QueuedBrokerRequest(msgspec.Struct, gc=False):
    """Pending singular embedding request queued in `EmbeddingBroker`."""

    text: str
    estimated_tokens: int
    future: asyncio.Future[VectorView]


class _SubprocessRpcRequest(msgspec.Struct, frozen=True, gc=False):
    """Line-delimited JSON request sent to the quarantined sidecar worker."""

    shm_name: str
    texts: list[str]
    dimensions: int
    factory: BackendFactory


class _SubprocessRpcResponse(msgspec.Struct, frozen=True, gc=False):
    """Line-delimited JSON response returned by the quarantined sidecar worker."""

    shm_name: str
    count: int
    dimensions: int
    error: str | None = None


class _BrokerLoopState(msgspec.Struct, gc=False):
    """Per-event-loop queue, flush task, lock, and sidecar process state for `EmbeddingBroker`."""

    pending: deque[_QueuedBrokerRequest]
    subprocess_lock: asyncio.Lock
    flush_task: asyncio.Task[None] | None = None
    subprocess: asyncio.subprocess.Process | None = None


def _unlink_shm_segment(shm_name: str) -> None:
    """Best-effort unlink of a POSIX `/dev/shm` named embedding slab segment."""
    clean_name = shm_name.lstrip("/")
    if not clean_name:
        return
    shm_path = f"/dev/shm/{clean_name}"
    with contextlib.suppress(FileNotFoundError, OSError):
        os.unlink(shm_path)


class EmbeddingBroker:
    """Single-instance embedding broker coordinating token-budgeted, length-sorted micro-batches."""

    def __init__(
        self,
        backend_fn: BackendInferenceCallable | None = None,
        *,
        dimensions: int = 768,
        config: EmbeddingBrokerConfig | None = None,
        backend_factory: BackendFactory | None = None,
    ) -> None:
        """Initialize the embedding broker with a backend callable or factory, dimensions, and batching config.

        Args:
            backend_fn: In-process inference callable ``(texts, dimensions) -> rows``.
            dimensions: Output embedding dimensionality.
            config: Micro-batching and subprocess quarantine settings.
            backend_factory: Importable factory used to build the backend in-process or in the sidecar.

        Raises:
            ImproperConfigurationError: When neither ``backend_fn`` nor ``backend_factory`` is provided.
        """
        if backend_fn is None and backend_factory is None:
            msg = "EmbeddingBroker requires backend_fn or backend_factory"
            raise ImproperConfigurationError(msg)
        self._has_custom_backend = backend_fn is not None
        self._backend_fn: BackendInferenceCallable | None = backend_fn
        self._backend_factory = backend_factory
        self._dimensions = dimensions
        self._config = config or EmbeddingBrokerConfig()
        self._loop_states: dict[asyncio.AbstractEventLoop, _BrokerLoopState] = {}
        self._loop_states_lock = threading.Lock()

    @staticmethod
    def _terminate_process_nowait(proc: asyncio.subprocess.Process | None) -> None:
        """Close stdin and send SIGTERM to a sidecar subprocess whose owning loop has already closed."""
        if proc is None:
            return
        if proc.stdin is not None:
            with contextlib.suppress(Exception):
                proc.stdin.close()
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError, OSError):
                proc.terminate()

    def _state_for_loop(self, loop: asyncio.AbstractEventLoop | None = None) -> _BrokerLoopState:
        """Return or create the per-event-loop state bucket while pruning closed loops."""
        target_loop = loop or asyncio.get_running_loop()
        with self._loop_states_lock:
            closed_loops = [
                existing for existing in self._loop_states if existing.is_closed() and existing is not target_loop
            ]
            for closed_loop in closed_loops:
                closed_state = self._loop_states.pop(closed_loop)
                self._terminate_process_nowait(closed_state.subprocess)
            state = self._loop_states.get(target_loop)
            if state is None:
                state = _BrokerLoopState(pending=deque(), subprocess_lock=asyncio.Lock())
                self._loop_states[target_loop] = state
            return state

    async def _reset_subprocess(self, state: _BrokerLoopState) -> None:
        """Terminate and clear a broken or exited sidecar subprocess for the active loop."""
        proc = state.subprocess
        state.subprocess = None
        if proc is None:
            return
        if proc.stdin is not None:
            with contextlib.suppress(Exception):
                proc.stdin.close()
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError, OSError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()

    @property
    def dimensions(self) -> int:
        """Return the broker's output vector dimensionality."""
        return self._dimensions

    @property
    def config(self) -> EmbeddingBrokerConfig:
        """Return the broker's `EmbeddingBrokerConfig`."""
        return self._config

    @property
    def backend_factory(self) -> BackendFactory | None:
        """Return the factory used for in-process and sidecar inference."""
        return self._backend_factory

    def should_quarantine_subprocess(self) -> bool:
        """Return True when running on free-threaded 3.14t/3.15t with subprocess GIL quarantine enabled."""
        return self._config.isolate_gil_subprocess and is_free_threaded() and not self._has_custom_backend

    async def submit_single(self, text: str) -> VectorView:
        """Submit a singular query/document for token-budgeted, length-sorted micro-batching."""
        if self.should_quarantine_subprocess():
            batch = await self.submit_batch_in_subprocess([text])
            return batch[0]

        loop = asyncio.get_running_loop()
        state = self._state_for_loop(loop)
        if len(state.pending) >= self._config.max_pending_requests:
            msg = f"EmbeddingBroker pending queue exceeded max_pending_requests ({self._config.max_pending_requests})"
            raise RuntimeError(msg)

        fut: asyncio.Future[VectorView] = loop.create_future()
        est_tokens = max(1, len(text) // 4)
        state.pending.append(
            _QueuedBrokerRequest(
                text=text,
                estimated_tokens=est_tokens,
                future=fut,
            )
        )
        if state.flush_task is None or state.flush_task.done():
            state.flush_task = loop.create_task(self._flush_loop(loop, state))
        return await fut

    async def submit_batch(self, texts: Sequence[str]) -> VectorBatchView:
        """Submit a bulk sequence of texts with chunked length-sorting and interactive query preemption."""
        text_list = list(texts)
        if not text_list:
            raise ValueError("texts must not be empty")
        if self.should_quarantine_subprocess():
            return await self.submit_batch_in_subprocess(text_list)

        state = self._state_for_loop()
        chunk_size = max(1, self._config.bulk_chunk_size)
        all_rows: list[list[float]] = [[] for _ in range(len(text_list))]
        for offset in range(0, len(text_list), chunk_size):
            if state.pending:
                await self._drain_pending_once(state)
            await asyncio.sleep(0)
            if state.pending:
                await self._drain_pending_once(state)

            chunk_texts = text_list[offset : offset + chunk_size]
            chunk_rows = await asyncio.to_thread(self._execute_length_sorted_rows, chunk_texts)
            for rel_idx, row in enumerate(chunk_rows):
                all_rows[offset + rel_idx] = row
            await asyncio.sleep(0.002)
            if state.pending:
                await self._drain_pending_once(state)

        return VectorBatchView.from_f32_rows(all_rows, normalize=True)

    async def submit_batch_in_subprocess(self, texts: Sequence[str]) -> VectorBatchView:
        """Execute batch inference in a quarantined subprocess and return a zero-copy SHM `VectorBatchView`.

        Raises:
            ValueError: When ``texts`` is empty.
            ImproperConfigurationError: When no ``backend_factory`` is configured.
            RuntimeError: When the sidecar terminates unexpectedly or reports an error.
        """
        text_list = list(texts)
        if not text_list:
            raise ValueError("texts must not be empty")
        if self._backend_factory is None:
            msg = "Subprocess inference requires a backend_factory"
            raise ImproperConfigurationError(msg)
        loop = asyncio.get_running_loop()
        state = self._state_for_loop(loop)
        async with state.subprocess_lock:
            proc = await self._ensure_subprocess(state)
            shm_name = f"/vtb-qvec-{os.getpid()}-{uuid.uuid4().hex[:10]}"
            shm_names_to_cleanup = {shm_name}
            req = _SubprocessRpcRequest(
                shm_name=shm_name,
                texts=text_list,
                dimensions=self._dimensions,
                factory=self._backend_factory,
            )
            try:
                if proc.stdin is None or proc.stdout is None:
                    await self._reset_subprocess(state)
                    raise RuntimeError("Quarantined embedding subprocess stdio pipes are unavailable")
                payload_line = encode_json(req) + "\n"
                try:
                    proc.stdin.write(payload_line.encode("utf-8"))
                    await proc.stdin.drain()
                    raw_line = await proc.stdout.readline()
                except Exception:
                    await self._reset_subprocess(state)
                    raise
                if not raw_line:
                    await self._reset_subprocess(state)
                    raise RuntimeError("Quarantined embedding subprocess terminated unexpectedly")
                try:
                    resp = decode_json(raw_line, type=_SubprocessRpcResponse)
                except Exception:
                    await self._reset_subprocess(state)
                    raise
                if resp.shm_name:
                    shm_names_to_cleanup.add(resp.shm_name)
                if resp.error:
                    raise RuntimeError(f"Quarantined embedding subprocess error: {resp.error}")
                return VectorBatchView.open_shm(resp.shm_name)
            finally:
                for segment_name in shm_names_to_cleanup:
                    _unlink_shm_segment(segment_name)

    async def close(self) -> None:
        """Terminate the active event loop's quarantined sidecar subprocess and prune closed loops."""
        loop = asyncio.get_running_loop()
        with self._loop_states_lock:
            closed_loops = [existing for existing in self._loop_states if existing.is_closed() and existing is not loop]
            for closed_loop in closed_loops:
                closed_state = self._loop_states.pop(closed_loop)
                self._terminate_process_nowait(closed_state.subprocess)
            state = self._loop_states.get(loop)
        if state is None:
            return
        async with state.subprocess_lock:
            proc = state.subprocess
            if proc is not None:
                if proc.stdin is not None:
                    with contextlib.suppress(Exception):
                        proc.stdin.close()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError, OSError):
                        proc.kill()
                    with contextlib.suppress(Exception):
                        await proc.wait()
                state.subprocess = None

    async def _flush_loop(self, loop: asyncio.AbstractEventLoop, state: _BrokerLoopState) -> None:
        """Wait for the coalesce window and drain pending singular requests on the owning loop."""
        try:
            if self._config.coalesce_window_ms > 0.0:
                await asyncio.sleep(self._config.coalesce_window_ms / 1000.0)
            while state.pending:
                await self._drain_pending_once(state)
        except asyncio.CancelledError:
            while state.pending:
                item = state.pending.popleft()
                if not item.future.done():
                    item.future.cancel()
            raise
        finally:
            if state.pending and not loop.is_closed():
                state.flush_task = loop.create_task(self._flush_loop(loop, state))
            elif state.flush_task is asyncio.current_task():
                state.flush_task = None

    async def _drain_pending_once(self, state: _BrokerLoopState) -> None:
        """Drain one token-budgeted batch of singular requests and resolve their futures."""
        if not state.pending:
            return
        batch_items: list[_QueuedBrokerRequest] = []
        token_sum = 0
        while state.pending and len(batch_items) < self._config.max_batch_requests:
            nxt = state.pending[0]
            if batch_items and token_sum + nxt.estimated_tokens > self._config.max_batch_tokens:
                break
            token_sum += nxt.estimated_tokens
            batch_items.append(state.pending.popleft())

        try:
            rows = await asyncio.to_thread(
                self._execute_length_sorted_rows,
                [item.text for item in batch_items],
            )
            batch_view = VectorBatchView.from_f32_rows(rows, normalize=True)
            for idx, item in enumerate(batch_items):
                if not item.future.done():
                    item.future.set_result(batch_view[idx])
        except Exception as exc:
            for item in batch_items:
                if not item.future.done():
                    item.future.set_exception(exc)

    def _execute_length_sorted_rows(self, texts: list[str]) -> list[list[float]]:
        """Sort texts by token count, split length outliers into sub-batches, and scatter back to caller order."""
        n = len(texts)
        if n == 1:
            return self._resolve_backend()(texts, self._dimensions)

        token_lengths = [max(1, len(t) // 4) for t in texts]
        sorted_indices = sorted(range(n), key=lambda i: token_lengths[i])
        scattered_rows: list[list[float]] = [[] for _ in range(n)]

        sub_indices: list[int] = []
        sub_token_sum = 0
        sub_min_len = 0

        for orig_idx in sorted_indices:
            t_len = token_lengths[orig_idx]
            exceeds_budget = bool(sub_indices and (sub_token_sum + t_len > self._config.max_batch_tokens))
            exceeds_ratio = bool(
                sub_indices and sub_min_len > 0 and (t_len / sub_min_len) > self._config.outlier_length_ratio
            )
            if exceeds_budget or exceeds_ratio:
                self._run_sub_batch(texts, sub_indices, scattered_rows)
                sub_indices = [orig_idx]
                sub_token_sum = t_len
                sub_min_len = t_len
            else:
                if not sub_indices:
                    sub_min_len = t_len
                sub_indices.append(orig_idx)
                sub_token_sum += t_len

        if sub_indices:
            self._run_sub_batch(texts, sub_indices, scattered_rows)

        return scattered_rows

    def _run_sub_batch(
        self,
        all_texts: list[str],
        sub_indices: list[int],
        scattered_rows: list[list[float]],
    ) -> None:
        """Execute backend inference on a length-homogeneous sub-batch and scatter outputs."""
        sub_texts = [all_texts[i] for i in sub_indices]
        sub_outputs = self._resolve_backend()(sub_texts, self._dimensions)
        for out_pos, orig_idx in enumerate(sub_indices):
            scattered_rows[orig_idx] = sub_outputs[out_pos]

    async def _ensure_subprocess(self, state: _BrokerLoopState) -> asyncio.subprocess.Process:
        """Spawn the sidecar subprocess worker for ``state`` if not already running."""
        if state.subprocess is not None and state.subprocess.returncode is None:
            return state.subprocess
        state.subprocess = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "from vertebra.embeddings._broker import _run_sidecar_worker_main; _run_sidecar_worker_main()",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        return state.subprocess

    def _resolve_backend(self) -> BackendInferenceCallable:
        """Return the in-process backend, building it from ``backend_factory`` on first use."""
        if self._backend_fn is None:
            self._backend_fn = cast("BackendFactory", self._backend_factory).build()
        return self._backend_fn


def _run_sidecar_worker_main() -> None:
    """Entrypoint for the quarantined subprocess worker reading JSON requests and writing SHM VectorSlabs."""
    backends: dict[tuple[str, str], BackendInferenceCallable] = {}
    for raw_line in sys.stdin:
        stripped = raw_line.strip()
        if not stripped:
            continue
        try:
            req = decode_json(stripped.encode("utf-8"), type=_SubprocessRpcRequest)
            cache_key = (req.factory.target, encode_json(req.factory.options))
            backend = backends.get(cache_key)
            if backend is None:
                backend = req.factory.build()
                backends[cache_key] = backend
            rows = backend(req.texts, req.dimensions)
            batch = VectorBatchView.create_shm(req.shm_name, rows, normalize=True)
            resp = _SubprocessRpcResponse(
                shm_name=batch.shm_name or req.shm_name,
                count=batch.count,
                dimensions=batch.dimensions,
                error=None,
            )
        except Exception as exc:
            resp = _SubprocessRpcResponse(
                shm_name="",
                count=0,
                dimensions=0,
                error=str(exc),
            )
        sys.stdout.write(encode_json(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    _run_sidecar_worker_main()
