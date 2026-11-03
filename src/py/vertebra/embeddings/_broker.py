"""Single-instance EmbeddingBroker with token-budgeted length-sorted batching and 3.14t/3.15t subprocess quarantine."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
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
    coalesce_window_ms: float = 1.0
    bulk_chunk_size: int = 128
    isolate_gil_subprocess: bool = True
    outlier_length_ratio: float = 4.0


class BackendFactory(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Importable factory that builds an embedding inference callable.

    The quarantined sidecar receives this value over its JSON pipe and imports ``target`` itself,
    so the factory must be importable in a fresh interpreter.

    Attributes:
        target: ``"package.module:qualname"`` of a callable returning a ``BackendInferenceCallable``.
        options: Keyword arguments passed to the target callable.
    """

    target: str
    options: dict[str, Any] = msgspec.field(default_factory=dict)

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
        self._pending: list[_QueuedBrokerRequest] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._subprocess: asyncio.subprocess.Process | None = None
        self._subprocess_lock = asyncio.Lock()

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
        fut: asyncio.Future[VectorView] = loop.create_future()
        est_tokens = max(1, len(text) // 4)
        self._pending.append(
            _QueuedBrokerRequest(
                text=text,
                estimated_tokens=est_tokens,
                future=fut,
            )
        )
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = loop.create_task(self._flush_loop())
        return await fut

    async def submit_batch(self, texts: Sequence[str]) -> VectorBatchView:
        """Submit a bulk sequence of texts with chunked length-sorting and interactive query preemption."""
        text_list = list(texts)
        if not text_list:
            raise ValueError("texts must not be empty")
        if self.should_quarantine_subprocess():
            return await self.submit_batch_in_subprocess(text_list)

        chunk_size = max(1, self._config.bulk_chunk_size)
        all_rows: list[list[float]] = [[] for _ in range(len(text_list))]
        for offset in range(0, len(text_list), chunk_size):
            if self._pending:
                await self._drain_pending_once()
            await asyncio.sleep(0)
            if self._pending:
                await self._drain_pending_once()

            chunk_texts = text_list[offset : offset + chunk_size]
            chunk_rows = self._execute_length_sorted_rows(chunk_texts)
            for rel_idx, row in enumerate(chunk_rows):
                all_rows[offset + rel_idx] = row
            await asyncio.sleep(0.002)
            if self._pending:
                await self._drain_pending_once()

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
        async with self._subprocess_lock:
            proc = await self._ensure_subprocess()
            shm_name = f"/vtb-qvec-{os.getpid()}-{uuid.uuid4().hex[:10]}"
            req = _SubprocessRpcRequest(
                shm_name=shm_name,
                texts=text_list,
                dimensions=self._dimensions,
                factory=self._backend_factory,
            )
            assert proc.stdin is not None
            assert proc.stdout is not None
            payload_line = encode_json(req) + "\n"
            proc.stdin.write(payload_line.encode("utf-8"))
            await proc.stdin.drain()
            raw_line = await proc.stdout.readline()
            if not raw_line:
                raise RuntimeError("Quarantined embedding subprocess terminated unexpectedly")
            resp = decode_json(raw_line, type=_SubprocessRpcResponse)
            if resp.error:
                raise RuntimeError(f"Quarantined embedding subprocess error: {resp.error}")
            return VectorBatchView.open_shm(resp.shm_name)

    async def close(self) -> None:
        """Terminate the quarantined sidecar subprocess if active."""
        async with self._subprocess_lock:
            if self._subprocess is not None:
                if self._subprocess.stdin is not None:
                    self._subprocess.stdin.close()
                try:
                    await asyncio.wait_for(self._subprocess.wait(), timeout=3.0)
                except TimeoutError:
                    self._subprocess.kill()
                    await self._subprocess.wait()
                self._subprocess = None

    async def _flush_loop(self) -> None:
        """Wait for the coalesce window and drain pending singular requests."""
        if self._config.coalesce_window_ms > 0.0:
            await asyncio.sleep(self._config.coalesce_window_ms / 1000.0)
        while self._pending:
            await self._drain_pending_once()

    async def _drain_pending_once(self) -> None:
        """Drain one token-budgeted batch of singular requests and resolve their futures."""
        if not self._pending:
            return
        batch_items: list[_QueuedBrokerRequest] = []
        token_sum = 0
        while self._pending and len(batch_items) < self._config.max_batch_requests:
            nxt = self._pending[0]
            if batch_items and token_sum + nxt.estimated_tokens > self._config.max_batch_tokens:
                break
            token_sum += nxt.estimated_tokens
            batch_items.append(self._pending.pop(0))

        try:
            rows = self._execute_length_sorted_rows([item.text for item in batch_items])
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

    async def _ensure_subprocess(self) -> asyncio.subprocess.Process:
        """Spawn the sidecar subprocess worker if not already running."""
        if self._subprocess is not None and self._subprocess.returncode is None:
            return self._subprocess
        self._subprocess = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "from vertebra.embeddings._broker import _run_sidecar_worker_main; _run_sidecar_worker_main()",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        return self._subprocess

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
