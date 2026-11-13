"""Unit tests for EmbeddingBroker, GemmaEmbedder, FastEmbedder, StaticEmbedder, and 3.14t/3.15t quarantine."""

from __future__ import annotations

import asyncio
import math
import os
import sys
import threading
import types
from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import VectorBatchView
from vertebra.embeddings import (
    MODEL_CATALOG,
    BackendFactory,
    BackendInferenceCallable,
    EmbeddingBroker,
    EmbeddingBrokerConfig,
    FastEmbedder,
    GemmaEmbedder,
    ModelSpec,
    StaticEmbedder,
    get_model_spec,
    list_model_specs,
)
from vertebra.typing import ImproperConfigurationError, MissingDependencyError

_LENGTH_BACKEND_TARGET = "tests.unit.embeddings.test_embedding_broker:build_length_backend"
_FASTEMBED_BACKEND_TARGET = "vertebra.embeddings._local:FastEmbedder.build_backend"


def build_length_backend(scale: float = 1.0) -> BackendInferenceCallable:
    """Return a backend that encodes each text as ``[len(text) * scale, 1.0, 0.0, 0.0]``."""

    def _infer(texts: list[str], dims: int) -> list[list[float]]:
        return [[float(len(text)) * scale, 1.0, 0.0, 0.0][:dims] for text in texts]

    return _infer


def build_failing_backend() -> BackendInferenceCallable:
    """Return a backend that raises RuntimeError on inference."""

    def _infer(texts: list[str], dims: int) -> list[list[float]]:
        _ = (texts, dims)
        raise RuntimeError("injected sidecar failure")

    return _infer


def build_crash_backend() -> BackendInferenceCallable:
    """Return a backend that abruptly exits the sidecar process mid-RPC."""

    def _infer(texts: list[str], dims: int) -> list[list[float]]:
        _ = (texts, dims)
        os._exit(1)

    return _infer


def test_model_catalog_and_gemma2_fp16_guard() -> None:
    """Verify ModelSpec catalog and GemmaEmbedder fp16 activation-overflow guard."""
    all_specs = list_model_specs()
    assert len(all_specs) >= 8
    assert "gemini-embedding-001" in MODEL_CATALOG
    assert "embeddinggemma-2-270m" in MODEL_CATALOG
    assert "embeddinggemma-300m" in MODEL_CATALOG
    assert "BAAI/bge-small-en-v1.5" in MODEL_CATALOG
    assert "minishlab/potion-base-8M" in MODEL_CATALOG

    gemma2_spec = get_model_spec("embeddinggemma-2-270m")
    assert isinstance(gemma2_spec, ModelSpec)
    assert gemma2_spec.forbid_fp16 is True

    with pytest.raises(ValueError, match="float16"):
        GemmaEmbedder("embeddinggemma-2-270m", precision="fp16")

    with pytest.raises(ValueError, match="float16"):
        GemmaEmbedder("embeddinggemma-2-270m", precision="float16")

    for allowed in ("int8", "q4", "bf16", "fp32"):
        emb = GemmaEmbedder(
            "embeddinggemma-2-270m",
            precision=allowed,
            dimensions=8,
            backend_fn=lambda texts, dims: [[1.0] * dims for _ in texts],
        )
        assert emb.precision == allowed


@pytest.mark.anyio
async def test_embedding_broker_token_budgeted_and_length_sorted_dynamic_batching() -> None:
    """Verify EmbeddingBroker coalesces requests under max_batch_tokens, splits outliers, and preserves order."""
    forward_batches: list[list[str]] = []

    def fake_backend(texts: list[str], dims: int) -> list[list[float]]:
        forward_batches.append(list(texts))
        out: list[list[float]] = []
        for t in texts:
            tag = float(int(t.split(":", 1)[0]))
            out.append([tag, 1.0, 0.0, 0.0][:dims])
        return out

    broker = EmbeddingBroker(
        backend_fn=fake_backend,
        dimensions=4,
        config=EmbeddingBrokerConfig(
            max_batch_requests=64,
            max_batch_tokens=200,
            coalesce_window_ms=5.0,
            bulk_chunk_size=4,
            isolate_gil_subprocess=False,
        ),
    )

    short_inputs = [f"{i}:short" for i in range(1, 21)]
    views = await asyncio.gather(*(broker.submit_single(text) for text in short_inputs))
    assert len(forward_batches) == 1
    for idx, view in enumerate(views, start=1):
        expected_first = idx / math.sqrt(idx * idx + 1.0)
        assert view.to_list()[0] == pytest.approx(expected_first, rel=1e-5)

    forward_batches.clear()
    mixed_inputs = [
        "1:tiny",
        f"2:{'x' * 1200}",
        "3:small",
        "4:compact",
    ]
    mixed_views = await asyncio.gather(*(broker.submit_single(t) for t in mixed_inputs))
    assert len(forward_batches) >= 2
    for idx, view in enumerate(mixed_views, start=1):
        expected_first = idx / math.sqrt(idx * idx + 1.0)
        assert view.to_list()[0] == pytest.approx(expected_first, rel=1e-5)


@pytest.mark.anyio
async def test_embedding_broker_bulk_preemption_for_interactive_query() -> None:
    """Verify bulk submit_batch yields between chunks so an interactive submit_single completes first."""
    completion_order: list[str] = []

    def slow_backend(texts: list[str], dims: int) -> list[list[float]]:
        return [[float(i + 1), 1.0, 0.0, 0.0][:dims] for i in range(len(texts))]

    broker = EmbeddingBroker(
        backend_fn=slow_backend,
        dimensions=4,
        config=EmbeddingBrokerConfig(
            max_batch_requests=16,
            max_batch_tokens=4096,
            coalesce_window_ms=0.5,
            bulk_chunk_size=2,
            isolate_gil_subprocess=False,
        ),
    )

    async def run_bulk() -> VectorBatchView:
        res = await broker.submit_batch([f"doc-{i}" for i in range(8)])
        completion_order.append("bulk")
        return res

    async def run_interactive() -> None:
        await asyncio.sleep(0.001)
        v = await broker.submit_single("interactive-query")
        assert v.dimensions == 4
        completion_order.append("interactive")

    bulk_batch, _ = await asyncio.gather(run_bulk(), run_interactive())
    assert bulk_batch.count == 8
    assert completion_order[0] == "interactive"


@pytest.mark.anyio
async def test_gemma_and_static_embedders_end_to_end() -> None:
    """Verify GemmaEmbedder formats task-steered prompts and StaticEmbedder executes in pure Rust."""
    seen_prompts: list[str] = []

    def spy_backend(texts: list[str], dims: int) -> list[list[float]]:
        seen_prompts.extend(texts)
        return [[float(i + 1), 2.0, 3.0, 4.0][:dims] for i in range(len(texts))]

    gemma = GemmaEmbedder(
        "embeddinggemma-2-270m",
        dimensions=4,
        precision="int8",
        backend_fn=spy_backend,
    )
    q_vec = await gemma.embed_query("how does futex work")
    assert q_vec.dimensions == 4
    assert any("task: search result | query: how does futex work" in p for p in seen_prompts)

    d_vec = await gemma.embed_document("Futex uses atomic words in shared memory.", title="Futex Guide")
    assert d_vec.dimensions == 4
    assert any("title: Futex Guide | text: Futex uses atomic words in shared memory." in p for p in seen_prompts)

    static_emb = StaticEmbedder(
        vocab={"futex": 0, "shm": 1, "vector": 2, "rust": 3},
        weights=[
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
        ],
        dimensions=4,
    )
    s_vec = await static_emb.embed_query("futex shm")
    assert s_vec.dimensions == 4
    inv_sqrt2 = 1.0 / math.sqrt(2.0)
    assert s_vec.to_list() == pytest.approx([inv_sqrt2, inv_sqrt2, 0.0, 0.0], rel=1e-5)

    s_batch = await static_emb.embed_many(["futex", "vector rust"], titles=["T1", None])
    assert s_batch.count == 2
    assert s_batch.dimensions == 4


@pytest.mark.anyio
async def test_subprocess_quarantine_shm_slab_exchange_and_fastembed_optional_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify EmbeddingBroker subprocess quarantine exchanges VectorSlab in /dev/shm and FastEmbedder guards import."""
    broker = EmbeddingBroker(
        dimensions=4,
        config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
        backend_factory=BackendFactory(target=_LENGTH_BACKEND_TARGET),
    )
    try:
        sub_batch = await broker.submit_batch_in_subprocess(["alpha", "beta", "gamma"])
        assert isinstance(sub_batch, VectorBatchView)
        assert sub_batch.count == 3
        assert sub_batch.dimensions == 4
        for idx in range(3):
            norm = math.sqrt(sum(v * v for v in sub_batch[idx].to_list()))
            assert norm == pytest.approx(1.0, rel=1e-5)
    finally:
        await broker.close()

    monkeypatch.setitem(sys.modules, "fastembed", None)
    with pytest.raises(MissingDependencyError, match="fastembed") as exc_info:
        FastEmbedder("BAAI/bge-small-en-v1.5", dimensions=4)
    assert "embeddings" in exc_info.value.install_package

    monkeypatch.setattr("vertebra.embeddings._broker.is_free_threaded", lambda: True)

    seen_ft_prompts: list[str] = []
    ft_custom_gemma = GemmaEmbedder(
        "embeddinggemma-2-270m",
        dimensions=4,
        precision="int8",
        backend_fn=lambda texts, dims: seen_ft_prompts.extend(texts) or [[1.0] * dims for _ in texts],
    )
    assert ft_custom_gemma.broker.should_quarantine_subprocess() is False
    ft_q = await ft_custom_gemma.embed_query("free threaded custom backend")
    assert ft_q.dimensions == 4
    assert any("free threaded custom backend" in p for p in seen_ft_prompts)

    ft_quarantined_gemma = GemmaEmbedder(
        "embeddinggemma-2-270m", dimensions=4, precision="int8", source="/models/gemma"
    )
    assert ft_quarantined_gemma.broker.should_quarantine_subprocess() is True
    assert ft_quarantined_gemma.broker.backend_factory == BackendFactory(
        target=_FASTEMBED_BACKEND_TARGET,
        options={"model_name": "embeddinggemma-2-270m", "specific_model_path": "/models/gemma", "precision": "int8"},
    )


@pytest.mark.anyio
async def test_sidecar_runs_configured_backend_factory() -> None:
    """The quarantined sidecar builds and runs the configured backend factory."""
    broker = EmbeddingBroker(
        dimensions=4,
        config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
        backend_factory=BackendFactory(target=_LENGTH_BACKEND_TARGET, options={"scale": 2.0}),
    )
    try:
        batch = await broker.submit_batch_in_subprocess(["ab", "abcd"])
        assert batch.to_list() == [
            pytest.approx([0.9701425, 0.2425356, 0.0, 0.0], rel=1e-5),
            pytest.approx([0.9922779, 0.1240347, 0.0, 0.0], rel=1e-5),
        ]
    finally:
        await broker.close()


def test_embedding_broker_requires_backend() -> None:
    """A broker without a backend callable or factory is a configuration error."""
    with pytest.raises(ImproperConfigurationError, match="backend_fn or backend_factory"):
        EmbeddingBroker(dimensions=4)


@pytest.mark.anyio
async def test_fast_embedder_runs_fastembed_backend_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """FastEmbedder runs fastembed.TextEmbedding in-process with the configured options."""
    init_calls: list[dict[str, Any]] = []

    class FakeTextEmbedding:
        def __init__(self, **kwargs: Any) -> None:
            init_calls.append(kwargs)

        def embed(self, texts: list[str]) -> list[list[float]]:
            return [[3.0, 4.0] if text == "a" else [0.0, 2.0] for text in texts]

    fake_module = types.ModuleType("fastembed")
    monkeypatch.setattr(fake_module, "TextEmbedding", FakeTextEmbedding, raising=False)
    monkeypatch.setitem(sys.modules, "fastembed", fake_module)

    embedder = FastEmbedder(
        "BAAI/bge-small-en-v1.5",
        dimensions=2,
        threads=2,
        broker_config=EmbeddingBrokerConfig(isolate_gil_subprocess=False, coalesce_window_ms=0.0),
    )
    assert init_calls == []
    batch = await embedder.embed_many(["a", "bb"])
    assert batch.to_list() == [pytest.approx([0.6, 0.8]), pytest.approx([0.0, 1.0])]
    assert init_calls == [{"model_name": "BAAI/bge-small-en-v1.5", "threads": 2}]


def test_local_embedders_configure_fastembed_backend_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    """FastEmbedder hands its broker the fastembed factory, which the free-threaded sidecar runs."""
    monkeypatch.setitem(sys.modules, "fastembed", types.ModuleType("fastembed"))
    monkeypatch.setattr("vertebra.embeddings._broker.is_free_threaded", lambda: True)
    embedder = FastEmbedder("BAAI/bge-small-en-v1.5", dimensions=4, cache_dir="/tmp/fe-cache")
    assert embedder.broker.should_quarantine_subprocess() is True
    assert embedder.broker.backend_factory == BackendFactory(
        target=_FASTEMBED_BACKEND_TARGET,
        options={
            "model_name": "BAAI/bge-small-en-v1.5",
            "cache_dir": "/tmp/fe-cache",
            "specific_model_path": None,
            "threads": None,
        },
    )


@pytest.mark.anyio
async def test_embedding_broker_max_pending_requests_and_thread_offload() -> None:
    """EmbeddingBroker enforces max_pending_requests and offloads local inference via asyncio.to_thread."""
    loop_tid = threading.get_ident()
    inference_tids: list[int] = []

    def thread_recording_backend(texts: list[str], dims: int) -> list[list[float]]:
        inference_tids.append(threading.get_ident())
        return [[1.0, 0.0, 0.0, 0.0][:dims] for _ in texts]

    broker = EmbeddingBroker(
        backend_fn=thread_recording_backend,
        dimensions=4,
        config=EmbeddingBrokerConfig(
            max_batch_requests=8,
            max_batch_tokens=1024,
            max_pending_requests=2,
            coalesce_window_ms=40.0,
            bulk_chunk_size=2,
            isolate_gil_subprocess=False,
        ),
    )

    task1 = asyncio.create_task(broker.submit_single("req-1"))
    task2 = asyncio.create_task(broker.submit_single("req-2"))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="max_pending_requests"):
        await broker.submit_single("req-3-overflow")

    v1, v2 = await asyncio.gather(task1, task2)
    assert v1.dimensions == 4
    assert v2.dimensions == 4
    assert inference_tids
    assert all(tid != loop_tid for tid in inference_tids)

    inference_tids.clear()
    batch = await broker.submit_batch(["b1", "b2", "b3"])
    assert batch.count == 3
    assert inference_tids
    assert all(tid != loop_tid for tid in inference_tids)


def test_embedding_broker_multi_loop_thread_isolation() -> None:
    """EmbeddingBroker constructed outside a loop isolates pending queues and locks across threaded loops."""
    broker = EmbeddingBroker(
        backend_fn=lambda texts, dims: [[float(len(t)), 1.0, 0.0, 0.0][:dims] for t in texts],
        dimensions=4,
        config=EmbeddingBrokerConfig(coalesce_window_ms=1.0, bulk_chunk_size=2, isolate_gil_subprocess=False),
    )
    results: dict[int, list[float]] = {}
    errors: list[BaseException] = []
    start_barrier = threading.Barrier(2)

    def worker(idx: int) -> None:
        async def _run() -> None:
            start_barrier.wait(timeout=5.0)
            single = await broker.submit_single(f"worker-{idx}")
            batch = await broker.submit_batch([f"a-{idx}", f"ab-{idx}"])
            results[idx] = [single.to_list()[0], float(batch.count)]
            await broker.close()

        try:
            asyncio.run(_run())
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors
    assert results[0][1] == 2.0
    assert results[1][1] == 2.0

    asyncio.run(broker.submit_single("prune-check"))
    assert len(broker._loop_states) == 1


@pytest.mark.anyio
async def test_embedding_broker_sidecar_shm_cleanup_and_crash_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sidecar execution unlinks /dev/shm/vtb-qvec-* segments on success, errors, and EOF crash recovery."""
    shm_prefix = f"vtb-qvec-{os.getpid()}-"

    def _current_pid_shm_entries() -> list[str]:
        shm_dir = Path("/dev/shm")
        if not shm_dir.is_dir():
            return []
        return [p.name for p in shm_dir.iterdir() if p.name.startswith(shm_prefix)]

    healthy = EmbeddingBroker(
        dimensions=4,
        config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
        backend_factory=BackendFactory(target=_LENGTH_BACKEND_TARGET),
    )
    try:
        batch = await healthy.submit_batch_in_subprocess(["one", "two"])
        assert batch.count == 2
        assert _current_pid_shm_entries() == []

        state = healthy._state_for_loop()
        assert state.subprocess is not None
        first_pid = state.subprocess.pid
        state.subprocess.kill()
        await state.subprocess.wait()
        recovered = await healthy.submit_batch_in_subprocess(["recovered-item"])
        assert recovered.count == 1
        assert state.subprocess is not None and state.subprocess.pid != first_pid
        assert _current_pid_shm_entries() == []
    finally:
        await healthy.close()

    crashing = EmbeddingBroker(
        dimensions=4,
        config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
        backend_factory=BackendFactory(target="tests.unit.embeddings.test_embedding_broker:build_crash_backend"),
    )
    try:
        with pytest.raises(RuntimeError, match="terminated unexpectedly"):
            await crashing.submit_batch_in_subprocess(["abrupt-eof"])
        assert crashing._state_for_loop().subprocess is None
        assert _current_pid_shm_entries() == []
    finally:
        await crashing.close()

    failing = EmbeddingBroker(
        dimensions=4,
        config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
        backend_factory=BackendFactory(target="tests.unit.embeddings.test_embedding_broker:build_failing_backend"),
    )
    try:
        created_segments: list[str] = []

        class _ResidualShmStdin:
            def __init__(self, target_name_holder: list[str]) -> None:
                self._holder = target_name_holder

            def write(self, data: bytes) -> None:
                text = data.decode("utf-8")
                marker = '"shm_name":"'
                idx = text.find(marker)
                if idx != -1:
                    rest = text[idx + len(marker) :]
                    seg = rest.split('"', 1)[0]
                    self._holder.append(seg)
                    Path(f"/dev/shm/{seg.lstrip('/')}").write_bytes(b"stale-shm")

            async def drain(self) -> None:
                return None

            def close(self) -> None:
                return None

        class _EofStdout:
            async def readline(self) -> bytes:
                return b""

        class _CrashProc:
            returncode = None
            stdin = _ResidualShmStdin(created_segments)
            stdout = _EofStdout()

            def kill(self) -> None:
                self.returncode = -9

            async def wait(self) -> int:
                return -9

        async def _fake_ensure(state: Any) -> Any:
            state.subprocess = _CrashProc()
            return state.subprocess

        monkeypatch.setattr(failing, "_ensure_subprocess", _fake_ensure)
        with pytest.raises(RuntimeError, match="terminated unexpectedly"):
            await failing.submit_batch_in_subprocess(["boom"])
        assert created_segments
        assert not Path(f"/dev/shm/{created_segments[0].lstrip('/')}").exists()
    finally:
        monkeypatch.undo()
        await failing.close()

    real_error_broker = EmbeddingBroker(
        dimensions=4,
        config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
        backend_factory=BackendFactory(target="tests.unit.embeddings.test_embedding_broker:build_failing_backend"),
    )
    try:
        with pytest.raises(RuntimeError, match="injected sidecar failure"):
            await real_error_broker.submit_batch_in_subprocess(["fail-rpc"])
        assert _current_pid_shm_entries() == []
    finally:
        await real_error_broker.close()


@pytest.mark.parametrize(
    ("precision", "expected_quantize"),
    [
        ("int8", True),
        ("q4", True),
        ("bf16", False),
        ("fp32", False),
    ],
)
def test_gemma_embedder_precision_validation_and_fastembed_forwarding(
    monkeypatch: pytest.MonkeyPatch,
    precision: str,
    expected_quantize: bool,
) -> None:
    """GemmaEmbedder validates precision, records it in BackendFactory.options, and forwards quantize."""
    recorded_kwargs: list[dict[str, Any]] = []

    class SpyTextEmbedding:
        def __init__(self, model_name: str, *, quantize: bool = True, **kwargs: Any) -> None:
            recorded_kwargs.append({"model_name": model_name, "quantize": quantize, **kwargs})

        def embed(self, texts: list[str]) -> list[list[float]]:
            return [[1.0, 0.0] for _ in texts]

    fake_module = types.ModuleType("fastembed")
    monkeypatch.setattr(fake_module, "TextEmbedding", SpyTextEmbedding, raising=False)
    monkeypatch.setitem(sys.modules, "fastembed", fake_module)

    embedder = GemmaEmbedder(
        "embeddinggemma-2-270m",
        dimensions=2,
        precision=precision,
        source="/models/gemma",
        backend_fn=lambda texts, dims: [[1.0] * dims for _ in texts],
    )
    assert embedder.broker.backend_factory is not None
    assert embedder.broker.backend_factory.options["precision"] == precision

    backend = embedder.broker.backend_factory.build()
    assert backend(["hello"], 2) == [[1.0, 0.0]]
    assert recorded_kwargs == [
        {
            "model_name": "embeddinggemma-2-270m",
            "specific_model_path": "/models/gemma",
            "quantize": expected_quantize,
        }
    ]

    with pytest.raises(ValueError, match="Unsupported precision"):
        GemmaEmbedder("embeddinggemma-2-270m", precision="int4_unknown")

    strict_kwargs: list[dict[str, Any]] = []

    class StrictTextEmbedding:
        def __init__(self, model_name: str, specific_model_path: str | None = None) -> None:
            strict_kwargs.append({"model_name": model_name, "specific_model_path": specific_model_path})

        def embed(self, texts: list[str]) -> list[list[float]]:
            return [[0.5, 0.5] for _ in texts]

    monkeypatch.setattr(fake_module, "TextEmbedding", StrictTextEmbedding, raising=False)
    fallback_backend = FastEmbedder.build_backend("embeddinggemma-2-270m", precision="fp32")
    assert fallback_backend(["hi"], 2) == [[0.5, 0.5]]
    assert strict_kwargs == [{"model_name": "embeddinggemma-2-270m", "specific_model_path": None}]
