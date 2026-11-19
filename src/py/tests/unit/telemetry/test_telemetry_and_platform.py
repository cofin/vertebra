"""Unit tests for Vertebra non-blocking logger, zero-RTT DB trace propagation, and platform z-pages."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import importlib
import logging
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import msgspec
import pytest

import vertebra
import vertebra.telemetry as telemetry_pkg
from vertebra.server._loops import EventLoopPool, LoopBackend
from vertebra.server.platform import (
    CgroupQuota,
    HealthProbeServer,
    LameDuckController,
    SHMAllocationPolicy,
    ZPagesRegistry,
)
from vertebra.server.worker import Worker
from vertebra.telemetry import (
    EventLoopMetricSample,
    EventLoopRecorder,
    GenAiMetricSample,
    GenAiRecorder,
    GenAiUsage,
    LoggingConfig,
    LogHandler,
    LogSink,
    SpanData,
    SpanExporter,
    SqlTracePropagator,
    TraceContext,
)
from vertebra.telemetry.otel import (
    DbPoolMetricSample,
    DbPoolRecorder,
)
from vertebra.utils.module_loader import reset_dependency_cache
from vertebra.utils.serializers import decode_json, encode_json_bytes


class DummyOracleConn:
    """In-memory stub for python-oracledb Thin connection attributes."""

    def __init__(self) -> None:
        self.module: str | None = None
        self.action: str | None = None
        self.ecid: str | None = None
        self.client_identifier: str | None = None


def test_w3c_traceparent_and_non_blocking_rust_log_handler() -> None:
    """Verify W3C traceparent parsing and non-blocking msgspec JSON log emission."""
    raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    parsed = TraceContext.parse(raw)
    assert parsed is not None
    assert parsed.is_sampled
    assert parsed.format() == raw

    TraceContext.set_current(parsed)
    assert TraceContext.current() == parsed
    sink = LogSink(capacity=4, overflow_policy="drop_oldest")
    handler = LogHandler(sink, trace_context_getter=TraceContext.current)
    logger = logging.getLogger("vertebra.test.stage10")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        logger.info("request completed")
    finally:
        logger.removeHandler(handler)
        TraceContext.set_current(None)

    drained = sink.drain()
    assert len(drained) == 1
    decoded = msgspec.json.decode(drained[0])
    assert decoded["message"] == "request completed"
    assert decoded["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert decoded["span_id"] == "00f067aa0ba902b7"


def test_structlog_zero_round_trip_bytes_sink(tmp_path: Path) -> None:
    """Verify structlog processor emits msgspec bytes directly to LogSink."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.vertebra.logging]\nlevel = "debug"\nqueue_capacity = 2048\n',
        encoding="utf-8",
    )
    cfg = LoggingConfig.from_pyproject(pyproject)
    assert cfg.level == "DEBUG"
    assert cfg.queue_capacity == 2048

    formatter = LoggingConfig.extract_litestar_formatter(
        SimpleNamespace(formatters={"standard": {"format": "%(levelname)s:%(message)s"}})
    )
    assert formatter is not None
    assert "INFO:boot" in formatter({"level": "INFO", "message": "boot"})

    sink = LogSink(capacity=8)
    processor = sink.build_structlog_processor(lambda: ("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"))
    write_fn = sink.build_structlog_sink()
    for seq in range(32):
        encoded_bytes = processor(None, "info", {"event": "db.connected", "seq": seq})
        assert isinstance(encoded_bytes, bytes)
        assert write_fn(encoded_bytes)
        drained = sink.drain()
        assert len(drained) >= 1
        record = msgspec.json.decode(drained[0])
        assert record["event"] == "db.connected"
        assert record["seq"] == seq
        assert record["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"


def test_zero_rtt_oracle_and_postgres_sqlcommenter_and_sanitizer() -> None:
    """Verify Oracle zero-RTT attribute piggybacking, Postgres sqlcommenter, and SQL sanitization."""
    trace = TraceContext(
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        span_id="00f067aa0ba902b7",
        trace_flags=1,
    )
    conn = DummyOracleConn()
    applied = SqlTracePropagator.apply_oracle_trace(
        conn,
        trace,
        module="VertebraAPI",
        action="orders.list",
        client_identifier="tenant-42",
    )
    assert applied["ecid"] == "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7"
    assert conn.module == "VertebraAPI"
    assert conn.action == "orders.list"
    assert conn.ecid == "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7"
    assert conn.client_identifier == "tenant-42"

    commented = SqlTracePropagator.append_sqlcommenter(
        "SELECT id FROM orders WHERE status = $1;",
        trace,
        route="/orders",
    )
    assert commented.endswith(
        "/*db_driver='vertebra-pg',route='%2Forders',traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'*/;"
    )

    sanitized = SqlTracePropagator.sanitize_sql("SELECT * FROM items WHERE id IN (10, 20, 30, 40)")
    assert sanitized == "SELECT * FROM items WHERE id IN (?)"


def test_span_ring_genai_metrics_cgroup_and_zpages(tmp_path: Path) -> None:
    """Verify SpanExporter, GenAiRecorder, cgroup v2 quota reader, and z-pages."""
    ring = SpanExporter(capacity=2)
    ring.record_span(
        SpanData(
            name="GET /orders",
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            span_id="00f067aa0ba902b7",
            parent_span_id=None,
            start_ns=100,
            duration_ns=500,
        )
    )
    assert len(ring.drain_spans()) == 1

    genai = GenAiRecorder()
    sample = genai.record_generation(
        model="gemini-2.5-pro",
        input_tokens=120,
        output_tokens=45,
        duration_seconds=0.42,
    )
    assert isinstance(sample, GenAiMetricSample)
    assert sample.input_tokens == 120
    assert len(genai.samples) == 1

    trace_sample = TraceContext(trace_id="4bf92f3577b34da6a3ce929d0e0e4736", span_id="00f067aa0ba902b7")
    usage_sample = GenAiUsage(input_tokens=10, output_tokens=5)
    pool_sample = DbPoolMetricSample(
        pool_name="primary",
        active_leases=2,
        primary_in_use=2,
        overflow_in_use=0,
        max_capacity=8,
        waiting_threads=0,
        total_timeouts=0,
        total_reclaimed=0,
        statement_cache_hits=10,
        statement_cache_misses=1,
        quarantined=1,
        total_quarantined=3,
        total_disposed=2,
    )
    span_sample = SpanData(
        name="GET /orders",
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        span_id="00f067aa0ba902b7",
        parent_span_id=None,
        start_ns=100,
        duration_ns=500,
    )
    decoded_pool = decode_json(encode_json_bytes(pool_sample))
    assert decoded_pool["pool_name"] == "primary"
    assert decoded_pool["active_leases"] == 2
    assert decoded_pool["primary_in_use"] == 2
    assert decoded_pool["overflow_in_use"] == 0
    assert decoded_pool["max_capacity"] == 8
    assert decoded_pool["total_reclaimed"] == 0
    assert decoded_pool["quarantined"] == 1
    assert decoded_pool["total_quarantined"] == 3
    assert decoded_pool["total_disposed"] == 2
    decoded_trace = decode_json(encode_json_bytes(trace_sample))
    assert decoded_trace["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert decoded_trace["span_id"] == "00f067aa0ba902b7"
    decoded_usage = decode_json(encode_json_bytes(usage_sample))
    assert decoded_usage["input_tokens"] == 10
    assert decoded_usage["output_tokens"] == 5
    decoded_genai = decode_json(encode_json_bytes(sample))
    assert decoded_genai["model"] == "gemini-2.5-pro"
    assert decoded_genai["input_tokens"] == 120
    assert decoded_genai["output_tokens"] == 45
    decoded_span = decode_json(encode_json_bytes(span_sample))
    assert decoded_span["name"] == "GET /orders"
    assert decoded_span["duration_ns"] == 500
    assert isinstance(hash(trace_sample), int)
    assert isinstance(hash(usage_sample), int)

    cpu_max = tmp_path / "cpu.max"
    mem_max = tmp_path / "memory.max"
    cpu_max.write_text("250000 100000\n", encoding="utf-8")
    mem_max.write_text("1073741824\n", encoding="utf-8")
    quota = CgroupQuota.detect(cpu_max_path=cpu_max, memory_max_path=mem_max)
    assert quota.cpu_quota_cores == 2.5
    assert quota.effective_workers == 3
    assert quota.memory_max_bytes == 1073741824

    lame_duck = LameDuckController()
    zpages = ZPagesRegistry("vertebra-api", lame_duck=lame_duck)
    assert isinstance(ZPagesRegistry.discover_adapters(), list)
    zpages.set_var("workers", quota.effective_workers)
    zpages.record_rpc("OrdersService/GetOrder", ok=True)

    code_ok, _ = zpages.handle_zpage("/healthz")
    assert code_ok == 200
    lame_duck.enter_lame_duck()
    code_drain, body_drain = zpages.handle_zpage("/healthz")
    assert code_drain == 503
    assert msgspec.json.decode(body_drain)["status"] == "lame_duck"


def test_genai_recorder_is_bounded_and_drains() -> None:
    """Verify GenAiRecorder bounds retained samples via deque maxlen and drains atomically."""
    recorder = GenAiRecorder(max_samples=8)
    for idx in range(20):
        recorder.record_generation(
            model=f"gemini-model-{idx}",
            input_tokens=10 + idx,
            output_tokens=5 + idx,
            duration_seconds=0.1,
        )

    assert len(recorder.samples) == 8
    assert recorder.samples[0].model == "gemini-model-12"
    assert recorder.samples[-1].model == "gemini-model-19"

    drained = recorder.drain()
    assert len(drained) == 8
    assert drained[0].model == "gemini-model-12"
    assert len(recorder.samples) == 0


def test_worker_sigterm_enters_lame_duck_and_validates_shm_policy() -> None:
    """Verify Worker.handle_sigterm validates SHMAllocationPolicy, enters lame-duck mode, and updates zPages."""
    validated_calls: list[bool] = []

    class TrackingShmPolicy(SHMAllocationPolicy, frozen=True):
        def validate_environment(self) -> bool:
            res = super().validate_environment()
            validated_calls.append(res)
            return res

    policy = TrackingShmPolicy(prefer_memfd=True)
    lame_duck = LameDuckController()
    zpages = ZPagesRegistry("vertebra-worker", lame_duck=lame_duck)
    stop_event = threading.Event()

    worker = Worker(
        {},
        shm_policy=policy,
        lame_duck=lame_duck,
        zpages=zpages,
        stop_event=stop_event,
    )
    assert not lame_duck.is_lame_duck
    code_before, _ = zpages.handle_zpage("/healthz")
    assert code_before == 200

    worker.handle_sigterm()
    assert validated_calls == [True]
    assert lame_duck.is_lame_duck is True
    assert stop_event.is_set() is True
    code_after, body_after = zpages.handle_zpage("/healthz")
    assert code_after == 503
    assert msgspec.json.decode(body_after)["status"] == "lame_duck"
    _, varz_body = zpages.handle_zpage("/varz")
    assert msgspec.json.decode(varz_body)["worker_state"] == "draining"


@pytest.mark.parametrize("recorder_kind", ["span", "genai", "db_pool"])
def test_telemetry_recorders_conserve_items_under_concurrent_record_and_drain(recorder_kind: str) -> None:
    """Verify SpanExporter, GenAiRecorder, and DbPoolRecorder lose or duplicate zero items under concurrency."""
    writer_count = 4
    items_per_writer = 200
    total_expected = writer_count * items_per_writer
    barrier = threading.Barrier(writer_count + 1)
    drained_ids: list[str] = []
    errors: list[BaseException] = []

    if recorder_kind == "span":
        exporter = SpanExporter(capacity=2048)

        def _write_span(worker_id: int) -> None:
            try:
                barrier.wait(timeout=5.0)
                for idx in range(items_per_writer):
                    exporter.record_span(
                        SpanData(
                            name=f"w{worker_id}:{idx}",
                            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
                            span_id="00f067aa0ba902b7",
                        )
                    )
            except BaseException as exc:
                errors.append(exc)

        def _drain_span() -> list[str]:
            return [item.name for item in exporter.drain_spans()]

        writer_fn = _write_span
        drain_fn = _drain_span
    elif recorder_kind == "genai":
        genai_rec = GenAiRecorder(max_samples=2048)

        def _write_genai(worker_id: int) -> None:
            try:
                barrier.wait(timeout=5.0)
                for idx in range(items_per_writer):
                    genai_rec.record_generation(
                        model=f"w{worker_id}:{idx}",
                        input_tokens=1,
                        output_tokens=1,
                        duration_seconds=0.01,
                        usd_cost=0.0,
                    )
            except BaseException as exc:
                errors.append(exc)

        def _drain_genai() -> list[str]:
            return [item.model for item in genai_rec.drain()]

        writer_fn = _write_genai
        drain_fn = _drain_genai
    else:
        pool_rec = DbPoolRecorder()

        def _write_pool(worker_id: int) -> None:
            try:
                barrier.wait(timeout=5.0)
                for idx in range(items_per_writer):
                    pool_rec.record_sample(
                        DbPoolMetricSample(
                            pool_name=f"w{worker_id}:{idx}",
                            active_leases=1,
                            primary_in_use=1,
                            overflow_in_use=0,
                            max_capacity=4,
                            waiting_threads=0,
                            total_timeouts=0,
                            total_reclaimed=0,
                            statement_cache_hits=0,
                            statement_cache_misses=0,
                        )
                    )
            except BaseException as exc:
                errors.append(exc)

        def _drain_pool() -> list[str]:
            return [item.pool_name for item in pool_rec.drain_samples()]

        writer_fn = _write_pool
        drain_fn = _drain_pool

    done_writing = threading.Event()

    def _drainer() -> None:
        try:
            barrier.wait(timeout=5.0)
            while not done_writing.is_set():
                drained_ids.extend(drain_fn())
            drained_ids.extend(drain_fn())
        except BaseException as exc:
            errors.append(exc)

    writers = [threading.Thread(target=writer_fn, args=(wid,)) for wid in range(writer_count)]
    drainer = threading.Thread(target=_drainer)
    drainer.start()
    for t in writers:
        t.start()
    for t in writers:
        t.join(timeout=10.0)
    done_writing.set()
    drainer.join(timeout=10.0)

    assert not errors
    assert len(drained_ids) == total_expected
    assert len(set(drained_ids)) == total_expected


def test_genai_module_places_tuple_dunder_all_at_module_top() -> None:
    """Verify telemetry/genai.py defines __all__ as a tuple ahead of functions and classes."""
    repo_root = Path(__file__).resolve().parents[5]
    genai_path = repo_root / "src" / "py" / "vertebra" / "telemetry" / "genai.py"
    tree = ast.parse(genai_path.read_text(encoding="utf-8"), filename=str(genai_path))

    all_assign_line: int | None = None
    first_def_line: int | None = None
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__all__" for target in stmt.targets
        ):
            assert isinstance(stmt.value, ast.Tuple)
            all_assign_line = stmt.lineno
        elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and first_def_line is None:
            first_def_line = stmt.lineno

    assert all_assign_line is not None
    assert first_def_line is not None
    assert all_assign_line < first_def_line


@pytest.mark.parametrize("max_samples", [4, 10])
def test_event_loop_recorder_ring_bounding_percentiles_and_latest_by_shard(max_samples: int) -> None:
    """Verify EventLoopRecorder ring bounding, lag_histogram percentiles, latest_by_shard, and thread safety."""
    recorder = EventLoopRecorder(max_samples=max_samples)
    assert recorder.samples() == []
    assert recorder.latest_by_shard() == {}
    empty_hist = recorder.lag_histogram()
    assert empty_hist.count == 0
    assert empty_hist.sum == 0.0
    assert empty_hist.p50 == 0.0
    assert empty_hist.p99 == 0.0

    total_items = max_samples + 6
    for idx in range(total_items):
        sample = EventLoopMetricSample(
            shard_id=idx % 2,
            backend="uvloop",
            lag_ms=float(idx + 1),
            active_tasks=idx + 2,
            pending_callbacks=idx % 3,
            unhandled_exceptions=1 if idx == total_items - 1 else 0,
            recorded_at_ns=1_000_000 + idx,
        )
        recorder.record(sample)

    retained = recorder.samples()
    assert len(retained) == max_samples
    assert retained[0].lag_ms == float(total_items - max_samples + 1)
    assert retained[-1].lag_ms == float(total_items)

    decoded_sample = decode_json(encode_json_bytes(retained[-1]))
    assert decoded_sample["backend"] == "uvloop"
    assert decoded_sample["unhandled_exceptions"] == 1
    assert decoded_sample["recorded_at_ns"] == 1_000_000 + total_items - 1

    latest = recorder.latest_by_shard()
    assert set(latest.keys()) == {0, 1}
    assert latest[(total_items - 1) % 2].lag_ms == float(total_items)
    assert latest[(total_items - 2) % 2].lag_ms == float(total_items - 1)
    latest.clear()
    assert len(recorder.latest_by_shard()) == 2

    hist = recorder.lag_histogram()
    assert hist.count == total_items
    assert hist.min == 1.0
    assert hist.max == float(total_items)
    assert hist.sum == float(sum(range(1, total_items + 1)))
    assert hist.p50 > 0.0
    assert hist.p90 >= hist.p50
    assert hist.p99 >= hist.p90
    assert any(bucket.count > 0 for bucket in hist.buckets)
    decoded_hist = decode_json(encode_json_bytes(hist))
    assert decoded_hist["count"] == total_items

    concurrent_recorder = EventLoopRecorder(max_samples=512)
    barrier = threading.Barrier(4)
    errors: list[BaseException] = []

    def _writer(shard_id: int) -> None:
        try:
            barrier.wait(timeout=5.0)
            for step in range(50):
                concurrent_recorder.record(
                    EventLoopMetricSample(
                        shard_id=shard_id,
                        backend="asyncio",
                        lag_ms=0.5 + step * 0.1,
                        active_tasks=step,
                        recorded_at_ns=step + 1,
                    )
                )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_writer, args=(sid,)) for sid in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)
    assert not errors
    assert concurrent_recorder.lag_histogram().count == 200
    assert set(concurrent_recorder.latest_by_shard()) == {0, 1, 2, 3}


@pytest.mark.parametrize("mode", ["gauge", "up_down_counter", "absent", "disabled"])
def test_event_loop_recorder_otel_meter_bridge_and_absent_fallback(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify optional opentelemetry.metrics emission and zero-overhead fallback when absent or disabled."""
    histogram_records: list[tuple[float, dict[str, Any]]] = []
    task_records: list[tuple[str, int, dict[str, Any]]] = []
    meter_requests: list[str] = []

    class _StubHistogram:
        def record(self, value: float, attributes: dict[str, Any] | None = None) -> None:
            histogram_records.append((value, dict(attributes or {})))

    class _StubGauge:
        def set(self, value: int, attributes: dict[str, Any] | None = None) -> None:
            task_records.append(("set", value, dict(attributes or {})))

    class _StubUpDownCounter:
        def add(self, value: int, attributes: dict[str, Any] | None = None) -> None:
            task_records.append(("add", value, dict(attributes or {})))

    if mode == "absent":
        monkeypatch.setitem(sys.modules, "opentelemetry", None)
        monkeypatch.setitem(sys.modules, "opentelemetry.metrics", None)
        reset_dependency_cache("opentelemetry")
        reset_dependency_cache("opentelemetry.metrics")
    else:

        class _StubOtelPackage(ModuleType):
            metrics: Any

        class _StubMetricsModule(ModuleType):
            get_meter: Any

        otel_pkg = _StubOtelPackage("opentelemetry")
        metrics_mod = _StubMetricsModule("opentelemetry.metrics")

        def _get_meter(name: str) -> Any:
            if mode == "disabled":
                raise AssertionError("get_meter must not be called when emit_otel=False")
            meter_requests.append(name)
            if mode == "gauge":
                return SimpleNamespace(
                    create_histogram=lambda *_a, **_kw: _StubHistogram(),
                    create_gauge=lambda *_a, **_kw: _StubGauge(),
                )
            return SimpleNamespace(
                create_histogram=lambda *_a, **_kw: _StubHistogram(),
                create_up_down_counter=lambda *_a, **_kw: _StubUpDownCounter(),
            )

        metrics_mod.get_meter = _get_meter
        otel_pkg.metrics = metrics_mod
        monkeypatch.setitem(sys.modules, "opentelemetry", otel_pkg)
        monkeypatch.setitem(sys.modules, "opentelemetry.metrics", metrics_mod)
        reset_dependency_cache("opentelemetry")
        reset_dependency_cache("opentelemetry.metrics")

    try:
        recorder = EventLoopRecorder(
            max_samples=16,
            emit_otel=(mode != "disabled"),
            meter_name="vertebra.test.loop",
        )
        recorder.record(
            EventLoopMetricSample(
                shard_id=0,
                backend="zuvloop",
                lag_ms=1.5,
                active_tasks=5,
                recorded_at_ns=100,
            )
        )
        recorder.record(
            EventLoopMetricSample(
                shard_id=0,
                backend="zuvloop",
                lag_ms=3.25,
                active_tasks=2,
                recorded_at_ns=200,
            )
        )

        assert len(recorder.samples()) == 2
        if mode == "gauge":
            assert meter_requests == ["vertebra.test.loop"]
            assert histogram_records == [
                (1.5, {"shard_id": 0, "backend": "zuvloop"}),
                (3.25, {"shard_id": 0, "backend": "zuvloop"}),
            ]
            assert task_records == [
                ("set", 5, {"shard_id": 0, "backend": "zuvloop"}),
                ("set", 2, {"shard_id": 0, "backend": "zuvloop"}),
            ]
        elif mode == "up_down_counter":
            assert meter_requests == ["vertebra.test.loop"]
            assert task_records == [
                ("add", 5, {"shard_id": 0, "backend": "zuvloop"}),
                ("add", -3, {"shard_id": 0, "backend": "zuvloop"}),
            ]
        else:
            assert meter_requests == []
            assert histogram_records == []
            assert task_records == []
    finally:
        reset_dependency_cache("opentelemetry")
        reset_dependency_cache("opentelemetry.metrics")


@pytest.mark.parametrize(
    ("py_version", "loop_kind", "expected"),
    [
        ((3, 14, 0), "stdlib", False),
        ((3, 13, 0), "zuvloop_attrs", False),
        ((3, 14, 0), "zuvloop_attrs", True),
        ((3, 15, 0), "zuvloop_hook", True),
        ((3, 14, 0), "none", False),
    ],
)
def test_event_loop_recorder_configure_zuvloop(
    py_version: tuple[int, int, int],
    loop_kind: str,
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify configure_zuvloop returns False on stdlib or <3.14 loops and True on supported zuvloop loops."""
    monkeypatch.setattr(sys, "version_info", py_version)
    recorder = EventLoopRecorder(meter_name="vertebra.zuvloop.test")

    if loop_kind == "stdlib":
        stdlib_loop = asyncio.new_event_loop()
        try:
            original_slow = stdlib_loop.slow_callback_duration
            assert recorder.configure_zuvloop(stdlib_loop, metrics_interval_ms=250, slow_callback_ms=50.0) is False
            assert stdlib_loop.slow_callback_duration == original_slow
        finally:
            stdlib_loop.close()
        return

    if loop_kind == "none":
        assert recorder.configure_zuvloop(None) is False
        return

    if loop_kind == "zuvloop_attrs":

        class _StubZuvLoop:
            __module__ = "zuvloop"

            def __init__(self) -> None:
                self.metrics_interval = 10.0
                self.slow_callback_cpu_time_enabled = False
                self.slow_callback_duration = 0.1

        stub_loop = _StubZuvLoop()
        result = recorder.configure_zuvloop(stub_loop, metrics_interval_ms=250, slow_callback_ms=50.0)
        assert result is expected
        if expected:
            assert stub_loop.metrics_interval == pytest.approx(0.25)
            assert stub_loop.slow_callback_cpu_time_enabled is True
            assert stub_loop.slow_callback_duration == pytest.approx(0.05)
        else:
            assert stub_loop.metrics_interval == 10.0
        return

    hook_calls: list[dict[str, Any]] = []
    registered_callbacks: list[Callable[[EventLoopMetricSample], None]] = []

    class _HookedZuvLoop:
        __module__ = "zuvloop._loop"

        def __init__(self) -> None:
            self.slow_callback_duration = 0.1

        def configure_telemetry(
            self,
            *,
            metrics_interval_ms: int,
            slow_callback_ms: float,
            meter_name: str,
        ) -> None:
            hook_calls.append(
                {
                    "metrics_interval_ms": metrics_interval_ms,
                    "slow_callback_ms": slow_callback_ms,
                    "meter_name": meter_name,
                }
            )

        def set_metrics_callback(self, callback: Callable[[EventLoopMetricSample], None]) -> None:
            registered_callbacks.append(callback)

    hooked_loop = _HookedZuvLoop()
    assert recorder.configure_zuvloop(hooked_loop, metrics_interval_ms=500, slow_callback_ms=75.0) is True
    assert hook_calls == [
        {
            "metrics_interval_ms": 500,
            "slow_callback_ms": 75.0,
            "meter_name": "vertebra.zuvloop.test",
        }
    ]
    assert len(registered_callbacks) == 1
    registered_callbacks[0](
        EventLoopMetricSample(
            shard_id=2,
            backend="zuvloop",
            lag_ms=0.75,
            active_tasks=3,
            recorded_at_ns=999,
        )
    )
    assert recorder.latest_by_shard()[2].lag_ms == pytest.approx(0.75)


async def _http_get_json(host: str, port: int, path: str) -> tuple[int, dict[str, Any]]:
    """Issue a minimal HTTP/1.1 GET over loopback and decode its JSON payload."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        request = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
        writer.write(request.encode("ascii"))
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=5.0)
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
    head, body = raw.split(b"\r\n\r\n", 1)
    status_code = int(head.split(b"\r\n", 1)[0].split(b" ")[1])
    parsed = decode_json(body)
    assert isinstance(parsed, dict)
    return status_code, parsed


@pytest.mark.parametrize("endpoint", ["/taskz", "/debug/zpages/tasks"])
@pytest.mark.parametrize("pool_mode", ["inline", "multi_shard"])
def test_zpages_and_health_probe_server_serve_taskz_across_inline_and_sharded_pools(
    endpoint: str,
    pool_mode: str,
) -> None:
    """Verify ZPagesRegistry and HealthProbeServer serve /taskz and /debug/zpages/tasks across inline and sharded pools."""
    zpages = ZPagesRegistry("vertebra-taskz-test")
    stopped_loop = asyncio.new_event_loop()
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()
    zpages.register_event_loop("stopped_extra", stopped_loop, shard_id=98)
    zpages.register_event_loop("closed_extra", closed_loop, shard_id=99)

    try:
        if pool_mode == "multi_shard":
            pool = EventLoopPool(size=2, loop_backend=LoopBackend.ASYNCIO)
            zpages.register_loop_pool("http", pool)
            started = threading.Event()
            release = threading.Event()

            async def _shard_worker() -> str:
                started.set()
                while not release.is_set():
                    await asyncio.sleep(0.01)
                current = asyncio.current_task()
                return current.get_name() if current is not None else ""

            try:
                first_fut = pool.schedule(_shard_worker(), worker_index=0)
                assert started.wait(timeout=5.0)

                sync_code, sync_raw = zpages.handle_zpage(endpoint)
                assert sync_code == 200
                sync_payload = decode_json(sync_raw)
                assert sync_payload["status"] == "ok"
                assert sync_payload["service"] == "vertebra-taskz-test"
                assert "stopped_extra" not in sync_payload["loops"]
                assert "closed_extra" not in sync_payload["loops"]
                assert sync_payload["loops"]["http.shard.1"] == []
                shard0_tasks = sync_payload["loops"]["http.shard.0"]
                assert len(shard0_tasks) == 1
                assert shard0_tasks[0]["task_name"] == "vertebra.loop.shard.0.task.1"
                assert shard0_tasks[0]["shard_id"] == 0
                assert "_shard_worker" in shard0_tasks[0]["coro_qualname"]
                assert shard0_tasks[0]["call_graph"]

                async def _probe_http() -> tuple[int, dict[str, Any]]:
                    probe_server = HealthProbeServer(zpages, host="127.0.0.1", port=0)
                    await probe_server.start()
                    try:
                        return await _http_get_json("127.0.0.1", probe_server.bound_port, endpoint)
                    finally:
                        await probe_server.stop()

                http_code, http_payload = asyncio.run(_probe_http())
                assert http_code == 200
                assert http_payload["status"] == "ok"
                assert http_payload["total_tasks"] == 1
                assert http_payload["loops"]["http.shard.1"] == []
                assert http_payload["loops"]["http.shard.0"][0]["task_name"] == "vertebra.loop.shard.0.task.1"

                async def _second_name() -> str:
                    current = asyncio.current_task()
                    return current.get_name() if current is not None else ""

                second_name = pool.schedule(_second_name(), worker_index=0).result(timeout=5.0)
                assert second_name == "vertebra.loop.shard.0.task.2"

                release.set()
                assert first_fut.result(timeout=5.0) == "vertebra.loop.shard.0.task.1"
            finally:
                release.set()
                pool.stop()

            after_stop = zpages.get_tasks_payload()
            assert after_stop["loops"] == {}
            assert after_stop["total_tasks"] == 0
            return

        inline_pool = EventLoopPool(size=1, inline_single_loop=True, loop_backend=LoopBackend.ASYNCIO)
        zpages.register_loop_pool("inline", inline_pool)
        try:

            async def _exercise_inline() -> None:
                gate = asyncio.Event()

                async def _inline_bg() -> None:
                    await gate.wait()

                inline_pool.spawn(_inline_bg())
                await asyncio.sleep(0)

                sync_code, sync_raw = zpages.handle_zpage(endpoint)
                assert sync_code == 200
                sync_payload = decode_json(sync_raw)
                assert "inline.shard.0" in sync_payload["loops"]
                inline_names = {item["task_name"] for item in sync_payload["loops"]["inline.shard.0"]}
                assert "vertebra.loop.shard.0.task.1" in inline_names

                probe_server = HealthProbeServer(zpages, host="127.0.0.1", port=0)
                await probe_server.start()
                try:
                    http_code, http_payload = await _http_get_json(
                        "127.0.0.1",
                        probe_server.bound_port,
                        endpoint,
                    )
                finally:
                    await probe_server.stop()

                assert http_code == 200
                assert http_payload["status"] == "ok"
                http_names = {item["task_name"] for item in http_payload["loops"]["inline.shard.0"]}
                assert "vertebra.loop.shard.0.task.1" in http_names

                gate.set()
                await asyncio.sleep(0)

            driver = inline_pool.primary_loop.create_task(_exercise_inline(), name="vertebra.test.inline_driver")
            inline_pool.primary_loop.run_until_complete(driver)
        finally:
            inline_pool.stop()

        after_inline_stop = zpages.get_tasks_payload()
        assert after_inline_stop["loops"] == {}
        assert after_inline_stop["total_tasks"] == 0
    finally:
        if not stopped_loop.is_closed():
            stopped_loop.close()


@pytest.mark.parametrize(
    ("export_name", "expected_submodule"),
    [
        ("EventLoopMetricSample", "vertebra.telemetry.otel"),
        ("EventLoopRecorder", "vertebra.telemetry.otel"),
    ],
)
def test_telemetry_package_lazy_exports_resolve_without_eager_optional_imports(
    export_name: str,
    expected_submodule: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vertebra.telemetry lazily exports EventLoopMetricSample and EventLoopRecorder without eager imports."""
    assert export_name in telemetry_pkg.__all__
    assert set(telemetry_pkg.__all__).issubset(set(dir(telemetry_pkg)))
    for public_name in telemetry_pkg.__all__:
        assert getattr(telemetry_pkg, public_name) is not None

    for mod_name in (
        "vertebra.telemetry",
        "vertebra.telemetry.db_propagation",
        "vertebra.telemetry.genai",
        "vertebra.telemetry.logger",
        "vertebra.telemetry.otel",
    ):
        monkeypatch.delitem(sys.modules, mod_name, raising=False)
    monkeypatch.delattr(vertebra, "telemetry", raising=False)
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    monkeypatch.setitem(sys.modules, "opentelemetry.metrics", None)
    reset_dependency_cache("opentelemetry")
    reset_dependency_cache("opentelemetry.metrics")

    try:
        fresh_pkg = importlib.import_module("vertebra.telemetry")
        assert expected_submodule not in sys.modules
        assert sys.modules["opentelemetry"] is None
        assert export_name in fresh_pkg.__all__
        assert export_name in dir(fresh_pkg)

        resolved = getattr(fresh_pkg, export_name)
        assert expected_submodule in sys.modules
        assert resolved.__module__ == expected_submodule
    finally:
        reset_dependency_cache("opentelemetry")
        reset_dependency_cache("opentelemetry.metrics")
