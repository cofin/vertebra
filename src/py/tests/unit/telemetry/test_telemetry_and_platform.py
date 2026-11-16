"""Unit tests for Vertebra non-blocking logger, zero-RTT DB trace propagation, and platform z-pages."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from types import SimpleNamespace

import msgspec

from vertebra.server.platform import (
    CgroupQuota,
    LameDuckController,
    SHMAllocationPolicy,
    ZPagesRegistry,
)
from vertebra.server.worker import Worker
from vertebra.telemetry import (
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
from vertebra.telemetry.otel import DbPoolMetricSample
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
