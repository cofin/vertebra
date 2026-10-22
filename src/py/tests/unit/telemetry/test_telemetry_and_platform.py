"""Unit tests for Vertebra non-blocking logger, zero-RTT DB trace propagation, and platform z-pages."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import msgspec

from vertebra.server.platform import (
    CgroupQuota,
    LameDuckController,
    ZPagesRegistry,
)
from vertebra.telemetry import (
    GenAiMetricSample,
    GenAiRecorder,
    LoggingConfig,
    LogHandler,
    LogSink,
    SpanData,
    SpanExporter,
    SqlTracePropagator,
    TraceContext,
)


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
    encoded_bytes = processor(None, "info", {"event": "db.connected"})
    assert isinstance(encoded_bytes, bytes)
    assert write_fn(encoded_bytes)
    record = msgspec.json.decode(sink.drain()[0])
    assert record["event"] == "db.connected"
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
