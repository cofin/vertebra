"""Zero-overhead W3C Trace Context, SpanExporter, and GenAI telemetry histograms."""

import secrets
import time
from collections import deque
from contextvars import ContextVar, Token
from typing import Any

import msgspec

from vertebra._vertebra import format_traceparent, parse_traceparent

__all__ = (
    "DbPoolMetricSample",
    "DbPoolRecorder",
    "GenAiMetricSample",
    "GenAiRecorder",
    "SpanData",
    "SpanExporter",
    "TraceContext",
)


class TraceContext(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """W3C Trace Context traceparent representation (00-<trace_id>-<span_id>-<flags>)."""

    trace_id: str
    span_id: str
    trace_flags: int = 1
    version: int = 0

    @classmethod
    def parse(cls, header: str) -> "TraceContext | None":
        """Parse a W3C traceparent header string using native Rust parser."""
        result = parse_traceparent(header)
        if result is None:
            return None
        trace_id, span_id, trace_flags = result
        return cls(
            trace_id=trace_id,
            span_id=span_id,
            trace_flags=trace_flags,
            version=0,
        )

    @classmethod
    def generate(cls, *, sampled: bool = True) -> "TraceContext":
        """Generate a new root TraceContext."""
        return cls(
            trace_id=secrets.token_hex(16),
            span_id=secrets.token_hex(8),
            trace_flags=1 if sampled else 0,
        )

    @staticmethod
    def current() -> "TraceContext | None":
        """Return the active TraceContext from the current execution context."""
        return _ACTIVE_TRACE_CTX.get()

    @staticmethod
    def set_current(ctx: "TraceContext | None") -> "Token[TraceContext | None]":
        """Bind ctx as the active W3C trace context for the current execution context."""
        return _ACTIVE_TRACE_CTX.set(ctx)

    def new_child(self) -> "TraceContext":
        """Create a child span context sharing trace_id."""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=secrets.token_hex(8),
            trace_flags=self.trace_flags,
            version=self.version,
        )

    def format(self) -> str:
        """Format as a canonical 55-character traceparent header."""
        return format_traceparent(self.trace_id, self.span_id, self.trace_flags)

    @property
    def is_sampled(self) -> bool:
        """Return whether the sampled bit (0x01) is set."""
        return bool(self.trace_flags & 0x01)


_ACTIVE_TRACE_CTX: ContextVar[TraceContext | None] = ContextVar(
    "_ACTIVE_TRACE_CTX",
    default=None,
)


class SpanData(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """Lightweight span record exported through SpanExporter."""

    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    start_ns: int = 0
    duration_ns: int = 0
    status_code: int = 200
    attributes: dict[str, Any] = msgspec.field(default_factory=dict)


class SpanExporter:
    """Bounded lock-free-style ring buffer for completed telemetry spans."""

    def __init__(self, capacity: int = 4096) -> None:
        self.capacity = max(1, capacity)
        self._ring: deque[SpanData] = deque(maxlen=self.capacity)

    def record_span(self, span: SpanData) -> None:
        """Append a completed span to the bounded ring buffer."""
        self._ring.append(span)

    def drain_spans(self) -> list[SpanData]:
        """Drain and return all buffered spans for batch OTLP export."""
        spans = list(self._ring)
        self._ring.clear()
        return spans


class GenAiMetricSample(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """OpenTelemetry gen_ai.* semantic convention metric sample."""

    model: str
    operation: str
    input_tokens: int
    output_tokens: int
    duration_seconds: float
    timestamp: float = msgspec.field(default_factory=time.time)


class GenAiRecorder:
    """Records gen_ai.client.token.usage and gen_ai.client.operation.duration metrics."""

    def __init__(self, max_samples: int = 1024) -> None:
        self.max_samples = max_samples
        self._samples: deque[GenAiMetricSample] = deque(maxlen=max_samples)

    @property
    def samples(self) -> tuple[GenAiMetricSample, ...]:
        """Return recorded GenAI telemetry samples."""
        return tuple(self._samples)

    def record_generation(
        self,
        *,
        model: str,
        operation: str = "chat",
        input_tokens: int,
        output_tokens: int,
        duration_seconds: float,
    ) -> GenAiMetricSample:
        """Record a model generation operation with token counts and latency."""
        sample = GenAiMetricSample(
            model=model,
            operation=operation,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            duration_seconds=duration_seconds,
        )
        self._samples.append(sample)
        return sample

    def drain(self) -> list[GenAiMetricSample]:
        """Drain and return all recorded GenAI telemetry samples."""
        drained = list(self._samples)
        self._samples.clear()
        return drained


class DbPoolMetricSample(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """OpenTelemetry db.client.connections.* semantic convention metric sample."""

    pool_name: str
    active_leases: int
    primary_in_use: int
    overflow_in_use: int
    max_capacity: int
    waiting_threads: int
    total_timeouts: int
    total_reclaimed: int
    statement_cache_hits: int
    statement_cache_misses: int
    wait_duration_ms: float = 0.0
    timestamp: float = msgspec.field(default_factory=time.time)


class DbPoolRecorder:
    """Records database connection pool and statement cache telemetry metrics."""

    def __init__(self) -> None:
        self._samples: list[DbPoolMetricSample] = []

    @property
    def samples(self) -> tuple[DbPoolMetricSample, ...]:
        """Return recorded database pool telemetry samples."""
        return tuple(self._samples)

    def record_sample(self, sample: DbPoolMetricSample) -> None:
        """Record a database pool telemetry metric sample."""
        self._samples.append(sample)

    def drain_samples(self) -> list[DbPoolMetricSample]:
        """Drain and return all buffered database pool metric samples."""
        samples = list(self._samples)
        self._samples.clear()
        return samples
