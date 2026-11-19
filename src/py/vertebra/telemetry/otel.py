"""Zero-overhead W3C Trace Context, SpanExporter, and GenAI telemetry histograms."""

from __future__ import annotations

import math
import secrets
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping
from contextlib import suppress
from contextvars import ContextVar, Token
from typing import Any, Literal

import msgspec

from vertebra._vertebra import format_traceparent, parse_traceparent
from vertebra.utils.module_loader import import_optional

__all__ = (
    "CostTable",
    "DbPoolMetricSample",
    "DbPoolRecorder",
    "EventLoopMetricSample",
    "EventLoopRecorder",
    "GenAiMetricSample",
    "GenAiRecorder",
    "GenAiUsage",
    "HistogramBucket",
    "HistogramSnapshot",
    "ModelPricing",
    "SpanData",
    "SpanExporter",
    "TraceContext",
    "calculate_percentile",
)


class TraceContext(msgspec.Struct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """W3C Trace Context traceparent representation (00-<trace_id>-<span_id>-<flags>)."""

    trace_id: str
    span_id: str
    trace_flags: int = 1
    version: int = 0

    @classmethod
    def parse(cls, header: str) -> TraceContext | None:
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
    def generate(cls, *, sampled: bool = True) -> TraceContext:
        """Generate a new root TraceContext."""
        return cls(
            trace_id=secrets.token_hex(16),
            span_id=secrets.token_hex(8),
            trace_flags=1 if sampled else 0,
        )

    @staticmethod
    def current() -> TraceContext | None:
        """Return the active TraceContext from the current execution context."""
        val = _ACTIVE_TRACE_CTX.get()
        return val if isinstance(val, TraceContext) else None

    @staticmethod
    def set_current(ctx: TraceContext | None) -> Token[TraceContext | None]:
        """Bind ctx as the active W3C trace context for the current execution context."""
        return _ACTIVE_TRACE_CTX.set(ctx)

    def new_child(self) -> TraceContext:
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


class SpanData(msgspec.Struct, frozen=True, gc=False, kw_only=True):
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
    """Bounded thread-safe ring buffer for completed telemetry spans."""

    __slots__ = ("_lock", "_ring", "capacity")

    def __init__(self, capacity: int = 4096) -> None:
        self.capacity = max(1, capacity)
        self._ring: deque[SpanData] = deque(maxlen=self.capacity)
        self._lock = threading.Lock()

    def record_span(self, span: SpanData) -> None:
        """Append a completed span to the bounded ring buffer."""
        with self._lock:
            self._ring.append(span)

    def get_spans(self) -> list[SpanData]:
        """Return all buffered spans without draining the ring buffer."""
        with self._lock:
            return list(self._ring)

    def drain_spans(self) -> list[SpanData]:
        """Drain and return all buffered spans for batch OTLP export."""
        with self._lock:
            spans = list(self._ring)
            self._ring.clear()
            return spans


class GenAiUsage(msgspec.Struct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Token usage breakdown for a single model generation or aggregated turn."""

    input_tokens: int = 0
    output_tokens: int = 0
    thought_tokens: int = 0
    cached_input_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        """Return the sum of input, output, and reasoning tokens."""
        return self.input_tokens + self.output_tokens + self.thought_tokens


class ModelPricing(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pricing rates in USD per million tokens."""

    input_per_million_usd: float
    output_per_million_usd: float
    cached_input_per_million_usd: float = 0.0
    thinking_per_million_usd: float = 0.0

    def calculate_cost(self, usage: GenAiUsage) -> float:
        """Compute the total estimated USD cost for a given token usage breakdown."""
        cost = 0.0
        active_input = max(0, usage.input_tokens - usage.cached_input_tokens)
        cost += (active_input / 1_000_000.0) * self.input_per_million_usd
        cost += (usage.cached_input_tokens / 1_000_000.0) * self.cached_input_per_million_usd
        cost += (usage.output_tokens / 1_000_000.0) * self.output_per_million_usd
        cost += (usage.thought_tokens / 1_000_000.0) * (self.thinking_per_million_usd or self.output_per_million_usd)
        return round(cost, 6)


class CostTable:
    """Registry of pricing models for calculating GenAI operation costs."""

    DEFAULT_PRICING: dict[str, ModelPricing] = {
        "gemini-2.5-pro": ModelPricing(
            input_per_million_usd=1.25,
            output_per_million_usd=5.00,
            cached_input_per_million_usd=0.3125,
            thinking_per_million_usd=5.00,
        ),
        "gemini-2.5-flash": ModelPricing(
            input_per_million_usd=0.075,
            output_per_million_usd=0.30,
            cached_input_per_million_usd=0.01875,
            thinking_per_million_usd=0.30,
        ),
        "gemini-flash-next": ModelPricing(
            input_per_million_usd=0.075,
            output_per_million_usd=0.30,
            cached_input_per_million_usd=0.01875,
            thinking_per_million_usd=0.30,
        ),
        "gemini-launch-candidate-high": ModelPricing(
            input_per_million_usd=1.25,
            output_per_million_usd=5.00,
            cached_input_per_million_usd=0.3125,
            thinking_per_million_usd=5.00,
        ),
    }

    def __init__(self, custom_rates: Mapping[str, ModelPricing] | None = None) -> None:
        self._rates: dict[str, ModelPricing] = dict(self.DEFAULT_PRICING)
        if custom_rates:
            self._rates.update(custom_rates)

    def register(self, model: str, pricing: ModelPricing) -> None:
        """Register or override pricing for a specific model name."""
        self._rates[model] = pricing

    def get_pricing(self, model: str) -> ModelPricing:
        """Retrieve pricing for a model, falling back to flash rates when unknown."""
        if model in self._rates:
            return self._rates[model]
        for key, pricing in self._rates.items():
            if key in model:
                return pricing
        return self._rates["gemini-2.5-flash"]

    def compute_cost(self, model: str, usage: GenAiUsage) -> float:
        """Compute the total estimated cost for token usage on a model."""
        pricing = self.get_pricing(model)
        return pricing.calculate_cost(usage)


class HistogramBucket(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """A single bucket in a distribution histogram."""

    upper_bound: float
    count: int


class HistogramSnapshot(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Statistical summary of recorded telemetry samples."""

    count: int
    sum: float
    min: float
    max: float
    buckets: tuple[HistogramBucket, ...] = ()
    p50: float = 0.0
    p90: float = 0.0
    p99: float = 0.0


def calculate_percentile(sorted_values: list[float], percentile: float) -> float:
    """Calculate a percentile value from a sorted list of float measurements."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


class GenAiMetricSample(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """OpenTelemetry gen_ai.* semantic convention metric sample."""

    model: str
    operation: str
    input_tokens: int
    output_tokens: int
    duration_seconds: float
    thought_tokens: int = 0
    cached_input_tokens: int = 0
    usd_cost: float = 0.0
    agent_name: str | None = None
    status: Literal["ok", "error"] = "ok"
    timestamp: float = msgspec.field(default_factory=time.time)


class GenAiRecorder:
    """Records gen_ai.client.token.usage and gen_ai.client.operation.duration metrics."""

    __slots__ = ("_cost_table", "_lock", "_samples", "max_samples")

    def __init__(self, cost_table: Any | None = None, *, max_samples: int = 1024) -> None:
        self.max_samples = max_samples
        self._samples: deque[GenAiMetricSample] = deque(maxlen=max_samples)
        self._cost_table = cost_table
        self._lock = threading.Lock()

    @property
    def samples(self) -> tuple[GenAiMetricSample, ...]:
        """Return recorded GenAI telemetry samples."""
        with self._lock:
            return tuple(self._samples)

    def record_generation(
        self,
        *,
        model: str,
        operation: str = "chat",
        input_tokens: int,
        output_tokens: int,
        duration_seconds: float,
        thought_tokens: int = 0,
        cached_input_tokens: int = 0,
        usd_cost: float | None = None,
        agent_name: str | None = None,
        status: Literal["ok", "error"] = "ok",
    ) -> GenAiMetricSample:
        """Record a model generation operation with token counts, latency, and cost."""
        if usd_cost is None:
            usage = GenAiUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_input_tokens=cached_input_tokens,
            )
            table = self._cost_table if self._cost_table is not None else CostTable()
            computed_cost = float(table.compute_cost(model, usage))
        else:
            computed_cost = usd_cost

        sample = GenAiMetricSample(
            model=model,
            operation=operation,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            duration_seconds=duration_seconds,
            thought_tokens=thought_tokens,
            cached_input_tokens=cached_input_tokens,
            usd_cost=computed_cost,
            agent_name=agent_name,
            status=status,
        )
        with self._lock:
            self._samples.append(sample)
        return sample

    def duration_histogram(self, operation: str) -> HistogramSnapshot:
        """Calculate statistical duration histogram for a specific operation."""
        with self._lock:
            matching = [s.duration_seconds for s in self._samples if s.operation == operation]
        if not matching:
            return HistogramSnapshot(
                count=0,
                sum=0.0,
                min=0.0,
                max=0.0,
                p50=0.0,
                p90=0.0,
                p99=0.0,
            )

        sorted_vals = sorted(matching)
        total_sum = sum(sorted_vals)
        count = len(sorted_vals)

        return HistogramSnapshot(
            count=count,
            sum=total_sum,
            min=sorted_vals[0],
            max=sorted_vals[-1],
            p50=calculate_percentile(sorted_vals, 50.0),
            p90=calculate_percentile(sorted_vals, 90.0),
            p99=calculate_percentile(sorted_vals, 99.0),
        )

    def total_cost(self, agent_name: str | None = None) -> float:
        """Return the sum of all recorded USD costs, optionally filtered by agent name."""
        with self._lock:
            return sum(s.usd_cost for s in self._samples if agent_name is None or s.agent_name == agent_name)

    def total_tokens(self, agent_name: str | None = None) -> int:
        """Return the sum of all recorded tokens, optionally filtered by agent name."""
        with self._lock:
            return sum(
                s.input_tokens + s.output_tokens + s.thought_tokens
                for s in self._samples
                if agent_name is None or s.agent_name == agent_name
            )

    def drain(self) -> list[GenAiMetricSample]:
        """Drain and return all recorded GenAI telemetry samples."""
        with self._lock:
            drained = list(self._samples)
            self._samples.clear()
            return drained


class DbPoolMetricSample(msgspec.Struct, frozen=True, gc=False, kw_only=True):
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
    quarantined: int = 0
    total_quarantined: int = 0
    total_disposed: int = 0
    wait_duration_ms: float = 0.0
    timestamp: float = msgspec.field(default_factory=time.time)


class DbPoolRecorder:
    """Records database connection pool and statement cache telemetry metrics."""

    __slots__ = ("_lock", "_samples")

    def __init__(self) -> None:
        self._samples: list[DbPoolMetricSample] = []
        self._lock = threading.Lock()

    @property
    def samples(self) -> tuple[DbPoolMetricSample, ...]:
        """Return recorded database pool telemetry samples."""
        with self._lock:
            return tuple(self._samples)

    def record_sample(self, sample: DbPoolMetricSample) -> None:
        """Record a database pool telemetry metric sample."""
        with self._lock:
            self._samples.append(sample)

    def drain_samples(self) -> list[DbPoolMetricSample]:
        """Drain and return all buffered database pool metric samples."""
        with self._lock:
            samples = list(self._samples)
            self._samples.clear()
            return samples


class EventLoopMetricSample(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Point-in-time event loop scheduling lag and active task telemetry sample."""

    shard_id: int
    """Zero-based event loop shard identifier."""

    backend: str
    """Resolved event loop backend name (for example ``uvloop``, ``zuvloop``, or ``asyncio``)."""

    lag_ms: float
    """Measured event loop scheduling lag in milliseconds."""

    active_tasks: int
    """Number of currently active ``asyncio.Task`` instances on the shard."""

    pending_callbacks: int = 0
    """Number of queued event loop callbacks awaiting execution."""

    unhandled_exceptions: int = 0
    """Count of unhandled event loop exceptions observed on the shard."""

    recorded_at_ns: int = msgspec.field(default_factory=time.time_ns)
    """Timestamp in nanoseconds when the metric sample was recorded."""


class EventLoopRecorder:
    """Thread-safe recorder for event loop scheduling lag, task counts, and optional OpenTelemetry metrics."""

    DEFAULT_LAG_BUCKETS_MS: tuple[float, ...] = (
        0.1,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        25.0,
        50.0,
        100.0,
        250.0,
        500.0,
        1000.0,
        5000.0,
    )

    __slots__ = (
        "_active_tasks_by_shard",
        "_active_tasks_instrument",
        "_active_tasks_is_gauge",
        "_bucket_bounds",
        "_bucket_counts",
        "_lag_count",
        "_lag_instrument",
        "_lag_max",
        "_lag_min",
        "_lag_sum",
        "_latest_by_shard",
        "_lock",
        "_otel_metrics_mod",
        "_samples",
        "emit_otel",
        "max_samples",
        "meter_name",
    )

    def __init__(
        self,
        max_samples: int = 1024,
        *,
        emit_otel: bool = False,
        meter_name: str = "vertebra.server.loop",
    ) -> None:
        self.max_samples = max(1, max_samples)
        self.emit_otel = emit_otel
        self.meter_name = meter_name
        self._samples: deque[EventLoopMetricSample] = deque(maxlen=self.max_samples)
        self._bucket_bounds: tuple[float, ...] = self.DEFAULT_LAG_BUCKETS_MS
        self._bucket_counts: list[int] = [0] * len(self._bucket_bounds)
        self._lag_count = 0
        self._lag_sum = 0.0
        self._lag_min = 0.0
        self._lag_max = 0.0
        self._latest_by_shard: dict[int, EventLoopMetricSample] = {}
        self._active_tasks_by_shard: dict[int, int] = {}
        self._otel_metrics_mod: Any | None = None
        self._lag_instrument: Any | None = None
        self._active_tasks_instrument: Any | None = None
        self._active_tasks_is_gauge = False
        self._lock = threading.Lock()

    def record(self, sample: EventLoopMetricSample) -> None:
        """Append an event loop sample, update lag histogram buckets, and optionally emit OTel metrics."""
        lag = float(sample.lag_ms)
        with self._lock:
            self._samples.append(sample)
            self._latest_by_shard[sample.shard_id] = sample
            prev_active = self._active_tasks_by_shard.get(sample.shard_id, 0)
            self._active_tasks_by_shard[sample.shard_id] = sample.active_tasks
            active_delta = sample.active_tasks - prev_active
            if self._lag_count == 0:
                self._lag_min = lag
                self._lag_max = lag
            else:
                if lag < self._lag_min:
                    self._lag_min = lag
                if lag > self._lag_max:
                    self._lag_max = lag
            self._lag_count += 1
            self._lag_sum += lag
            for idx, upper_bound in enumerate(self._bucket_bounds):
                if lag <= upper_bound:
                    self._bucket_counts[idx] += 1
            if self.emit_otel:
                self._emit_otel_sample(sample, active_delta)

    def configure_zuvloop(
        self,
        loop: Any,
        *,
        metrics_interval_ms: int = 1000,
        slow_callback_ms: float = 100.0,
    ) -> bool:
        """Attach native zuvloop OpenTelemetry and slow-callback hooks when supported on Python 3.14+."""
        if loop is None or sys.version_info < (3, 14):
            return False

        loop_module = getattr(type(loop), "__module__", "") or ""
        is_zuvloop_type = loop_module == "zuvloop" or loop_module.startswith("zuvloop.")
        configure_fn = getattr(loop, "configure_telemetry", None)
        set_callback_fn = getattr(loop, "set_metrics_callback", None)
        has_native_attrs = hasattr(loop, "metrics_interval") or hasattr(loop, "slow_callback_cpu_time_enabled")

        if not (is_zuvloop_type or callable(configure_fn) or callable(set_callback_fn) or has_native_attrs):
            return False

        interval_seconds = max(0.001, float(metrics_interval_ms) / 1000.0)
        slow_seconds = max(0.0, float(slow_callback_ms) / 1000.0)
        configured = False

        if callable(configure_fn):
            candidate_kwargs: tuple[dict[str, Any], ...] = (
                {
                    "metrics_interval_ms": metrics_interval_ms,
                    "slow_callback_ms": slow_callback_ms,
                    "meter_name": self.meter_name,
                },
                {
                    "metrics_interval_ms": metrics_interval_ms,
                    "slow_callback_ms": slow_callback_ms,
                },
                {},
            )
            for kwargs in candidate_kwargs:
                try:
                    configure_fn(**kwargs)
                    configured = True
                    break
                except TypeError:
                    continue
                except (AttributeError, ValueError):
                    break

        if callable(set_callback_fn):
            with suppress(AttributeError, TypeError, ValueError):
                set_callback_fn(self.record)
                configured = True

        if is_zuvloop_type or hasattr(loop, "metrics_interval"):
            with suppress(AttributeError, TypeError, ValueError):
                loop.metrics_interval = interval_seconds
                configured = True

        if is_zuvloop_type or hasattr(loop, "slow_callback_cpu_time_enabled"):
            with suppress(AttributeError, TypeError, ValueError):
                loop.slow_callback_cpu_time_enabled = True
                configured = True

        if configured or is_zuvloop_type:
            with suppress(AttributeError, TypeError, ValueError):
                loop.slow_callback_duration = slow_seconds
                configured = True

        return configured

    def lag_histogram(self) -> HistogramSnapshot:
        """Return a statistical lag histogram snapshot across recorded loop samples."""
        with self._lock:
            count = self._lag_count
            total_sum = self._lag_sum
            min_lag = self._lag_min
            max_lag = self._lag_max
            buckets = tuple(
                HistogramBucket(upper_bound=bound, count=bucket_count)
                for bound, bucket_count in zip(self._bucket_bounds, self._bucket_counts, strict=True)
            )
            window_lags = [sample.lag_ms for sample in self._samples]

        if count == 0:
            return HistogramSnapshot(
                count=0,
                sum=0.0,
                min=0.0,
                max=0.0,
                buckets=buckets,
                p50=0.0,
                p90=0.0,
                p99=0.0,
            )

        sorted_lags = sorted(window_lags)
        return HistogramSnapshot(
            count=count,
            sum=total_sum,
            min=min_lag,
            max=max_lag,
            buckets=buckets,
            p50=calculate_percentile(sorted_lags, 50.0),
            p90=calculate_percentile(sorted_lags, 90.0),
            p99=calculate_percentile(sorted_lags, 99.0),
        )

    def latest_by_shard(self) -> dict[int, EventLoopMetricSample]:
        """Return the most recently recorded telemetry sample for each shard."""
        with self._lock:
            return dict(self._latest_by_shard)

    def samples(self) -> list[EventLoopMetricSample]:
        """Return buffered event loop telemetry samples in chronological order."""
        with self._lock:
            return list(self._samples)

    def _emit_otel_sample(self, sample: EventLoopMetricSample, active_tasks_delta: int) -> None:
        """Emit loop lag and active task metrics through optional opentelemetry.metrics."""
        metrics_mod = import_optional("opentelemetry.metrics")
        if metrics_mod is None:
            self._otel_metrics_mod = None
            self._lag_instrument = None
            self._active_tasks_instrument = None
            return

        if metrics_mod is not self._otel_metrics_mod:
            get_meter = getattr(metrics_mod, "get_meter", None)
            if not callable(get_meter):
                return
            meter = get_meter(self.meter_name)
            create_histogram = getattr(meter, "create_histogram", None)
            self._lag_instrument = (
                create_histogram(
                    "vertebra.loop.lag_ms",
                    unit="ms",
                    description="Event loop scheduling lag in milliseconds",
                )
                if callable(create_histogram)
                else None
            )
            create_gauge = getattr(meter, "create_gauge", None)
            create_up_down = getattr(meter, "create_up_down_counter", None)
            if callable(create_gauge):
                self._active_tasks_instrument = create_gauge(
                    "vertebra.loop.active_tasks",
                    unit="{task}",
                    description="Active asyncio tasks on the event loop shard",
                )
                self._active_tasks_is_gauge = True
            elif callable(create_up_down):
                self._active_tasks_instrument = create_up_down(
                    "vertebra.loop.active_tasks",
                    unit="{task}",
                    description="Active asyncio tasks on the event loop shard",
                )
                self._active_tasks_is_gauge = False
            else:
                self._active_tasks_instrument = None
                self._active_tasks_is_gauge = False
            self._otel_metrics_mod = metrics_mod

        attrs: dict[str, str | int] = {
            "shard_id": sample.shard_id,
            "backend": sample.backend,
        }
        if self._lag_instrument is not None:
            self._lag_instrument.record(sample.lag_ms, attributes=attrs)
        if self._active_tasks_instrument is not None:
            if self._active_tasks_is_gauge:
                self._active_tasks_instrument.set(sample.active_tasks, attributes=attrs)
            else:
                self._active_tasks_instrument.add(active_tasks_delta, attributes=attrs)
