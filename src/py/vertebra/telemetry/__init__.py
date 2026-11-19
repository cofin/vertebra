"""Native OpenTelemetry, non-blocking logging, and zero-RTT DB trace propagation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.telemetry.db_propagation import SqlTracePropagator
    from vertebra.telemetry.genai import (
        CostTable,
        GenAiUsage,
        HistogramBucket,
        HistogramSnapshot,
        ModelPricing,
        extract_w3c_trace_context,
        inject_w3c_trace_context,
    )
    from vertebra.telemetry.logger import (
        LoggingConfig,
        LogHandler,
        LogOverflowPolicy,
        LogSink,
    )
    from vertebra.telemetry.otel import (
        EventLoopMetricSample,
        EventLoopRecorder,
        GenAiMetricSample,
        GenAiRecorder,
        SpanData,
        SpanExporter,
        TraceContext,
    )

__all__ = (
    "CostTable",
    "EventLoopMetricSample",
    "EventLoopRecorder",
    "GenAiMetricSample",
    "GenAiRecorder",
    "GenAiUsage",
    "HistogramBucket",
    "HistogramSnapshot",
    "LogHandler",
    "LogOverflowPolicy",
    "LogSink",
    "LoggingConfig",
    "ModelPricing",
    "SpanData",
    "SpanExporter",
    "SqlTracePropagator",
    "TraceContext",
    "extract_w3c_trace_context",
    "inject_w3c_trace_context",
)

__lazy_modules__ = [
    "vertebra.telemetry.db_propagation",
    "vertebra.telemetry.genai",
    "vertebra.telemetry.logger",
    "vertebra.telemetry.otel",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "CostTable": ("vertebra.telemetry.genai", "CostTable"),
    "EventLoopMetricSample": ("vertebra.telemetry.otel", "EventLoopMetricSample"),
    "EventLoopRecorder": ("vertebra.telemetry.otel", "EventLoopRecorder"),
    "GenAiMetricSample": ("vertebra.telemetry.otel", "GenAiMetricSample"),
    "GenAiRecorder": ("vertebra.telemetry.otel", "GenAiRecorder"),
    "GenAiUsage": ("vertebra.telemetry.genai", "GenAiUsage"),
    "HistogramBucket": ("vertebra.telemetry.genai", "HistogramBucket"),
    "HistogramSnapshot": ("vertebra.telemetry.genai", "HistogramSnapshot"),
    "LogHandler": ("vertebra.telemetry.logger", "LogHandler"),
    "LogOverflowPolicy": ("vertebra.telemetry.logger", "LogOverflowPolicy"),
    "LogSink": ("vertebra.telemetry.logger", "LogSink"),
    "LoggingConfig": ("vertebra.telemetry.logger", "LoggingConfig"),
    "ModelPricing": ("vertebra.telemetry.genai", "ModelPricing"),
    "SpanData": ("vertebra.telemetry.otel", "SpanData"),
    "SpanExporter": ("vertebra.telemetry.otel", "SpanExporter"),
    "SqlTracePropagator": ("vertebra.telemetry.db_propagation", "SqlTracePropagator"),
    "TraceContext": ("vertebra.telemetry.otel", "TraceContext"),
    "extract_w3c_trace_context": ("vertebra.telemetry.genai", "extract_w3c_trace_context"),
    "inject_w3c_trace_context": ("vertebra.telemetry.genai", "inject_w3c_trace_context"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
