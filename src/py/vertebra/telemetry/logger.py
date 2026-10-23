"""Non-blocking GIL-releasing structured logger and Litestar LoggingConfig bridge."""

import logging
import time
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, cast

import msgspec

from vertebra._vertebra import LogSink as _CoreLogSink
from vertebra.telemetry.otel import TraceContext
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "LogHandler",
    "LogOverflowPolicy",
    "LogSink",
    "LoggingConfig",
)

type LogOverflowPolicy = Literal["drop_oldest", "drop_newest", "block"]


class _CallableLogFormatter(logging.Formatter):
    """Standard library logging.Formatter that is also callable on event dicts."""

    def __call__(self, record: dict[str, Any] | logging.LogRecord) -> str:
        """Format a structured event dictionary or logging.LogRecord into a string."""
        if isinstance(record, logging.LogRecord):
            return self.format(record)
        level_name = str(record.get("level", "INFO")).upper()
        level_no = getattr(logging, level_name, logging.INFO)
        log_record = logging.LogRecord(
            name=str(record.get("logger", "vertebra")),
            level=level_no,
            pathname="",
            lineno=0,
            msg=str(record.get("message", record.get("event", ""))),
            args=(),
            exc_info=None,
        )
        return self.format(log_record)


class LoggingConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Vertebra's non-blocking structured and access logging."""

    level: str = "INFO"
    json_format: bool = True
    queue_capacity: int = 8192
    overflow_policy: LogOverflowPolicy = "drop_oldest"
    access_log_enabled: bool = True
    access_log_format: str = '%h - - "%m %U %H" %s %b "%R" "%A" %D'

    @classmethod
    def from_pyproject(cls, pyproject_path: str | Path = "pyproject.toml") -> "LoggingConfig":
        """Load [tool.vertebra.logging] settings from pyproject.toml."""
        path = Path(pyproject_path)
        if not path.is_file():
            return cls()
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        section = raw.get("tool", {}).get("vertebra", {}).get("logging", {})
        if not isinstance(section, dict):
            return cls()
        raw_policy = str(section.get("overflow_policy", "drop_oldest"))
        policy: LogOverflowPolicy = (
            cast("LogOverflowPolicy", raw_policy)
            if raw_policy in ("drop_oldest", "drop_newest", "block")
            else "drop_oldest"
        )
        return cls(
            level=str(section.get("level", "INFO")).upper(),
            json_format=bool(section.get("json_format", True)),
            queue_capacity=int(section.get("queue_capacity", 8192)),
            overflow_policy=policy,
            access_log_enabled=bool(section.get("access_log_enabled", True)),
            access_log_format=str(section.get("access_log_format", '%h - - "%m %U %H" %s %b "%R" "%A" %D')),
        )

    @staticmethod
    def extract_litestar_formatter(
        logging_config: Any,
    ) -> Callable[[dict[str, Any]], str] | None:
        """Reconstruct the active formatter from a Litestar LoggingConfig."""
        if logging_config is None:
            return None
        formatter_fn = getattr(logging_config, "formatter", None)
        if callable(formatter_fn):
            return cast("Callable[[dict[str, Any]], str]", formatter_fn)
        formatters = getattr(logging_config, "formatters", None)
        if isinstance(formatters, dict):
            std_cfg = formatters.get("standard") or next(iter(formatters.values()), None)
            if isinstance(std_cfg, dict):
                fmt = std_cfg.get("format", "%(levelname)s - %(asctime)s - %(name)s - %(message)s")
                datefmt = std_cfg.get("datefmt")
                return _CallableLogFormatter(fmt=str(fmt), datefmt=datefmt)
        return _CallableLogFormatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")

    def configure_stdlib(self, root_logger_name: str = "") -> "LogHandler":
        """Configure standard library logging to emit through LogSink off-GIL."""
        sink = LogSink(
            capacity=self.queue_capacity,
            overflow_policy=self.overflow_policy,
            target="stdout",
        )
        handler = LogHandler(sink)
        root = logging.getLogger(root_logger_name)
        root.setLevel(getattr(logging, self.level.upper(), logging.INFO))
        root.addHandler(handler)
        return handler

    def to_litestar_config(self) -> Any:
        """Create a Litestar-compatible logging configuration dictionary."""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(levelname)s - %(name)s - %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "class": "vertebra.telemetry.logger.LogHandler",
                    "level": self.level,
                },
            },
            "loggers": {
                "_root": {
                    "handlers": ["default"],
                    "level": self.level,
                },
            },
        }


class LogSink:
    """Bounded non-blocking byte queue backed by vertebra._vertebra.LogSink."""

    def __init__(
        self,
        capacity: int = 8192,
        overflow_policy: LogOverflowPolicy = "drop_oldest",
        target: str = "memory",
        flush_interval_ms: int = 100,
    ) -> None:
        self.capacity = max(1, capacity)
        self.overflow_policy: LogOverflowPolicy = overflow_policy
        self._target = target
        self._flush_interval_ms = flush_interval_ms
        self._native = _CoreLogSink(
            target=target,
            capacity=self.capacity,
            overflow_policy=overflow_policy,
            flush_interval_ms=flush_interval_ms,
        )

    @property
    def enqueued_count(self) -> int:
        """Return the number of enqueued log records."""
        return self._native.metrics()[0]

    @property
    def dropped_count(self) -> int:
        """Return the number of dropped log records."""
        return self._native.dropped_count()

    def write_bytes(self, payload: bytes) -> bool:
        """Enqueue pre-encoded UTF-8 JSON bytes without blocking."""
        try:
            self._native.emit(payload)
            return True
        except RuntimeError:
            return False

    def drain(self) -> list[bytes]:
        """Drain all queued byte payloads."""
        return self._native.drain()

    def flush(self) -> None:
        """Flush the native logger sink."""
        self._native.flush()

    def build_structlog_processor(
        self,
        trace_context_getter: Callable[[], TraceContext | tuple[str | None, str | None] | None] | None = None,
    ) -> Callable[[Any, str, dict[str, Any]], bytes]:
        """Build a final structlog processor that encodes event dicts directly to UTF-8 JSON bytes."""
        getter = trace_context_getter or TraceContext.current

        def _processor(_logger: Any, method_name: str, event_dict: dict[str, Any]) -> bytes:
            event_dict.setdefault("level", method_name.upper())
            event_dict.setdefault("timestamp", time.time())
            ctx = getter()
            if isinstance(ctx, TraceContext):
                event_dict.setdefault("trace_id", ctx.trace_id)
                event_dict.setdefault("span_id", ctx.span_id)
            elif isinstance(ctx, tuple) and len(ctx) == 2:
                trace_id, span_id = ctx
                if trace_id is not None:
                    event_dict.setdefault("trace_id", trace_id)
                if span_id is not None:
                    event_dict.setdefault("span_id", span_id)
            return encode_json_bytes(event_dict)

        return _processor

    def build_structlog_sink(self) -> Callable[[bytes | str | dict[str, Any]], bool]:
        """Build a zero-round-trip structlog sink that accepts pre-encoded UTF-8 JSON bytes directly."""

        def _write(event: bytes | str | dict[str, Any]) -> bool:
            if isinstance(event, bytes):
                return self.write_bytes(event)
            if isinstance(event, str):
                return self.write_bytes(event.encode("utf-8"))
            return self.write_bytes(encode_json_bytes(event))

        return _write


class LogHandler(logging.Handler):
    """Non-blocking logging.Handler encoding log records via msgspec.json."""

    def __init__(
        self,
        sink: LogSink | None = None,
        *,
        trace_context_getter: Callable[[], TraceContext | tuple[str | None, str | None] | None] | None = None,
    ) -> None:
        super().__init__()
        self.sink = sink or LogSink()
        self._trace_context_getter = trace_context_getter or TraceContext.current

    def emit(self, record: logging.LogRecord) -> None:
        """Encode record to UTF-8 JSON bytes and enqueue into self.sink."""
        trace_id: str | None = getattr(record, "trace_id", None)
        span_id: str | None = getattr(record, "span_id", None)
        if (trace_id is None or span_id is None) and self._trace_context_getter is not None:
            ctx = self._trace_context_getter()
            if isinstance(ctx, TraceContext):
                trace_id = trace_id or ctx.trace_id
                span_id = span_id or ctx.span_id
            elif isinstance(ctx, tuple) and len(ctx) == 2:
                t_id, s_id = ctx
                trace_id = trace_id or t_id
                span_id = span_id or s_id
        payload: dict[str, Any] = {
            "timestamp": record.created,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if trace_id is not None:
            payload["trace_id"] = trace_id
        if span_id is not None:
            payload["span_id"] = span_id
        self.sink.write_bytes(encode_json_bytes(payload))
