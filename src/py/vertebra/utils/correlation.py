"""Request and task correlation ID propagation across ASGI, gRPC, queues, and structured logs."""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, ClassVar

from vertebra.utils.uuids import uuid7

__all__ = (
    "CorrelationContext",
    "CorrelationLoggerAdapter",
    "correlation_context",
    "get_correlation_adapter",
)

_CORRELATION_ID_VAR: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "vertebra_correlation_id",
    default=None,
)


class CorrelationLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """Logging adapter that injects the active ``correlation_id`` into log record ``extra`` fields."""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        """Attach ``correlation_id`` to ``kwargs['extra']`` and prefix the formatted message when active."""
        cid = CorrelationContext.get() or (self.extra.get("correlation_id") if isinstance(self.extra, Mapping) else None)
        extra = dict(kwargs.get("extra") or {})
        extra["correlation_id"] = cid
        kwargs["extra"] = extra
        if cid:
            return f"[correlation_id={cid}] {msg}", kwargs
        return msg, kwargs


class CorrelationContext:
    """ContextVar-backed correlation ID manager for HTTP headers, gRPC metadata, and background jobs."""

    DEFAULT_HEADERS: ClassVar[tuple[str, ...]] = (
        "x-correlation-id",
        "x-request-id",
        "x-amzn-trace-id",
        "x-cloud-trace-context",
        "traceparent",
    )

    __slots__ = ()

    @staticmethod
    def get() -> str | None:
        """Return the active correlation ID for the current execution context, or ``None``."""
        return _CORRELATION_ID_VAR.get()

    @staticmethod
    def set(correlation_id: str | None) -> contextvars.Token[str | None]:
        """Set the correlation ID for the current execution context and return the reset token."""
        return _CORRELATION_ID_VAR.set(correlation_id)

    @staticmethod
    def clear() -> None:
        """Clear the correlation ID in the current execution context."""
        _CORRELATION_ID_VAR.set(None)

    @staticmethod
    def generate() -> str:
        """Generate a new monotonic RFC 9562 UUIDv7 correlation ID string."""
        return str(uuid7())

    @classmethod
    @contextmanager
    def context(
        cls,
        correlation_id: str | None = None,
        *,
        auto_generate: bool = True,
    ) -> Iterator[str | None]:
        """Enter a scoped correlation ID context and restore the previous ID on exit."""
        resolved_id = correlation_id
        if resolved_id is None and auto_generate:
            resolved_id = cls.generate()
        token = _CORRELATION_ID_VAR.set(resolved_id)
        try:
            yield resolved_id
        finally:
            _CORRELATION_ID_VAR.reset(token)

    @classmethod
    def to_headers(cls, header_name: str = "x-correlation-id") -> dict[str, str]:
        """Return a single-entry header dictionary when a correlation ID is active, or ``{}``."""
        current = cls.get()
        if not current:
            return {}
        return {header_name: current}

    @classmethod
    def extract_from_headers(
        cls,
        headers: Mapping[str, str] | Sequence[tuple[bytes | str, bytes | str]],
        header_names: tuple[str, ...] = DEFAULT_HEADERS,
    ) -> str | None:
        """Extract a correlation or trace ID from HTTP/gRPC headers or ASGI raw header pairs."""
        normalized_map: dict[str, str] = {}
        if isinstance(headers, Mapping):
            for k, v in headers.items():
                if v:
                    normalized_map[str(k).lower()] = str(v).strip()
        else:
            for raw_k, raw_v in headers:
                key_str = raw_k.decode("latin-1") if isinstance(raw_k, (bytes, bytearray)) else str(raw_k)
                val_str = raw_v.decode("latin-1") if isinstance(raw_v, (bytes, bytearray)) else str(raw_v)
                if val_str:
                    normalized_map.setdefault(key_str.lower(), val_str.strip())

        for candidate in header_names:
            lowered = candidate.lower()
            raw_val = normalized_map.get(lowered)
            if not raw_val:
                continue
            if lowered == "traceparent":
                parts = raw_val.split("-")
                if len(parts) >= 2 and len(parts[1]) == 32:
                    return parts[1]
            if lowered == "x-cloud-trace-context" and "/" in raw_val:
                return raw_val.split("/", 1)[0].strip()
            return raw_val
        return None


@contextmanager
def correlation_context(
    correlation_id: str | None = None,
    *,
    auto_generate: bool = True,
) -> Iterator[str | None]:
    """Convenience context manager alias for :meth:`CorrelationContext.context`."""
    with CorrelationContext.context(correlation_id, auto_generate=auto_generate) as active_id:
        yield active_id


def get_correlation_adapter(
    logger: logging.Logger,
    extra: Mapping[str, Any] | None = None,
) -> CorrelationLoggerAdapter:
    """Wrap ``logger`` in a :class:`CorrelationLoggerAdapter` that injects the active correlation ID."""
    return CorrelationLoggerAdapter(logger, dict(extra or {}))
