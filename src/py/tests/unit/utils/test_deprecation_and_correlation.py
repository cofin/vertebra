"""Unit tests for vertebra.utils.deprecation and vertebra.utils.correlation."""

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any

import pytest

from vertebra.utils.correlation import (
    CorrelationContext,
    CorrelationLoggerAdapter,
    correlation_context,
    get_correlation_adapter,
)
from vertebra.utils.deprecation import deprecated, warn_deprecation

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _reset_correlation_context() -> Iterator[None]:
    """Ensure CorrelationContext is clean before and after each test."""
    CorrelationContext.clear()
    yield
    CorrelationContext.clear()


@pytest.fixture
def captured_warnings() -> Iterator[list[warnings.WarningMessage]]:
    """Capture all warnings emitted within a test body."""
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        yield recorded


@pytest.fixture
def correlation_logger_adapter() -> CorrelationLoggerAdapter:
    """Provide a CorrelationLoggerAdapter bound to the test logger."""
    base_logger = logging.getLogger("vertebra.test.correlation")
    return get_correlation_adapter(base_logger)


@pytest.mark.parametrize(
    ("pending", "expected_category", "alternative", "details"),
    [
        (False, DeprecationWarning, "new_helper", "Removed in v2.0."),
        (True, PendingDeprecationWarning, None, None),
    ],
)
def test_warn_deprecation_and_sync_decorator(
    captured_warnings: list[warnings.WarningMessage],
    pending: bool,
    expected_category: type[Warning],
    alternative: str | None,
    details: str | None,
) -> None:
    """Verify warn_deprecation and @deprecated emit the expected warning category and message."""
    @deprecated("1.2.0", alternative=alternative, details=details, pending=pending)
    def old_func(x: int) -> int:
        return x + 1

    assert old_func(5) == 6
    assert len(captured_warnings) == 1
    assert issubclass(captured_warnings[0].category, expected_category)
    msg = str(captured_warnings[0].message)
    assert "old_func" in msg
    assert "1.2.0" in msg
    if alternative:
        assert alternative in msg
    if details:
        assert details in msg


@pytest.mark.anyio
async def test_deprecated_async_function_and_class(
    captured_warnings: list[warnings.WarningMessage],
) -> None:
    """Verify @deprecated preserves async coroutines and class instantiation."""
    @deprecated("1.3.0", alternative="async_new")
    async def async_old(val: str) -> str:
        return f"ok:{val}"

    @deprecated("1.3.0", alternative="NewService", kind="class")
    class OldService:
        def __init__(self, name: str) -> None:
            self.name = name

    assert await async_old("vtb") == "ok:vtb"
    svc = OldService("core")
    assert svc.name == "core"
    assert len(captured_warnings) == 2

    warn_deprecation("1.4.0", deprecated_name="legacy_flag", kind="parameter")
    assert len(captured_warnings) == 3
    assert "legacy_flag" in str(captured_warnings[2].message)


@pytest.mark.parametrize(
    ("headers", "expected_id"),
    [
        ({"X-Correlation-ID": "corr-123"}, "corr-123"),
        ({"x-request-id": "req-456"}, "req-456"),
        ({"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}, "4bf92f3577b34da6a3ce929d0e0e4736"),
        ([(b"x-correlation-id", b"bytes-789")], "bytes-789"),
        ({}, None),
    ],
)
def test_correlation_context_extract_from_headers(
    headers: Any,
    expected_id: str | None,
) -> None:
    """Verify CorrelationContext.extract_from_headers across mapping and ASGI raw headers."""
    assert CorrelationContext.extract_from_headers(headers) == expected_id


def test_correlation_context_lifecycle_and_logger_adapter(
    correlation_logger_adapter: CorrelationLoggerAdapter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify CorrelationContext context manager, auto-generation, headers, and logger adapter."""
    assert CorrelationContext.get() is None
    assert CorrelationContext.to_headers() == {}

    generated = CorrelationContext.generate()
    assert len(generated) == 36

    with correlation_context("outer-id") as outer:
        assert outer == "outer-id"
        assert CorrelationContext.get() == "outer-id"
        assert CorrelationContext.to_headers() == {"x-correlation-id": "outer-id"}

        with CorrelationContext.context(auto_generate=True) as inner:
            assert inner is not None
            assert inner != "outer-id"
            assert CorrelationContext.get() == inner

        assert CorrelationContext.get() == "outer-id"

        with caplog.at_level(logging.INFO, logger="vertebra.test.correlation"):
            correlation_logger_adapter.info("hello correlation")
        assert len(caplog.records) == 1
        assert caplog.records[0].getMessage() == "[correlation_id=outer-id] hello correlation"
        assert getattr(caplog.records[0], "correlation_id", None) == "outer-id"

    assert CorrelationContext.get() is None
