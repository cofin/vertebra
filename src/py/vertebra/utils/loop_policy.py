"""Event loop policy management and diagnostics.

Provides uvloop auto-configuration, mismatch detection, and diagnostic
snapshots.
"""

import asyncio
import logging
import os
import sys
import threading
from typing import Any

from vertebra.utils.module_loader import module_available

__all__ = (
    "LoopMismatchError",
    "check_loop_policy_match",
    "check_loop_thread_affinity",
    "check_nested_loop",
    "check_running_loop",
    "configure_event_loop_policy",
    "diagnose_loop_state",
    "get_current_loop_policy_name",
    "is_uvloop_disabled_by_env",
    "is_uvloop_supported",
)

logger = logging.getLogger("vertebra.utils.loop_policy")

_UVLOOP_ENV_KEY = "VERTEBRA_UVLOOP"
_DISABLE_VALUES = frozenset({"0", "false", "no", "off", "disable", "disabled"})


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class LoopMismatchError(RuntimeError):
    """Raised when an event loop mismatch is detected."""

    def __init__(self, message: str, *, suggestion: str = "") -> None:
        full = f"{message}\n  Suggestion: {suggestion}" if suggestion else message
        super().__init__(full)
        self.suggestion = suggestion


# ---------------------------------------------------------------------------
# Policy helpers
# ---------------------------------------------------------------------------


def is_uvloop_supported() -> bool:
    """Return ``True`` if uvloop is installed and the platform supports it."""
    if sys.platform == "win32":
        return False
    return module_available("uvloop")


def is_uvloop_disabled_by_env() -> bool:
    """Return ``True`` if the user explicitly disabled uvloop via env."""
    value = os.environ.get(_UVLOOP_ENV_KEY, "").strip().lower()
    return value in _DISABLE_VALUES


def configure_event_loop_policy() -> bool:
    """Configure the event loop policy, preferring uvloop when available.

    Returns ``True`` if uvloop was activated, ``False`` otherwise.
    Never raises.
    """
    if is_uvloop_disabled_by_env():
        logger.debug("uvloop disabled by %s environment variable", _UVLOOP_ENV_KEY)
        return False

    if not is_uvloop_supported():
        if sys.platform == "win32":
            logger.debug("uvloop not supported on Windows, using default policy")
        elif not module_available("uvloop"):
            logger.debug("uvloop not installed; install with: pip install vertebra[uvloop]")
        return False

    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("uvloop event loop policy activated")
    except Exception:
        logger.warning("Failed to activate uvloop, falling back to default policy", exc_info=True)
        return False
    else:
        return True


def get_current_loop_policy_name() -> str:
    """Return a human-readable name for the current event loop policy."""
    policy = asyncio.get_event_loop_policy()
    cls = type(policy)
    module = cls.__module__ or ""
    if "uvloop" in module:
        return "uvloop"
    if "asyncio" in module:
        return "asyncio (default)"
    return f"{module}.{cls.__qualname__}"


# ---------------------------------------------------------------------------
# Diagnostic checks
# ---------------------------------------------------------------------------


def check_running_loop() -> asyncio.AbstractEventLoop | None:
    """Return the running event loop, or ``None`` if not inside one."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def check_nested_loop() -> None:
    """Raise :class:`LoopMismatchError` if called inside a running loop."""
    loop = check_running_loop()
    if loop is not None:
        msg = (
            f"Cannot start a new event loop because one is already running "
            f"in this thread ({threading.current_thread().name}). "
            f"This typically happens when sync code calls asyncio.run() "
            f"from within an async handler."
        )
        raise LoopMismatchError(
            msg, suggestion="Use 'await' instead of asyncio.run(), or run the sync code in a separate thread."
        )


def check_loop_thread_affinity(expected_loop: asyncio.AbstractEventLoop) -> None:
    """Verify *expected_loop* is the one running in the current thread."""
    current = check_running_loop()
    if current is None:
        msg = (
            f"No event loop running in thread {threading.current_thread().name!r}. "
            f"Database operations must be called from within an async context."
        )
        raise LoopMismatchError(msg, suggestion="Ensure this code is called from an async function with 'await'.")
    if current is not expected_loop:
        msg = (
            f"Event loop mismatch: expected loop {id(expected_loop):#x} "
            f"but thread {threading.current_thread().name!r} is running loop {id(current):#x}."
        )
        raise LoopMismatchError(msg, suggestion="Ensure provider and request handler share the same event loop.")


def check_loop_policy_match(provider_policy_name: str) -> None:
    """Warn if the current loop policy differs from the provider's."""
    current = get_current_loop_policy_name()
    if current != provider_policy_name:
        logger.warning(
            "Event loop policy mismatch: provider uses %r but client uses %r.", provider_policy_name, current
        )


def diagnose_loop_state() -> dict[str, Any]:
    """Return a diagnostic snapshot of the current event loop state."""
    current = check_running_loop()
    return {
        "thread": threading.current_thread().name,
        "loop_policy": get_current_loop_policy_name(),
        "has_running_loop": current is not None,
        "loop_id": f"{id(current):#x}" if current else None,
        "loop_is_closed": current.is_closed() if current else None,
    }
