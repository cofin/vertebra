"""Shared memory test helpers for isolation and cleanup."""

import contextlib
import os
import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator


def unique_shm_name(prefix: str = "vertebra-test") -> str:
    """Generate a unique shared memory name for test isolation.

    Uses UUID4 for guaranteed uniqueness across parallel test workers,
    repeated test runs, and multiple test sessions.

    Args:
        prefix: Name prefix. Default "vertebra-test" ensures cleanup
                patterns match.

    Returns:
        Unique name like "vertebra-test-abc12345"

    Example:
        >>> name = unique_shm_name("my-test")
        >>> name.startswith("my-test-")
        True
        >>> unique_shm_name("my-test") != unique_shm_name("my-test")
        True
    """
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def skip_if_shm_unavailable(*, require_shm_lib: bool = True) -> None:
    """Skip tests when shared memory is unavailable or not writable.

    Args:
        require_shm_lib: When True, require the PyO3 shm bindings to be available.
            When False, only validate /dev/shm access.
    """
    if require_shm_lib:
        try:
            from vertebra.typing import SpscRing  # noqa: F401
        except ImportError as exc:
            pytest.skip(f"Shared memory bindings unavailable: {exc}")

    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        pytest.skip("/dev/shm not available")
    if not os.access(shm_dir, os.W_OK | os.X_OK):
        pytest.skip("/dev/shm not writable in this environment")

    try:
        with tempfile.NamedTemporaryFile(dir=shm_dir, prefix="vertebra-test-", delete=True):
            pass
    except OSError as exc:
        pytest.skip(f"/dev/shm not usable: {exc}")


@contextlib.contextmanager
def tracked_shm_cleanup(initial: "list[str] | None" = None) -> "Generator[list[str], None, None]":
    """Context manager that tracks and cleans up shared memory names.

    Records all shared memory names appended to the yielded list and
    ensures they are unlinked when the context exits, even on exception.

    Args:
        initial: Optional list of shared memory names to pre-populate.
                 Useful when names are known upfront.

    Yields:
        List to append created shared memory names to. Each name should
        include the leading "/" required by shm_open().

    Example:
        # Pattern 1: Pre-populated names
        with tracked_shm_cleanup(["/test-ring"]) as names:
            ring = SpscRing.create_named(...)
            # Test code...

        # Pattern 2: Dynamic tracking
        with tracked_shm_cleanup() as names:
            name = f"/{unique_shm_name()}.control"
            ring = SpscRing.create_named(name, ...)
            names.append(name)
            # Test code...
        # Ring is automatically unlinked here
    """
    created: list[str] = list(initial) if initial else []
    try:
        yield created
    finally:
        # Import late to handle case where bindings unavailable
        try:
            from vertebra.typing import shm_unlink

            # Unlink each tracked name
            for name in created:
                with contextlib.suppress(Exception):
                    shm_unlink(name)
        except ImportError:
            pass

        # Also clean up any files in /dev/shm that match the names
        # This handles the case where shm_unlink fails but file exists
        shm_dir = Path("/dev/shm")
        if shm_dir.is_dir():
            for name in created:
                clean_name = name.lstrip("/")
                for path in shm_dir.glob(f"{clean_name}*"):
                    with contextlib.suppress(OSError):
                        path.unlink()
