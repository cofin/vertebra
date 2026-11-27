"""Opaque URL-safe cursor pagination for agentic collections.

This module provides tamper-resistant cursor encoding and deterministic pagination
slicing across MCP tools, resources, prompts, skills, and A2A tasks.
"""

import base64
from collections.abc import Sequence

import msgspec

from vertebra.agent.core.jsonrpc import INVALID_PARAMS, JSONRPCError, JSONRPCException


class CursorPage[T](msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Immutable page of items bounded by an opaque cursor."""

    items: tuple[T, ...]
    next_cursor: str | None = None
    total_count: int | None = None


def encode_cursor(offset: int) -> str:
    """Encode an integer item offset into an opaque URL-safe cursor string."""
    if offset < 0:
        msg = f"Offset must be non-negative: {offset}"
        raise ValueError(msg)
    raw = f"offset:{offset}".encode()
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> int:
    """Decode an opaque cursor string into an integer offset.

    Raises:
        JSONRPCException: If the cursor is malformed, not valid Base64,
            or contains a negative offset.
    """
    if not cursor or not isinstance(cursor, str):
        error = JSONRPCError(code=INVALID_PARAMS, message="Invalid cursor: cursor must be non-empty")
        raise JSONRPCException(error)

    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except Exception as exc:
        error = JSONRPCError(code=INVALID_PARAMS, message=f"Invalid cursor encoding: {exc}")
        raise JSONRPCException(error) from exc

    if not raw.startswith("offset:"):
        error = JSONRPCError(code=INVALID_PARAMS, message="Invalid cursor format")
        raise JSONRPCException(error)

    try:
        offset = int(raw.split(":", 1)[1])
        if offset < 0:
            error = JSONRPCError(code=INVALID_PARAMS, message="Invalid cursor offset: cannot be negative")
            raise JSONRPCException(error)
        return offset
    except (ValueError, IndexError) as exc:
        error = JSONRPCError(code=INVALID_PARAMS, message="Invalid cursor offset value")
        raise JSONRPCException(error) from exc


def paginate_sequence[T](
    items: Sequence[T],
    cursor: str | None,
    page_size: int = 50,
) -> CursorPage[T]:
    """Slice an in-memory sequence into an opaque CursorPage.

    Args:
        items: The full sequence to paginate.
        cursor: The opaque pagination cursor from a previous page request.
        page_size: Maximum number of elements to include in this page.

    Returns:
        A CursorPage containing the current slice, next_cursor, and total_count.
    """
    if page_size < 1:
        msg = f"Page size must be at least 1: {page_size}"
        raise ValueError(msg)

    offset = 0 if cursor is None else decode_cursor(cursor)
    total = len(items)

    if offset >= total:
        return CursorPage(items=(), next_cursor=None, total_count=total)

    end = min(offset + page_size, total)
    sliced = items[offset:end]
    next_cursor = encode_cursor(end) if end < total else None

    return CursorPage(items=tuple(sliced), next_cursor=next_cursor, total_count=total)


__all__ = [
    "CursorPage",
    "decode_cursor",
    "encode_cursor",
    "paginate_sequence",
]
