"""JSON-RPC frame classification accelerated by Rust runtime."""

from __future__ import annotations

from vertebra._vertebra import classify_jsonrpc_frame
from vertebra.utils.module_loader import ensure_module


class JSONRPCFrameAccelerator:
    """Classifies incoming JSON-RPC frames using zero-allocation Rust parser."""

    __slots__ = ()

    def __init__(self) -> None:
        """Initialize the JSON-RPC frame accelerator."""
        ensure_module("litestar_mcp", extra="mcp")

    @staticmethod
    def classify(payload: bytes) -> tuple[int, str | None, str | None]:
        """Classify payload into (frame_kind, method, id) without Python object allocation."""
        return classify_jsonrpc_frame(payload)

    @staticmethod
    def is_call(payload: bytes) -> bool:
        """Return True if the JSON-RPC payload is a request call with an id."""
        frame_kind, _, msg_id = classify_jsonrpc_frame(payload)
        return frame_kind == 1 and msg_id is not None

    @staticmethod
    def is_notification(payload: bytes) -> bool:
        """Return True if the JSON-RPC payload is a notification without an id."""
        frame_kind, _, msg_id = classify_jsonrpc_frame(payload)
        return frame_kind == 0 or msg_id is None

    @staticmethod
    def is_batch(payload: bytes) -> bool:
        """Return True if the JSON-RPC payload is a batch array rejected in MCP 2026-07-28."""
        return payload.lstrip().startswith(b"[")
