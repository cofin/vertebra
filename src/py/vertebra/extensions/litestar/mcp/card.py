"""Well-known agent card offload metadata and response handler."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import msgspec
from litestar.response import Response

from vertebra.agent.mcp.routes import render_well_known_mcp_json
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from litestar.connection import ASGIConnection


class WellKnownAgentCardOffload(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Offload specification and pre-serialized bytes for /.well-known/agent-card.json."""

    path: str = "/.well-known/agent-card.json"
    body_bytes: bytes
    etag: str
    max_age: int = 300

    @classmethod
    def from_card(
        cls,
        card: Any,
        *,
        path: str = "/.well-known/agent-card.json",
        max_age: int = 300,
    ) -> WellKnownAgentCardOffload:
        """Construct an offload descriptor from an AgentCard object or dictionary."""
        ensure_module("litestar_mcp", extra="mcp")
        if hasattr(card, "model_dump_json"):
            raw_json = card.model_dump_json()
            payload = raw_json.encode("utf-8") if isinstance(raw_json, str) else bytes(raw_json)
        elif isinstance(card, (bytes, bytearray)):
            payload = bytes(card)
        elif isinstance(card, str):
            payload = card.encode("utf-8")
        else:
            payload = encode_json_bytes(card if isinstance(card, dict) else getattr(card, "__dict__", {}))

        digest = hashlib.sha256(payload).hexdigest()
        etag = f'"{digest}"'
        return cls(path=path, body_bytes=payload, etag=etag, max_age=max_age)

    @classmethod
    def from_mcp_config(
        cls,
        config: Any,
        registry: Any = None,
        *,
        tool_names: Iterable[str] | None = None,
        path: str = "/.well-known/mcp.json",
        max_age: int = 300,
    ) -> WellKnownAgentCardOffload:
        """Construct an offload descriptor for canonical `/.well-known/mcp.json` discovery."""
        ensure_module("litestar_mcp", extra="mcp")
        payload = render_well_known_mcp_json(config, registry, tool_names=tool_names)
        digest = hashlib.sha256(payload).hexdigest()
        etag = f'"{digest}"'
        return cls(path=path, body_bytes=payload, etag=etag, max_age=max_age)

    def matches_if_none_match(self, connection: ASGIConnection[Any, Any, Any, Any]) -> bool:
        """Return True if the incoming request If-None-Match header matches this card's ETag."""
        header_val = connection.headers.get("if-none-match")
        if not header_val:
            return False
        return header_val.strip() in {self.etag, f"W/{self.etag}", "*"}

    def create_response(self, connection: ASGIConnection[Any, Any, Any, Any]) -> Response[bytes]:
        """Create an HTTP 200 or 304 response with ETag and Cache-Control headers."""
        ensure_module("litestar_mcp", extra="mcp")

        headers = {
            "etag": self.etag,
            "cache-control": f"public, max-age={self.max_age}",
        }
        if self.matches_if_none_match(connection):
            return Response(b"", status_code=304, headers=headers)
        return Response(
            self.body_bytes,
            status_code=200,
            media_type="application/json",
            headers=headers,
        )
