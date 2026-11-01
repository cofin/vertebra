"""Streamable HTTP and SSE route handler for Agent-to-Agent (A2A) 1.0.

This module provides A2AHttpHandler for serving /.well-known/agent-card.json
with ETag 304 caching and POST /a2a with unary or text/event-stream dispatch.
"""

import hashlib
from collections.abc import AsyncIterable, Mapping
from typing import Any

from vertebra.agent.a2a.service import A2AConfig, A2AService
from vertebra.agent.core.context import AgentContext
from vertebra.utils.serializers import decode_json


class A2AHttpHandler:
    """Streamable HTTP route handler coordinating validation, caching, and dispatch for A2A 1.0."""

    def __init__(self, service: A2AService) -> None:
        self.service = service
        self.config: A2AConfig = service.config

    def _match_etag(self, if_none_match: str | None, current_etag: str) -> bool:
        if not if_none_match:
            return False
        tokens = [t.strip() for t in if_none_match.split(",")]
        normalized_current = current_etag.strip('"')
        for token in tokens:
            if token == "*":
                return True
            raw = token.removeprefix("W/").removeprefix("w/").strip('"')
            if raw == normalized_current:
                return True
        return False

    def register_in_discovery_cache(self, cache: Any) -> None:
        """Register the AgentCard in Rust WellKnownDiscoveryCache when URLs are static."""
        if self.config.agent_card is None:
            return
        if self.config.base_url is None and not self.config.agent_card.has_explicit_interfaces():
            return
        resolved_card = self.service.resolve_agent_card()
        if resolved_card is None:
            return
        card_bytes = resolved_card.to_json_bytes()
        if hasattr(cache, "register_agent_card"):
            cache.register_agent_card(card_bytes)
        elif hasattr(cache, "register_document"):
            cache.register_document(
                self.config.agent_card_path,
                "application/json",
                ("a2a-version", "1.0"),
                card_bytes,
            )

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        context: AgentContext | None = None,
    ) -> tuple[int, dict[str, str], bytes | AsyncIterable[bytes]]:
        """Process incoming HTTP request for agent card discovery or JSON-RPC dispatch."""
        lower_headers = {k.lower(): v for k, v in headers.items()}
        normalized_path = "/" + path.strip("/")

        if method == "OPTIONS":
            cors_headers = {
                "Access-Control-Allow-Origin": lower_headers.get("origin") or "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, A2A-Version, If-None-Match, Authorization, Last-Event-ID",
            }
            return 204, cors_headers, b""

        agent_card_path = "/" + self.config.agent_card_path.strip("/")
        if method == "GET" and (
            normalized_path == agent_card_path or normalized_path == "/.well-known/agent-card.json"
        ):
            resolved_card = self.service.resolve_agent_card(headers=lower_headers)
            if resolved_card is None:
                return 404, {"Content-Type": "text/plain"}, b"AgentCard not configured"
            card_bytes = resolved_card.to_json_bytes()
            etag_hash = hashlib.sha256(card_bytes).hexdigest()[:16]
            current_etag = f'"{etag_hash}"'

            if self._match_etag(lower_headers.get("if-none-match"), current_etag):
                return 304, {"ETag": current_etag}, b""

            resp_headers = {
                "Content-Type": "application/json",
                "ETag": current_etag,
                "A2A-Version": "1.0",
                "Cache-Control": "public, max-age=300",
                "Vary": "Host, X-Forwarded-Host, X-Forwarded-Proto, Forwarded",
            }
            return 200, resp_headers, card_bytes

        service_path = "/" + self.config.path.strip("/")
        if method == "POST" and normalized_path == service_path:
            try:
                parsed = decode_json(body)
            except Exception:
                resp_bytes = await self.service.handle_payload(body, context)
                return 200, {"Content-Type": "application/json", "A2A-Version": "1.0"}, resp_bytes

            if isinstance(parsed, dict) and parsed.get("method") == "message/stream":
                params = parsed.get("params") or {}

                async def _stream_message_bytes() -> AsyncIterable[bytes]:
                    async for frame in self.service.stream_message(params, context):
                        yield frame.encode()

                sse_headers = {
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "A2A-Version": "1.0",
                }
                return 200, sse_headers, _stream_message_bytes()

            if isinstance(parsed, dict) and parsed.get("method") == "tasks/resubscribe":
                params = parsed.get("params") or {}
                if "lastEventId" not in params and "last-event-id" in lower_headers:
                    params["lastEventId"] = lower_headers["last-event-id"]

                async def _stream_resubscribe_bytes() -> AsyncIterable[bytes]:
                    async for frame in self.service.stream_resubscribe(params, context):
                        yield frame.encode()

                sse_headers = {
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "A2A-Version": "1.0",
                }
                return 200, sse_headers, _stream_resubscribe_bytes()

            resp_bytes = await self.service.handle_payload(body, context)
            return 200, {"Content-Type": "application/json", "A2A-Version": "1.0"}, resp_bytes

        return 404, {"Content-Type": "text/plain"}, b"Not Found"


__all__ = [
    "A2AHttpHandler",
]
