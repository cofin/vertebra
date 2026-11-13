"""Unified AgentPlugin mounting agent harness on native vertebra App and Litestar.

This module provides AgentPlugin and AgentPluginConfig for mounting MCP 2026-07-28,
A2A 1.0, AgentChatController, and .well-known discovery manifests on both native
vertebra.framework.App and Litestar applications with automatic opt-attribute handler discovery.
"""

from typing import TYPE_CHECKING, Any, cast

import msgspec

from vertebra.agent.a2a.service import A2AConfig, A2AService
from vertebra.agent.controller import AgentChatController
from vertebra.agent.core.context import AgentContext
from vertebra.agent.mcp.controllers import PromptController, SkillController
from vertebra.agent.mcp.models import MCPConfig, McpHttpRequest
from vertebra.agent.mcp.registry import ToolRegistry
from vertebra.agent.mcp.routes import build_well_known_mcp_document
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.mcp.webmcp import build_webmcp_manifest
from vertebra.agent.runtime import AgentRuntime
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.framework._app import App
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from litestar import Request as LitestarRequest
    from litestar import Response as LitestarResponse

__all__ = [
    "AgentPlugin",
    "AgentPluginConfig",
]


class AgentPluginConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration options for mounting the agentic harness."""

    mcp: MCPConfig | None = msgspec.field(default_factory=MCPConfig)
    """Model Context Protocol server configuration, or `None` to disable MCP endpoints."""
    a2a: A2AConfig | None = None
    """Agent-to-Agent 1.0 protocol configuration, or `None` to disable A2A endpoints."""
    runtime: AgentRuntime | None = None
    """Pre-configured `AgentRuntime` instance executing multi-turn agent requests."""
    agent_base_path: str = "/agent"
    """HTTP route prefix for chat, turn streaming, confirmation, and session endpoints."""
    observability: AgentTelemetryRecorder | None = None
    """OpenTelemetry `gen_ai.*` span and audit recorder attached to protocol services."""
    queue_service: Any = None
    """Optional background `QueueService` for durable async turn execution."""
    session_store: Any = None
    """Optional `AgentSessionStore` backend persisting multi-turn conversation history."""


class AgentPlugin:
    """Agentic harness plugin mounting protocols on native App and Litestar."""

    def __init__(self, config: AgentPluginConfig | None = None) -> None:
        self.config = config or AgentPluginConfig()
        self.observability = self.config.observability or AgentTelemetryRecorder()
        recorder = getattr(self.observability, "recorder", self.observability)
        self.runtime = self.config.runtime or AgentRuntime(telemetry=recorder)
        self.mcp_registry = ToolRegistry(confirmation_gate=self.runtime.confirmation_gate)
        self.mcp_service = (
            MCPCore(
                registry=self.mcp_registry,
                config=self.config.mcp,
                telemetry=recorder,
                confirmation_gate=self.runtime.confirmation_gate,
            )
            if self.config.mcp is not None
            else None
        )
        self.a2a_service = (
            A2AService(
                config=self.config.a2a,
                telemetry=recorder,
                tool_registry=self.mcp_registry,
            )
            if self.config.a2a is not None
            else None
        )
        self.chat_controller = AgentChatController(
            runtime=self.runtime,
            session_store=self.config.session_store,
            base_path=self.config.agent_base_path,
            queue_service=self.config.queue_service,
        )

    def mount_native(self, app: App) -> None:
        """Mount agent routes and discovery endpoints on native vertebra App."""
        self.mcp_registry.discover_app_routes(app)
        prefix = self.config.agent_base_path.rstrip("/")

        if self.mcp_service is not None:
            srv = self.mcp_service

            @app.get(srv.config.well_known_path)
            async def _mcp_well_known() -> dict[str, Any]:
                return build_well_known_mcp_document(srv.config, srv.registry)

            if srv.config.webmcp is not None and srv.config.webmcp.enabled:
                webmcp_cfg = srv.config.webmcp

                @app.get(webmcp_cfg.manifest_path)
                async def _webmcp_well_known() -> dict[str, Any]:
                    return cast(dict[str, Any], msgspec.to_builtins(build_webmcp_manifest(srv.registry, webmcp_cfg)))

            @app.post(srv.config.path)
            async def _mcp_endpoint(
                body: bytes = b"",
                headers: Any = None,
            ) -> tuple[int, dict[str, str], bytes]:
                header_map = self._header_dict(headers)
                mcp_resp = await srv.handle(
                    McpHttpRequest(
                        method="POST",
                        path=srv.config.path,
                        headers=header_map,
                        body=body,
                        context=AgentContext.from_headers(header_map, identity_source=srv.config.identity_source),
                    )
                )
                return mcp_resp.status, mcp_resp.headers, mcp_resp.body

            @app.get(srv.config.path)
            async def _mcp_get_endpoint(
                body: bytes = b"",
                headers: Any = None,
            ) -> tuple[int, dict[str, str], bytes]:
                header_map = self._header_dict(headers)
                mcp_resp = await srv.handle(
                    McpHttpRequest(
                        method="GET",
                        path=srv.config.path,
                        headers=header_map,
                        body=body,
                        context=AgentContext.from_headers(header_map, identity_source=srv.config.identity_source),
                    )
                )
                return mcp_resp.status, mcp_resp.headers, mcp_resp.body

            @app.delete(srv.config.path)
            async def _mcp_delete_endpoint(body: bytes = b"", headers: Any = None) -> tuple[int, dict[str, str], bytes]:
                header_map = self._header_dict(headers)
                mcp_resp = await srv.handle(
                    McpHttpRequest(
                        method="DELETE",
                        path=srv.config.path,
                        headers=header_map,
                        body=body,
                        context=AgentContext.from_headers(header_map, identity_source=srv.config.identity_source),
                    )
                )
                return mcp_resp.status, mcp_resp.headers, mcp_resp.body

        if self.a2a_service is not None:
            a2a_srv = self.a2a_service

            @app.get("/.well-known/agent-card.json")
            async def _a2a_agent_card(headers: dict[str, str] | None = None) -> dict[str, Any]:
                card = a2a_srv.resolve_agent_card(headers=headers, registry=self.mcp_registry)
                if card is None:
                    return {}
                return cast(dict[str, Any], decode_json(card.to_json_bytes()))

            @app.post("/a2a")
            async def _a2a_endpoint(body: bytes) -> tuple[int, dict[str, str], bytes]:
                resp = await a2a_srv.handle_payload(body)
                return 200, {"content-type": "application/json"}, resp

        ctrl = self.chat_controller

        @app.post(f"{prefix}/chat")
        async def _chat_endpoint(body: bytes) -> Any:
            return await ctrl.handle_request("POST", f"{prefix}/chat", {}, body)

        @app.post(f"{prefix}/turns")
        async def _turns_endpoint(body: bytes) -> Any:
            return await ctrl.handle_request("POST", f"{prefix}/turns", {}, body)

        @app.post(f"{prefix}/turns/async")
        async def _turns_async_endpoint(body: bytes) -> Any:
            return await ctrl.handle_request("POST", f"{prefix}/turns/async", {}, body)

        @app.get(f"{prefix}/turns/{{turn_id}}/stream")
        async def _turns_stream_endpoint(turn_id: str, headers: dict[str, str] | None = None) -> Any:
            req_headers = headers or {}
            return await ctrl.handle_request("GET", f"{prefix}/turns/{turn_id}/stream", req_headers, b"")

        @app.post(f"{prefix}/confirmations/{{token}}")
        async def _confirmations_endpoint(token: str, body: bytes) -> Any:
            return await ctrl.handle_request("POST", f"{prefix}/confirmations/{token}", {}, body)

        @app.get(f"{prefix}/sessions/{{session_id}}")
        async def _sessions_get_endpoint(session_id: str) -> Any:
            return await ctrl.handle_request("GET", f"{prefix}/sessions/{session_id}", {}, b"")

        @app.delete(f"{prefix}/sessions/{{session_id}}")
        async def _sessions_delete_endpoint(session_id: str) -> Any:
            return await ctrl.handle_request("DELETE", f"{prefix}/sessions/{session_id}", {}, b"")

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol callback auto-discovering handlers and mounting routes."""
        if isinstance(app_config, App):
            self.mount_native(app_config)
            return app_config

        if hasattr(app_config, "state"):
            app_config.state["mcp_registry"] = self.mcp_registry
            app_config.state["mcp_service"] = self.mcp_service
            app_config.state["a2a_service"] = self.a2a_service
            app_config.state["agent_runtime"] = self.runtime
            app_config.state["agent_observability"] = self.observability
            app_config.state["agent_chat_controller"] = self.chat_controller

        if hasattr(app_config, "dependencies") and isinstance(app_config.dependencies, dict):
            di_mod = import_optional("litestar.di")
            provide_cls = getattr(di_mod, "Provide", None) if di_mod is not None else None
            if provide_cls is not None:
                app_config.dependencies["mcp_registry"] = provide_cls(
                    lambda: self.mcp_registry,
                    sync_to_thread=False,
                )
                app_config.dependencies["agent_runtime"] = provide_cls(
                    lambda: self.runtime,
                    sync_to_thread=False,
                )
            else:
                app_config.dependencies["mcp_registry"] = lambda: self.mcp_registry
                app_config.dependencies["agent_runtime"] = lambda: self.runtime

        handlers = getattr(app_config, "route_handlers", None)
        if handlers is not None:
            self._discover_handlers(handlers)
            self._mount_litestar_routes(handlers, app_config=app_config)

        return app_config

    @staticmethod
    def _header_dict(headers: Any) -> dict[str, str]:
        """Normalize native header views and ordinary header mappings."""
        if headers is None:
            return {}
        return dict(headers.as_dict()) if hasattr(headers, "as_dict") else dict(headers)

    def _discover_handlers(self, handlers: list[Any]) -> None:
        """Walk route handler hierarchy and register annotated tools, resources, and prompts."""
        for item in handlers:
            self._inspect_item(item)

    def _inspect_item(self, item: Any) -> None:
        """Inspect an individual handler, controller, or router."""
        opt = getattr(item, "opt", None) or {}
        fn = getattr(item, "fn", item)
        if (
            "mcp_tool" in opt
            or "mcp_resource" in opt
            or "mcp_resource_template" in opt
            or "mcp_prompt" in opt
            or hasattr(fn, "_mcp_tool_meta")
            or hasattr(fn, "_mcp_resource")
            or hasattr(fn, "_mcp_resource_template")
            or hasattr(fn, "_mcp_prompt")
        ):
            self.mcp_registry.register_route_handler(item, opt=opt)

        if isinstance(item, type):
            if issubclass(item, (SkillController, PromptController)):
                self.mcp_registry.register_controller(item)
        elif isinstance(item, (SkillController, PromptController)):
            self.mcp_registry.register_controller(item)

        sub_handlers = getattr(item, "route_handlers", None)
        if sub_handlers is not None and isinstance(sub_handlers, list):
            for sub in sub_handlers:
                self._inspect_item(sub)

    @staticmethod
    def _has_route_path(handlers: list[Any], target_path: str) -> bool:
        for h in handlers:
            paths = getattr(h, "paths", None)
            if paths and target_path in paths:
                return True
            if getattr(h, "path", None) == target_path:
                return True
        return False

    def _mount_litestar_routes(self, route_handlers: list[Any], *, app_config: Any = None) -> None:
        """Mount Litestar route handlers for MCP, A2A, and chat controller."""
        litestar_mod = import_optional("litestar")
        if litestar_mod is None:
            return
        get_fn = getattr(litestar_mod, "get", None)
        post_fn = getattr(litestar_mod, "post", None)
        if get_fn is None or post_fn is None:
            return

        plugins = list(getattr(app_config, "plugins", None) or ())
        litestar_mcp_plugins = [p for p in plugins if type(p).__name__ == "LitestarMCP"]
        has_litestar_mcp = bool(litestar_mcp_plugins)

        srv = self.mcp_service
        if srv is not None:
            well_known_already_mounted = self._has_route_path(route_handlers, srv.config.well_known_path) or any(
                self._has_route_path(list(getattr(p, "_dynamic_handlers", None) or ()), srv.config.well_known_path)
                for p in litestar_mcp_plugins
            )
            if not well_known_already_mounted:

                async def _litestar_mcp_well_known() -> dict[str, Any]:
                    return build_well_known_mcp_document(srv.config, srv.registry)

                route_handlers.append(
                    get_fn(
                        srv.config.well_known_path,
                        include_in_schema=False,
                        opt={"exclude_from_auth": True},
                    )(_litestar_mcp_well_known)
                )

            if not has_litestar_mcp and not self._has_route_path(route_handlers, srv.config.path):

                async def _litestar_mcp_endpoint(
                    request: "LitestarRequest[Any, Any, Any]",
                ) -> "LitestarResponse[bytes]":
                    mcp_resp = await srv.handle(
                        McpHttpRequest(
                            method=request.method,
                            path=srv.config.path,
                            headers=dict(request.headers),
                            body=await request.body(),
                            context=AgentContext.from_scope(request.scope, identity_source=srv.config.identity_source),
                        )
                    )
                    return cast(
                        "LitestarResponse[bytes]",
                        litestar_mod.Response(
                            content=mcp_resp.body,
                            status_code=mcp_resp.status,
                            headers=mcp_resp.headers,
                            media_type=mcp_resp.headers.get("Content-Type", "application/json"),
                        ),
                    )

                route_handlers.append(
                    litestar_mod.route(
                        srv.config.path,
                        http_method=["POST", "GET", "DELETE"],
                        include_in_schema=False,
                        signature_namespace={
                            "LitestarRequest": litestar_mod.Request,
                            "LitestarResponse": litestar_mod.Response,
                        },
                    )(_litestar_mcp_endpoint)
                )

            if (
                srv.config.webmcp is not None
                and srv.config.webmcp.enabled
                and not self._has_route_path(route_handlers, srv.config.webmcp.manifest_path)
            ):
                webmcp_cfg = srv.config.webmcp

                async def _litestar_webmcp_well_known() -> dict[str, Any]:
                    return cast(dict[str, Any], msgspec.to_builtins(build_webmcp_manifest(srv.registry, webmcp_cfg)))

                route_handlers.append(
                    get_fn(
                        webmcp_cfg.manifest_path,
                        include_in_schema=False,
                        opt={"exclude_from_auth": True},
                    )(_litestar_webmcp_well_known)
                )

        a2a_srv = self.a2a_service
        if a2a_srv is not None:

            async def _litestar_a2a_agent_card(headers: dict[str, str] | None = None) -> dict[str, Any]:
                card = a2a_srv.resolve_agent_card(headers=headers, registry=self.mcp_registry)
                if card is None:
                    return {}
                return cast(dict[str, Any], decode_json(card.to_json_bytes()))

            async def _litestar_a2a_endpoint(body: bytes) -> dict[str, Any]:
                resp = await a2a_srv.handle_payload(body)
                return cast(dict[str, Any], decode_json(resp) if resp else {})

            route_handlers.append(
                get_fn("/.well-known/agent-card.json", include_in_schema=False)(_litestar_a2a_agent_card)
            )
            route_handlers.append(post_fn("/a2a", include_in_schema=False)(_litestar_a2a_endpoint))
