"""Unified AgentPlugin mounting agent harness on native vertebra App and Litestar.

This module provides AgentPlugin and AgentPluginConfig for mounting MCP 2026-07-28,
A2A 1.0, AgentChatController, and .well-known discovery manifests on both native
vertebra.framework.App and Litestar applications with automatic opt-attribute handler discovery.
"""

from typing import Any, cast

import msgspec

from vertebra.agent.a2a.service import A2AConfig, A2AService
from vertebra.agent.controller import AgentChatController
from vertebra.agent.core.schema import Tool
from vertebra.agent.mcp.controllers import PromptController, SkillController
from vertebra.agent.mcp.models import MCPConfig, PromptEntry, ResourceEntry
from vertebra.agent.mcp.registry import MCPRegistry
from vertebra.agent.mcp.service import MCPService
from vertebra.agent.runtime import AgentRuntime
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.framework._app import App
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes


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
        self.mcp_registry = MCPRegistry()
        self.mcp_service = (
            MCPService(
                registry=self.mcp_registry,
                config=self.config.mcp,
                telemetry=recorder,
            )
            if self.config.mcp is not None
            else None
        )
        self.a2a_service = (
            A2AService(
                config=self.config.a2a,
                telemetry=recorder,
            )
            if self.config.a2a is not None
            else None
        )
        self.runtime = self.config.runtime or AgentRuntime(telemetry=recorder)
        self.chat_controller = AgentChatController(
            runtime=self.runtime,
            session_store=self.config.session_store,
            base_path=self.config.agent_base_path,
            queue_service=self.config.queue_service,
        )

    def mount_native(self, app: App) -> None:
        """Mount agent routes and discovery endpoints on native vertebra App."""
        prefix = self.config.agent_base_path.rstrip("/")

        if self.mcp_service is not None:
            srv = self.mcp_service

            @app.get("/.well-known/mcp.json")
            async def _mcp_well_known() -> dict[str, Any]:
                tools = [t.name for t in srv.registry.list_tools()]
                return {
                    "mcp_version": "2026-07-28",
                    "server": srv.config.server_name,
                    "tools": tools,
                }

            @app.post("/mcp")
            async def _mcp_endpoint(body: bytes) -> tuple[int, dict[str, str], bytes]:
                resp = await srv.handle_payload(body)
                return 200, {"content-type": "application/json"}, resp or b""

        if self.a2a_service is not None:
            a2a_srv = self.a2a_service

            @app.get("/.well-known/agent-card.json")
            async def _a2a_agent_card() -> dict[str, Any]:
                card = a2a_srv.config.agent_card
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
        if hasattr(app_config, "state"):
            app_config.state["mcp_registry"] = self.mcp_registry
            app_config.state["mcp_service"] = self.mcp_service
            app_config.state["a2a_service"] = self.a2a_service
            app_config.state["agent_runtime"] = self.runtime
            app_config.state["agent_observability"] = self.observability
            app_config.state["agent_chat_controller"] = self.chat_controller

        if hasattr(app_config, "dependencies") and isinstance(app_config.dependencies, dict):
            app_config.dependencies["mcp_registry"] = lambda: self.mcp_registry
            app_config.dependencies["agent_runtime"] = lambda: self.runtime

        handlers = getattr(app_config, "route_handlers", None)
        if handlers is not None:
            self._discover_handlers(handlers)
            self._mount_litestar_routes(handlers)

        return app_config

    def _discover_handlers(self, handlers: list[Any]) -> None:
        """Walk route handler hierarchy and register annotated tools, resources, and prompts."""
        for item in handlers:
            self._inspect_item(item)

    def _inspect_item(self, item: Any) -> None:
        """Inspect an individual handler, controller, or router."""
        opt = getattr(item, "opt", None) or {}
        if "mcp_tool" in opt:
            tool_name = opt["mcp_tool"]
            name = tool_name if isinstance(tool_name, str) else getattr(item, "__name__", "tool")
            fn = getattr(item, "fn", item)
            self.mcp_registry.register_tool(
                Tool(
                    name=name,
                    description=getattr(fn, "__doc__", "") or "",
                    fn=fn,
                    input_schema={},
                )
            )

        if "mcp_resource" in opt:
            uri = opt["mcp_resource"]
            fn = getattr(item, "fn", item)
            self.mcp_registry.register_resource(ResourceEntry(uri=uri, name=uri, reader=fn))

        if "mcp_prompt" in opt:
            prompt_name = opt["mcp_prompt"]
            fn = getattr(item, "fn", item)
            self.mcp_registry.register_prompt(PromptEntry(name=prompt_name, getter=fn))

        if isinstance(item, type):
            if issubclass(item, SkillController):
                skill_controller_instance = item()
                for t in skill_controller_instance.get_tools():
                    self.mcp_registry.register_tool(t)
                for r in skill_controller_instance.get_resources():
                    self.mcp_registry.register_resource(r)
                for p in skill_controller_instance.get_prompts():
                    self.mcp_registry.register_prompt(p)
            elif issubclass(item, PromptController):
                prompt_controller_instance = item()
                for p in prompt_controller_instance.get_prompts():
                    self.mcp_registry.register_prompt(p)

        sub_handlers = getattr(item, "route_handlers", None)
        if sub_handlers is not None and isinstance(sub_handlers, list):
            for sub in sub_handlers:
                self._inspect_item(sub)

    def _mount_litestar_routes(self, route_handlers: list[Any]) -> None:
        """Mount Litestar route handlers for MCP, A2A, and chat controller."""
        litestar_mod = import_optional("litestar")
        if litestar_mod is None:
            return
        get_fn = getattr(litestar_mod, "get", None)
        post_fn = getattr(litestar_mod, "post", None)
        if get_fn is None or post_fn is None:
            return

        srv = self.mcp_service
        if srv is not None:

            async def _litestar_mcp_well_known() -> dict[str, Any]:
                tools = [t.name for t in srv.registry.list_tools()]
                return {
                    "mcp_version": "2026-07-28",
                    "server": srv.config.server_name,
                    "tools": tools,
                }

            async def _litestar_mcp_endpoint(data: dict[str, Any]) -> dict[str, Any]:
                resp = await srv.handle_payload(encode_json_bytes(data))
                return cast(dict[str, Any], decode_json(resp) if resp else {})

            route_handlers.append(get_fn("/.well-known/mcp.json", include_in_schema=False)(_litestar_mcp_well_known))
            route_handlers.append(post_fn("/mcp", include_in_schema=False)(_litestar_mcp_endpoint))

        a2a_srv = self.a2a_service
        if a2a_srv is not None:

            async def _litestar_a2a_agent_card() -> dict[str, Any]:
                card = a2a_srv.config.agent_card
                if card is None:
                    return {}
                return cast(dict[str, Any], decode_json(card.to_json_bytes()))

            async def _litestar_a2a_endpoint(data: dict[str, Any]) -> dict[str, Any]:
                resp = await a2a_srv.handle_payload(encode_json_bytes(data))
                return cast(dict[str, Any], decode_json(resp) if resp else {})

            route_handlers.append(
                get_fn("/.well-known/agent-card.json", include_in_schema=False)(_litestar_a2a_agent_card)
            )
            route_handlers.append(post_fn("/a2a", include_in_schema=False)(_litestar_a2a_endpoint))


__all__ = [
    "AgentPlugin",
    "AgentPluginConfig",
]
