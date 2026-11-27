"""Route-annotation/controller-driven MCP and browser WebMCP integration for Django."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from vertebra._vertebra import Request, Response
from vertebra.agent.core.context import AgentContext
from vertebra.agent.mcp.controllers import (
    PromptController,
    SkillController,
    mcp_prompt,
    mcp_resource,
    mcp_tool,
)
from vertebra.agent.mcp.models import MCPConfig
from vertebra.agent.mcp.registry import ToolRegistry
from vertebra.agent.mcp.routes import WsgiMCPApp, render_well_known_mcp_json
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.mcp.skills import MCPSkillsConfig, SkillCatalog
from vertebra.agent.mcp.webmcp import (
    WebMCPConfig,
    WebMCPManifest,
    WebMCPToolDescriptor,
    build_webmcp_manifest,
    render_webmcp_manifest_json,
)
from vertebra.extensions.django.router import RouteCompiler
from vertebra.utils.module_loader import import_optional

__all__ = (
    "MCPConfig",
    "MCPCore",
    "PromptController",
    "SkillController",
    "ToolRegistry",
    "WebMCPConfig",
    "WebMCPManifest",
    "WebMCPToolDescriptor",
    "WsgiMCPApp",
    "attach_mcp_routes",
    "build_webmcp_manifest",
    "dispatch_mcp_request",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "render_webmcp_manifest_json",
)


def dispatch_mcp_request(
    registry: ToolRegistry,
    request: Request | bytes | Any,
    config: MCPConfig | None = None,
    *,
    skills: MCPSkillsConfig | SkillCatalog | None = None,
) -> Any:
    """Execute an MCP HTTP request via shared ``WsgiMCPApp`` over ``MCPCore``."""
    resolved = config or MCPConfig(server_name="vertebra-django-mcp")
    cached_app = getattr(registry, "_mcp_app", None)
    if (
        not isinstance(cached_app, WsgiMCPApp)
        or cached_app.config != resolved
        or (skills is not None and getattr(cached_app, "_skills_source", None) != skills)
    ):
        resolved_skills = SkillCatalog.coerce(skills)
        core = MCPCore(config=resolved, registry=registry, skill_catalog=resolved_skills)
        core.validate()
        cached_app = WsgiMCPApp(core)
        cast("Any", cached_app)._skills_source = skills
        cast("Any", registry)._mcp_app = cached_app

    if isinstance(request, Request):
        method = request.method.upper()
        path = request.path or resolved.path
        headers = request.headers.as_dict()
        body = bytes(request.body or b"")
        req_id = int(request.request_id)
        context = AgentContext.from_headers(headers, identity_source=resolved.identity_source)
    elif isinstance(request, (bytes, bytearray, memoryview)):
        method = "POST"
        path = resolved.path
        headers = {"content-type": "application/json"}
        body = bytes(request)
        req_id = 0
        context = AgentContext()
    else:
        method = str(getattr(request, "method", "POST") or "POST").upper()
        path = str(getattr(request, "path", resolved.path) or resolved.path)
        raw_headers = getattr(request, "headers", None)
        headers = {str(k): str(v) for k, v in raw_headers.items()} if isinstance(raw_headers, Mapping) else {}
        body = bytes(getattr(request, "body", b"") or b"")
        req_id = 0
        meta = getattr(request, "META", None)
        context = (
            AgentContext.from_wsgi_environ(meta, identity_source=resolved.identity_source)
            if isinstance(meta, Mapping)
            else AgentContext.from_headers(headers, identity_source=resolved.identity_source)
        )

    resp = cached_app.handle_http(method, path, headers, body, context=context)
    if resp.stream is not None:
        django_http = import_optional("django.http")
        streaming_cls = getattr(django_http, "StreamingHttpResponse", None) if django_http is not None else None
        if streaming_cls is not None:
            stream_iter = cached_app.iter_stream(resp.stream)
            django_resp = streaming_cls(stream_iter, status=resp.status)
            for key, value in resp.headers.items():
                django_resp[key] = value
            return django_resp
    return Response(
        status=resp.status,
        headers=list(resp.headers.items()),
        body=resp.body,
        request_id=req_id,
    )


def attach_mcp_routes(
    compiler_or_views: RouteCompiler | list[Any] | None = None,
    config: MCPConfig | None = None,
    *,
    skills: MCPSkillsConfig | SkillCatalog | None = None,
) -> tuple[ToolRegistry, dict[str, Response]]:
    """Scan Django views for ``@mcp_tool``, ``@mcp_resource``, and ``@mcp_prompt``."""
    resolved = config or MCPConfig(server_name="vertebra-django-mcp")
    registry = ToolRegistry()

    if isinstance(compiler_or_views, RouteCompiler):
        for entry in compiler_or_views.entries:
            registry.register_route_handler(entry.callback)
    elif isinstance(compiler_or_views, list):
        for item in compiler_or_views:
            cb = getattr(item, "callback", item)
            registry.register_route_handler(cb)

    resolved_skills = SkillCatalog.coerce(skills)
    core = MCPCore(config=resolved, registry=registry, skill_catalog=resolved_skills)
    core.validate()
    mcp_app = WsgiMCPApp(core)
    cast("Any", mcp_app)._skills_source = skills
    cast("Any", registry)._mcp_app = mcp_app

    static_responses: dict[str, Response] = {}
    if resolved.expose_well_known:
        skill_names = [s.name for s in resolved_skills.list_skills()] if resolved_skills is not None else None
        well_known_bytes = render_well_known_mcp_json(resolved, registry, skill_names=skill_names)
        static_responses[resolved.well_known_path] = Response(
            status=200,
            headers=[("content-type", "application/json")],
            body=well_known_bytes,
        )
    if isinstance(resolved.webmcp, WebMCPConfig) and resolved.webmcp.enabled:
        webmcp_bytes = render_webmcp_manifest_json(
            registry,
            config=resolved.webmcp,
        ).encode("utf-8")
        static_responses[resolved.webmcp.manifest_path] = Response(
            status=200,
            headers=[("content-type", "application/json")],
            body=webmcp_bytes,
        )

    return registry, static_responses
