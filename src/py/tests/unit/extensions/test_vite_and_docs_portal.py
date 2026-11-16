"""Unit tests for Phase 2: AssetManifest, Inertia v3 hardening, QuickJS ESM/timeout, Docs Portal, and WebMCP."""

from __future__ import annotations

from pathlib import Path

import pytest

from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.guards import GuardProtocol
from vertebra.agent.core.schema import Tool, tool
from vertebra.agent.mcp import (
    MCPRegistry,
    SkillController,
    WebMCPConfig,
    build_webmcp_manifest,
    mcp_prompt,
    mcp_resource,
    mcp_tool,
    render_webmcp_manifest_json,
)
from vertebra.build import (
    AssetManifest,
    escape_html_json,
    is_fingerprinted_asset,
    is_protected_build_metadata_path,
)
from vertebra.extensions.litestar.openapi import build_docs_route_handlers
from vertebra.extensions.litestar.vite import (
    _StaticAfterRequestHook,
    _StaticBeforeRequestHook,
)
from vertebra.framework import App
from vertebra.framework.inertia import AsyncInertiaMiddleware, InertiaConfig, SyncInertiaMiddleware, inertia
from vertebra.framework.openapi import (
    AsyncAPIDocsConfig,
    ElementsDocsConfig,
    OpenAPIConfig,
    RapiDocDocsConfig,
    RedocDocsConfig,
    ScalarDocsConfig,
    ScalarSpecSource,
    SwaggerDocsConfig,
)
from vertebra.framework.ssr import SSRRenderTimeoutError, SyncEmbeddedSSRClient, render_page_shell
from vertebra.utils.serializers import decode_json


def test_asset_manifest_cycle_safe_recursive_resolution_and_early_hints() -> None:
    manifest = AssetManifest.from_dict(
        {
            "index.html": {
                "file": "assets/index-AbCd1234.js",
                "src": "index.html",
                "isEntry": True,
                "imports": ["_vendor-XyZ98765.js"],
                "dynamicImports": ["_lazy-LmNo4567.js"],
                "css": ["assets/index-11223344.css"],
                "integrity": "sha384-root-hash",
            },
            "_vendor-XyZ98765.js": {
                "file": "assets/vendor-XyZ98765.js",
                "imports": ["_shared-QqWwEeRr.js", "index.html"],
                "css": ["assets/vendor-55667788.css"],
                "integrity": "sha384-vendor-hash",
            },
            "_shared-QqWwEeRr.js": {
                "file": "assets/shared-QqWwEeRr.js",
                "imports": ["_vendor-XyZ98765.js"],
                "css": ["assets/shared-9900aabb.css"],
            },
            "_lazy-LmNo4567.js": {
                "file": "assets/lazy-LmNo4567.js",
                "isDynamicEntry": True,
                "imports": ["_shared-QqWwEeRr.js"],
            },
        }
    )

    resolved = manifest.resolve_entry(
        "index.html",
        asset_url="/static/",
        csp_nonce="nonce-secret-123",
        preload_dynamic=True,
    )

    assert resolved.entry_file == "/static/assets/index-AbCd1234.js"
    assert resolved.css_files == (
        "/static/assets/index-11223344.css",
        "/static/assets/vendor-55667788.css",
        "/static/assets/shared-9900aabb.css",
    )
    assert resolved.preload_files == (
        "/static/assets/vendor-XyZ98765.js",
        "/static/assets/shared-QqWwEeRr.js",
    )
    assert resolved.dynamic_preload_files == ("/static/assets/lazy-LmNo4567.js",)

    html_out = resolved.to_html()
    assert 'nonce="nonce-secret-123"' in html_out
    assert 'integrity="sha384-root-hash" crossorigin="anonymous"' in html_out
    assert 'integrity="sha384-vendor-hash" crossorigin="anonymous"' in html_out

    asgi_event = resolved.early_hints.to_asgi_event()
    assert asgi_event["type"] == "http.response.early_hint"
    assert b"</static/assets/index-AbCd1234.js>; rel=modulepreload; as=script" in asgi_event["links"]
    assert b"</static/assets/vendor-55667788.css>; rel=preload; as=style" in asgi_event["links"]


def test_escape_html_json_and_inertia_early_hints_and_static_hooks() -> None:
    malicious = '{"xss": "</script><script>alert(1)</script>&"}'
    escaped = escape_html_json(malicious)
    assert "</script>" not in escaped
    assert "\\u003c/script\\u003e" in escaped
    assert "\\u0026" in escaped

    shell_html = render_page_shell(
        {"component": "Dashboard", "props": {"bio": "</script><script>evil()</script>"}},
        csp_nonce="csp-999",
        static_props={"env": "prod</script>"},
    )
    assert "</script><script>evil()" not in shell_html
    assert 'nonce="csp-999"' in shell_html
    assert 'id="vertebra-static-props"' in shell_html

    manifest = AssetManifest.from_dict(
        {
            "index.html": {
                "file": "assets/main-AbCdEf12.js",
                "isEntry": True,
                "css": ["assets/main-12345678.css"],
            }
        }
    )
    mw = SyncInertiaMiddleware(
        InertiaConfig(
            enabled=True,
            asset_manifest=manifest,
            entry="index.html",
            early_hints=True,
            static_props={"appTitle": "Vertebra"},
        )
    )
    assert AsyncInertiaMiddleware is not None

    class _FakeReq:
        method = "GET"
        path = "/dashboard"
        headers = {"X-CSP-Nonce": "req-nonce-42"}

    resp = mw.process(_FakeReq(), inertia("Dashboard", {"user": "Ada"}))
    assert resp.status == 200
    assert "Link" in resp.headers
    assert "</static/assets/main-AbCdEf12.js>; rel=modulepreload; as=script" in resp.headers["Link"]
    assert resp.early_hints is not None
    assert b"req-nonce-42" in resp.body

    assert is_protected_build_metadata_path("/static/.vite/manifest.json") is True
    assert is_protected_build_metadata_path("ssr-manifest.json") is True
    assert is_protected_build_metadata_path("hot") is True
    assert is_protected_build_metadata_path("assets/app-12345678.js") is False
    assert is_fingerprinted_asset("assets/app-12345678.js") is True
    assert is_fingerprinted_asset("assets/app.js") is False

    before_hook = _StaticBeforeRequestHook("/static/")

    class _BlockedReq:
        path = "/static/.vite/manifest.json"

    blocked_resp = before_hook(_BlockedReq())
    assert blocked_resp is not None
    assert blocked_resp.status_code == 404

    after_hook = _StaticAfterRequestHook()

    class _StaticResp:
        file_path = "/var/www/static/assets/index-AbCdEf12.js"
        headers: dict[str, str] = {}

    after_resp = after_hook(_StaticResp())
    assert after_resp.headers["Cache-Control"] == "public, max-age=31536000, immutable"


def test_quickjs_esm_module_and_timeout_recovery(tmp_path: Path) -> None:
    esm_bundle = tmp_path / "ssr.mjs"
    esm_bundle.write_text(
        """
        export function render(component, props) {
            if (component === "InfiniteLoop") {
                while (true) {}
            }
            return {
                head: ["<title>ESM: " + component + "</title>"],
                body: "<main data-ssr='esm'>" + (props.props ? props.props.greeting : "hi") + "</main>"
            };
        }
        """,
        encoding="utf-8",
    )

    client = SyncEmbeddedSSRClient(str(esm_bundle), pool_size=1, timeout_ms=50)
    try:
        res = client.render({"component": "Home", "props": {"greeting": "Hello ESM"}})
        assert res.head == ["<title>ESM: Home</title>"]
        assert "Hello ESM" in res.body

        with pytest.raises(SSRRenderTimeoutError):
            client.render({"component": "InfiniteLoop", "props": {}})

        recovered = client.render({"component": "Recovered", "props": {"greeting": "After Timeout"}})
        assert recovered.head == ["<title>ESM: Recovered</title>"]
        assert "After Timeout" in recovered.body
    finally:
        client.stop()


def test_configurable_docs_portal_and_route_annotated_webmcp() -> None:
    cfg = OpenAPIConfig(
        title="Unified Portal",
        version="2.0.0",
        default_docs="scalar",
        enabled_docs=("scalar", "swagger", "redoc", "elements", "rapidoc", "asyncapi"),
        scalar=ScalarDocsConfig(
            path="/docs",
            sources=(
                ScalarSpecSource(title="REST API", slug="rest", url="/openapi.json", default=True),
                ScalarSpecSource(title="AsyncAPI", slug="events", url="/asyncapi.json", spec_type="asyncapi"),
            ),
            mcp_endpoint="/mcp",
        ),
        swagger=SwaggerDocsConfig(path="/swagger"),
        redoc=RedocDocsConfig(path="/redoc"),
        elements=ElementsDocsConfig(path="/elements"),
        rapidoc=RapiDocDocsConfig(path="/rapidoc"),
        asyncapi=AsyncAPIDocsConfig(path="/asyncapi", renderer="scalar"),
    )

    rendered = cfg.render_docs_routes()
    assert set(rendered.keys()) == {"/docs", "/swagger", "/redoc", "/elements", "/rapidoc", "/asyncapi"}
    assert "api-reference" in rendered["/docs"]
    assert "/asyncapi.json" in rendered["/docs"]
    assert "SwaggerUIBundle" in rendered["/swagger"]
    assert "<redoc" in rendered["/redoc"]
    assert "<elements-api" in rendered["/elements"]
    assert "<rapi-doc" in rendered["/rapidoc"]
    assert "asyncapi" in rendered["/asyncapi"]

    handlers = build_docs_route_handlers(cfg)
    assert len(handlers) == 6

    class WorkspaceSkillController(SkillController):
        skill_id = "workspace_tools"
        skill_name = "Workspace Tools"

        @mcp_tool(name="archive_workspace", description="Archive a workspace by ID", webmcp=True)
        async def archive(self, workspace_id: str) -> dict[str, str]:
            return {"archived": workspace_id}

        @mcp_resource(uri="workspace://config", name="workspace_config")
        async def read_config(self) -> str:
            return "env=prod"

        @mcp_resource(uri="workspace://{workspace_id}/summary", name="workspace_summary")
        async def read_summary(self, workspace_id: str) -> str:
            return f"summary:{workspace_id}"

        @mcp_prompt(name="review_workspace", description="Prompt to review workspace")
        async def review_prompt(self) -> list[object]:
            return []

    app = App()
    app.enable_docs(cfg)

    @app.get("/health")
    def health_check() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tickets")
    @mcp_tool(name="create_ticket", description="Create a support ticket")
    def create_ticket(title: str, priority: int = 1) -> dict[str, object]:
        return {"title": title, "priority": priority}

    registry = MCPRegistry()
    registry.register_controller(WorkspaceSkillController())
    registry.discover_app_routes(app)

    tool_names = [t.name for t in registry.list_tools()]
    assert tool_names == ["archive_workspace", "create_ticket"]
    assert "health_check" not in tool_names

    assert [r.uri for r in registry.list_resources()] == ["workspace://config"]
    assert [rt.uriTemplate for rt in registry.list_resource_templates()] == ["workspace://{workspace_id}/summary"]
    assert [p.name for p in registry.list_prompts()] == ["review_workspace"]

    webmcp_json = render_webmcp_manifest_json(registry, WebMCPConfig(mcp_endpoint="/mcp"))
    webmcp_data = decode_json(webmcp_json)
    assert webmcp_data["mcpEndpoint"] == "/mcp"
    assert [t["name"] for t in webmcp_data["tools"]] == ["archive_workspace"]
    manifest_obj = build_webmcp_manifest(registry)
    assert [t.name for t in manifest_obj.tools] == ["archive_workspace"]


def test_webmcp_manifest_lists_only_tagged_tools() -> None:
    """build_webmcp_manifest lists @mcp_tool(webmcp=True), opt['mcp_tool'] with webmcp=True, and guarded controller tools, omitting untagged tools."""

    class _AllowGuard(GuardProtocol):
        async def __call__(self, ctx: AgentContext) -> None:
            _ = ctx

    class GuardedSkillController(SkillController):
        guards = (_AllowGuard(),)

        @mcp_tool(name="guarded_browser_action", description="Guarded browser action", webmcp=True)
        def guarded_browser(self, item: str) -> str:
            return item

        @tool(name="guarded_server_action", description="Guarded server action")
        def guarded_server(self, item: str) -> str:
            return item

    @mcp_tool(name="browser_search", description="Search from the browser", webmcp=True)
    def browser_search(query: str) -> list[str]:
        return [query]

    @mcp_tool(name="server_only", description="Server-side only tool")
    def server_only(query: str) -> list[str]:
        return [query]

    def opt_browser_ping(message: str) -> str:
        """Echo a message from an opt-configured route."""
        return message

    def opt_server_only(message: str) -> str:
        return message

    def bigquery_query(sql: str) -> dict[str, str]:
        return {"sql": sql}

    registry = MCPRegistry()
    registry.register_route_handler(browser_search)
    registry.register_route_handler(server_only)
    registry.register_route_handler(
        opt_browser_ping,
        opt={"mcp_tool": "opt_browser_ping", "webmcp": True, "mcp_annotations": {"readOnlyHint": True}},
    )
    registry.register_route_handler(opt_server_only, opt={"mcp_tool": "opt_server_only"})
    registry.register_controller(GuardedSkillController())
    registry.register_tool(Tool(bigquery_query, name="bigquery_query", description="Run a governed query"))

    manifest = build_webmcp_manifest(registry, WebMCPConfig())
    assert [t.name for t in manifest.tools] == ["browser_search", "guarded_browser_action", "opt_browser_ping"]
    tools_by_name = {t.name: t for t in manifest.tools}
    assert tools_by_name["opt_browser_ping"].annotations == {"readOnlyHint": True}
