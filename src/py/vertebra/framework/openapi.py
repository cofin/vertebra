"""OpenAPI 3.1/3.2 specification generator and configurable multi-backend documentation portal."""

from __future__ import annotations

import html
import inspect
import typing
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.build._assets import escape_html_json
from vertebra.framework.di import inspect_callable_bindings
from vertebra.utils.serializers import encode_json, format_json

if TYPE_CHECKING:
    from vertebra.framework._app import App

__all__ = (
    "AsyncAPIDocsConfig",
    "DocsBackendType",
    "ElementsDocsConfig",
    "OpenAPIConfig",
    "OpenAPISpecBuilder",
    "RapiDocDocsConfig",
    "RedocDocsConfig",
    "ScalarDocsConfig",
    "ScalarSpecSource",
    "SwaggerDocsConfig",
)

DocsBackendType = Literal["scalar", "swagger", "redoc", "elements", "rapidoc", "asyncapi"]


class ScalarSpecSource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Single OpenAPI or AsyncAPI specification source in a Scalar multi-spec portal."""

    title: str
    slug: str
    url: str
    default: bool = False
    spec_type: Literal["openapi", "asyncapi"] = "openapi"


class ScalarDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Scalar interactive API documentation UI configuration (default backend)."""

    path: str = "/docs"
    theme: str = "kepler"
    dark_mode: bool = True
    layout: Literal["modern", "classic"] = "modern"
    show_sidebar: bool = True
    default_http_client: tuple[str, str] = ("typescript", "fetch")
    sources: tuple[ScalarSpecSource, ...] = ()
    offline: bool = False
    bundle_path: str = "/_vertebra/scalar/standalone.js"
    mcp_endpoint: str | None = "/mcp"

    def to_scalar_options(self, default_schema_url: str = "/openapi.json") -> dict[str, Any]:
        """Build the Scalar configuration dictionary serialized into data-configuration."""
        opts: dict[str, Any] = {
            "theme": self.theme,
            "darkMode": self.dark_mode,
            "layout": self.layout,
            "showSidebar": self.show_sidebar,
            "defaultHttpClient": {
                "targetKey": self.default_http_client[0],
                "clientKey": self.default_http_client[1],
            },
        }
        if self.sources:
            opts["sources"] = [
                {
                    "title": src.title,
                    "slug": src.slug,
                    "url": src.url,
                    "default": src.default,
                }
                for src in self.sources
            ]
        else:
            opts["url"] = default_schema_url
        if self.mcp_endpoint:
            opts["mcp"] = {"endpoint": self.mcp_endpoint}
        return opts

    def render_html(self, title: str = "Vertebra API", default_schema_url: str = "/openapi.json") -> str:
        """Render the HTML document for the Scalar API reference portal."""
        script_src = self.bundle_path if self.offline else "https://cdn.jsdelivr.net/npm/@scalar/api-reference"
        config_json = escape_html_json(encode_json(self.to_scalar_options(default_schema_url)))
        safe_title = html.escape(title)
        safe_url = html.escape(default_schema_url, quote=True)
        return (
            f"<!doctype html><html><head><title>{safe_title}</title><meta charset='utf-8' />"
            f"<meta name='viewport' content='width=device-width, initial-scale=1' /></head>"
            f"<body><script id='api-reference' data-url='{safe_url}' "
            f"data-configuration='{html.escape(config_json, quote=True)}'></script>"
            f"<script src='{html.escape(script_src, quote=True)}'></script></body></html>"
        )


class SwaggerDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Standard Swagger UI documentation configuration (available at /swagger)."""

    path: str = "/swagger"
    deep_linking: bool = True
    persist_authorization: bool = True
    display_request_duration: bool = True
    filter: bool = True
    try_it_out_enabled: bool = True
    offline: bool = False
    bundle_js_path: str = "/_vertebra/swagger/swagger-ui-bundle.js"
    bundle_css_path: str = "/_vertebra/swagger/swagger-ui.css"

    def render_html(self, title: str = "Vertebra API", schema_url: str = "/openapi.json") -> str:
        """Render the HTML document for Swagger UI."""
        js_src = (
            self.bundle_js_path
            if self.offline
            else "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"
        )
        css_src = (
            self.bundle_css_path if self.offline else "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"
        )
        safe_title = html.escape(title)
        opts = {
            "url": schema_url,
            "dom_id": "#swagger-ui",
            "deepLinking": self.deep_linking,
            "persistAuthorization": self.persist_authorization,
            "displayRequestDuration": self.display_request_duration,
            "filter": self.filter,
            "tryItOutEnabled": self.try_it_out_enabled,
        }
        opts_json = escape_html_json(encode_json(opts))
        return (
            f"<!doctype html><html><head><title>{safe_title} - Swagger UI</title>"
            f"<meta charset='utf-8' /><meta name='viewport' content='width=device-width, initial-scale=1' />"
            f"<link rel='stylesheet' href='{html.escape(css_src, quote=True)}' /></head>"
            f"<body><div id='swagger-ui'></div>"
            f"<script src='{html.escape(js_src, quote=True)}'></script>"
            f"<script>window.ui = SwaggerUIBundle({opts_json});</script></body></html>"
        )


class RedocDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Redoc documentation UI configuration."""

    path: str = "/redoc"
    expand_responses: str = "200,201"
    hide_download_button: bool = False
    offline: bool = False
    bundle_js_path: str = "/_vertebra/redoc/redoc.standalone.js"

    def render_html(self, title: str = "Vertebra API", schema_url: str = "/openapi.json") -> str:
        """Render the HTML document for Redoc."""
        js_src = (
            self.bundle_js_path
            if self.offline
            else "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"
        )
        safe_title = html.escape(title)
        return (
            f"<!doctype html><html><head><title>{safe_title} - Redoc</title>"
            f"<meta charset='utf-8' /><meta name='viewport' content='width=device-width, initial-scale=1' /></head>"
            f"<body><redoc spec-url='{html.escape(schema_url, quote=True)}' "
            f"expand-responses='{html.escape(self.expand_responses, quote=True)}' "
            f"hide-download-button='{str(self.hide_download_button).lower()}'></redoc>"
            f"<script src='{html.escape(js_src, quote=True)}'></script></body></html>"
        )


class ElementsDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Stoplight Elements documentation UI configuration."""

    path: str = "/elements"
    router: Literal["hash", "memory", "history"] = "hash"
    layout: Literal["sidebar", "stacked"] = "sidebar"
    offline: bool = False

    def render_html(self, title: str = "Vertebra API", schema_url: str = "/openapi.json") -> str:
        """Render the HTML document for Stoplight Elements."""
        safe_title = html.escape(title)
        return (
            f"<!doctype html><html><head><title>{safe_title} - Elements</title>"
            f"<meta charset='utf-8' /><meta name='viewport' content='width=device-width, initial-scale=1' />"
            f"<link rel='stylesheet' href='https://unpkg.com/@stoplight/elements/styles.min.css' />"
            f"<script src='https://unpkg.com/@stoplight/elements/web-components.min.js'></script></head>"
            f"<body><elements-api apiDescriptionUrl='{html.escape(schema_url, quote=True)}' "
            f"router='{self.router}' layout='{self.layout}' /></body></html>"
        )


class RapiDocDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """RapiDoc documentation UI configuration."""

    path: str = "/rapidoc"
    render_style: Literal["read", "view", "focused"] = "read"
    theme: Literal["light", "dark"] = "dark"
    offline: bool = False

    def render_html(self, title: str = "Vertebra API", schema_url: str = "/openapi.json") -> str:
        """Render the HTML document for RapiDoc."""
        safe_title = html.escape(title)
        return (
            f"<!doctype html><html><head><title>{safe_title} - RapiDoc</title>"
            f"<meta charset='utf-8' /><meta name='viewport' content='width=device-width, initial-scale=1' />"
            f"<script type='module' src='https://unpkg.com/rapidoc/dist/rapidoc-min.js'></script></head>"
            f"<body><rapi-doc spec-url='{html.escape(schema_url, quote=True)}' "
            f"render-style='{self.render_style}' theme='{self.theme}'></rapi-doc></body></html>"
        )


class AsyncAPIDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """AsyncAPI 3.0 interactive documentation portal and WebSocket playground configuration."""

    path: str = "/asyncapi"
    schema_path: str = "/asyncapi.json"
    renderer: Literal["scalar", "asyncapi-react"] = "scalar"
    sidebar_show_operations: str = "byDefault"
    expand_message_examples: bool = True
    offline: bool = False
    bundle_js_path: str = "/_vertebra/asyncapi/asyncapi-standalone.js"
    bundle_css_path: str = "/_vertebra/asyncapi/default.min.css"
    ws_playground: bool = True

    def render_html(self, title: str = "Vertebra Realtime API") -> str:
        """Render the HTML document for the AsyncAPI 3.0 documentation viewer."""
        safe_title = html.escape(title)
        safe_schema = html.escape(self.schema_path, quote=True)
        if self.renderer == "scalar":
            script_src = (
                "/_vertebra/scalar/standalone.js"
                if self.offline
                else "https://cdn.jsdelivr.net/npm/@scalar/api-reference"
            )
            cfg_json = escape_html_json(
                encode_json(
                    {
                        "url": self.schema_path,
                        "specType": "asyncapi",
                        "wsPlayground": self.ws_playground,
                    }
                )
            )
            return (
                f"<!doctype html><html><head><title>{safe_title} - AsyncAPI</title>"
                f"<meta charset='utf-8' /><meta name='viewport' content='width=device-width, initial-scale=1' /></head>"
                f"<body><script id='api-reference' data-url='{safe_schema}' "
                f"data-configuration='{html.escape(cfg_json, quote=True)}'></script>"
                f"<script src='{html.escape(script_src, quote=True)}'></script></body></html>"
            )
        js_src = (
            self.bundle_js_path
            if self.offline
            else "https://unpkg.com/@asyncapi/react-component@latest/browser/standalone/index.js"
        )
        css_src = (
            self.bundle_css_path
            if self.offline
            else "https://unpkg.com/@asyncapi/react-component@latest/styles/default.min.css"
        )
        cfg_json = escape_html_json(
            encode_json(
                {
                    "schema": {"url": self.schema_path},
                    "config": {
                        "show": {"sidebar": True, "operations": self.sidebar_show_operations},
                        "expand": {"messageExamples": self.expand_message_examples},
                    },
                }
            )
        )
        return (
            f"<!doctype html><html><head><title>{safe_title} - AsyncAPI</title>"
            f"<meta charset='utf-8' /><link rel='stylesheet' href='{html.escape(css_src, quote=True)}' /></head>"
            f"<body><div id='asyncapi'></div>"
            f"<script src='{html.escape(js_src, quote=True)}'></script>"
            f"<script>AsyncApiStandalone.render({cfg_json}, document.getElementById('asyncapi'));</script></body></html>"
        )


class OpenAPIConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """OpenAPI 3.1/3.2 document and multi-backend interactive UI configuration."""

    title: str = "Vertebra API"
    version: str = "0.1.0"
    description: str = ""
    openapi_version: str = "3.1.0"
    schema_path: str = "/openapi.json"
    default_docs: DocsBackendType = "scalar"
    enabled_docs: tuple[DocsBackendType, ...] = ("scalar", "swagger", "asyncapi")
    scalar: ScalarDocsConfig = msgspec.field(default_factory=ScalarDocsConfig)
    swagger: SwaggerDocsConfig = msgspec.field(default_factory=SwaggerDocsConfig)
    redoc: RedocDocsConfig = msgspec.field(default_factory=RedocDocsConfig)
    elements: ElementsDocsConfig = msgspec.field(default_factory=ElementsDocsConfig)
    rapidoc: RapiDocDocsConfig = msgspec.field(default_factory=RapiDocDocsConfig)
    asyncapi: AsyncAPIDocsConfig = msgspec.field(default_factory=AsyncAPIDocsConfig)

    def render_docs_routes(self) -> dict[str, str]:
        """Return a mapping of route path to rendered HTML for all enabled documentation backends."""
        rendered: dict[str, str] = {}
        active = set(self.enabled_docs) | {self.default_docs}
        if "scalar" in active:
            rendered[self.scalar.path] = self.scalar.render_html(
                title=self.title,
                default_schema_url=self.schema_path,
            )
        if "swagger" in active:
            rendered[self.swagger.path] = self.swagger.render_html(
                title=self.title,
                schema_url=self.schema_path,
            )
        if "redoc" in active:
            rendered[self.redoc.path] = self.redoc.render_html(
                title=self.title,
                schema_url=self.schema_path,
            )
        if "elements" in active:
            rendered[self.elements.path] = self.elements.render_html(
                title=self.title,
                schema_url=self.schema_path,
            )
        if "rapidoc" in active:
            rendered[self.rapidoc.path] = self.rapidoc.render_html(
                title=self.title,
                schema_url=self.schema_path,
            )
        if "asyncapi" in active:
            rendered[self.asyncapi.path] = self.asyncapi.render_html(title=self.title)
        return rendered


class OpenAPISpecBuilder:
    """Constructs OpenAPI 3.1/3.2 documents from route metadata and msgspec.Struct types."""

    __slots__ = ("_config", "_paths", "_schemas")

    def __init__(self, config: OpenAPIConfig | None = None) -> None:
        """Initialize builder with OpenAPI configuration."""
        self._config = config or OpenAPIConfig()
        self._paths: dict[str, dict[str, Any]] = {}
        self._schemas: dict[str, dict[str, Any]] = {}

    @property
    def config(self) -> OpenAPIConfig:
        """Return the OpenAPIConfig."""
        return self._config

    @classmethod
    def from_app(cls, app: App, config: OpenAPIConfig | None = None) -> OpenAPISpecBuilder:
        """Build an OpenAPISpecBuilder by inspecting routes on an App instance."""
        cfg = config or OpenAPIConfig()
        builder = cls(config=cfg)
        excluded_paths = {
            cfg.schema_path,
            cfg.scalar.path,
            cfg.swagger.path,
            cfg.redoc.path,
            cfg.elements.path,
            cfg.rapidoc.path,
            cfg.asyncapi.path,
            cfg.asyncapi.schema_path,
        }
        for route in app.routes:
            if route.path in excluded_paths:
                continue
            handler = route.handler
            resp_schema = None
            try:
                type_hints = typing.get_type_hints(handler)
                resp_schema = type_hints.get("return")
            except Exception:
                sig = inspect.signature(handler)
                if sig.return_annotation is not inspect.Signature.empty and isinstance(sig.return_annotation, type):
                    resp_schema = sig.return_annotation

            if resp_schema is type(None):
                resp_schema = None

            route_container = app.container.create_child(route.dependencies)
            request_body_schema: type[Any] | None = None
            for binding in inspect_callable_bindings(handler):
                if binding.is_explicit_inject:
                    continue
                dep_key = route_container.graph.resolve_param_key(binding)
                if dep_key is not None and dep_key in route_container.graph.providers:
                    continue
                if inspect.isclass(binding.annotation) and issubclass(binding.annotation, msgspec.Struct):
                    request_body_schema = binding.annotation
                    break

            builder.add_operation(
                path=route.path,
                method=route.method,
                summary=inspect.getdoc(handler) or "",
                operation_id=getattr(handler, "__name__", None),
                response_schema=resp_schema,
                request_body_schema=request_body_schema,
                tags=route.tags,
            )
        return builder

    def add_operation(
        self,
        path: str,
        method: str,
        *,
        summary: str = "",
        operation_id: str | None = None,
        response_schema: type[Any] | None = None,
        request_body_schema: type[Any] | None = None,
        tags: tuple[str, ...] = (),
    ) -> None:
        """Register an HTTP operation in the OpenAPI path map."""
        method_lower = method.lower()
        op: dict[str, Any] = {
            "summary": summary,
            "operationId": operation_id or f"{method_lower}_{path.strip('/').replace('/', '_') or 'root'}",
            "responses": {"200": {"description": "Successful Response"}},
        }
        if tags:
            op["tags"] = list(tags)
        if request_body_schema is not None and inspect.isclass(request_body_schema):
            body_name = getattr(request_body_schema, "__name__", "RequestModel")
            _, body_components = msgspec.json.schema_components((request_body_schema,))
            self._schemas.update(body_components)
            op["requestBody"] = {
                "required": True,
                "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{body_name}"}}},
            }
        if response_schema is not None and inspect.isclass(response_schema):
            schema_name = getattr(response_schema, "__name__", "ResponseModel")
            _, components = msgspec.json.schema_components((response_schema,))
            self._schemas.update(components)
            op["responses"]["200"]["content"] = {
                "application/json": {"schema": {"$ref": f"#/components/schemas/{schema_name}"}}
            }
        self._paths.setdefault(path, {})[method_lower] = op

    def build(self) -> dict[str, Any]:
        """Build and return the complete OpenAPI specification dictionary."""
        doc: dict[str, Any] = {
            "openapi": self._config.openapi_version,
            "info": {
                "title": self._config.title,
                "version": self._config.version,
                "description": self._config.description,
            },
            "paths": dict(self._paths),
        }
        if self._schemas:
            doc["components"] = {"schemas": dict(self._schemas)}
        return doc

    def to_json(self) -> str:
        """Serialize the complete OpenAPI specification dictionary to formatted JSON."""
        return format_json(self.build())
