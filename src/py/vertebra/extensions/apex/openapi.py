"""ORDS REST catalog and Oracle 26ai JSON Relational Duality View OpenAPI/MCP generator."""

from __future__ import annotations

import re
import urllib.parse
from collections.abc import Callable
from typing import Any, Protocol

import msgspec

from vertebra.agent.a2a.models import AgentSkill
from vertebra.agent.core.schema import Tool
from vertebra.agent.mcp.models import ResourceEntry
from vertebra.telemetry.genai import inject_w3c_trace_context
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "ApexOpenAPIGenerator",
    "DualityViewMcpHandler",
    "DualityViewSpec",
    "OrdsEndpointSpec",
    "OrdsMcpSynthesizer",
    "OrdsTransportProtocol",
)


class OrdsTransportProtocol(Protocol):
    """Protocol for sending HTTP requests to an Oracle REST Data Services (ORDS) endpoint."""

    async def send_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        """Dispatch HTTP request and return (status_code, headers, body_bytes)."""
        ...


class OrdsEndpointSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Metadata for an Oracle REST Data Services (ORDS) endpoint."""

    module_name: str
    uri_template: str
    method: str
    operation_id: str
    description: str = ""
    parameters: dict[str, Any] = msgspec.field(default_factory=dict)
    input_schema: dict[str, Any] = msgspec.field(default_factory=dict)


class DualityViewSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Metadata for an Oracle 26ai JSON Relational Duality View."""

    view_name: str
    root_table: str
    etag_checked: bool = True
    json_schema: dict[str, Any] = msgspec.field(default_factory=dict)


class DualityViewMcpHandler:
    """Handles CRUD operations over Oracle 26ai JSON Relational Duality Views with ETag checks."""

    def __init__(
        self,
        base_url: str,
        spec: DualityViewSpec,
        transport: OrdsTransportProtocol | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.spec = spec
        self.transport = transport

    async def get_document(self, id: str | int, context: Any = None) -> dict[str, Any]:
        """Fetch a duality view document by its root key."""
        url = f"{self.base_url}/duality/{self.spec.view_name}/{id}"
        headers: dict[str, str] = {"Accept": "application/json"}
        if context is not None and getattr(context, "trace_context", None) is not None:
            inject_w3c_trace_context(context.trace_context, headers=headers)

        if self.transport is not None:
            status, _, body = await self.transport.send_request("GET", url, headers)
            if status >= 400:
                raise RuntimeError(
                    f"ORDS Duality View GET returned HTTP {status}: {body.decode('utf-8', errors='replace')}"
                )
            res = decode_json(body)
            return dict(res) if isinstance(res, dict) else {}
        return {"view": self.spec.view_name, "id": str(id), "root_table": self.spec.root_table}

    async def put_document(
        self,
        id: str | int,
        document: dict[str, Any],
        etag: str | None = None,
        context: Any = None,
    ) -> dict[str, Any]:
        """Update a duality view document with optimistic concurrency ETag enforcement."""
        url = f"{self.base_url}/duality/{self.spec.view_name}/{id}"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.spec.etag_checked and etag:
            headers["If-Match"] = etag
        if context is not None and getattr(context, "trace_context", None) is not None:
            inject_w3c_trace_context(context.trace_context, headers=headers)

        body_bytes = encode_json_bytes(document)
        if self.transport is not None:
            status, _, body = await self.transport.send_request("PUT", url, headers, body=body_bytes)
            if status >= 400:
                raise RuntimeError(
                    f"ORDS Duality View PUT returned HTTP {status}: {body.decode('utf-8', errors='replace')}"
                )
            res = decode_json(body)
            return dict(res) if isinstance(res, dict) else {}
        return {"status": "updated", "view": self.spec.view_name, "id": str(id)}


class OrdsMcpSynthesizer:
    """Synthesizes executable Tool and ResourceEntry objects from ORDS and Duality View specs."""

    def __init__(
        self,
        generator: ApexOpenAPIGenerator,
        transport: OrdsTransportProtocol | None = None,
    ) -> None:
        self.generator = generator
        self.transport = transport

    def build_tools(self) -> list[Tool]:
        """Construct executable Tool instances for all registered endpoints and duality views."""
        tools: list[Tool] = []
        base_url = self.generator.base_ords_url

        for ep in self.generator.endpoints:

            def _create_endpoint_fn(endpoint: OrdsEndpointSpec) -> Callable[..., Any]:
                async def _endpoint_fn(**kwargs: Any) -> Any:
                    path = endpoint.uri_template
                    path_params = re.findall(r"\{([a-zA-Z0-9_]+)\}", path)
                    query_params: dict[str, Any] = {}
                    body_data: dict[str, Any] = {}

                    for k, v in kwargs.items():
                        if k in path_params:
                            path = path.replace(f"{{{k}}}", str(v))
                        elif endpoint.method in ("POST", "PUT", "PATCH"):
                            body_data[k] = v
                        else:
                            query_params[k] = v

                    url = f"{base_url}{path}"
                    if query_params:
                        url = f"{url}?{urllib.parse.urlencode(query_params)}"

                    headers = {"Content-Type": "application/json", "Accept": "application/json"}
                    body_bytes = encode_json_bytes(body_data) if body_data else None

                    if self.transport is not None:
                        status, _, resp_body = await self.transport.send_request(
                            endpoint.method, url, headers, body=body_bytes
                        )
                        if status >= 400:
                            raise RuntimeError(f"ORDS request {endpoint.method} {url} returned HTTP {status}")
                        return decode_json(resp_body) if resp_body else {}
                    return {"status": "dispatched", "url": url, "method": endpoint.method, "params": kwargs}

                return _endpoint_fn

            tools.append(
                Tool(
                    name=ep.operation_id,
                    description=ep.description or f"{ep.method} {ep.uri_template}",
                    fn=_create_endpoint_fn(ep),
                    input_schema=ep.input_schema or ep.parameters or {"type": "object", "properties": {}},
                )
            )

        for dv in self.generator.duality_views:
            handler = DualityViewMcpHandler(base_url, dv, transport=self.transport)

            def _create_dv_fn(h: DualityViewMcpHandler) -> Callable[..., Any]:
                async def _dv_fn(
                    id: str | int = "",
                    document: dict[str, Any] | None = None,
                    etag: str | None = None,
                    **kwargs: Any,
                ) -> Any:
                    if document is not None:
                        return await h.put_document(id, document, etag=etag)
                    if id:
                        return await h.get_document(id)
                    return {"view": h.spec.view_name, "root_table": h.spec.root_table}

                return _dv_fn

            schema = (
                dict(dv.json_schema)
                if dv.json_schema
                else {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Document identifier"},
                        "document": {"type": "object", "description": "Document payload for updates"},
                        "etag": {"type": "string", "description": "Optimistic concurrency ETag"},
                    },
                }
            )
            tools.append(
                Tool(
                    name=f"duality_{dv.view_name.lower()}",
                    description=(
                        f"Oracle 26ai JSON Relational Duality View {dv.view_name} "
                        f"over {dv.root_table} (ETag optimistic concurrency: {dv.etag_checked})"
                    ),
                    fn=_create_dv_fn(handler),
                    input_schema=schema,
                )
            )

        return tools

    def build_resources(self) -> list[ResourceEntry]:
        """Construct ResourceEntry descriptors for all registered duality views."""
        resources: list[ResourceEntry] = []
        base_url = self.generator.base_ords_url

        for dv in self.generator.duality_views:
            handler = DualityViewMcpHandler(base_url, dv, transport=self.transport)

            async def _dv_reader(_h: DualityViewMcpHandler = handler) -> str:
                return encode_json_bytes(
                    {
                        "viewName": _h.spec.view_name,
                        "rootTable": _h.spec.root_table,
                        "etagChecked": _h.spec.etag_checked,
                        "schema": _h.spec.json_schema,
                    }
                ).decode("utf-8")

            resources.append(
                ResourceEntry(
                    uri=f"ords://duality/{dv.view_name.lower()}",
                    name=dv.view_name,
                    description=f"Oracle 26ai Duality View {dv.view_name} over {dv.root_table}",
                    mimeType="application/json",
                    reader=_dv_reader,
                )
            )

        return resources


class ApexOpenAPIGenerator:
    """Compiles ORDS OpenAPI 3.0 catalogs and 26ai Duality Views into MCP tools and A2A skills."""

    __slots__ = ("_duality_views", "_endpoints", "base_ords_url")

    def __init__(self, base_ords_url: str) -> None:
        self.base_ords_url = base_ords_url.rstrip("/")
        self._endpoints: list[OrdsEndpointSpec] = []
        self._duality_views: list[DualityViewSpec] = []

    @property
    def endpoints(self) -> tuple[OrdsEndpointSpec, ...]:
        """Return registered ORDS endpoint specifications."""
        return tuple(self._endpoints)

    @property
    def duality_views(self) -> tuple[DualityViewSpec, ...]:
        """Return registered Oracle 26ai JSON Relational Duality View specifications."""
        return tuple(self._duality_views)

    def register_endpoint(self, spec: OrdsEndpointSpec) -> None:
        """Register an ORDS REST endpoint specification."""
        self._endpoints.append(spec)

    def register_duality_view(self, spec: DualityViewSpec) -> None:
        """Register an Oracle 26ai JSON Relational Duality View specification."""
        self._duality_views.append(spec)

    def load_ords_openapi(
        self,
        module_name: str,
        openapi_doc: dict[str, Any],
    ) -> list[OrdsEndpointSpec]:
        """Parse an ORDS OpenAPI 3.0 document and register discovered endpoints."""
        discovered: list[OrdsEndpointSpec] = []
        paths = openapi_doc.get("paths", {})
        if not isinstance(paths, dict):
            return discovered
        for path_template, operations in paths.items():
            if not isinstance(operations, dict):
                continue
            path_params = operations.get("parameters", [])
            for method_name, op_data in operations.items():
                if not isinstance(op_data, dict) or method_name == "parameters":
                    continue
                op_id = str(
                    op_data.get(
                        "operationId",
                        f"{module_name}_{method_name.lower()}_{path_template.strip('/').replace('/', '_')}",
                    )
                )

                props: dict[str, Any] = {}
                required_fields: list[str] = []
                all_params = list(path_params)
                if "parameters" in op_data and isinstance(op_data["parameters"], list):
                    all_params.extend(op_data["parameters"])

                for p in all_params:
                    if isinstance(p, dict) and "name" in p:
                        p_name = p["name"]
                        props[p_name] = p.get("schema", {"type": "string"})
                        if p.get("required"):
                            required_fields.append(p_name)

                req_body = op_data.get("requestBody", {})
                if isinstance(req_body, dict):
                    content = req_body.get("content", {})
                    json_c = content.get("application/json", {})
                    body_schema = json_c.get("schema", {})
                    if isinstance(body_schema, dict) and "properties" in body_schema:
                        props.update(body_schema["properties"])
                        if "required" in body_schema and isinstance(body_schema["required"], list):
                            required_fields.extend(body_schema["required"])

                input_schema: dict[str, Any] = {
                    "type": "object",
                    "properties": props,
                    "parameters": {
                        "type": "object",
                        "properties": props,
                        "required": sorted(set(required_fields)),
                    },
                }
                if required_fields:
                    input_schema["required"] = sorted(set(required_fields))

                spec = OrdsEndpointSpec(
                    module_name=module_name,
                    uri_template=path_template,
                    method=method_name.upper(),
                    operation_id=op_id,
                    description=str(op_data.get("summary") or op_data.get("description") or ""),
                    parameters=dict(op_data.get("requestBody", {})),
                    input_schema=input_schema,
                )
                self._endpoints.append(spec)
                discovered.append(spec)
        return discovered

    def to_mcp_tools(self, transport: OrdsTransportProtocol | None = None) -> list[Tool]:
        """Convert registered ORDS endpoints and Duality Views into executable MCP tools."""
        synthesizer = OrdsMcpSynthesizer(self, transport=transport)
        return synthesizer.build_tools()

    def to_mcp_resources(self, transport: OrdsTransportProtocol | None = None) -> list[ResourceEntry]:
        """Convert registered Duality Views into static MCP ResourceEntry descriptors."""
        synthesizer = OrdsMcpSynthesizer(self, transport=transport)
        return synthesizer.build_resources()

    def to_a2a_skills(self) -> list[AgentSkill]:
        """Convert registered ORDS endpoints and Duality Views into A2A agent skills."""
        skills: list[AgentSkill] = []
        for ep in self._endpoints:
            skills.append(
                AgentSkill(
                    id=ep.operation_id,
                    name=ep.operation_id,
                    description=ep.description or f"ORDS {ep.method} {ep.uri_template}",
                    tags=("oracle", "ords", ep.module_name),
                )
            )
        for dv in self._duality_views:
            skills.append(
                AgentSkill(
                    id=f"duality_{dv.view_name.lower()}",
                    name=dv.view_name,
                    description=f"Duality view over {dv.root_table}",
                    tags=("oracle", "26ai", "duality-view"),
                )
            )
        return skills
