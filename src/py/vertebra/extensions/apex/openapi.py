"""ORDS REST catalog and Oracle 26ai JSON Relational Duality View OpenAPI/MCP generator."""

from __future__ import annotations

from typing import Any

import msgspec

from vertebra.framework.a2a import AgentSkill
from vertebra.framework.mcp import McpTool

__all__ = (
    "ApexOpenAPIGenerator",
    "DualityViewSpec",
    "OrdsEndpointSpec",
)


class OrdsEndpointSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Metadata for an Oracle REST Data Services (ORDS) endpoint."""

    module_name: str
    uri_template: str
    method: str
    operation_id: str
    description: str = ""
    parameters: dict[str, Any] = msgspec.field(default_factory=dict)


class DualityViewSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Metadata for an Oracle 26ai JSON Relational Duality View."""

    view_name: str
    root_table: str
    etag_checked: bool = True
    json_schema: dict[str, Any] = msgspec.field(default_factory=dict)


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
            for method_name, op_data in operations.items():
                if not isinstance(op_data, dict):
                    continue
                op_id = str(
                    op_data.get(
                        "operationId",
                        f"{module_name}_{method_name.lower()}_{path_template.strip('/').replace('/', '_')}",
                    )
                )
                spec = OrdsEndpointSpec(
                    module_name=module_name,
                    uri_template=path_template,
                    method=method_name.upper(),
                    operation_id=op_id,
                    description=str(op_data.get("summary") or op_data.get("description") or ""),
                    parameters=dict(op_data.get("requestBody", {})),
                )
                self._endpoints.append(spec)
                discovered.append(spec)
        return discovered

    def to_mcp_tools(self) -> list[McpTool]:
        """Convert registered ORDS endpoints and Duality Views into MCP tool specifications."""
        tools: list[McpTool] = []
        for ep in self._endpoints:
            tools.append(
                McpTool(
                    name=ep.operation_id,
                    description=ep.description or f"{ep.method} {ep.uri_template}",
                    fn=_noop_tool_handler,
                    input_schema=ep.parameters or {"type": "object", "properties": {}},
                )
            )
        for dv in self._duality_views:
            tools.append(
                McpTool(
                    name=f"duality_{dv.view_name.lower()}",
                    description=(
                        f"Oracle 26ai JSON Relational Duality View {dv.view_name} "
                        f"over {dv.root_table} (ETag optimistic concurrency: {dv.etag_checked})"
                    ),
                    fn=_noop_tool_handler,
                    input_schema=dv.json_schema or {"type": "object", "properties": {}},
                )
            )
        return tools

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


def _noop_tool_handler(**kwargs: Any) -> dict[str, Any]:
    return dict(kwargs)
