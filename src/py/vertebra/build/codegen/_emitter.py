"""Shared SchemaEmitter base class for schema and client generators."""

from collections.abc import Iterable
from typing import TYPE_CHECKING

from vertebra.build.codegen._ir import ObjectType, TypeNode
from vertebra.build.codegen._json_schema import resolve_parameter_node

if TYPE_CHECKING:
    from vertebra.build.codegen._metadata import RouteMetadata

__all__ = ("SchemaEmitter",)


class SchemaEmitter:
    """Base schema emitter tracking declared named object types across routes and IR trees."""

    __slots__ = ("_declared",)

    def __init__(self, declared: dict[str, ObjectType] | None = None) -> None:
        """Initialize the emitter with optional pre-declared types."""
        self._declared: dict[str, ObjectType] = declared if declared is not None else {}

    @property
    def declared(self) -> dict[str, ObjectType]:
        """Return the dictionary of declared named ObjectType instances."""
        return self._declared

    @declared.setter
    def declared(self, value: dict[str, ObjectType]) -> None:
        """Set the dictionary of declared named ObjectType instances."""
        self._declared = value

    def reset(self) -> None:
        """Clear all declared named types."""
        self._declared.clear()

    def _collect_objects(self, node: TypeNode) -> None:
        """Recursively collect named ObjectType instances from a TypeNode IR subtree."""
        if isinstance(node, ObjectType) and node.name:
            self._declared.setdefault(node.name, node)
        for child in node.iter_children():
            self._collect_objects(child)

    def _collect_named_types(self, nodes: Iterable[TypeNode]) -> None:
        """Collect named ObjectType instances across an iterable of TypeNodes."""
        for node in nodes:
            self._collect_objects(node)

    def _collect_route_objects(self, routes: Iterable["RouteMetadata"]) -> list["RouteMetadata"]:
        """Collect named objects across route parameters and return types, returning routes as a list."""
        route_list = list(routes)
        for route in route_list:
            for param_name, param_node in route.params.items():
                resolution = resolve_parameter_node(param_name, param_node)
                if not resolution.include_in_schema:
                    continue
                self._collect_objects(resolution.base_type)
            self._collect_objects(route.return_type)
        return route_list
