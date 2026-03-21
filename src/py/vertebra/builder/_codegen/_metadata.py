"""Route metadata collection for frontend codegen."""

import inspect
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from vertebra.builder._codegen._extractor import TypeExtractor
from vertebra.builder._codegen._ir import TypeNode, UnknownType
from vertebra.framework._router import RouteDef


@dataclass(frozen=True, slots=True)
class RouteMetadata:
    """Codegen metadata derived from a registered HTTP route."""

    operation_id: str
    handler_fn: Any
    source_file: Path
    params: dict[str, TypeNode]
    return_type: TypeNode
    path: str
    method: str
    handler_name: str


class RouteCollector:
    """Collect route metadata from a ``VertebraApp`` instance."""

    def __init__(self, extractor: TypeExtractor | None = None) -> None:
        self._extractor = extractor or TypeExtractor()

    def collect(self, app: Any) -> list[RouteMetadata]:
        return [self._from_route(route) for route in app.routes]

    def _from_route(self, route: RouteDef) -> RouteMetadata:
        handler = route.handler
        signature = inspect.signature(handler)
        closure = inspect.getclosurevars(handler)
        localns = dict(handler.__globals__)
        localns.update(closure.nonlocals)
        try:
            hints = typing.get_type_hints(handler, globalns=handler.__globals__, localns=localns, include_extras=True)
        except Exception:
            hints = getattr(handler, "__annotations__", {})

        params: dict[str, TypeNode] = {}
        for name, parameter in signature.parameters.items():
            if name in {"self", "cls"}:
                continue
            annotation = hints.get(name, parameter.annotation)
            params[name] = self._extractor.from_hint(annotation)

        return_annotation = hints.get("return", signature.return_annotation)
        return_type = self._extractor.from_hint(return_annotation)
        handler_name = getattr(handler, "__name__", getattr(handler, "__qualname__", "handler"))
        source_file = Path(inspect.getsourcefile(handler) or "<unknown>")

        return RouteMetadata(
            operation_id=_operation_id(route.method, route.path),
            handler_fn=handler,
            source_file=source_file,
            params=params,
            return_type=return_type if not isinstance(return_type, UnknownType) else UnknownType(),
            path=route.path,
            method=route.method,
            handler_name=handler_name,
        )


def _operation_id(method: str, path: str) -> str:
    parts = [method.lower()]
    for segment in path.strip("/").split("/"):
        if not segment:
            continue
        if segment.startswith("{") and segment.endswith("}"):
            name = segment[1:-1].split(":", 1)[0].strip()
            parts.append(f"by_{name}")
            continue
        parts.append(segment.replace("-", "_"))
    if len(parts) == 1:
        parts.append("root")
    return ".".join(parts)


__all__ = ("RouteCollector", "RouteMetadata")
