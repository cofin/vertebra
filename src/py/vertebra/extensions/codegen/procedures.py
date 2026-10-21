"""Litestar v2/v3 parameter markers, @procedure RPC registry, and TypeScript client generator."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import threading
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar, Literal, Self, TypeVar, cast, overload

from vertebra.builder._codegen._extractor import TypeExtractor
from vertebra.builder._codegen._ir import AnnotatedType, NamedTypeRef, ObjectType, TypeNode

__all__ = (
    "Body",
    "BodyMarker",
    "FromCookie",
    "FromHeader",
    "FromPath",
    "FromQuery",
    "JSONBody",
    "LocationMarker",
    "MsgPackBody",
    "MultipartBody",
    "NamedDependency",
    "NamedDependencyMarker",
    "Parameter",
    "ParameterMarker",
    "ParameterResolution",
    "ProcedureClientGenerator",
    "ProcedureInfo",
    "ProcedureParam",
    "ProcedureRegistry",
    "SkipValidation",
    "SkipValidationMarker",
    "URLEncodedBody",
    "get_procedure_registry",
    "procedure",
    "resolve_parameter_node",
)

T = TypeVar("T")

type ParameterLocation = Literal["path", "query", "header", "cookie", "body"]


@dataclass(frozen=True, slots=True)
class LocationMarker:
    """Litestar v3 parameter location marker attached via typing.Annotated."""

    location: ParameterLocation
    media_type: str | None = None


@dataclass(frozen=True, slots=True)
class NamedDependencyMarker:
    """Litestar v3 marker indicating a dependency-injected parameter excluded from schemas."""

    key: str | None = None


@dataclass(frozen=True, slots=True)
class SkipValidationMarker:
    """Litestar v3 marker indicating runtime validation is skipped while retaining schema type."""


@dataclass(frozen=True, slots=True)
class ParameterMarker:
    """Litestar v2 compatible Parameter() specification for query, header, path, and cookie constraints."""

    query: str | None = None
    header: str | None = None
    cookie: str | None = None
    title: str | None = None
    description: str | None = None
    ge: int | float | None = None
    gt: int | float | None = None
    le: int | float | None = None
    lt: int | float | None = None
    multiple_of: int | float | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    default: Any = None
    required: bool = True
    include_in_schema: bool = True


@dataclass(frozen=True, slots=True)
class BodyMarker:
    """Litestar v2 compatible Body() specification for request payload encoding and constraints."""

    media_type: str = "application/json"
    title: str | None = None
    description: str | None = None
    include_in_schema: bool = True


def Parameter(
    *,
    query: str | None = None,
    header: str | None = None,
    cookie: str | None = None,
    title: str | None = None,
    description: str | None = None,
    ge: int | float | None = None,
    gt: int | float | None = None,
    le: int | float | None = None,
    lt: int | float | None = None,
    multiple_of: int | float | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
    default: Any = None,
    required: bool = True,
    include_in_schema: bool = True,
) -> ParameterMarker:
    """Construct a Litestar v2 compatible Parameter constraint marker."""
    return ParameterMarker(
        query=query,
        header=header,
        cookie=cookie,
        title=title,
        description=description,
        ge=ge,
        gt=gt,
        le=le,
        lt=lt,
        multiple_of=multiple_of,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
        default=default,
        required=required,
        include_in_schema=include_in_schema,
    )


def Body(
    *,
    media_type: str = "application/json",
    title: str | None = None,
    description: str | None = None,
    include_in_schema: bool = True,
) -> BodyMarker:
    """Construct a Litestar v2 compatible Body specification marker."""
    return BodyMarker(
        media_type=media_type,
        title=title,
        description=description,
        include_in_schema=include_in_schema,
    )


FromPath = Annotated[T, LocationMarker("path")]
FromQuery = Annotated[T, LocationMarker("query")]
FromHeader = Annotated[T, LocationMarker("header")]
FromCookie = Annotated[T, LocationMarker("cookie")]
JSONBody = Annotated[T, LocationMarker("body", "application/json")]
MsgPackBody = Annotated[T, LocationMarker("body", "application/x-msgpack")]
MultipartBody = Annotated[T, LocationMarker("body", "multipart/form-data")]
URLEncodedBody = Annotated[T, LocationMarker("body", "application/x-www-form-urlencoded")]
NamedDependency = Annotated[T, NamedDependencyMarker()]
SkipValidation = Annotated[T, SkipValidationMarker()]


@dataclass(frozen=True, slots=True)
class ParameterResolution:
    """Normalized parameter metadata extracted from Litestar v2 and v3 Annotated markers."""

    name: str
    external_name: str
    base_type: TypeNode
    location: ParameterLocation
    media_type: str | None
    constraints: tuple[tuple[str, object], ...]
    include_in_schema: bool
    skip_validation: bool
    required: bool = True


def resolve_parameter_node(
    param_name: str,
    node: TypeNode,
    *,
    path_param_names: set[str] | None = None,
) -> ParameterResolution:
    """Inspect a TypeNode and unwrap Litestar v2 and v3 Annotated markers into a ParameterResolution."""
    metadata_items: list[object] = []
    cursor = node
    while isinstance(cursor, AnnotatedType):
        metadata_items.extend(cursor.metadata)
        cursor = cursor.base

    location: ParameterLocation = "path" if (path_param_names and param_name in path_param_names) else "query"
    external_name = param_name
    media_type: str | None = None
    include_in_schema = True
    skip_validation = False
    required = True
    constraints: list[tuple[str, object]] = []

    for meta in metadata_items:
        if isinstance(meta, NamedDependencyMarker) or type(meta).__name__ in {
            "DependencyKwarg",
            "NamedDependencyMarker",
        }:
            include_in_schema = False
            continue

        if isinstance(meta, SkipValidationMarker) or getattr(meta, "skip_validation", False) is True:
            skip_validation = True
            continue

        if isinstance(meta, LocationMarker):
            location = meta.location
            if meta.media_type is not None:
                media_type = meta.media_type
            continue

        if isinstance(meta, BodyMarker) or type(meta).__name__ == "BodyKwarg":
            location = "body"
            raw_media = getattr(meta, "media_type", "application/json")
            media_type = str(raw_media) if raw_media else "application/json"
            if getattr(meta, "include_in_schema", True) is False:
                include_in_schema = False
            continue

        meta_cls_name = type(meta).__name__
        if isinstance(meta, ParameterMarker) or meta_cls_name in {
            "ParameterKwarg",
            "PathParameter",
            "QueryParameter",
            "HeaderParameter",
            "CookieParameter",
        }:
            if getattr(meta, "include_in_schema", True) is False:
                include_in_schema = False
            if meta_cls_name == "PathParameter":
                location = "path"
            elif meta_cls_name == "QueryParameter":
                location = "query"
            elif meta_cls_name == "HeaderParameter":
                location = "header"
            elif meta_cls_name == "CookieParameter":
                location = "cookie"
            alias_name = getattr(meta, "name", None)
            if alias_name:
                external_name = str(alias_name)
            header_val = getattr(meta, "header", None)
            cookie_val = getattr(meta, "cookie", None)
            query_val = getattr(meta, "query", None)
            if header_val:
                location = "header"
                external_name = str(header_val)
            elif cookie_val:
                location = "cookie"
                external_name = str(cookie_val)
            elif query_val:
                location = "query"
                external_name = str(query_val)
            if getattr(meta, "required", True) is False:
                required = False
            for key in ("ge", "gt", "le", "lt", "multiple_of", "min_length", "max_length", "pattern", "default"):
                val = getattr(meta, key, None)
                if val is not None:
                    constraints.append((key, val))
            continue

        if isinstance(meta, tuple) and len(meta) == 2 and isinstance(meta[0], str):
            constraints.append((meta[0], meta[1]))
            continue

        for key in ("ge", "gt", "le", "lt", "multiple_of", "min_length", "max_length", "pattern"):
            val = getattr(meta, key, None)
            if val is not None:
                constraints.append((key, val))

    if (
        isinstance(cursor, ObjectType)
        and cursor.name
        and location == "query"
        and not (path_param_names and param_name in path_param_names)
        and not any(
            isinstance(m, (LocationMarker, ParameterMarker))
            or type(m).__name__
            in {
                "ParameterKwarg",
                "PathParameter",
                "QueryParameter",
                "HeaderParameter",
                "CookieParameter",
            }
            for m in metadata_items
        )
    ):
        location = "body"
        media_type = media_type or "application/json"

    return ParameterResolution(
        name=param_name,
        external_name=external_name,
        base_type=cursor,
        location=location,
        media_type=media_type,
        constraints=tuple(constraints),
        include_in_schema=include_in_schema,
        skip_validation=skip_validation,
        required=required,
    )


@dataclass(frozen=True, slots=True)
class ProcedureParam:
    """Metadata for a single schema-visible parameter on a @procedure function."""

    name: str
    type_node: TypeNode
    resolution: ParameterResolution
    optional: bool = False


@dataclass(frozen=True, slots=True)
class ProcedureInfo:
    """Metadata record for a registered @procedure RPC callable."""

    name: str
    func: Callable[..., Any]
    params: dict[str, ProcedureParam]
    return_type: TypeNode
    is_async: bool
    mcp_enabled: bool = False
    mcp_description: str | None = None
    mcp_hints: dict[str, bool] | None = None
    grpc_enabled: bool = False


class ProcedureRegistry:
    """Thread-safe registry of @procedure RPC functions."""

    _instance: ClassVar[ProcedureRegistry | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()
    _procedures: dict[str, ProcedureInfo]
    _version: int
    _initialized: bool

    def __new__(cls) -> Self:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cast("Self", cls._instance)

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._procedures = {}
        self._version = 0
        self._initialized = True

    def register(self, info: ProcedureInfo) -> None:
        """Register a ProcedureInfo entry by unique procedure name."""
        with self._lock:
            if info.name in self._procedures:
                msg = f"Procedure '{info.name}' is already registered"
                raise ValueError(msg)
            self._procedures[info.name] = info
            self._version += 1

    def unregister(self, name: str) -> bool:
        """Remove a registered procedure by name."""
        with self._lock:
            if name in self._procedures:
                del self._procedures[name]
                self._version += 1
                return True
            return False

    def get(self, name: str) -> ProcedureInfo | None:
        """Look up a registered ProcedureInfo by name."""
        return self._procedures.get(name)

    def get_all(self) -> dict[str, ProcedureInfo]:
        """Return a shallow copy of all registered procedures."""
        return dict(self._procedures)

    def get_version(self) -> str:
        """Compute a deterministic 16-char SHA-256 fingerprint of registered procedure signatures."""
        parts: list[str] = []
        for name in sorted(self._procedures):
            proc = self._procedures[name]
            param_sig = ",".join(f"{p.name}:{p.type_node.to_typescript()}" for p in proc.params.values())
            parts.append(f"{name}({param_sig})->{proc.return_type.to_typescript()}")
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

    def clear(self) -> None:
        """Clear all registered procedures."""
        with self._lock:
            self._procedures.clear()
            self._version += 1

    def __len__(self) -> int:
        return len(self._procedures)

    def __contains__(self, name: str) -> bool:
        return name in self._procedures


def get_procedure_registry() -> ProcedureRegistry:
    """Return the singleton ProcedureRegistry instance."""
    return ProcedureRegistry()


@overload
def procedure[**P, R](
    func: Callable[P, R],
    *,
    name: str | None = None,
    mcp: bool = False,
    mcp_description: str | None = None,
    mcp_hints: dict[str, bool] | None = None,
    grpc: bool = False,
) -> Callable[P, R]: ...


@overload
def procedure[**P, R](
    func: None = None,
    *,
    name: str | None = None,
    mcp: bool = False,
    mcp_description: str | None = None,
    mcp_hints: dict[str, bool] | None = None,
    grpc: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def procedure[**P, R](
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    mcp: bool = False,
    mcp_description: str | None = None,
    mcp_hints: dict[str, bool] | None = None,
    grpc: bool = False,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Register a typed function or coroutine in the global ProcedureRegistry."""
    info_attr = "__procedure_info__"

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        proc_name = name if name is not None else fn.__name__
        extractor = TypeExtractor()
        raw_params, return_node = extractor.extract(fn)
        sig = inspect.signature(fn)

        visible_params: dict[str, ProcedureParam] = {}
        for param_name, param_node in raw_params.items():
            resolution = resolve_parameter_node(param_name, param_node)
            if not resolution.include_in_schema:
                continue
            sig_param = sig.parameters.get(param_name)
            has_default = sig_param is not None and sig_param.default is not inspect.Parameter.empty
            visible_params[param_name] = ProcedureParam(
                name=param_name,
                type_node=resolution.base_type,
                resolution=resolution,
                optional=has_default or not resolution.required,
            )

        is_async = asyncio.iscoroutinefunction(fn)
        info = ProcedureInfo(
            name=proc_name,
            func=fn,
            params=visible_params,
            return_type=return_node,
            is_async=is_async,
            mcp_enabled=mcp,
            mcp_description=mcp_description or inspect.getdoc(fn),
            mcp_hints=mcp_hints,
            grpc_enabled=grpc,
        )
        get_procedure_registry().register(info)

        if is_async:
            async_fn = cast("Callable[P, Coroutine[Any, Any, Any]]", fn)

            @functools.wraps(fn)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                return await async_fn(*args, **kwargs)

            setattr(async_wrapper, info_attr, info)
            return cast("Callable[P, R]", async_wrapper)

        @functools.wraps(fn)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return fn(*args, **kwargs)

        setattr(sync_wrapper, info_attr, info)
        return sync_wrapper

    if func is not None:
        return decorator(func)
    return decorator


@dataclass(slots=True)
class ProcedureClientGenerator:
    """Emit typed TypeScript RPC client declarations from ProcedureRegistry."""

    registry: ProcedureRegistry = field(default_factory=get_procedure_registry)
    declared: dict[str, ObjectType] = field(default_factory=dict)

    def generate_typescript_client(self) -> str:
        """Render TypeScript interfaces and the ProcedureRpcClient map for all registered procedures."""
        procedures = self.registry.get_all()
        for proc in procedures.values():
            self._collect_named_types(p.type_node for p in proc.params.values())
            self._collect_named_types((proc.return_type,))

        lines: list[str] = ["// DO NOT EDIT: generated by Vertebra procedure codegen."]
        for type_name in sorted(self.declared):
            lines.append(self._render_interface(type_name, self.declared[type_name]))

        lines.append("export interface ProcedureRpcClient {")
        for proc_name in sorted(procedures):
            proc = procedures[proc_name]
            param_entries: list[str] = []
            for param in proc.params.values():
                opt = "?" if param.optional else ""
                param_entries.append(f"{param.name}{opt}: {self._type_ref(param.type_node)}")
            params_sig = f"{{ {'; '.join(param_entries)} }}" if param_entries else "Record<string, never>"
            ret_sig = self._type_ref(proc.return_type)
            lines.append(f'  "{proc_name}": (params: {params_sig}) => Promise<{ret_sig}>;')
        lines.append("}")
        return "\n\n".join(lines).strip() + "\n"

    def _collect_named_types(self, nodes: Iterable[TypeNode]) -> None:
        for node in nodes:
            if isinstance(node, ObjectType) and node.name:
                self.declared.setdefault(node.name, node)
            for child in node.iter_children():
                self._collect_named_types((child,))

    def _render_interface(self, name: str, obj: ObjectType) -> str:
        rows = [f"export interface {name} {{"]
        for field_name, field_type in obj.fields.items():
            optional = "?" if field_name in obj.optional_fields else ""
            base = field_type.base if isinstance(field_type, AnnotatedType) else field_type
            rows.append(f"  {field_name}{optional}: {self._type_ref(base)};")
        rows.append("}")
        return "\n".join(rows)

    def _type_ref(self, node: TypeNode) -> str:
        if isinstance(node, AnnotatedType):
            return self._type_ref(node.base)
        if isinstance(node, ObjectType) and node.name:
            return node.name
        if isinstance(node, NamedTypeRef):
            return node.name
        return node.to_typescript()
