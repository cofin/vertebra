"""Litestar v2/v3 parameter markers, @procedure RPC registry, and TypeScript client generator."""

from __future__ import annotations

import functools
import hashlib
import inspect
import threading
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar, Self, cast, overload

import msgspec

from vertebra.build.codegen._emitter import SchemaEmitter
from vertebra.build.codegen._extractor import TypeExtractor
from vertebra.build.codegen._ir import (
    ObjectType,
    TypeNode,
)
from vertebra.build.codegen._json_schema import (
    ParameterResolution,
    json_schema_to_type_node,
    resolve_parameter_node,
)
from vertebra.build.codegen._typescript import TypeScriptGenerator
from vertebra.build.codegen.markers import (
    Body,
    BodyMarker,
    Dependency,
    FromBody,
    FromCookie,
    FromHeader,
    FromPath,
    FromQuery,
    JSONBody,
    LocationMarker,
    MsgPackBody,
    MultipartBody,
    NamedDependency,
    NamedDependencyMarker,
    Parameter,
    ParameterLocation,
    ParameterMarker,
    Provide,
    SkipValidation,
    SkipValidationMarker,
    URLEncodedBody,
)

__all__ = (
    "Body",
    "BodyMarker",
    "Dependency",
    "FromBody",
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
    "ParameterLocation",
    "ParameterMarker",
    "ParameterResolution",
    "ProcedureClientGenerator",
    "ProcedureInfo",
    "ProcedureParam",
    "ProcedureRegistry",
    "Provide",
    "SkipValidation",
    "SkipValidationMarker",
    "URLEncodedBody",
    "get_procedure_registry",
    "json_schema_to_type_node",
    "procedure",
    "resolve_parameter_node",
)


class ProcedureParam(msgspec.Struct, frozen=True, gc=False):
    """Metadata for a single schema-visible parameter on a @procedure function."""

    name: str
    type_node: TypeNode
    resolution: ParameterResolution
    optional: bool = False


class ProcedureInfo(msgspec.Struct, frozen=True, gc=False):
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

        is_async = inspect.iscoroutinefunction(fn)
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


class ProcedureClientGenerator(SchemaEmitter):
    """Emit typed TypeScript RPC client declarations from ProcedureRegistry."""

    __slots__ = ("_ts_gen", "registry")

    def __init__(
        self,
        registry: ProcedureRegistry | None = None,
        declared: dict[str, ObjectType] | None = None,
    ) -> None:
        super().__init__(declared=declared)
        self.registry = registry if registry is not None else get_procedure_registry()
        self._ts_gen = TypeScriptGenerator(declared=self._declared)

    def generate_typescript_client(self) -> str:
        """Render TypeScript interfaces and the ProcedureRpcClient map for all registered procedures."""
        procedures = self.registry.get_all()
        for proc in procedures.values():
            self._collect_named_types(p.type_node for p in proc.params.values())
            self._collect_named_types((proc.return_type,))

        lines: list[str] = ["// DO NOT EDIT: generated by Vertebra procedure codegen."]
        for type_name in sorted(self._declared):
            lines.append(self._render_interface(type_name, self._declared[type_name]))

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

    def _render_interface(self, name: str, obj: ObjectType) -> str:
        return self._ts_gen._render_interface(name, obj)

    def _type_ref(self, node: TypeNode) -> str:
        return self._ts_gen._type_ref(node)
