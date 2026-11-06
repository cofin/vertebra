"""JSON Schema 2020-12 builder, ToolSpec, and tool descriptor for agentic harnesses.

This module provides msgspec-backed schema derivation (`typing.get_type_hints` ->
`msgspec.defstruct` -> `msgspec.json.schema_components`) for functions and methods,
immutable `ToolSpec` and `ToolAnnotations` structs, and `msgspec.convert` argument coercion.
"""

from __future__ import annotations

import copy
import inspect
import types
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Literal, Union, cast, get_args, get_origin, get_type_hints

import anyio
import msgspec

from vertebra.typing import FrozenBaseStruct

try:
    from vertebra import _vertebra

    _NativeSchemaDigestCache = getattr(_vertebra, "SchemaDigestCache", None)
except ImportError:
    _NativeSchemaDigestCache = None


type ToolExposure = Literal["mcp", "webmcp", "a2a"]

DEFAULT_TOOL_EXPOSURE: frozenset[ToolExposure] = frozenset({"mcp", "a2a"})


class _PySchemaDigestCache:
    """Pure-Python fallback for SchemaDigestCache."""

    FNV_OFFSET_BASIS: int = 0xCBF29CE484222325
    FNV_PRIME: int = 0x100000001B3
    MASK64: int = 0xFFFFFFFFFFFFFFFF

    def __init__(self) -> None:
        self._entries: dict[str, tuple[int, str, bytes]] = {}

    @classmethod
    def compute_schema_digest(cls, data: bytes) -> int:
        h = cls.FNV_OFFSET_BASIS
        for b in data:
            h ^= b
            h = (h * cls.FNV_PRIME) & cls.MASK64
        return h

    def put(self, key: str, json_bytes: bytes) -> tuple[int, str]:
        digest = self.compute_schema_digest(json_bytes)
        etag = f'"{digest:016x}"'
        self._entries[key] = (digest, etag, json_bytes)
        return digest, etag

    def get_etag(self, key: str) -> str | None:
        entry = self._entries.get(key)
        return entry[1] if entry is not None else None

    def get_bytes(self, key: str) -> bytes | None:
        entry = self._entries.get(key)
        return entry[2] if entry is not None else None

    def combined_etag(self) -> str:
        h = self.FNV_OFFSET_BASIS
        for key in sorted(self._entries.keys()):
            for b in key.encode("utf-8"):
                h ^= b
                h = (h * self.FNV_PRIME) & self.MASK64
            digest, _, _ = self._entries[key]
            h ^= digest
            h = (h * self.FNV_PRIME) & self.MASK64
        return f'"W/{h:016x}"'


if TYPE_CHECKING:

    class SchemaDigestCache(_PySchemaDigestCache):
        """Type-checking stub for SchemaDigestCache."""
else:
    SchemaDigestCache = _NativeSchemaDigestCache if _NativeSchemaDigestCache is not None else _PySchemaDigestCache

STRIP_TYPE_NAMES: frozenset[str] = frozenset(
    {
        "AgentContext",
        "Request",
        "ASGIConnection",
        "FromDishka",
        "State",
    }
)


class ToolAnnotations(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Semantic behavioral annotations for MCP and WebMCP tools."""

    read_only: bool = False
    """Whether the tool only reads state without mutating its environment."""
    destructive: bool = False
    """Whether the tool may perform destructive or irreversible mutations."""
    idempotent: bool = False
    """Whether repeated calls with identical arguments have no additional effect."""
    open_world: bool = False
    """Whether the tool interacts with external systems beyond the local runtime."""
    consequential: bool = False
    """Whether the tool carries high-impact side effects requiring user confirmation."""

    @classmethod
    def from_value(
        cls,
        raw: ToolAnnotations | Mapping[str, Any] | None = None,
        *,
        requires_confirmation: bool = False,
    ) -> ToolAnnotations:
        """Construct normalized `ToolAnnotations` from a mapping or existing instance."""
        if isinstance(raw, ToolAnnotations):
            if requires_confirmation and not raw.consequential:
                return msgspec.structs.replace(raw, consequential=True)
            return raw
        if not isinstance(raw, Mapping):
            return cls(consequential=bool(requires_confirmation))
        read_only = bool(raw.get("read_only", raw.get("readOnlyHint", False)))
        destructive = bool(raw.get("destructive", raw.get("destructiveHint", False)))
        idempotent = bool(raw.get("idempotent", raw.get("idempotentHint", False)))
        open_world = bool(
            raw.get(
                "open_world",
                raw.get("openWorldHint", raw.get("untrustedContentHint", False)),
            )
        )
        consequential = bool(raw.get("consequential", raw.get("consequentialHint", False)) or requires_confirmation)
        return cls(
            read_only=read_only,
            destructive=destructive,
            idempotent=idempotent,
            open_world=open_world,
            consequential=consequential,
        )

    def to_mcp_dict(self) -> dict[str, bool]:
        """Project annotations into MCP 2026-07-28 camelCase hint keys."""
        out: dict[str, bool] = {}
        if self.read_only:
            out["readOnlyHint"] = True
        if self.destructive:
            out["destructiveHint"] = True
        if self.idempotent:
            out["idempotentHint"] = True
        if self.open_world:
            out["openWorldHint"] = True
        if self.consequential:
            out["consequentialHint"] = True
        return out

    def to_webmcp_dict(self, *, requires_confirmation: bool = False) -> dict[str, bool]:
        """Project annotations into browser WebMCP hint keys."""
        out = self.to_mcp_dict()
        if requires_confirmation or self.consequential:
            out["consequentialHint"] = True
        return out


class ToolSpec(FrozenBaseStruct, frozen=True, kw_only=True):
    """Canonical immutable tool specification shared across MCP, WebMCP, and A2A."""

    name: str
    """Unique identifier of the tool."""
    title: str = ""
    """Human-readable display title for the tool."""
    description: str = ""
    """Summary explaining what the tool does and when to invoke it."""
    input_schema: dict[str, Any] = msgspec.field(default_factory=dict)
    """JSON Schema 2020-12 object schema describing tool input parameters."""
    output_schema: dict[str, Any] | None = None
    """Optional JSON Schema 2020-12 schema describing structured tool output."""
    annotations: ToolAnnotations = msgspec.field(default_factory=ToolAnnotations)
    """Semantic behavioral hints for MCP and WebMCP clients."""
    exposure: frozenset[ToolExposure] = msgspec.field(default_factory=lambda: DEFAULT_TOOL_EXPOSURE)
    """Protocol surfaces where this tool is advertised (`"mcp"`, `"webmcp"`, `"a2a"`)."""
    requires_confirmation: bool = False
    """Whether invoking this tool requires an approved human confirmation ticket."""
    guards: tuple[Any, ...] = ()
    """Ordered sequence of authorization guards enforced before tool execution."""
    timeout_seconds: float | None = None
    """Optional execution timeout in seconds."""


class ToolCall(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Tool execution request from a model or caller."""

    id: str
    """Unique correlation identifier for this tool invocation."""
    name: str
    """Registered tool name to execute."""
    arguments: dict[str, Any] = msgspec.field(default_factory=dict)
    """Input argument dictionary validated against the tool's JSON Schema."""


class ToolResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized result of a tool execution."""

    call_id: str
    """Correlation identifier matching the originating `ToolCall.id`."""
    name: str
    """Executed tool name."""
    output: Any = None
    """Return value produced by the tool callable when `is_error` is `False`."""
    error: str | None = None
    """Error message string when tool execution failed (`is_error` is `True`)."""
    is_error: bool = False
    """Whether the tool invocation raised an exception or failed validation."""


class ToolArgumentError(Exception):
    """Raised when an argument provided to a tool fails schema validation."""

    def __init__(
        self,
        param_name: str,
        message: str,
        pointer: str = "",
    ) -> None:
        super().__init__(f"Validation failed for argument '{param_name}': {message}")
        self.param_name = param_name
        self.message = message
        self.pointer = pointer or f"/{param_name}"


class SchemaBuilder:
    """Derives JSON Schema 2020-12 schemas from Python type hints via msgspec."""

    def __init__(self) -> None:
        self._defs: dict[str, Any] = {}

    @property
    def defs(self) -> dict[str, Any]:
        """Return collected schema definition components."""
        return dict(self._defs)

    @staticmethod
    def is_stripped_annotation(ann: Any, strip_names: set[str] | frozenset[str] = STRIP_TYPE_NAMES) -> bool:
        """Return whether a parameter annotation represents a framework-injected dependency."""
        if getattr(ann, "__name__", "") in strip_names:
            return True
        if isinstance(ann, str):
            stripped_str = ann.replace(" ", "")
            for name in strip_names:
                if stripped_str in (name, f"{name}|None", f"None|{name}", f"Optional[{name}]"):
                    return True
        origin = get_origin(ann)
        if origin is Annotated:
            ann_args = get_args(ann)
            if ann_args and SchemaBuilder.is_stripped_annotation(ann_args[0], strip_names):
                return True
            return any(hasattr(meta, "dependency") for meta in ann_args[1:])
        if origin is types.UnionType or origin is Union:
            non_none = [arg for arg in get_args(ann) if arg is not types.NoneType]
            return bool(non_none) and all(SchemaBuilder.is_stripped_annotation(arg, strip_names) for arg in non_none)
        return False

    @staticmethod
    def resolve_callable_hints(fn: Callable[..., Any]) -> dict[str, Any]:
        """Resolve type hints for a callable using typing.get_type_hints with include_extras=True."""
        target: Any = inspect.unwrap(fn)
        globalns: dict[str, Any] = dict(getattr(target, "__globals__", {}) or {})
        globalns.setdefault("Annotated", Annotated)
        globalns.setdefault("Literal", Literal)
        globalns.setdefault("Union", Union)
        globalns.setdefault("Any", Any)
        globalns.setdefault("msgspec", msgspec)
        try:
            hints = dict(get_type_hints(target, globalns=globalns, include_extras=True))
        except Exception:
            hints = {}
            raw_annotations = getattr(target, "__annotations__", None) or {}
            for name, val in raw_annotations.items():
                if not isinstance(val, str):
                    hints[name] = val
        sig = inspect.signature(fn)
        for param_name, param in sig.parameters.items():
            if param_name not in hints and param.annotation is not inspect.Parameter.empty:
                hints[param_name] = param.annotation
        if "return" not in hints and sig.return_annotation is not inspect.Signature.empty:
            hints["return"] = sig.return_annotation
        return hints

    @classmethod
    def resolve_parameter_hints(
        cls,
        fn: Callable[..., Any],
        *,
        strip_types: Sequence[type | str] = (),
    ) -> dict[str, Any]:
        """Return resolved parameter annotations for non-stripped callable parameters."""
        sig = inspect.signature(fn)
        hints = cls.resolve_callable_hints(fn)
        strip_names = cls._build_strip_names(strip_types)
        resolved: dict[str, Any] = {}
        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            if hasattr(param.default, "dependency"):
                continue
            ann = hints.get(param_name, param.annotation)
            if cls.is_stripped_annotation(ann, strip_names):
                continue
            resolved[param_name] = cls._normalize_annotated(ann)
        return resolved

    def build_callable_schema(
        self,
        fn: Callable[..., Any],
        *,
        strip_types: Sequence[type | str] = (),
    ) -> dict[str, Any]:
        """Derive an input JSON Schema for a callable via msgspec.defstruct and schema_components."""
        sig = inspect.signature(fn)
        hints = self.resolve_callable_hints(fn)
        strip_names = self._build_strip_names(strip_types)

        fields: list[tuple[str, Any] | tuple[str, Any, Any]] = []
        defaults_to_attach: dict[str, Any] = {}

        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            if hasattr(param.default, "dependency"):
                continue

            ann = hints.get(param_name, param.annotation)
            if self.is_stripped_annotation(ann, strip_names):
                continue

            normalized_ann = self._normalize_annotated(ann)
            field_type = (
                Any if normalized_ann is inspect.Parameter.empty or isinstance(normalized_ann, str) else normalized_ann
            )

            if param.default is inspect.Parameter.empty:
                fields.append((param_name, field_type))
            else:
                default_val = param.default
                if isinstance(default_val, (str, int, float, bool, list, dict)) or default_val is None:
                    defaults_to_attach[param_name] = default_val
                if isinstance(default_val, (list, dict, set)):
                    fields.append(
                        (
                            param_name,
                            field_type,
                            msgspec.field(default_factory=self._make_default_factory(default_val)),
                        )
                    )
                else:
                    fields.append((param_name, field_type, default_val))

        struct_name = f"_{getattr(fn, '__name__', 'Tool').lstrip('_') or 'Tool'}Input"
        param_struct = msgspec.defstruct(struct_name, fields, kw_only=True, gc=False)
        (_,), component_defs = msgspec.json.schema_components(
            (param_struct,),
            schema_hook=self._fallback_schema_hook,
        )
        root_def = dict(component_defs.pop(struct_name, {}))
        self._defs.update(component_defs)

        properties: dict[str, Any] = {
            prop_name: dict(prop_schema) for prop_name, prop_schema in (root_def.get("properties") or {}).items()
        }
        for param_name, default_val in defaults_to_attach.items():
            if param_name in properties and "default" not in properties[param_name]:
                properties[param_name]["default"] = default_val

        required = list(root_def.get("required") or [])
        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = required
        if self._defs:
            schema["$defs"] = dict(self._defs)
        return schema

    def build_output_schema(self, fn: Callable[..., Any]) -> dict[str, Any] | None:
        """Derive an output JSON Schema for a callable's return annotation via schema_components."""
        hints = self.resolve_callable_hints(fn)
        return_ann = hints.get("return", inspect.Signature.empty)
        return_ann = self._unwrap_awaitable_return(return_ann)
        if return_ann in (inspect.Signature.empty, None, types.NoneType, Any) or isinstance(return_ann, str):
            return None
        normalized = self._normalize_annotated(return_ann)
        try:
            (root_schema,), component_defs = msgspec.json.schema_components(
                (normalized,),
                schema_hook=self._fallback_schema_hook,
            )
        except Exception:
            return None

        schema_dict = dict(root_schema)
        ref = schema_dict.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            ref_name = ref.removeprefix("#/$defs/")
            if ref_name in component_defs:
                target_def = dict(component_defs.pop(ref_name))
                if component_defs:
                    target_def["$defs"] = dict(component_defs)
                return target_def
        if component_defs:
            schema_dict["$defs"] = dict(component_defs)
        return schema_dict

    @staticmethod
    def _build_strip_names(strip_types: Sequence[type | str]) -> set[str]:
        strip_names = set(STRIP_TYPE_NAMES)
        for st in strip_types:
            if isinstance(st, str):
                strip_names.add(st)
            elif hasattr(st, "__name__"):
                strip_names.add(st.__name__)
        return strip_names

    @classmethod
    def _normalize_annotated(cls, tp: Any) -> Any:
        if tp is inspect.Parameter.empty or tp is Any:
            return tp
        origin = get_origin(tp)
        if origin is Annotated:
            args = get_args(tp)
            base = cls._normalize_annotated(args[0])
            new_meta: list[Any] = []
            for item in args[1:]:
                if isinstance(item, str):
                    new_meta.append(msgspec.Meta(description=item))
                else:
                    new_meta.append(item)
            return Annotated[(base, *new_meta)]
        return tp

    @staticmethod
    def _unwrap_awaitable_return(tp: Any) -> Any:
        origin = get_origin(tp)
        if origin is Coroutine:
            args = get_args(tp)
            return args[2] if len(args) == 3 else Any
        if origin is Awaitable:
            args = get_args(tp)
            return args[0] if args else Any
        return tp

    @staticmethod
    def _make_default_factory(default_val: Any) -> Callable[[], Any]:
        def _factory() -> Any:
            return copy.copy(default_val)

        return _factory

    @staticmethod
    def _fallback_schema_hook(tp: type) -> dict[str, Any]:
        if isinstance(tp, type) and issubclass(tp, (bytes, bytearray, memoryview)):
            return {"type": "string"}
        name = getattr(tp, "__name__", "Object")
        return {"type": "object", "title": name}


class Tool:
    """Method and function descriptor encapsulating callable logic and a frozen ToolSpec."""

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        requires_confirmation: bool = False,
        timeout_seconds: float | None = None,
        webmcp: bool | None = None,
        exposure: Iterable[ToolExposure] | None = None,
        annotations: ToolAnnotations | Mapping[str, Any] | None = None,
        guards: Sequence[Any] = (),
        spec: ToolSpec | None = None,
        instance: Any = None,
    ) -> None:
        meta = getattr(fn, "_mcp_tool_meta", None)
        meta_dict = meta if isinstance(meta, dict) else {}
        self.fn = fn
        self._instance = instance
        self._signature = inspect.signature(fn)
        self._param_hints = SchemaBuilder.resolve_parameter_hints(fn)

        if spec is not None:
            resolved_guards = tuple(guards) if guards else spec.guards
            self.spec = (
                msgspec.structs.replace(spec, guards=resolved_guards) if resolved_guards != spec.guards else spec
            )
            self.name = self.spec.name
            self.title = self.spec.title
            self.description = self.spec.description
            self.input_schema = self.spec.input_schema
            self.output_schema = self.spec.output_schema
            self.requires_confirmation = self.spec.requires_confirmation
            self.timeout_seconds = self.spec.timeout_seconds
            self.exposure = self.spec.exposure
            self.webmcp = "webmcp" in self.spec.exposure
            self.guards = self.spec.guards
            raw_ann = annotations if isinstance(annotations, Mapping) else meta_dict.get("annotations")
            self.annotations: dict[str, Any] = (
                dict(raw_ann) if isinstance(raw_ann, Mapping) else self.spec.annotations.to_mcp_dict()
            )
            return

        resolved_name = str(name or meta_dict.get("name") or getattr(fn, "__name__", "tool"))
        resolved_title = str(title if title is not None else (meta_dict.get("title") or ""))
        resolved_description = str(
            description if description is not None else (meta_dict.get("description") or inspect.getdoc(fn) or "")
        )
        resolved_confirmation = bool(requires_confirmation or meta_dict.get("requires_confirmation", False))
        resolved_timeout = timeout_seconds if timeout_seconds is not None else meta_dict.get("timeout_seconds")

        raw_exposure = exposure if exposure is not None else meta_dict.get("exposure")
        resolved_webmcp = bool(meta_dict.get("webmcp", False)) if webmcp is None else bool(webmcp)
        if raw_exposure is not None:
            exposure_set: set[ToolExposure] = {
                cast("ToolExposure", item) for item in raw_exposure if item in ("mcp", "webmcp", "a2a")
            }
            if resolved_webmcp:
                exposure_set.add("webmcp")
            resolved_exposure = frozenset(exposure_set)
        else:
            base_exposure: set[ToolExposure] = set(DEFAULT_TOOL_EXPOSURE)
            if resolved_webmcp:
                base_exposure.add("webmcp")
            resolved_exposure = frozenset(base_exposure)

        raw_annotations = annotations if annotations is not None else meta_dict.get("annotations")
        struct_annotations = ToolAnnotations.from_value(
            raw_annotations,
            requires_confirmation=resolved_confirmation,
        )
        dict_annotations = (
            dict(raw_annotations) if isinstance(raw_annotations, Mapping) else struct_annotations.to_mcp_dict()
        )

        builder = SchemaBuilder()
        resolved_input_schema = builder.build_callable_schema(fn) if input_schema is None else dict(input_schema)
        resolved_output_schema = (
            builder.build_output_schema(fn)
            if output_schema is None and "output_schema" not in meta_dict
            else (output_schema if output_schema is not None else meta_dict.get("output_schema"))
        )
        resolved_guards = tuple(guards or meta_dict.get("guards", ()))

        self.spec = ToolSpec(
            name=resolved_name,
            title=resolved_title,
            description=resolved_description,
            input_schema=resolved_input_schema,
            output_schema=resolved_output_schema,
            annotations=struct_annotations,
            exposure=resolved_exposure,
            requires_confirmation=resolved_confirmation,
            guards=resolved_guards,
            timeout_seconds=resolved_timeout,
        )
        self.name = self.spec.name
        self.title = self.spec.title
        self.description = self.spec.description
        self.input_schema = self.spec.input_schema
        self.output_schema = self.spec.output_schema
        self.requires_confirmation = self.spec.requires_confirmation
        self.timeout_seconds = self.spec.timeout_seconds
        self.exposure = self.spec.exposure
        self.webmcp = "webmcp" in self.spec.exposure
        self.annotations = dict_annotations
        self.guards = self.spec.guards

    def __get__(self, instance: Any, owner: Any = None) -> Tool:
        """Bind tool to an instance when accessed as an attribute on an object."""
        if instance is None:
            return self
        return Tool(
            self.fn,
            annotations=self.annotations,
            spec=self.spec,
            instance=instance,
        )

    async def invoke(
        self,
        arguments: dict[str, Any],
        context: Any = None,
    ) -> Any:
        """Invoke the tool with validated arguments and context injection."""
        if self.guards:
            for guard in self.guards:
                check_fn = getattr(guard, "check", None)
                target = check_fn if callable(check_fn) else guard
                guard_res = target(context)
                if inspect.isawaitable(guard_res):
                    await guard_res

        sig = self._signature
        call_kwargs: dict[str, Any] = {}
        has_var_keyword = False

        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue

            if param.kind == inspect.Parameter.VAR_KEYWORD:
                has_var_keyword = True
                continue
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                continue

            ann = self._param_hints.get(param_name, param.annotation)
            if (
                param_name not in self._param_hints and SchemaBuilder.is_stripped_annotation(ann)
            ) or param_name == "context":
                if context is not None:
                    call_kwargs[param_name] = context
                continue

            if param_name not in arguments:
                if hasattr(param.default, "dependency") and callable(param.default.dependency):
                    dep_res = param.default.dependency()
                    if inspect.isawaitable(dep_res):
                        dep_res = await dep_res
                    call_kwargs[param_name] = dep_res
                    continue
                if param.default is inspect.Parameter.empty:
                    raise ToolArgumentError(
                        param_name=param_name,
                        message=f"Missing required parameter: {param_name}",
                        pointer=f"/{param_name}",
                    )
                continue

            raw_val = arguments[param_name]
            call_kwargs[param_name] = self._convert_argument(param_name, raw_val, ann)

        if has_var_keyword:
            for k, v in arguments.items():
                if k not in call_kwargs:
                    call_kwargs[k] = v

        res = self.fn(self._instance, **call_kwargs) if self._instance is not None else self.fn(**call_kwargs)

        if inspect.isawaitable(res):
            return await res
        return res

    def _convert_argument(self, param_name: str, value: Any, annotation: Any) -> Any:
        """Convert and validate a single argument against its resolved parameter type hint."""
        if annotation is inspect.Parameter.empty or annotation is Any:
            return value
        if isinstance(annotation, str):
            raise ToolArgumentError(
                param_name=param_name,
                message=f"Unresolved type annotation {annotation!r}",
                pointer=f"/{param_name}",
            )
        try:
            return msgspec.convert(value, annotation)
        except Exception as exc:
            raise ToolArgumentError(
                param_name=param_name,
                message=str(exc),
                pointer=f"/{param_name}",
            ) from exc


def tool(
    name: str | None = None,
    description: str | None = None,
    *,
    title: str | None = None,
    requires_confirmation: bool = False,
    timeout_seconds: float | None = None,
    webmcp: bool | None = None,
    exposure: Iterable[ToolExposure] | None = None,
    annotations: ToolAnnotations | Mapping[str, Any] | None = None,
    guards: Sequence[Any] = (),
) -> Callable[[Callable[..., Any]], Tool]:
    """Decorator converting a function or method into a Tool descriptor."""

    def decorator(fn: Callable[..., Any]) -> Tool:
        return Tool(
            fn,
            name=name,
            title=title,
            description=description,
            requires_confirmation=requires_confirmation,
            timeout_seconds=timeout_seconds,
            webmcp=webmcp,
            exposure=exposure,
            annotations=annotations,
            guards=guards,
        )

    return decorator


async def execute_tool_calls(
    tools: Mapping[str, Tool],
    calls: Sequence[ToolCall],
    *,
    context: Any = None,
    max_concurrency: int = 16,
) -> list[ToolResult]:
    """Execute multiple tool calls concurrently and return structured ToolResult items."""
    results: list[ToolResult | None] = [None] * len(calls)
    limiter = anyio.CapacityLimiter(max_concurrency)

    async def run_single(index: int, call: ToolCall) -> None:
        target_tool = tools.get(call.name)
        if target_tool is None:
            results[index] = ToolResult(
                call_id=call.id,
                name=call.name,
                error=f"Tool not found: {call.name}",
                is_error=True,
            )
            return

        async with limiter:
            try:
                output = await target_tool.invoke(call.arguments, context=context)
                results[index] = ToolResult(
                    call_id=call.id,
                    name=call.name,
                    output=output,
                    is_error=False,
                )
            except Exception as exc:
                results[index] = ToolResult(
                    call_id=call.id,
                    name=call.name,
                    error=str(exc),
                    is_error=True,
                )

    async with anyio.create_task_group() as tg:
        for idx, call in enumerate(calls):
            tg.start_soon(run_single, idx, call)

    return [res for res in results if res is not None]


__all__ = [
    "DEFAULT_TOOL_EXPOSURE",
    "STRIP_TYPE_NAMES",
    "SchemaBuilder",
    "SchemaDigestCache",
    "Tool",
    "ToolAnnotations",
    "ToolArgumentError",
    "ToolCall",
    "ToolExposure",
    "ToolResult",
    "ToolSpec",
    "execute_tool_calls",
    "tool",
]
