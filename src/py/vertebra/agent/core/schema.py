"""JSON Schema 2020-12 builder and tool descriptor for agentic harnesses.

This module provides reflection-based schema derivation for functions and methods,
handling msgspec.Struct, dataclasses, TypedDict, Enum, Literal, and Annotated types,
while stripping dependency-injected parameters such as AgentContext.
"""

import dataclasses
import inspect
import types
from collections.abc import Callable, Mapping, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, Literal, Union, get_args, get_origin, is_typeddict

import anyio
import msgspec

try:
    from vertebra import _vertebra

    _NativeSchemaDigestCache = getattr(_vertebra, "SchemaDigestCache", None)
except ImportError:
    _NativeSchemaDigestCache = None


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


class ToolCall(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Tool execution request from a model or caller."""

    id: str
    name: str
    arguments: dict[str, Any] = msgspec.field(default_factory=dict)


class ToolResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized result of a tool execution."""

    call_id: str
    name: str
    output: Any = None
    error: str | None = None
    is_error: bool = False


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
    """Derives JSON Schema 2020-12 schemas from Python type hints."""

    def __init__(self) -> None:
        self._defs: dict[str, Any] = {}

    @property
    def defs(self) -> dict[str, Any]:
        """Return collected schema definition components."""
        return dict(self._defs)

    def _type_to_schema(self, tp: Any) -> dict[str, Any]:
        """Convert a single Python type into a JSON schema dict, populating defs."""
        if tp is Any or tp is inspect.Parameter.empty:
            return {}

        origin = get_origin(tp)

        if origin is Annotated:
            args = get_args(tp)
            base_schema = self._type_to_schema(args[0])
            for meta in args[1:]:
                if isinstance(meta, str):
                    base_schema["description"] = meta
                elif isinstance(meta, msgspec.Meta):
                    if meta.description is not None:
                        base_schema["description"] = meta.description
                    if meta.ge is not None:
                        base_schema["minimum"] = meta.ge
                    if meta.gt is not None:
                        base_schema["exclusiveMinimum"] = meta.gt
                    if meta.le is not None:
                        base_schema["maximum"] = meta.le
                    if meta.lt is not None:
                        base_schema["exclusiveMaximum"] = meta.lt
                    if meta.min_length is not None:
                        base_schema["minLength"] = meta.min_length
                    if meta.max_length is not None:
                        base_schema["maxLength"] = meta.max_length
                    if meta.pattern is not None:
                        base_schema["pattern"] = meta.pattern
            return base_schema

        if origin is Literal:
            literal_values = list(get_args(tp))
            return {"enum": literal_values}

        if origin is types.UnionType or origin is Union:
            union_args = get_args(tp)
            schemas = [self._type_to_schema(arg) for arg in union_args]
            return {"anyOf": schemas}

        if isinstance(tp, type) and issubclass(tp, (str, bytes)):
            return {"type": "string"}
        if isinstance(tp, type) and issubclass(tp, bool):
            return {"type": "boolean"}
        if isinstance(tp, type) and issubclass(tp, int):
            return {"type": "integer"}
        if isinstance(tp, type) and issubclass(tp, float):
            return {"type": "number"}

        if isinstance(tp, type) and issubclass(tp, Enum):
            enum_values = [e.value for e in tp]
            name = tp.__name__
            if name not in self._defs:
                self._defs[name] = {"title": name, "enum": enum_values}
            return {"$ref": f"#/$defs/{name}"}

        if isinstance(tp, type) and issubclass(tp, msgspec.Struct):
            struct_schema = msgspec.json.schema(tp)
            if "$defs" in struct_schema:
                self._defs.update(struct_schema["defs" if "defs" in struct_schema else "$defs"])
            if "$ref" in struct_schema:
                return {"$ref": struct_schema["$ref"]}
            return struct_schema

        if dataclasses.is_dataclass(tp):
            name = tp.__name__ if hasattr(tp, "__name__") else "Dataclass"
            if name not in self._defs:
                properties: dict[str, Any] = {}
                required: list[str] = []
                for field in dataclasses.fields(tp):
                    properties[field.name] = self._type_to_schema(field.type)
                    if field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING:
                        required.append(field.name)
                schema_def: dict[str, Any] = {
                    "title": name,
                    "type": "object",
                    "properties": properties,
                }
                if required:
                    schema_def["required"] = required
                self._defs[name] = schema_def
            return {"$ref": f"#/$defs/{name}"}

        if is_typeddict(tp):
            name = tp.__name__ if hasattr(tp, "__name__") else "TypedDict"
            if name not in self._defs:
                properties = {}
                required = []
                for k, v in getattr(tp, "__annotations__", {}).items():
                    properties[k] = self._type_to_schema(v)
                    if k in getattr(tp, "__required_keys__", ()):
                        required.append(k)
                td_def: dict[str, Any] = {
                    "title": name,
                    "type": "object",
                    "properties": properties,
                }
                if required:
                    td_def["required"] = required
                self._defs[name] = td_def
            return {"$ref": f"#/$defs/{name}"}

        if origin in (list, Sequence, tuple, set, frozenset):
            args = get_args(tp)
            item_schema = self._type_to_schema(args[0]) if args else {}
            return {"type": "array", "items": item_schema}

        if origin in (dict, Mapping):
            args = get_args(tp)
            value_schema = self._type_to_schema(args[1]) if len(args) > 1 else {}
            return {"type": "object", "additionalProperties": value_schema}

        if isinstance(tp, type):
            name = tp.__name__
            return {"type": "object", "title": name}

        return {}

    def build_callable_schema(
        self,
        fn: Callable[..., Any],
        *,
        strip_types: Sequence[type | str] = (),
    ) -> dict[str, Any]:
        """Derive an input JSON Schema for a callable function or method."""
        sig = inspect.signature(fn)
        properties: dict[str, Any] = {}
        required: list[str] = []

        strip_names = set(STRIP_TYPE_NAMES)
        for st in strip_types:
            if isinstance(st, str):
                strip_names.add(st)
            elif hasattr(st, "__name__"):
                strip_names.add(st.__name__)

        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue

            ann = param.annotation
            ann_name = getattr(ann, "__name__", "")
            if ann_name in strip_names:
                continue

            origin = get_origin(ann)
            if origin is Annotated:
                base_ann = get_args(ann)[0]
                if getattr(base_ann, "__name__", "") in strip_names:
                    continue

            param_schema = self._type_to_schema(ann)
            if param.default is not inspect.Parameter.empty:
                param_schema["default"] = param.default
            else:
                required.append(param_name)

            properties[param_name] = param_schema

        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = required
        if self._defs:
            schema["$defs"] = dict(self._defs)

        return schema


class Tool:
    """Method and function descriptor encapsulating callable logic and JSON schema."""

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        description: str | None = None,
        input_schema: dict[str, Any] | None = None,
        requires_confirmation: bool = False,
        timeout_seconds: float | None = None,
        instance: Any = None,
    ) -> None:
        self.fn = fn
        self.name = name or fn.__name__
        self.description = description or inspect.getdoc(fn) or ""
        self.requires_confirmation = requires_confirmation
        self.timeout_seconds = timeout_seconds
        self._instance = instance

        if input_schema is None:
            builder = SchemaBuilder()
            self.input_schema = builder.build_callable_schema(fn)
        else:
            self.input_schema = input_schema

    def __get__(self, instance: Any, owner: Any = None) -> "Tool":
        """Bind tool to an instance when accessed as an attribute on an object."""
        if instance is None:
            return self
        return Tool(
            self.fn,
            name=self.name,
            description=self.description,
            input_schema=self.input_schema,
            requires_confirmation=self.requires_confirmation,
            timeout_seconds=self.timeout_seconds,
            instance=instance,
        )

    def _convert_argument(self, param_name: str, value: Any, annotation: Any) -> Any:
        """Convert and validate a single argument against parameter type hint."""
        if annotation is inspect.Parameter.empty or annotation is Any:
            return value

        resolved_ann = annotation
        if isinstance(resolved_ann, str):
            try:
                resolved_ann = eval(
                    resolved_ann,
                    {"int": int, "str": str, "float": float, "bool": bool, "dict": dict, "list": list, "Any": Any},
                )
            except Exception:
                return value

        try:
            return msgspec.convert(value, resolved_ann)
        except Exception as exc:
            pointer = f"/{param_name}"
            raise ToolArgumentError(
                param_name=param_name,
                message=str(exc),
                pointer=pointer,
            ) from exc

    async def invoke(
        self,
        arguments: dict[str, Any],
        context: Any = None,
    ) -> Any:
        """Invoke the tool with validated arguments and context injection."""
        sig = inspect.signature(self.fn)
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

            ann = param.annotation
            ann_name = getattr(ann, "__name__", "")
            if ann_name in STRIP_TYPE_NAMES or param_name == "context":
                if context is not None:
                    call_kwargs[param_name] = context
                continue

            if param_name not in arguments:
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


def tool(
    name: str | None = None,
    description: str | None = None,
    *,
    requires_confirmation: bool = False,
    timeout_seconds: float | None = None,
) -> Callable[[Callable[..., Any]], Tool]:
    """Decorator converting a function or method into a Tool descriptor."""

    def decorator(fn: Callable[..., Any]) -> Tool:
        return Tool(
            fn,
            name=name,
            description=description,
            requires_confirmation=requires_confirmation,
            timeout_seconds=timeout_seconds,
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
    "STRIP_TYPE_NAMES",
    "SchemaBuilder",
    "SchemaDigestCache",
    "Tool",
    "ToolArgumentError",
    "ToolCall",
    "ToolResult",
    "execute_tool_calls",
    "tool",
]
