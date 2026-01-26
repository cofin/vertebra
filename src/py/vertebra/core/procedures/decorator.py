"""Procedure decorator for Vertebra type bridge."""

import inspect
from collections.abc import Callable, Coroutine, Mapping
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, Protocol, TypeVar, get_type_hints, overload

from vertebra.core.codegen.extractor import TypeExtractor
from vertebra.core.codegen.ir import RawType
from vertebra.core.protocols import Serializer
from vertebra.core.procedures.registry import ProcedureMetadata, ProcedureRegistry
from vertebra.core.procedures.serialization import detect_serializer

_MISSING = object()

P = ParamSpec("P")
R = TypeVar("R")


class _ProcedureDecorator(Protocol):
    """Protocol for decorator returned by procedure() with arguments."""

    @overload
    def __call__(self, fn: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]: ...

    @overload
    def __call__(self, fn: Callable[P, R]) -> Callable[P, R]: ...

    def __call__(self, fn: Callable[P, R]) -> Callable[P, R]: ...


@overload
def procedure[**P, R](
    _fn: Callable[P, Coroutine[Any, Any, R]],
    *,
    name: str | None = ...,
    serializer: str | Serializer | None = ...,
    metadata: Mapping[str, Any] | None = ...,
    type_overrides: Mapping[str, str] | None = ...,
) -> Callable[P, Coroutine[Any, Any, R]]: ...


@overload
def procedure[**P, R](
    _fn: Callable[P, R],
    *,
    name: str | None = ...,
    serializer: str | Serializer | None = ...,
    metadata: Mapping[str, Any] | None = ...,
    type_overrides: Mapping[str, str] | None = ...,
) -> Callable[P, R]: ...


@overload
def procedure(
    _fn: None = ...,
    *,
    name: str | None = ...,
    serializer: str | Serializer | None = ...,
    metadata: Mapping[str, Any] | None = ...,
    type_overrides: Mapping[str, str] | None = ...,
) -> _ProcedureDecorator: ...


def procedure[**P, R](
    _fn: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    serializer: str | Serializer | None = "msgspec",
    metadata: Mapping[str, Any] | None = None,
    type_overrides: Mapping[str, str] | None = None,
) -> Callable[P, R] | _ProcedureDecorator:
    """Register a handler for type extraction and client generation."""

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        registry = ProcedureRegistry.instance()
        extractor = TypeExtractor()
        try:
            hints = get_type_hints(fn, include_extras=True)
        except (AttributeError, NameError, TypeError, ValueError):
            hints = getattr(fn, "__annotations__", {}) or {}
        signature = inspect.signature(fn)

        params: dict[str, Any] = {}
        for param in signature.parameters.values():
            if param.kind in {param.VAR_KEYWORD, param.VAR_POSITIONAL}:
                continue
            params[param.name] = hints.get(param.name, _MISSING)

        overrides = dict(type_overrides or {})
        type_params = {
            key: RawType(overrides[key]) if key in overrides else extractor.from_hint(value)
            for key, value in params.items()
        }
        from vertebra.core.codegen.ir import StreamType

        return_hint = hints.get("return", _MISSING)
        return_type = RawType(overrides["return"]) if "return" in overrides else extractor.from_hint(return_hint)

        final_metadata = dict(metadata or {})
        if isinstance(return_type, StreamType):
            final_metadata["type"] = "subscription"

        serializer_value: str | Serializer
        if serializer is None:
            serializer_value = detect_serializer(return_hint if return_hint is not _MISSING else None)
        else:
            serializer_value = serializer

        procedure_id = name or f"{fn.__module__}.{fn.__qualname__}"
        source_path = Path(inspect.getsourcefile(fn) or inspect.getfile(fn))
        meta = ProcedureMetadata(
            procedure_id=procedure_id,
            handler_fn=fn,
            source_file=source_path,
            params=type_params,
            return_type=return_type,
            serializer=serializer_value,
            metadata=final_metadata,
        )
        registry.register(meta)

        opt = getattr(fn, "opt", None)
        if isinstance(opt, dict):
            opt["vertebra"] = {
                "procedure_id": procedure_id,
                "serializer": serializer_value
                if isinstance(serializer_value, str)
                else serializer_value.__class__.__name__,
                "metadata": final_metadata,
            }

        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return fn(*args, **kwargs)

        setattr(wrapper, "__vertebra_procedure__", meta)
        return wrapper

    if _fn is not None:
        return decorator(_fn)
    return decorator  # type: ignore[return-value]
