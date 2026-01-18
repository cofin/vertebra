"""Procedure decorator for Vertebra type bridge."""

from __future__ import annotations

import inspect
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_type_hints

from vertebra.procedures.registry import ProcedureMetadata, ProcedureRegistry
from vertebra.procedures.serialization import Serializer, detect_serializer
from vertebra.types.extractor import TypeExtractor
from vertebra.types.ir import RawType

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_MISSING = object()

def procedure(
    _fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    serializer: str | Serializer | None = "msgspec",
    metadata: Mapping[str, Any] | None = None,
    type_overrides: Mapping[str, str] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register a handler for type extraction and client generation."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
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
        return_hint = hints.get("return", _MISSING)
        return_type = RawType(overrides["return"]) if "return" in overrides else extractor.from_hint(return_hint)

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
            metadata=dict(metadata or {}),
        )
        registry.register(meta)

        opt = getattr(fn, "opt", None)
        if isinstance(opt, dict):
            opt["vertebra"] = {
                "procedure_id": procedure_id,
                "serializer": serializer_value
                if isinstance(serializer_value, str)
                else serializer_value.__class__.__name__,
                "metadata": dict(metadata or {}),
            }

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        setattr(wrapper, "__vertebra_procedure__", meta)
        return wrapper

    if _fn is not None:
        return decorator(_fn)
    return decorator
