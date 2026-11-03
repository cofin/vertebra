"""Structured deprecation warnings and PEP 695 @deprecated decorator with caller stack attribution."""

from __future__ import annotations

import functools
import inspect
import sys
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from types import FrameType

__all__ = (
    "DeprecatedKind",
    "deprecated",
    "warn_deprecation",
)

type DeprecatedKind = Literal[
    "function",
    "method",
    "coroutine",
    "class",
    "parameter",
    "property",
    "attribute",
    "import",
]

_VERTEBRA_ROOT: Path = Path(__file__).resolve().parents[1]


def warn_deprecation(
    version: str,
    deprecated_name: str,
    kind: DeprecatedKind,
    *,
    removal_in: str | None = None,
    alternative: str | None = None,
    info: str | None = None,
    pending: bool = False,
) -> None:
    """Emit a ``DeprecationWarning`` or ``PendingDeprecationWarning`` attributed to the external caller frame."""
    warning_cls = PendingDeprecationWarning if pending else DeprecationWarning
    action = "will be deprecated" if pending else "is deprecated"
    parts = [f"{kind.capitalize()} {deprecated_name!r} {action} since v{version}"]
    if removal_in:
        parts.append(f"and will be removed in v{removal_in}")
    sentence = " ".join(parts) + "."
    if alternative:
        sentence = f"{sentence} Use {alternative!r} instead."
    if info:
        sentence = f"{sentence} {info}"
    warnings.warn(sentence, category=warning_cls, stacklevel=_external_caller_stacklevel())


def deprecated[**P, T](
    version: str,
    *,
    removal_in: str | None = None,
    alternative: str | None = None,
    details: str | None = None,
    info: str | None = None,
    pending: bool = False,
    kind: DeprecatedKind | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorate a function, method, coroutine, or class to emit a deprecation warning when called."""
    extra_info = details if details is not None else info

    def _decorator(target: Callable[P, T]) -> Callable[P, T]:
        name = getattr(target, "__qualname__", getattr(target, "__name__", repr(target)))
        if isinstance(target, type):
            resolved_kind: DeprecatedKind = kind or "class"
            cls_target: Any = target
            orig_init = cls_target.__init__

            @functools.wraps(orig_init)
            def _wrapped_init(self: Any, *args: Any, **kwargs: Any) -> None:
                warn_deprecation(
                    version,
                    name,
                    resolved_kind,
                    removal_in=removal_in,
                    alternative=alternative,
                    info=extra_info,
                    pending=pending,
                )
                orig_init(self, *args, **kwargs)

            cls_target.__init__ = _wrapped_init
            return target

        if inspect.iscoroutinefunction(target):
            resolved_async_kind: DeprecatedKind = kind or "coroutine"

            @functools.wraps(target)
            async def _async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                warn_deprecation(
                    version,
                    name,
                    resolved_async_kind,
                    removal_in=removal_in,
                    alternative=alternative,
                    info=extra_info,
                    pending=pending,
                )
                return await target(*args, **kwargs)

            return cast("Callable[P, T]", _async_wrapper)

        resolved_sync_kind: DeprecatedKind = kind or ("method" if "." in name else "function")

        @functools.wraps(target)
        def _sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            warn_deprecation(
                version,
                name,
                resolved_sync_kind,
                removal_in=removal_in,
                alternative=alternative,
                info=extra_info,
                pending=pending,
            )
            return target(*args, **kwargs)

        return _sync_wrapper

    return _decorator


def _external_caller_stacklevel() -> int:
    """Compute ``stacklevel`` pointing to the first frame outside ``vertebra/``."""
    frame: FrameType | None = sys._getframe(1)
    level = 1
    while frame is not None:
        filename = frame.f_code.co_filename
        try:
            resolved = Path(filename).resolve()
            is_internal = resolved.is_relative_to(_VERTEBRA_ROOT)
        except Exception:
            is_internal = False
        if not is_internal:
            return max(2, level)
        frame = frame.f_back
        level += 1
    return 2
