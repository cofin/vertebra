"""Free-threaded CPython 3.14t GIL re-enablement auditor and extension quarantine."""

from __future__ import annotations

import logging
import os
import sys
import sysconfig
from dataclasses import dataclass

__all__ = (
    "GilAuditReport",
    "audit_gil_state",
    "is_free_threaded_build",
    "quarantine_unsafe_314t_modules",
    "snapshot_native_extensions",
)

log = logging.getLogger(__name__)
_NATIVE_SUFFIXES = (".so", ".pyd", ".dylib")


@dataclass(frozen=True, slots=True)
class GilAuditReport:
    """Result of auditing CPython's free-threaded GIL state around application import."""

    is_free_threaded_build: bool
    gil_enabled_before: bool
    gil_enabled_after: bool
    gil_reenabled_on_import: bool
    native_extensions_before: frozenset[str]
    native_extensions_after: frozenset[str]
    suspect_extensions: tuple[str, ...]
    quarantined_modules: tuple[str, ...]


def is_free_threaded_build() -> bool:
    """Return True when the interpreter was compiled with ``Py_GIL_Disabled``."""
    return bool(sysconfig.get_config_var("Py_GIL_Disabled"))


def _is_gil_currently_enabled() -> bool:
    checker = getattr(sys, "_is_gil_enabled", None)
    if checker is None:
        return True
    return bool(checker())


def snapshot_native_extensions() -> frozenset[str]:
    """Return the set of currently loaded native C/Rust extension module names in ``sys.modules``."""
    loaded: set[str] = set()
    for name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        file_attr = getattr(mod, "__file__", None)
        if isinstance(file_attr, str) and file_attr.endswith(_NATIVE_SUFFIXES):
            loaded.add(name)
    return frozenset(loaded)


def quarantine_unsafe_314t_modules() -> tuple[str, ...]:
    """Detect and quarantine known 3.14t-unsafe event loop modules such as ``uvloop <= 0.22.1``."""
    if not is_free_threaded_build() and _is_gil_currently_enabled():
        return ()
    quarantined: list[str] = []
    uvloop_mod = sys.modules.get("uvloop")
    if uvloop_mod is not None:
        raw_ver = str(getattr(uvloop_mod, "__version__", "0.0.0"))
        parts: list[int] = []
        for token in raw_ver.split(".")[:3]:
            digits = "".join(ch for ch in token if ch.isdigit())
            parts.append(int(digits) if digits else 0)
        version_tuple = tuple(parts)
        if version_tuple <= (0, 22, 1):
            quarantined.append(f"uvloop=={raw_ver}")
            log.warning(
                "Quarantining uvloop %s on free-threaded Python; falling back to standard asyncio loop",
                raw_ver,
            )
    return tuple(quarantined)


def audit_gil_state(
    *,
    before_extensions: frozenset[str] | None = None,
    gil_enabled_before: bool | None = None,
    gil_enabled_after: bool | None = None,
    strict: bool | None = None,
) -> GilAuditReport:
    """Audit whether importing application modules silently re-enabled the CPython GIL."""
    ft_build = is_free_threaded_build()
    before_ext = before_extensions if before_extensions is not None else frozenset()
    after_ext = snapshot_native_extensions()
    before_gil = (
        gil_enabled_before
        if gil_enabled_before is not None
        else (_is_gil_currently_enabled() if not ft_build else False)
    )
    after_gil = gil_enabled_after if gil_enabled_after is not None else _is_gil_currently_enabled()
    reenabled = not before_gil and after_gil
    suspect = tuple(sorted(after_ext - before_ext)) if reenabled else ()
    quarantined = quarantine_unsafe_314t_modules()

    enforce_strict = (
        strict
        if strict is not None
        else (os.environ.get("VERTEBRA_STRICT_FREE_THREADING", "0") in {"1", "true", "TRUE"})
    )

    if reenabled:
        suspect_list = ", ".join(suspect) if suspect else "<unknown native extension>"
        msg = (
            f"CPython GIL was silently re-enabled during module import on a free-threaded build. "
            f"Suspect native extensions lacking Py_mod_gil: {suspect_list}"
        )
        log.warning(msg)
        if enforce_strict:
            raise RuntimeError(msg)

    return GilAuditReport(
        is_free_threaded_build=ft_build,
        gil_enabled_before=before_gil,
        gil_enabled_after=after_gil,
        gil_reenabled_on_import=reenabled,
        native_extensions_before=before_ext,
        native_extensions_after=after_ext,
        suspect_extensions=suspect,
        quarantined_modules=quarantined,
    )
