"""Free-threaded CPython 3.14t GIL re-enablement auditor and extension quarantine."""

import asyncio
import logging
import os
import sys
import sysconfig
from collections.abc import Iterable

import msgspec

__all__ = (
    "GilAuditReport",
    "is_free_threaded",
)

log = logging.getLogger(__name__)
_NATIVE_SUFFIXES = (".so", ".pyd", ".dylib")


def is_free_threaded() -> bool:
    """Return True when running on a free-threaded Python interpreter with the GIL disabled."""
    return not GilAuditReport.gil_enabled()


class GilAuditReport(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Result of auditing CPython's free-threaded GIL state around application import."""

    free_threaded_build: bool = msgspec.field(name="is_free_threaded_build", default=False)
    """Whether the running interpreter was compiled with `Py_GIL_Disabled` (e.g. Python 3.14t/3.15t)."""
    gil_enabled_before: bool = True
    """Whether the CPython GIL was active prior to importing the target application module."""
    gil_enabled_after: bool = True
    """Whether the CPython GIL is active after importing the target application module."""
    gil_reenabled_on_import: bool = False
    """True when importing the application caused CPython to fall back and re-enable the GIL."""
    native_extensions_before: frozenset[str] = frozenset()
    """Set of native `.so`/`.pyd`/`.dylib` extension module names loaded before import."""
    native_extensions_after: frozenset[str] = frozenset()
    """Set of native `.so`/`.pyd`/`.dylib` extension module names loaded after import."""
    suspect_extensions: tuple[str, ...] = ()
    """Newly imported native extensions present when `gil_reenabled_on_import` is True."""
    quarantined_modules: tuple[str, ...] = ()
    """Loaded modules known to be unsafe under free-threaded execution (e.g. `uvloop<=0.22.1`)."""

    @staticmethod
    def gil_enabled() -> bool:
        """Return True when the CPython GIL is currently enabled at runtime."""
        checker = getattr(sys, "_is_gil_enabled", None)
        if checker is None:
            return True
        return bool(checker())

    @staticmethod
    def is_free_threaded_build() -> bool:
        """Return True when the interpreter was compiled with Py_GIL_Disabled."""
        return bool(sysconfig.get_config_var("Py_GIL_Disabled"))

    @staticmethod
    def snapshot_extensions() -> frozenset[str]:
        """Return the set of currently loaded native C/Rust extension module names in sys.modules."""
        loaded: set[str] = set()
        for name, mod in list(sys.modules.items()):
            if mod is None:
                continue
            file_attr = getattr(mod, "__file__", None)
            if isinstance(file_attr, str) and file_attr.endswith(_NATIVE_SUFFIXES):
                loaded.add(name)
        return frozenset(loaded)

    @classmethod
    def snapshot_loaded_extensions(cls) -> frozenset[str]:
        """Snapshot the currently loaded native extensions before application import."""
        return cls.snapshot_extensions()

    def quarantine_unsafe_modules(self, requested_shards: int) -> int:
        """Enforce standard asyncio policy and single-shard execution when GIL is re-enabled or extensions are suspect."""
        if self.gil_reenabled_on_import or bool(self.suspect_extensions):
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
            return 1
        return max(1, requested_shards)

    @staticmethod
    def detect_unsafe_modules(allowlist: Iterable[str] | None = None) -> tuple[str, ...]:
        """Detect known 3.14t-unsafe event loop modules such as uvloop <= 0.22.1."""
        allowed = set(allowlist) if allowlist is not None else set()
        if not GilAuditReport.is_free_threaded_build() and GilAuditReport.gil_enabled():
            return ()
        quarantined: list[str] = []
        if "uvloop" not in allowed:
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

    @classmethod
    def audit(
        cls,
        *,
        before_extensions: frozenset[str] | None = None,
        gil_enabled_before: bool | None = None,
        gil_enabled_after: bool | None = None,
        strict: bool | None = None,
        allowlist: Iterable[str] | None = None,
    ) -> "GilAuditReport":
        """Audit whether importing application modules silently re-enabled the CPython GIL."""
        ft_build = cls.is_free_threaded_build()
        resolved_before = before_extensions if before_extensions is not None else frozenset()
        after_ext = cls.snapshot_extensions()
        before_gil = (
            gil_enabled_before if gil_enabled_before is not None else (cls.gil_enabled() if not ft_build else False)
        )
        after_gil = gil_enabled_after if gil_enabled_after is not None else cls.gil_enabled()
        reenabled = not before_gil and after_gil
        suspect = tuple(sorted(after_ext - resolved_before)) if reenabled else ()
        quarantined = cls.detect_unsafe_modules(allowlist=allowlist)

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

        return cls(
            free_threaded_build=ft_build,
            gil_enabled_before=before_gil,
            gil_enabled_after=after_gil,
            gil_reenabled_on_import=reenabled,
            native_extensions_before=resolved_before,
            native_extensions_after=after_ext,
            suspect_extensions=suspect,
            quarantined_modules=quarantined,
        )
