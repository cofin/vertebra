"""Zero-skip conformance lane gate and versioned pytest-databases fixture matrix."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Self

import pytest

if TYPE_CHECKING:
    from _pytest.nodes import Item
    from _pytest.reports import TestReport

__all__ = (
    "ConformanceGate",
    "ServiceVersionMatrix",
)


class ConformanceGate:
    """Lane selector and zero-skip session gate controlled by ``VERTEBRA_CONFORMANCE_ENGINES``."""

    ENGINES_ENV: ClassVar[str] = "VERTEBRA_CONFORMANCE_ENGINES"
    POSTGRES_ZERO_SKIP_ENV: ClassVar[str] = "VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS"
    ENGINE_MARKER: ClassVar[str] = "driver_engine"
    LEGACY_MARKER: ClassVar[str] = "conformance"
    SUPPORTED_ENGINES: ClassVar[frozenset[str]] = frozenset(
        {"duckdb", "mariadb", "mssql", "mysql", "oracle", "redis", "sqlite"}
    )

    def __init__(self, engines: frozenset[str] = frozenset()) -> None:
        self._engines: frozenset[str] = frozenset(engines)
        self._executed_by_engine: dict[str, set[str]] = {engine: set() for engine in self._engines}
        self._skipped: list[tuple[str, str, str]] = []
        self._xfailed: list[tuple[str, str, str]] = []

    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> Self:
        """Parse ``VERTEBRA_CONFORMANCE_ENGINES`` from ``environ`` and validate requested engines."""
        raw = environ.get(cls.ENGINES_ENV, "")
        tokens = [tok.strip().lower() for tok in raw.split(",") if tok.strip()]
        if not tokens:
            return cls(frozenset())

        if "postgres" in tokens:
            if environ.get(cls.POSTGRES_ZERO_SKIP_ENV, "").strip() == "1":
                tokens = [tok for tok in tokens if tok != "postgres"]
                if not tokens:
                    return cls(frozenset())
            else:
                msg = (
                    "PostgreSQL conformance is gated via "
                    f"{cls.POSTGRES_ZERO_SKIP_ENV}=1; remove 'postgres' from "
                    f"{cls.ENGINES_ENV} or set {cls.POSTGRES_ZERO_SKIP_ENV}=1."
                )
                raise pytest.UsageError(msg)

        unknown = sorted({tok for tok in tokens if tok not in cls.SUPPORTED_ENGINES})
        if unknown:
            allowed = ", ".join(sorted(cls.SUPPORTED_ENGINES))
            msg = f"Unsupported {cls.ENGINES_ENV} value(s): {', '.join(unknown)}. Allowed engines: {allowed}"
            raise pytest.UsageError(msg)
        return cls(frozenset(tokens))

    @property
    def engines(self) -> frozenset[str]:
        """Return the set of active conformance engines."""
        return self._engines

    @property
    def active(self) -> bool:
        """Return ``True`` when at least one conformance engine is selected."""
        return bool(self._engines)

    @classmethod
    def item_engines(cls, item: Item) -> frozenset[str]:
        """Extract normalized engine names declared on ``item`` via markers or parameter specs."""
        discovered: set[str] = set()
        for marker_name in (cls.ENGINE_MARKER, cls.LEGACY_MARKER):
            for marker in item.iter_markers(marker_name):
                for arg in marker.args:
                    if isinstance(arg, str) and arg.strip():
                        discovered.add(arg.strip().lower())

        callspec = getattr(item, "callspec", None)
        if callspec is not None:
            params = getattr(callspec, "params", {})
            if isinstance(params, Mapping):
                for param_val in params.values():
                    engine_attr = getattr(param_val, "engine", None)
                    if isinstance(engine_attr, str) and engine_attr.strip():
                        discovered.add(engine_attr.strip().lower())
        return frozenset(discovered)

    def selects(self, item: Item, integration_root: Path) -> bool:
        """Return ``True`` when ``item`` should remain selected in the current pytest session."""
        if not self.active:
            return True
        raw_path = getattr(item, "path", getattr(item, "fspath", None))
        if raw_path is None:
            return False
        item_path = Path(str(raw_path)).resolve()
        if not item_path.is_relative_to(integration_root.resolve()):
            return True
        return bool(self.item_engines(item) & self._engines)

    def record(self, nodeid: str, engines: frozenset[str], report: TestReport) -> None:
        """Record a test report phase for a selected conformance item."""
        if not self.active:
            return
        matched = engines & self._engines
        if not matched:
            return
        when = str(getattr(report, "when", ""))
        wasxfail = getattr(report, "wasxfail", None)
        if wasxfail is not None and when in {"setup", "call"}:
            self._xfailed.append((nodeid, when, str(wasxfail)))
            return
        if getattr(report, "skipped", False) and when in {"setup", "call"}:
            longrepr = getattr(report, "longrepr", "")
            reason = str(longrepr[2]) if isinstance(longrepr, tuple) and len(longrepr) >= 3 else str(longrepr)
            self._skipped.append((nodeid, when, reason))
            return
        if when == "call" and (getattr(report, "passed", False) or getattr(report, "failed", False)):
            for eng in matched:
                self._executed_by_engine.setdefault(eng, set()).add(nodeid)

    def failures(self) -> list[str]:
        """Return human-readable zero-skip failure messages for the completed session."""
        if not self.active:
            return []
        errors: list[str] = []
        if self._skipped:
            sample = ", ".join(f"{nodeid} ({when}: {reason})" for nodeid, when, reason in self._skipped[:5])
            errors.append(f"{self.ENGINES_ENV} rejected {len(self._skipped)} skipped test(s): {sample}")
        if self._xfailed:
            sample = ", ".join(f"{nodeid} ({when}: {reason})" for nodeid, when, reason in self._xfailed[:5])
            errors.append(f"{self.ENGINES_ENV} rejected {len(self._xfailed)} xfail/xpass test(s): {sample}")
        for eng in sorted(self._engines):
            if not self._executed_by_engine.get(eng):
                errors.append(f"{self.ENGINES_ENV} engine {eng!r} executed 0 conformance tests")
        return errors


class ServiceVersionMatrix:
    """Maps ``VERTEBRA_CONFORMANCE_<FAMILY>_VERSION`` onto versioned ``pytest-databases`` fixtures."""

    FIXTURES: ClassVar[dict[str, dict[str, str]]] = {
        "mysql": {
            "8.0": "mysql_8_service",
            "8.4": "mysql_84_service",
            "9.6": "mysql_96_service",
            "9.7": "mysql_97_service",
        },
        "mariadb": {
            "11.3": "mariadb_113_service",
            "11.4": "mariadb_114_service",
            "11.8": "mariadb_118_service",
            "12.2": "mariadb_122_service",
            "12.3": "mariadb_123_service",
            "13.0": "mariadb_130_service",
        },
        "mssql": {
            "2022": "mssql_2022_service",
            "2025": "mssql_2025_service",
        },
        "oracle": {
            "23ai": "oracle_23ai_service",
        },
    }

    DEFAULTS: ClassVar[dict[str, str]] = {
        "mysql": "8.4",
        "mariadb": "11.4",
        "mssql": "2022",
        "oracle": "23ai",
    }

    @classmethod
    def version_env_var(cls, family: str) -> str:
        """Return the environment variable name controlling the version for ``family``."""
        return f"VERTEBRA_CONFORMANCE_{family.strip().upper()}_VERSION"

    @classmethod
    def fixture_name(cls, family: str, environ: Mapping[str, str] | None = None) -> str:
        """Resolve the ``pytest-databases`` service fixture name for ``family`` and ``environ``."""
        key = family.strip().lower()
        if key not in cls.FIXTURES:
            allowed_families = ", ".join(sorted(cls.FIXTURES))
            msg = f"Unsupported service family {family!r}. Allowed families: {allowed_families}"
            raise pytest.UsageError(msg)

        resolved_env = os.environ if environ is None else environ
        env_var = cls.version_env_var(key)
        raw_version = resolved_env.get(env_var, "").strip()
        version = raw_version if raw_version else cls.DEFAULTS[key]
        family_map = cls.FIXTURES[key]
        if version not in family_map:
            allowed_versions = ", ".join(sorted(family_map))
            msg = f"Unsupported {env_var}={version!r} for {key}. Allowed versions: {allowed_versions}"
            raise pytest.UsageError(msg)
        return family_map[version]
