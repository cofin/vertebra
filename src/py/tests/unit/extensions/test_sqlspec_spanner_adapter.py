"""Unit tests for Vertebra SQLSpec Cloud Spanner and Spangres modular adapter package."""

import ast
import asyncio
import json
import subprocess
import sys
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
from sqlspec.core import SQL, ArrowResult, StackResult, StatementStack, TypedParameter
from sqlspec.data_dictionary._types import MetadataCapabilityProfile
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    DialectNotSupportedError,
    IntegrityError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.exceptions import (
    ImproperConfigurationError as SQLSpecImproperConfigurationError,
)
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class

from vertebra._spanner import SyncSpannerCursor
from vertebra._vertebra import SpannerAbortedError, SpannerError
from vertebra.db.adapters.spanner import (
    Mutation,
    MutationGroup,
    SpannerConfig,
    StalenessBound,
)
from vertebra.db.arrow import ArrowCursor
from vertebra.db.dbapi import connect
from vertebra.db.driver import NotSupportedError
from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.spanner import (
    SpangresAsyncConfig,
    SpangresSyncConfig,
    SpannerAsyncADKArtifactStore,
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerAsyncConfig,
    SpannerAsyncConnectionContext,
    SpannerAsyncDataDictionary,
    SpannerAsyncDriver,
    SpannerAsyncSessionContext,
    SpannerConnectionBatch,
    SpannerConnectionParams,
    SpannerConnectionShim,
    SpannerDriverFeatures,
    SpannerEventQueueStore,
    SpannerLitestarStore,
    SpannerPoolParams,
    SpannerSyncADKArtifactStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
    SpannerSyncConfig,
    SpannerSyncConnectionContext,
    SpannerSyncDataDictionary,
    SpannerSyncDriver,
    SpannerSyncSessionContext,
    encode_spanner_parameters,
    map_spanner_exception,
)
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    compile_spanner_statement,
    default_statement_config,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError, MissingDependencyError

_SPANNER_ADAPTER_DIR = (
    Path(__file__).resolve().parents[3] / "vertebra" / "extensions" / "sqlspec" / "adapters" / "spanner"
)


class _CodedSpannerError(SpannerError):
    """Test subclass enabling explicit ``code`` attachment on ``SpannerError`` instances."""


def _make_spanner_error(message: str, code: int | None = None) -> SpannerError:
    if code is None:
        return SpannerError(message)
    exc = _CodedSpannerError(message)
    exc.code = code
    return exc


def _extract_imported_modules(source: str) -> list[str]:
    tree = ast.parse(source)
    imports = [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    from_imports = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ]
    return imports + from_imports


class _RecordingSyncSnapshot:
    """Offline synchronous snapshot handle for ``SpannerSyncConfig.provide_session`` tests."""

    def __init__(self, parent: "_RecordingSpannerSyncNative", staleness: StalenessBound | None) -> None:
        self.parent = parent
        self.staleness = staleness
        self.closed = False

    def query(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.parent.calls.append(("snapshot_query", (sql, parameters, self.staleness), dict(kwargs)))
        if self.parent.description_cols:
            return [dict(zip(self.parent.description_cols, row, strict=False)) for row in self.parent.description_rows]
        return list(self.parent.select_rows)

    def close(self) -> None:
        self.closed = True


class _RecordingSyncCursor:
    """PEP 249 cursor double backed by ``_RecordingSpannerSyncNative``."""

    def __init__(self, parent: "_RecordingSpannerSyncNative", **kwargs: Any) -> None:
        self._parent = parent
        self._kwargs = dict(kwargs)
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: Any = None
        self.arraysize: int = 1
        self._rows: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> "_RecordingSyncCursor":
        self._parent.calls.append(("execute", (sql, params), dict(self._kwargs)))
        stripped = sql.lstrip().upper()
        is_returning = "THEN RETURN" in stripped or "RETURNING" in stripped
        if stripped.startswith(("SELECT", "WITH", "GRAPH")) or is_returning:
            if self._parent.description_cols:
                self.description = tuple(
                    (col, None, None, None, None, None, None) for col in self._parent.description_cols
                )
                self._rows = [
                    tuple(row)
                    if isinstance(row, (tuple, list))
                    else tuple(row[col] for col in self._parent.description_cols)
                    for row in self._parent.description_rows
                ]
            elif self._parent.select_rows:
                cols = list(self._parent.select_rows[0].keys())
                self.description = tuple((col, None, None, None, None, None, None) for col in cols)
                self._rows = [tuple(row[col] for col in cols) for row in self._parent.select_rows]
            else:
                self.description = ()
                self._rows = []
            self.rowcount = len(self._rows)
            return self
        self.description = None
        self._rows = []
        self.rowcount = 1
        return self

    def executemany(self, sql: str, seq_of_params: Any) -> "_RecordingSyncCursor":
        batch = list(seq_of_params)
        self._parent.calls.append(("executemany", (sql, batch), dict(self._kwargs)))
        self.description = None
        self._rows = []
        self.rowcount = len(batch)
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = list(self._rows)
        self._rows = []
        return rows

    def close(self) -> None:
        self.closed = True


class _RecordingAsyncCursor:
    """Async PEP 249 cursor double backed by ``_RecordingSpannerAsyncNative``."""

    def __init__(self, parent: "_RecordingSpannerAsyncNative", **kwargs: Any) -> None:
        self._parent = parent
        self._kwargs = dict(kwargs)
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: Any = None
        self.arraysize: int = 1
        self._rows: list[tuple[Any, ...]] = []
        self.closed = False

    async def execute(self, sql: str, params: Any = None) -> "_RecordingAsyncCursor":
        self._parent.calls.append(("execute", (sql, params), dict(self._kwargs)))
        stripped = sql.lstrip().upper()
        is_returning = "THEN RETURN" in stripped or "RETURNING" in stripped
        if stripped.startswith(("SELECT", "WITH", "GRAPH")) or is_returning:
            if self._parent.description_cols:
                self.description = tuple(
                    (col, None, None, None, None, None, None) for col in self._parent.description_cols
                )
                self._rows = [
                    tuple(row)
                    if isinstance(row, (tuple, list))
                    else tuple(row[col] for col in self._parent.description_cols)
                    for row in self._parent.description_rows
                ]
            elif self._parent.select_rows:
                cols = list(self._parent.select_rows[0].keys())
                self.description = tuple((col, None, None, None, None, None, None) for col in cols)
                self._rows = [tuple(row[col] for col in cols) for row in self._parent.select_rows]
            else:
                self.description = ()
                self._rows = []
            self.rowcount = len(self._rows)
            return self
        self.description = None
        self._rows = []
        self.rowcount = 1
        return self

    async def executemany(self, sql: str, seq_of_params: Any) -> "_RecordingAsyncCursor":
        batch = list(seq_of_params)
        self._parent.calls.append(("executemany", (sql, batch), dict(self._kwargs)))
        self.description = None
        self._rows = []
        self.rowcount = len(batch)
        return self

    async def fetchall(self) -> list[tuple[Any, ...]]:
        rows = list(self._rows)
        self._rows = []
        return rows

    async def close(self) -> None:
        self.closed = True


class _RecordingSpannerSyncNative:
    """Offline synchronous double simulating ``SyncSpannerDriver`` calls."""

    def __init__(self, *, dialect: str = "googlesql") -> None:
        self.project_id = "proj-spanner"
        self.instance_id = "inst-spanner"
        self.database_id = "db-spanner"
        self.dialect = dialect
        self.spanner_dialect = dialect
        self.config = SpannerConfig(
            project_id="proj-spanner",
            instance_id="inst-spanner",
            database_id="db-spanner",
            dialect="spangres" if dialect == "spangres" else "googlesql",
        )
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.select_rows: list[dict[str, Any]] = []
        self.description_rows: list[Any] = []
        self.description_cols: list[str] = []
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def cursor(self, **kwargs: Any) -> _RecordingSyncCursor:
        return _RecordingSyncCursor(self, **kwargs)

    @contextmanager
    def snapshot(self, staleness: StalenessBound | None = None, *, multi_use: bool = True) -> Any:
        snap = _RecordingSyncSnapshot(self, staleness)
        self.calls.append(("snapshot", (staleness, multi_use), {}))
        try:
            yield snap
        finally:
            snap.close()

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        self.calls.append(("execute", (sql, parameters), dict(kwargs)))
        return 1

    def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        batch = list(param_sets or [])
        self.calls.append(("execute_many", (sql, batch), dict(kwargs)))
        return len(batch)

    def execute_batch_dml(self, statements: Any, **kwargs: Any) -> list[int]:
        items = list(statements)
        self.calls.append(("execute_batch_dml", items, dict(kwargs)))
        return [1 for _ in items]

    def execute_partitioned_dml(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        self.calls.append(("execute_partitioned_dml", (sql, parameters), dict(kwargs)))
        return 7

    def execute_script(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("execute_script", (sql, parameters), dict(kwargs)))
        return [{"ok": True}]

    def execute_with_description(self, sql: str, parameters: Any = None, **kwargs: Any) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", (sql, parameters), dict(kwargs)))
        return self.description_rows, self.description_cols

    def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("select", (sql, parameters), dict(kwargs)))
        return list(self.select_rows)

    def select_to_arrow(self, sql: str, parameters: Any = None, **kwargs: Any) -> ArrowCursor:
        self.calls.append(("select_to_arrow", (sql, parameters), dict(kwargs)))
        return ArrowCursor.from_rows([{"id": 1, "name": "alpha"}], threshold_rows=1)

    def load_from_arrow(self, table: str, source: Any, **kwargs: Any) -> int:
        self.calls.append(("load_from_arrow", (table, source), dict(kwargs)))
        return int(getattr(source, "num_rows", 2))

    def run_in_transaction(self, fn: Any, **kwargs: Any) -> Any:
        self.calls.append(("run_in_transaction", fn, dict(kwargs)))
        return fn(self)

    def apply_mutations(self, mutations: Any, **kwargs: Any) -> dict[str, Any]:
        items = list(mutations)
        self.calls.append(("apply_mutations", items, dict(kwargs)))
        return {"commit_timestamp": "2026-11-16T00:00:00Z"}

    def batch_write(self, mutation_groups: Any, **kwargs: Any) -> list[dict[str, Any]]:
        groups = list(mutation_groups)
        self.calls.append(("batch_write", groups, dict(kwargs)))
        return [{"status": "OK"} for _ in groups]

    def update_ddl(self, statements: Any, **kwargs: Any) -> str:
        stmts = [statements] if isinstance(statements, str) else list(statements)
        self.calls.append(("update_ddl", stmts, dict(kwargs)))
        return "operations/ddl-1"

    def begin(self) -> None:
        self.calls.append(("begin", None, {}))

    def commit(self) -> None:
        self.calls.append(("commit", None, {}))

    def rollback(self) -> None:
        self.calls.append(("rollback", None, {}))


class _RecordingAsyncSnapshot:
    """Offline asynchronous snapshot handle for ``SpannerAsyncConfig.provide_session`` tests."""

    def __init__(self, parent: "_RecordingSpannerAsyncNative", staleness: StalenessBound | None) -> None:
        self.parent = parent
        self.staleness = staleness
        self.closed = False

    async def query(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.parent.calls.append(("snapshot_query", (sql, parameters, self.staleness), dict(kwargs)))
        if self.parent.description_cols:
            return [dict(zip(self.parent.description_cols, row, strict=False)) for row in self.parent.description_rows]
        return list(self.parent.select_rows)

    async def close(self) -> None:
        self.closed = True


class _RecordingSpannerAsyncNative:
    """Offline asynchronous double simulating ``AsyncSpannerDriver`` calls."""

    def __init__(self, *, dialect: str = "googlesql") -> None:
        self.project_id = "proj-async"
        self.instance_id = "inst-async"
        self.database_id = "db-async"
        self.dialect = dialect
        self.spanner_dialect = dialect
        self.config = SpannerConfig(
            project_id="proj-async",
            instance_id="inst-async",
            database_id="db-async",
            dialect="spangres" if dialect == "spangres" else "googlesql",
        )
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.select_rows: list[dict[str, Any]] = []
        self.description_rows: list[Any] = []
        self.description_cols: list[str] = []
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    def cursor(self, **kwargs: Any) -> _RecordingAsyncCursor:
        return _RecordingAsyncCursor(self, **kwargs)

    @asynccontextmanager
    async def snapshot(self, staleness: StalenessBound | None = None, *, multi_use: bool = True) -> Any:
        snap = _RecordingAsyncSnapshot(self, staleness)
        self.calls.append(("snapshot", (staleness, multi_use), {}))
        try:
            yield snap
        finally:
            await snap.close()

    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        self.calls.append(("execute", (sql, parameters), dict(kwargs)))
        return 1

    async def execute_batch_dml(self, statements: Any, **kwargs: Any) -> list[int]:
        items = list(statements)
        self.calls.append(("execute_batch_dml", items, dict(kwargs)))
        return [1 for _ in items]

    async def execute_partitioned_dml(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        self.calls.append(("execute_partitioned_dml", (sql, parameters), dict(kwargs)))
        return 4

    async def execute_with_description(
        self, sql: str, parameters: Any = None, **kwargs: Any
    ) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", (sql, parameters), dict(kwargs)))
        return self.description_rows, self.description_cols

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("select", (sql, parameters), dict(kwargs)))
        return list(self.select_rows)

    async def select_to_arrow(self, sql: str, parameters: Any = None, **kwargs: Any) -> ArrowCursor:
        self.calls.append(("select_to_arrow", (sql, parameters), dict(kwargs)))
        return ArrowCursor.from_rows([{"id": 2, "name": "beta"}], threshold_rows=1)

    async def load_from_arrow(self, table: str, source: Any, **kwargs: Any) -> int:
        self.calls.append(("load_from_arrow", (table, source), dict(kwargs)))
        return int(getattr(source, "num_rows", 2))

    async def batch_write(self, mutation_groups: Any, **kwargs: Any) -> list[dict[str, Any]]:
        groups = list(mutation_groups)
        self.calls.append(("batch_write", groups, dict(kwargs)))
        return [{"status": "OK"} for _ in groups]

    async def update_ddl(self, statements: Any, **kwargs: Any) -> str:
        stmts = [statements] if isinstance(statements, str) else list(statements)
        self.calls.append(("update_ddl", stmts, dict(kwargs)))
        return "operations/ddl-async"


def test_spanner_lazy_exports_and_no_adk_eager_import() -> None:
    """Verify PEP 562 lazy Spanner exports do not eagerly import ``pyarrow`` or ``google.adk``."""
    probe = (
        "import sys; "
        "import vertebra.extensions.sqlspec.adapters.spanner as sp; "
        "cfg = sp.SpannerSyncConfig(project_id='p', instance_id='i', database_id='d'); "
        "acfg = sp.SpannerAsyncConfig(project_id='p', instance_id='i', database_id='d'); "
        "assert cfg.dialect == 'spanner' and acfg.dialect == 'spanner'; "
        "assert 'pyarrow' not in sys.modules; "
        "assert 'google.adk' not in sys.modules"
    )
    proc = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr

    pkg = import_module("vertebra.extensions.sqlspec.adapters.spanner")
    assert pkg.SpannerSyncConfig is SpannerSyncConfig
    assert pkg.SpannerAsyncConfig is SpannerAsyncConfig
    assert pkg.SpangresSyncConfig is SpangresSyncConfig
    assert pkg.SpangresAsyncConfig is SpangresAsyncConfig
    assert pkg.SpannerSyncDriver is SpannerSyncDriver
    assert pkg.SpannerAsyncDriver is SpannerAsyncDriver
    assert pkg.SpannerSyncConnectionContext is SpannerSyncConnectionContext
    assert pkg.SpannerAsyncConnectionContext is SpannerAsyncConnectionContext
    assert pkg.SpannerSyncSessionContext is SpannerSyncSessionContext
    assert pkg.SpannerAsyncSessionContext is SpannerAsyncSessionContext
    assert pkg.SpannerConnectionParams is SpannerConnectionParams
    assert pkg.SpannerPoolParams is SpannerPoolParams
    assert pkg.SpannerDriverFeatures is SpannerDriverFeatures
    assert pkg.SpannerSyncADKStore is SpannerSyncADKStore
    assert pkg.SpannerAsyncADKStore is SpannerAsyncADKStore
    assert pkg.SpannerSyncADKMemoryStore is SpannerSyncADKMemoryStore
    assert pkg.SpannerAsyncADKMemoryStore is SpannerAsyncADKMemoryStore
    assert pkg.SpannerSyncADKArtifactStore is SpannerSyncADKArtifactStore
    assert pkg.SpannerAsyncADKArtifactStore is SpannerAsyncADKArtifactStore
    assert pkg.SpannerEventQueueStore is SpannerEventQueueStore
    assert pkg.SpannerLitestarStore is SpannerLitestarStore
    assert pkg.SpannerSyncDataDictionary is SpannerSyncDataDictionary
    assert pkg.SpannerAsyncDataDictionary is SpannerAsyncDataDictionary
    assert pkg.SpannerConnectionShim is SpannerConnectionShim
    assert pkg.SpannerConnectionBatch is SpannerConnectionBatch
    with pytest.raises(AttributeError):
        _ = pkg.NonExistentSpannerExport


@pytest.mark.parametrize("module_path", sorted(_SPANNER_ADAPTER_DIR.glob("*.py")))
def test_spanner_package_has_no_forbidden_imports(module_path: Path) -> None:
    """Enforce zero Google client SDK, upstream sqlspec.adapters, or BigQuery imports."""
    imported = _extract_imported_modules(module_path.read_text(encoding="utf-8"))
    forbidden = [
        name
        for name in imported
        if name == "google" or name.startswith(("google.", "sqlspec.adapters")) or "bigquery" in name
    ]
    assert forbidden == []


def test_spanner_and_spangres_config_and_store_resolution() -> None:
    """Verify GoogleSQL and Spangres SQLSpec dialect wiring, adapter resolution, and shim unwrapping."""
    native = _RecordingSpannerSyncNative(dialect="googlesql")
    shim = SpannerConnectionShim(cast(Any, native))
    assert shim.project_id == "proj-spanner"
    assert shim.database_path == "projects/proj-spanner/instances/inst-spanner/databases/db-spanner"

    sync_cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=shim,
    )
    async_cfg = SpannerAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=_RecordingSpannerAsyncNative(dialect="googlesql"),
    )
    pg_sync_cfg = SpangresSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=_RecordingSpannerSyncNative(dialect="spangres"),
    )
    pg_async_cfg = SpangresAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=_RecordingSpannerAsyncNative(dialect="spangres"),
    )
    pg_via_flag = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        dialect="postgresql",
    )

    assert sync_cfg.adapter_name == "vertebra_spanner"
    assert sync_cfg.dialect == "spanner"
    assert sync_cfg.spanner_dialect == "googlesql"
    assert sync_cfg._resolve_core_driver() is native
    assert async_cfg.dialect == "spanner"
    assert async_cfg.spanner_dialect == "googlesql"
    assert pg_sync_cfg.dialect == "spangres"
    assert pg_sync_cfg.spanner_dialect == "spangres"
    assert pg_async_cfg.dialect == "spangres"
    assert pg_via_flag.dialect == "spangres"
    assert pg_via_flag.spanner_dialect == "spangres"

    assert resolve_adapter_name(sync_cfg) == "vertebra_spanner"
    assert resolve_adapter_name(pg_sync_cfg) == "vertebra_spanner"
    assert (
        resolve_extension_store_class(sync_cfg, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is SpannerSyncADKStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg,
            extension="adk",
            store_attr="adk_memory_store_class",
            store_suffix="ADKMemoryStore",
        )
        is SpannerSyncADKMemoryStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg,
            extension="events",
            store_attr="events_store_class",
            store_suffix="EventQueueStore",
        )
        is SpannerEventQueueStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg,
            extension="litestar",
            store_attr="litestar_store_class",
            store_suffix="LitestarStore",
        )
        is SpannerLitestarStore
    )
    assert (
        resolve_extension_store_class(
            pg_async_cfg, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore"
        )
        is SpannerAsyncADKStore
    )
    assert (
        resolve_extension_store_class(
            pg_async_cfg,
            extension="adk",
            store_attr="adk_memory_store_class",
            store_suffix="ADKMemoryStore",
        )
        is SpannerAsyncADKMemoryStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is SpannerSyncADKArtifactStore
    )
    assert (
        resolve_extension_store_class(
            async_cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is SpannerAsyncADKArtifactStore
    )
    assert (
        resolve_extension_store_class(
            pg_sync_cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is SpannerSyncADKArtifactStore
    )
    assert (
        resolve_extension_store_class(
            pg_async_cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is SpannerAsyncADKArtifactStore
    )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: SpannerSyncConfig(project_id="p", instance_id="i", database_id="d", tenant_predicate="tenant_id = @t"),
        lambda: SpannerAsyncConfig(project_id="p", instance_id="i", database_id="d", tenant_id="t-1"),
        lambda: SpannerSyncConfig(
            project_id="p",
            instance_id="i",
            database_id="d",
            driver_features={"tenant_predicate": "tenant_id = @t"},
        ),
        lambda: SpannerSyncDriver(_RecordingSpannerSyncNative(), tenant_id="t-2"),
        lambda: SpannerAsyncDriver(_RecordingSpannerAsyncNative(), tenant_predicate="tenant_id = @t"),
    ],
)
def test_spanner_rejects_tenant_enforcement(factory: Any) -> None:
    """Confirm unsupported tenant session enforcement fails loudly with ``NotSupportedError``."""
    with pytest.raises(NotSupportedError, match="Tenant session enforcement"):
        factory()


def test_spanner_encode_parameters_googlesql_and_spangres() -> None:
    """Verify typed parameter encoding and Spangres ``$N`` to ``pN`` normalization."""
    ts = datetime(2026, 11, 16, 1, 2, 3, tzinfo=UTC)
    google_encoded = encode_spanner_parameters(
        {
            "@id": 42,
            "raw": b"payload",
            "meta": {"a": 1},
            "ts": ts,
            "wrapped": TypedParameter("ok", str, semantic_name="STRING"),
            "nums": TypedParameter([1, 2], list, semantic_name="ARRAY<INT64>"),
        },
        dialect="googlesql",
    )
    assert google_encoded["id"] == 42
    assert google_encoded["raw"] == {"type": "BYTES", "value": "cGF5bG9hZA=="}
    assert google_encoded["meta"] == {"type": "JSON", "value": '{"a":1}'}
    assert google_encoded["ts"] == {"type": "TIMESTAMP", "value": "2026-11-16T01:02:03Z"}
    assert google_encoded["wrapped"] == {"type": "STRING", "value": "ok"}
    assert google_encoded["nums"] == {"type": "ARRAY", "array_type": "INT64", "value": [1, 2]}

    spangres_pos = encode_spanner_parameters([10, "hello"], dialect="spangres")
    assert spangres_pos == {"p1": 10, "p2": "hello"}

    spangres_named = encode_spanner_parameters({"$1": 99, "$2": "world"}, dialect="spangres")
    assert spangres_named == {"p1": 99, "p2": "world"}


def test_spanner_provide_session_snapshot_read_only_and_rw_transaction() -> None:
    """Verify ReadOnly snapshot guardrails, ReadWrite primitives, and savepoint rejection."""
    native = _RecordingSpannerSyncNative(dialect="googlesql")
    native.description_cols = ["id", "name"]
    native.description_rows = [(1, "alice")]
    staleness = StalenessBound.exact_staleness(5000)
    cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=native,
    )

    with cfg.provide_session(transaction=False, staleness=staleness) as ro_driver:
        assert ro_driver.is_read_only is True
        assert ro_driver.in_transaction is False
        assert ro_driver.staleness is staleness
        selected = ro_driver.select("SELECT id, name FROM users WHERE id = @id", {"id": 1})
        assert selected == [{"id": 1, "name": "alice"}]
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.execute("DELETE FROM users WHERE id = @id", {"id": 1})
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.execute_partitioned_dml("DELETE FROM users WHERE TRUE")
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.execute_batch_dml([("DELETE FROM users WHERE id = @id", {"id": 1})])
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.batch_write([MutationGroup((Mutation("insert", "users", ("id",), ((1,),)),))])
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.update_ddl("CREATE TABLE t (id INT64) PRIMARY KEY (id)")
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.load_from_arrow("users", pa.table({"id": [1]}))
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            ro_driver.begin()

    with cfg.provide_session(transaction=True) as rw_driver:
        assert rw_driver.is_read_only is False
        assert rw_driver.in_transaction is True
        pdml_count = rw_driver.execute_partitioned_dml(
            "DELETE FROM users WHERE active = @active",
            {"active": False},
            exclude_txn_from_change_streams=True,
        )
        assert pdml_count == 7
        batch_counts = rw_driver.execute_batch_dml(
            [("INSERT INTO users (id) VALUES (@id)", {"id": 1})],
            request_tag="req-1",
            transaction_tag="tx-1",
        )
        assert batch_counts == [1]
        bw_res = rw_driver.batch_write(
            [MutationGroup((Mutation("insert", "users", ("id",), ((2,),)),))],
            exclude_txn_from_change_streams=True,
        )
        assert bw_res == [{"status": "OK"}]
        bw_call = [c for c in native.calls if c[0] == "batch_write"][-1]
        assert "max_commit_delay_ms" not in bw_call[2]
        assert bw_call[2] == {"exclude_txn_from_change_streams": True}
        assert rw_driver.update_ddl("ALTER TABLE users ADD COLUMN bio STRING(MAX)") == "operations/ddl-1"
        with pytest.raises(NotSupportedError, match="savepoints"):
            rw_driver.create_savepoint("sp_1")


def test_spanner_async_provide_session_and_driver() -> None:
    """Verify asynchronous snapshot read-only enforcement, ReadWrite execution, and Arrow ops."""
    async_native = _RecordingSpannerAsyncNative(dialect="googlesql")
    async_native.description_cols = ["id", "name"]
    async_native.description_rows = [(2, "bob")]
    cfg = SpannerAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=async_native,
    )

    async def _run() -> None:
        async with cfg.provide_session(transaction=False, read_only=True) as ro:
            assert ro.is_read_only is True
            rows = await ro.select("SELECT id, name FROM users")
            assert rows == [{"id": 2, "name": "bob"}]
            with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
                await ro.execute("DELETE FROM users WHERE id = 2")
            arrow_res = await ro.select_to_arrow(
                "SELECT id, name FROM users",
                data_boost=True,
                max_partitions=6,
            )
            assert isinstance(arrow_res, ArrowResult)
            assert arrow_res.rows_affected == 1

        async with cfg.provide_session(transaction=True) as rw:
            assert rw.in_transaction is True
            assert await rw.execute_partitioned_dml("DELETE FROM users WHERE TRUE") == 4
            job = await rw.load_from_arrow("users", pa.table({"id": [10, 20]}), overwrite=True)
            assert job.telemetry["rows_processed"] == 2
            load_call = [c for c in async_native.calls if c[0] == "load_from_arrow"][-1]
            assert load_call[2]["upsert"] is False
            with pytest.raises(NotSupportedError, match="savepoints"):
                await rw.create_savepoint("sp_async")

    asyncio.run(_run())
    arrow_call = [c for c in async_native.calls if c[0] == "select_to_arrow"][-1]
    assert arrow_call[2]["data_boost_enabled"] is True
    assert arrow_call[2]["max_partitions"] == 6


def test_spanner_statement_stack_batch_dml_and_arrow_bridge() -> None:
    """Verify pure DML ``StatementStack`` coalescing vs mixed fallback plus Arrow bridge semantics."""
    native = _RecordingSpannerSyncNative(dialect="googlesql")
    native.description_cols = ["cnt"]
    native.description_rows = [(2,)]
    drv = SpannerSyncDriver(native, read_only=False, in_transaction=True)

    dml_stack = (
        StatementStack()
        .push_execute("INSERT INTO events (id) VALUES (@id)", {"id": 1})
        .push_execute("INSERT INTO events (id) VALUES (@id)", {"id": 2})
    )
    results = drv.execute_stack(dml_stack)
    assert len(results) == 2
    assert all(isinstance(item, StackResult) and item.rows_affected == 1 for item in results)
    assert any(call[0] == "execute_batch_dml" and len(call[1]) == 2 for call in native.calls)

    native.calls.clear()
    mixed_stack = (
        StatementStack()
        .push_execute("SELECT COUNT(*) AS cnt FROM events")
        .push_execute("DELETE FROM events WHERE id = @id", {"id": 99})
    )
    mixed_results = drv.execute_stack(mixed_stack)
    assert len(mixed_results) == 2
    assert not any(call[0] == "execute_batch_dml" for call in native.calls)

    staleness = StalenessBound.exact_staleness(2500)
    arrow_res = drv.select_to_arrow(
        "SELECT id, name FROM users",
        data_boost=True,
        max_partitions=8,
        staleness=staleness,
    )
    assert isinstance(arrow_res, ArrowResult)
    assert arrow_res.rows_affected == 1
    arrow_call = [c for c in native.calls if c[0] == "select_to_arrow"][-1]
    assert arrow_call[2] == {
        "data_boost_enabled": True,
        "max_partitions": 8,
        "staleness": staleness,
    }

    tbl = pa.table({"id": [1, 2, 3]})
    job = drv.load_from_arrow("events", tbl, overwrite=False)
    assert job.status == "completed"
    assert job.telemetry["rows_processed"] == 3
    load_call = [c for c in native.calls if c[0] == "load_from_arrow"][-1]
    assert load_call[2]["upsert"] is True


def test_spanner_data_dictionary_capabilities() -> None:
    """Verify GoogleSQL and Spangres data dictionary metadata capability profiles and column queries."""
    google_native = _RecordingSpannerSyncNative(dialect="googlesql")
    google_native.description_cols = ["table_name", "column_name", "data_type", "is_nullable", "column_default"]
    google_native.description_rows = [("users", "id", "INT64", "NO", None)]
    spangres_native = _RecordingSpannerSyncNative(dialect="spangres")
    spangres_native.description_cols = ["table_name", "column_name", "data_type", "is_nullable", "column_default"]
    spangres_native.description_rows = [("users", "id", "bigint", "NO", None)]

    google_sync_drv = SpannerSyncDriver(google_native)
    spangres_sync_drv = SpannerSyncDriver(spangres_native)
    google_async_drv = SpannerAsyncDriver(_RecordingSpannerAsyncNative(dialect="googlesql"))

    sync_dd = SpannerSyncDataDictionary()
    spangres_dd = SpannerSyncDataDictionary(dialect="spangres")
    async_dd = SpannerAsyncDataDictionary()

    assert sync_dd.get_version(google_sync_drv) is None
    cap_google = sync_dd.get_metadata_capabilities(google_sync_drv)
    cap_spangres = spangres_dd.get_metadata_capabilities(spangres_sync_drv)
    cap_async = asyncio.run(async_dd.get_metadata_capabilities(google_async_drv))

    assert isinstance(cap_google, MetadataCapabilityProfile)
    assert isinstance(cap_spangres, MetadataCapabilityProfile)
    assert isinstance(cap_async, MetadataCapabilityProfile)
    assert cap_google.dialect == "spanner"
    assert cap_spangres.dialect == "spangres"
    assert cap_google.get("columns") is not None
    assert cap_google.get("change_streams") is not None
    assert cap_google.get("property_graphs") is not None
    assert cap_spangres.get("columns") is not None

    google_cols = sync_dd.get_columns(google_sync_drv, table="users")
    spangres_cols = spangres_dd.get_columns(spangres_sync_drv, table="users")
    assert len(google_cols) == 1 and google_cols[0]["column_name"] == "id"
    assert len(spangres_cols) == 1 and spangres_cols[0]["column_name"] == "id"
    assert any("INFORMATION_SCHEMA.COLUMNS" in c[1][0].upper() for c in google_native.calls)
    assert any("INFORMATION_SCHEMA.COLUMNS" in c[1][0].upper() for c in spangres_native.calls)


def test_spanner_stores_adk_artifacts_events_and_litestar() -> None:
    """Verify Spanner ADK, Memory RRF, Artifact, EventQueue, and Litestar stores across dialects."""
    google_native = _RecordingSpannerSyncNative(dialect="googlesql")
    google_native.select_rows = [
        {
            "id": "mem-1",
            "session_id": "s1",
            "app_name": "app1",
            "user_id": "user1",
            "scope": "user",
            "event_id": "e1",
            "author": "agent",
            "timestamp": "2026-11-16T00:00:00Z",
            "content_json": '{"text":"alpha beta"}',
            "content_text": "alpha beta",
            "metadata_json": "{}",
            "inserted_at": "2026-11-16T00:00:00Z",
            "embedding": "[1.0, 0.0]",
        },
        {
            "id": "mem-2",
            "session_id": "s1",
            "app_name": "app1",
            "user_id": "user1",
            "scope": "user",
            "event_id": "e2",
            "author": "agent",
            "timestamp": "2026-11-16T00:01:00Z",
            "content_json": '{"text":"gamma delta"}',
            "content_text": "gamma delta",
            "metadata_json": "{}",
            "inserted_at": "2026-11-16T00:01:00Z",
            "embedding": "[0.0, 1.0]",
        },
    ]
    google_native.description_cols = list(google_native.select_rows[0].keys())
    google_native.description_rows = [tuple(row.values()) for row in google_native.select_rows]
    backend = ObjectStoreBackend("memory://")
    google_cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        storage_backend=backend,
        connection_instance=google_native,
    )
    spangres_cfg = SpangresSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=_RecordingSpannerSyncNative(dialect="spangres"),
    )

    adk_google = SpannerSyncADKStore(google_cfg)
    google_ddls = adk_google.ddl_statements()
    assert any("INTERLEAVE IN PARENT" in stmt and "ON DELETE CASCADE" in stmt for stmt in google_ddls)
    assert any("ROW DELETION POLICY" in stmt for stmt in google_ddls)
    adk_google.create_tables()
    assert sum(1 for c in google_native.calls if c[0] == "execute" and "CREATE TABLE IF NOT EXISTS" in c[1][0]) == len(
        google_ddls
    )

    adk_spangres = SpannerSyncADKStore(spangres_cfg)
    spangres_ddls = adk_spangres.ddl_statements()
    assert any("INTERLEAVE IN PARENT" in stmt and "TTL INTERVAL" in stmt and "JSONB" in stmt for stmt in spangres_ddls)

    mem_store = SpannerSyncADKMemoryStore(google_cfg)
    vec_idx = mem_store.build_vector_index_sql("idx_vec")
    assert "CREATE VECTOR INDEX IF NOT EXISTS idx_vec" in vec_idx
    assert "tree_type = 'SCANN'" in vec_idx
    assert "CREATE SEARCH INDEX IF NOT EXISTS idx_fts" in mem_store.build_search_index_sql("idx_fts")
    assert "CREATE OR REPLACE PROPERTY GRAPH mem_g" in mem_store.build_property_graph_sql("mem_g")
    vec_sql, vec_params = mem_store.build_vector_search_sql(app_name="app1", user_id="user1", top_k=5)
    assert "APPROX_COSINE_DISTANCE" in vec_sql and vec_params["top_k"] == 5
    fts_sql, fts_params = mem_store.build_fts_search_sql(app_name="app1", user_id="user1", query="alpha", top_k=5)
    assert "SEARCH(content_tokens, @query)" in fts_sql and fts_params["query"] == "alpha"
    graph_sql, _ = mem_store.build_graph_search_sql(app_name="app1", user_id="user1", graph_name="mem_g")
    assert "GRAPH mem_g MATCH" in graph_sql
    rrf_hits = mem_store.hybrid_rrf_search(
        app_name="app1", user_id="user1", query="alpha", embedding=[1.0, 0.0], top_k=2
    )
    assert len(rrf_hits) == 2 and rrf_hits[0]["id"] == "mem-1"

    google_native.select_rows = []
    google_native.description_cols = []
    google_native.description_rows = []
    art_store = SpannerSyncADKArtifactStore(google_cfg, storage_backend=backend)
    saved_ref = art_store.save_artifact_object(
        "app1", "user1", "doc.bin", b"spanner-bytes", mime_type="application/octet-stream"
    )
    assert isinstance(saved_ref, ArtifactReference)
    assert saved_ref.size == 13
    stored_keys = backend.list_objects_sync()
    assert len(stored_keys) == 1
    assert backend.read_bytes_sync(stored_keys[0]) == b"spanner-bytes"

    ev_google = SpannerEventQueueStore(google_cfg)
    ev_google_stmts = ev_google.create_statements()
    assert any("FARM_FINGERPRINT" in stmt and "ROW DELETION POLICY" in stmt for stmt in ev_google_stmts)
    assert 0 <= ev_google.compute_shard_id("evt-123") < 16
    ev_spangres = SpannerEventQueueStore(spangres_cfg)
    assert any("TIMESTAMPTZ" in stmt and "TTL INTERVAL" in stmt for stmt in ev_spangres.create_statements())

    lit_google = SpannerLitestarStore(google_cfg)
    assert "BYTES(MAX)" in lit_google._table_ddl()
    assert "ROW DELETION POLICY" in lit_google._table_ddl()
    assert 0 <= lit_google.compute_shard_id("sess-1") < 16
    asyncio.run(lit_google.create_table())
    asyncio.run(lit_google.set("sess-1", b"session-data", expires_in=timedelta(seconds=60)))
    assert any("INSERT OR UPDATE" in c[1][0] or "MERGE" in c[1][0] for c in google_native.calls if c[0] == "execute")

    lit_spangres = SpannerLitestarStore(spangres_cfg)
    assert "BYTEA NOT NULL" in lit_spangres._table_ddl()
    assert "TTL INTERVAL" in lit_spangres._table_ddl()


@pytest.mark.parametrize(
    ("exc", "expected_cls"),
    [
        (SQLSpecError("already-sqlspec"), type(None)),
        (SpannerAbortedError("transaction aborted"), SerializationConflictError),
        (_make_spanner_error("aborted by lock", 10), SerializationConflictError),
        (_make_spanner_error("already exists row", 6), UniqueViolationError),
        (_make_spanner_error("failed precondition", 9), IntegrityError),
        (_make_spanner_error("row not found", 5), DataError),
        (_make_spanner_error("permission denied", 7), PermissionDeniedError),
        (_make_spanner_error("unauthenticated token", 16), PermissionDeniedError),
        (_make_spanner_error("deadline exceeded", 4), QueryTimeoutError),
        (_make_spanner_error("client cancelled", 1), OperationCancelledError),
        (_make_spanner_error("service unavailable", 14), DatabaseConnectionError),
        (_make_spanner_error("unimplemented feature", 12), DialectNotSupportedError),
        (_make_spanner_error("syntax error near FROM", 3), SQLParsingError),
        (_make_spanner_error("invalid argument type", 3), DataError),
        (_make_spanner_error("unknown internal error"), OperationalError),
    ],
)
def test_map_spanner_exception_translations(exc: BaseException, expected_cls: type[Exception] | type[None]) -> None:
    """Verify gRPC and Vertebra Spanner error translations onto SQLSpec exception hierarchy."""
    mapped = map_spanner_exception(exc)
    if expected_cls is type(None):
        assert mapped is None
    else:
        assert isinstance(mapped, expected_cls)


@pytest.mark.parametrize("method_name", ["select_to_arrow", "load_from_arrow"])
def test_spanner_arrow_missing_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    """Verify Arrow methods raise ``MissingDependencyError`` with ``extra='arrow'`` when ``pyarrow`` is absent."""
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    sync_cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=_RecordingSpannerSyncNative(),
    )
    async_cfg = SpannerAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=_RecordingSpannerAsyncNative(),
    )

    with sync_cfg.provide_session() as sync_driver, pytest.raises(MissingDependencyError, match="arrow"):
        if method_name == "select_to_arrow":
            sync_driver.select_to_arrow("SELECT 1")
        else:
            sync_driver.load_from_arrow("items", b"arrow-bytes")

    async def _run_async() -> None:
        async with async_cfg.provide_session() as async_driver:
            with pytest.raises(MissingDependencyError, match="arrow"):
                if method_name == "select_to_arrow":
                    await async_driver.select_to_arrow("SELECT 1")
                else:
                    await async_driver.load_from_arrow("items", b"arrow-bytes")

    asyncio.run(_run_async())


def test_spanner_sessions_distinct_drivers_closed_and_stale_reads() -> None:
    """Verify concurrent/repeated ``provide_session`` returns distinct drivers, marks closed, and preserves stale reads."""
    sync_native = _RecordingSpannerSyncNative()
    sync_cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=sync_native,
        reset_session_on_release=True,
    )
    with sync_cfg.provide_session() as driver_one, sync_cfg.provide_session() as driver_two:
        assert driver_one is not driver_two
        assert driver_one.connection is sync_native
        assert driver_two.connection is sync_native
    assert not driver_one.connection
    assert not driver_two.connection
    assert repr(driver_one.connection) == "<ClosedConnection>"
    assert repr(driver_two.connection) == "<ClosedConnection>"

    with sync_cfg.provide_connection("ignored-arg", custom_flag=True) as conn:
        assert conn is sync_native

    staleness = StalenessBound.exact_staleness(2500)
    with sync_cfg.provide_session(transaction=False, read_only=True, staleness=staleness) as ro_drv:
        assert ro_drv.is_read_only is True
        assert ro_drv.in_transaction is False
        assert ro_drv.staleness is staleness
    assert not any(call[0] == "begin" for call in sync_native.calls)

    with pytest.raises(RuntimeError, match="boom"), sync_cfg.provide_session(transaction=True) as tx_drv:
        assert tx_drv.in_transaction is True
        raise RuntimeError("boom")
    assert tx_drv.in_transaction is False
    assert any(call[0] == "rollback" for call in sync_native.calls)

    async_native = _RecordingSpannerAsyncNative()
    async_cfg = SpannerAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=async_native,
        reset_session_on_release=True,
    )

    async def _verify_async() -> None:
        async with async_cfg.provide_session() as a_one, async_cfg.provide_session() as a_two:
            assert a_one is not a_two
            assert a_one.connection is async_native
            assert a_two.connection is async_native
        assert not a_one.connection
        assert not a_two.connection
        assert repr(a_one.connection) == "<ClosedConnection>"
        assert repr(a_two.connection) == "<ClosedConnection>"

        async with async_cfg.provide_connection("ignored-arg", custom_flag=True) as a_conn:
            assert a_conn is async_native

        async with async_cfg.provide_session(transaction=False, read_only=True, staleness=staleness) as a_ro:
            assert a_ro.is_read_only is True
            assert a_ro.in_transaction is False
            assert a_ro.staleness is staleness

    asyncio.run(_verify_async())


@pytest.mark.parametrize(
    ("replica_spec", "expected_project", "expected_instance", "expected_database"),
    [
        ("spanner://ro-proj/ro-inst/ro-db", "ro-proj", "ro-inst", "ro-db"),
        ("db-ro", "primary-proj", "primary-inst", "db-ro"),
        (
            {"database_id": "db-dict", "staleness": StalenessBound.exact_staleness(1500)},
            "primary-proj",
            "primary-inst",
            "db-dict",
        ),
    ],
)
def test_spanner_replicas_routing_and_validation(
    replica_spec: str | dict[str, Any],
    expected_project: str,
    expected_instance: str,
    expected_database: str,
) -> None:
    """Verify ``replicas=`` configures derived Spanner and Spangres replica pools and rejects ``replicas=True``."""
    sync_primary = SpannerSyncConfig(
        project_id="primary-proj",
        instance_id="primary-inst",
        database_id="db-primary",
        replicas=[replica_spec],
    )
    assert "default" in sync_primary.replica_groups
    replica_cfg = sync_primary.replica_groups["default"].specs[0].config
    assert isinstance(replica_cfg, SpannerSyncConfig)
    assert replica_cfg.is_replica is True
    assert replica_cfg.resolve_spanner_config().project_id == expected_project
    assert replica_cfg.resolve_spanner_config().instance_id == expected_instance
    assert replica_cfg.resolve_spanner_config().database_id == expected_database

    async_primary = SpannerAsyncConfig(
        project_id="primary-proj",
        instance_id="primary-inst",
        database_id="db-primary",
        replicas=[replica_spec],
    )
    assert "default" in async_primary.replica_groups
    async_replica = async_primary.replica_groups["default"].specs[0].config
    assert isinstance(async_replica, SpannerAsyncConfig)
    assert async_replica.is_replica is True
    assert async_replica.resolve_spanner_config().database_id == expected_database

    pg_primary = SpangresSyncConfig(
        project_id="primary-proj",
        instance_id="primary-inst",
        database_id="db-primary",
        replicas=[replica_spec],
    )
    pg_replica = pg_primary.replica_groups["default"].specs[0].config
    assert isinstance(pg_replica, SpangresSyncConfig)
    assert pg_replica.dialect == "spangres"

    with pytest.raises((ImproperConfigurationError, SQLSpecImproperConfigurationError)):
        SpannerSyncConfig(
            project_id="primary-proj",
            instance_id="primary-inst",
            database_id="db-primary",
            replicas=True,
        )


def test_spanner_select_to_storage_streaming(tmp_path: Path) -> None:
    """Verify ``select_to_storage`` streams rows through ``_select_for_storage`` and ``_write_storage_result``."""
    sync_native = _RecordingSpannerSyncNative()
    sync_native.description_cols = ["id", "name"]
    sync_native.description_rows = [(1, "alpha"), (2, "beta")]
    sync_cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=sync_native,
    )
    sync_out = tmp_path / "sync-export.parquet"
    with sync_cfg.provide_session() as sync_drv:
        sync_job = sync_drv.select_to_storage("SELECT id, name FROM users", sync_out, format_hint="parquet")
    assert sync_job.status == "completed"
    assert sync_out.exists()
    assert sync_job.telemetry["rows_processed"] == 2

    async_native = _RecordingSpannerAsyncNative()
    async_native.description_cols = ["id", "name"]
    async_native.description_rows = [(3, "gamma")]
    async_cfg = SpannerAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=async_native,
    )
    async_out = tmp_path / "async-export.parquet"

    async def _run_async() -> Any:
        async with async_cfg.provide_session() as async_drv:
            return await async_drv.select_to_storage("SELECT id, name FROM users", async_out, format_hint="parquet")

    async_job = asyncio.run(_run_async())
    assert async_job.status == "completed"
    assert async_out.exists()
    assert async_job.telemetry["rows_processed"] == 1


def test_spanner_adapter_classes_use_slots() -> None:
    """Verify Spanner config, driver, and session/connection context classes enforce ``__slots__``."""
    sync_cfg = SpannerSyncConfig(
        project_id="proj-spanner",
        instance_id="inst-spanner",
        database_id="db-spanner",
        connection_instance=_RecordingSpannerSyncNative(),
    )
    async_cfg = SpannerAsyncConfig(
        project_id="proj-async",
        instance_id="inst-async",
        database_id="db-async",
        connection_instance=_RecordingSpannerAsyncNative(),
    )
    pg_sync = SpangresSyncConfig(project_id="proj-spanner", instance_id="inst-spanner", database_id="db-spanner")
    pg_async = SpangresAsyncConfig(project_id="proj-async", instance_id="inst-async", database_id="db-async")

    instances: list[Any] = [
        sync_cfg,
        async_cfg,
        pg_sync,
        pg_async,
        sync_cfg.create_driver(),
        async_cfg.create_driver(),
        SpannerSyncConnectionContext(sync_cfg),
        SpannerAsyncConnectionContext(async_cfg),
        SpannerSyncSessionContext(sync_cfg),
        SpannerAsyncSessionContext(async_cfg),
    ]
    for obj in instances:
        assert not hasattr(obj, "__dict__"), f"{type(obj).__name__} unexpectedly exposes __dict__"


class _CountingSyncCursor:
    """Wrapped sync cursor counting native execute, executemany, fetchall, and close calls."""

    def __init__(
        self,
        *,
        rows: list[tuple[Any, ...]] | None = None,
        description: tuple[tuple[Any, ...], ...] | None = None,
        rowcount: int = 0,
        error: BaseException | None = None,
    ) -> None:
        self._rows = list(rows or [])
        self._description = description
        self._rowcount = rowcount
        self._error = error
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.execute_calls: list[tuple[str, Any]] = []
        self.executemany_calls: list[tuple[str, list[Any]]] = []
        self.fetchall_calls: int = 0
        self.close_calls: int = 0

    def execute(self, sql: str, params: Any = None) -> "_CountingSyncCursor":
        self.execute_calls.append((sql, params))
        if self._error is not None:
            raise self._error
        self.description = self._description
        self.rowcount = len(self._rows) if self._description is not None else self._rowcount
        return self

    def executemany(self, sql: str, seq_of_params: Any) -> "_CountingSyncCursor":
        batch = list(seq_of_params)
        self.executemany_calls.append((sql, batch))
        self.description = None
        self.rowcount = len(batch)
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        self.fetchall_calls += 1
        return list(self._rows)

    def close(self) -> None:
        self.close_calls += 1


class _CountingAsyncCursor:
    """Wrapped async cursor counting native execute, executemany, fetchall, and close calls."""

    def __init__(self, inner: _CountingSyncCursor) -> None:
        self._inner = inner

    @property
    def description(self) -> tuple[tuple[Any, ...], ...] | None:
        return self._inner.description

    @property
    def rowcount(self) -> int:
        return self._inner.rowcount

    async def execute(self, sql: str, params: Any = None) -> "_CountingAsyncCursor":
        self._inner.execute(sql, params)
        return self

    async def executemany(self, sql: str, seq_of_params: Any) -> "_CountingAsyncCursor":
        self._inner.executemany(sql, seq_of_params)
        return self

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._inner.fetchall()

    async def close(self) -> None:
        self._inner.close()


class _CountingSpannerConnection:
    """Minimal Spanner connection double issuing counting sync or async cursors."""

    def __init__(
        self,
        *,
        asynchronous: bool = False,
        rows: list[tuple[Any, ...]] | None = None,
        description: tuple[tuple[Any, ...], ...] | None = None,
        rowcount: int = 0,
        error: BaseException | None = None,
    ) -> None:
        self.config = SpannerConfig(project_id="p", instance_id="i", database_id="d")
        self.dialect = "googlesql"
        self.spanner_dialect = "googlesql"
        self._asynchronous = asynchronous
        self._rows = rows
        self._description = description
        self._rowcount = rowcount
        self._error = error
        self.cursor_kwargs: list[dict[str, Any]] = []
        self.cursors: list[_CountingSyncCursor] = []

    def cursor(self, **kwargs: Any) -> Any:
        self.cursor_kwargs.append(dict(kwargs))
        sync_cursor = _CountingSyncCursor(
            rows=self._rows,
            description=self._description,
            rowcount=self._rowcount,
            error=self._error,
        )
        self.cursors.append(sync_cursor)
        return _CountingAsyncCursor(sync_cursor) if self._asynchronous else sync_cursor


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_native_cursor_cache_hit_and_rows(mode: str) -> None:
    """Second identical select performs exactly one native execute and one fetchall."""
    conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=[(1,)],
        description=(("a", None, None, None, None, None, None),),
    )
    if mode == "sync":
        driver = SpannerSyncDriver(conn)
        first = driver.select("SELECT 1 AS a")
        second = driver.select("SELECT 1 AS a")
    else:
        async_driver = SpannerAsyncDriver(conn)

        async def _run() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            return await async_driver.select("SELECT 1 AS a"), await async_driver.select("SELECT 1 AS a")

        first, second = asyncio.run(_run())

    assert first == [{"a": 1}]
    assert second == [{"a": 1}]
    assert len(conn.cursors) == 2
    second_cursor = conn.cursors[1]
    assert len(second_cursor.execute_calls) == 1
    assert second_cursor.fetchall_calls == 1
    assert second_cursor.close_calls == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_native_cursor_dml_returning_and_execute_many(mode: str) -> None:
    """Verify UPDATE rowcount, INSERT ... THEN RETURN rows, and execute_many batching."""
    dml_conn = _CountingSpannerConnection(asynchronous=(mode == "async"), rowcount=4)
    ret_conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=[(42,)],
        description=(("id", None, None, None, None, None, None),),
    )
    many_conn = _CountingSpannerConnection(asynchronous=(mode == "async"))

    if mode == "sync":
        dml_res = SpannerSyncDriver(dml_conn).execute(
            "UPDATE users SET active = @a WHERE id = @id", {"a": True, "id": 1}
        )
        ret_rows = SpannerSyncDriver(ret_conn).select("INSERT INTO users (id) VALUES (@id) THEN RETURN id", {"id": 42})
        many_res = SpannerSyncDriver(many_conn).execute_many(
            "INSERT INTO users (id) VALUES (@id)",
            [{"id": 1}, {"id": 2}, {"id": 3}],
        )
    else:

        async def _run() -> tuple[Any, list[dict[str, Any]], Any]:
            r1 = await SpannerAsyncDriver(dml_conn).execute(
                "UPDATE users SET active = @a WHERE id = @id", {"a": True, "id": 1}
            )
            r2 = await SpannerAsyncDriver(ret_conn).select(
                "INSERT INTO users (id) VALUES (@id) THEN RETURN id", {"id": 42}
            )
            r3 = await SpannerAsyncDriver(many_conn).execute_many(
                "INSERT INTO users (id) VALUES (@id)",
                [{"id": 1}, {"id": 2}, {"id": 3}],
            )
            return r1, r2, r3

        dml_res, ret_rows, many_res = asyncio.run(_run())

    assert dml_res.rows_affected == 4
    assert ret_rows == [{"id": 42}]
    assert many_res.rows_affected == 3
    assert len(many_conn.cursors[0].executemany_calls) == 1
    batch_sql, batch_params = many_conn.cursors[0].executemany_calls[0]
    assert "INSERT INTO users" in batch_sql
    assert [json.loads(item) for item in batch_params] == [{"id": 1}, {"id": 2}, {"id": 3}]


@pytest.mark.parametrize("dialect", ["spanner", "spangres"])
def test_spanner_compile_statement_typed_parameter_and_cursor_tags(dialect: str) -> None:
    """compile_spanner_statement encodes TypedParameter timestamps and with_cursor forwards staleness and tags."""
    ts = datetime(2026, 11, 20, 12, 30, tzinfo=UTC)
    stmt_cfg = default_statement_config(dialect)
    sql_text = "SELECT $1 AS ts" if dialect == "spangres" else "SELECT @ts AS ts"
    params_arg: Any = (
        [TypedParameter(ts, semantic_name="TIMESTAMP")]
        if dialect == "spangres"
        else {"ts": TypedParameter(ts, semantic_name="TIMESTAMP")}
    )
    stmt = SQL(sql_text, params_arg, statement_config=stmt_cfg)

    compiled_sql, params_json = compile_spanner_statement(stmt, dialect=dialect)
    assert "SELECT" in compiled_sql
    assert params_json is not None
    decoded = json.loads(params_json)
    expected_key = "p1" if dialect == "spangres" else "ts"
    assert decoded[expected_key] == {"type": "TIMESTAMP", "value": "2026-11-20T12:30:00Z"}

    staleness = StalenessBound.exact_staleness(250)
    conn = _CountingSpannerConnection(
        rows=[(1,)],
        description=(("a", None, None, None, None, None, None),),
    )
    driver = SpannerSyncDriver(
        conn,
        driver_features={"request_tag": "req-tag-1", "transaction_tag": "tx-tag-1"},
        staleness=staleness,
    )
    assert driver.select("SELECT 1 AS a") == [{"a": 1}]
    assert conn.cursor_kwargs == [{"staleness": staleness, "request_tag": "req-tag-1", "transaction_tag": "tx-tag-1"}]
    assert conn.cursors[0].close_calls == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_native_cursor_already_exists_maps_to_unique_violation(mode: str) -> None:
    """Duplicate key SpannerError(code=6) raised by the cursor maps to UniqueViolationError."""
    err = _make_spanner_error("Row already exists", code=6)
    conn = _CountingSpannerConnection(asynchronous=(mode == "async"), error=err)
    if mode == "sync":
        with pytest.raises(UniqueViolationError):
            SpannerSyncDriver(conn).execute("INSERT INTO users (id) VALUES (1)")
    else:

        async def _run() -> None:
            await SpannerAsyncDriver(conn).execute("INSERT INTO users (id) VALUES (1)")

        with pytest.raises(UniqueViolationError):
            asyncio.run(_run())


@pytest.mark.parametrize(
    "spanner_url",
    [
        "spanner://proj/inst/db?endpoint=http://127.0.0.1:9010",
        "spangres://proj/inst/db?endpoint=http://127.0.0.1:9010",
    ],
)
def test_spanner_dbapi_cursor_is_native(spanner_url: str) -> None:
    """Spanner DB-API connect().cursor() returns SyncSpannerCursor."""
    conn = connect(spanner_url)
    cursor = conn.cursor()
    try:
        assert isinstance(cursor, SyncSpannerCursor)
    finally:
        cursor.close()
        if conn._driver is not None:
            conn._driver.close()


def _instrument_spanner_cache_hit_state(adapter: Any, *, asynchronous: bool) -> dict[str, int]:
    """Wrap adapter._cache_hit_state callables with invocation counters."""
    counts = {"fetch": 0, "dml": 0}
    orig_fetch, orig_dml, orig_mapper = adapter._cache_hit_state
    if asynchronous:

        async def _wrapped_async_fetch(*args: Any, **kwargs: Any) -> Any:
            counts["fetch"] += 1
            return await orig_fetch(*args, **kwargs)

        async def _wrapped_async_dml(*args: Any, **kwargs: Any) -> Any:
            counts["dml"] += 1
            return await orig_dml(*args, **kwargs)

        adapter._cache_hit_state = (_wrapped_async_fetch, _wrapped_async_dml, orig_mapper)
    else:

        def _wrapped_sync_fetch(*args: Any, **kwargs: Any) -> Any:
            counts["fetch"] += 1
            return orig_fetch(*args, **kwargs)

        def _wrapped_sync_dml(*args: Any, **kwargs: Any) -> Any:
            counts["dml"] += 1
            return orig_dml(*args, **kwargs)

        adapter._cache_hit_state = (_wrapped_sync_fetch, _wrapped_sync_dml, orig_mapper)
    return counts


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_cache_hit_executor_is_registered(mode: str) -> None:
    """Spanner sync and async adapters register the base cache-hit executor with a 3-tuple state."""
    conn = _CountingSpannerConnection(asynchronous=(mode == "async"))
    if mode == "sync":
        adapter: Any = SpannerSyncDriver(conn)
        assert adapter._cache_hit_executor is BaseSyncDriver._run_cached_statement
    else:
        adapter = SpannerAsyncDriver(conn)
        assert adapter._cache_hit_executor is BaseAsyncDriver._run_cached_statement
    assert isinstance(adapter._cache_hit_state, tuple)
    assert len(adapter._cache_hit_state) == 3


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("sql", "kind", "rows", "description", "rowcount"),
    [
        pytest.param(
            "SELECT 1 AS a",
            "fetch",
            [(1,)],
            (("a", None, None, None, None, None, None),),
            0,
            id="select",
        ),
        pytest.param(
            "UPDATE users SET active = TRUE WHERE id = 1",
            "dml",
            None,
            None,
            3,
            id="update",
        ),
    ],
)
def test_spanner_cache_hit_makes_one_adapter_call_and_one_cursor(
    mode: str,
    sql: str,
    kind: str,
    rows: list[tuple[Any, ...]] | None,
    description: tuple[tuple[Any, ...], ...] | None,
    rowcount: int,
) -> None:
    """A cached Spanner statement invokes one adapter callable and opens and closes one cursor."""
    conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=rows,
        description=description,
        rowcount=rowcount,
    )
    if mode == "sync":
        adapter: Any = SpannerSyncDriver(conn)
        counts = _instrument_spanner_cache_hit_state(adapter, asynchronous=False)
        adapter.execute(sql)
        conn.cursor_kwargs.clear()
        conn.cursors.clear()
        counts["fetch"] = 0
        counts["dml"] = 0
        adapter.execute(sql)
    else:
        adapter = SpannerAsyncDriver(conn)
        counts = _instrument_spanner_cache_hit_state(adapter, asynchronous=True)

        async def _run() -> None:
            await adapter.execute(sql)
            conn.cursor_kwargs.clear()
            conn.cursors.clear()
            counts["fetch"] = 0
            counts["dml"] = 0
            await adapter.execute(sql)

        asyncio.run(_run())

    expected_other = "dml" if kind == "fetch" else "fetch"
    assert counts[kind] == 1
    assert counts[expected_other] == 0
    assert len(conn.cursor_kwargs) == 1
    assert len(conn.cursors) == 1
    assert len(conn.cursors[0].execute_calls) == 1
    assert conn.cursors[0].close_calls == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("sql", "rows", "description", "rowcount"),
    [
        pytest.param(
            "SELECT 1 AS a",
            [(1,)],
            (("a", None, None, None, None, None, None),),
            0,
            id="select",
        ),
        pytest.param(
            "SELECT 1 AS a WHERE FALSE",
            [],
            (("a", None, None, None, None, None, None),),
            0,
            id="empty_select",
        ),
        pytest.param(
            "UPDATE users SET active = TRUE WHERE id = 1",
            None,
            None,
            4,
            id="update",
        ),
        pytest.param(
            "INSERT INTO users (id) VALUES (42) THEN RETURN id",
            [(42,)],
            (("id", None, None, None, None, None, None),),
            0,
            id="dml_returning",
        ),
    ],
)
def test_spanner_cache_hit_result_matches_method_path(
    mode: str,
    sql: str,
    rows: list[tuple[Any, ...]] | None,
    description: tuple[tuple[Any, ...], ...] | None,
    rowcount: int,
) -> None:
    """Cached Spanner executor results match the method path across selects, empty selects, and DML."""
    fast_conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=rows,
        description=description,
        rowcount=rowcount,
    )
    method_conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=rows,
        description=description,
        rowcount=rowcount,
    )
    if mode == "sync":
        fast_adapter: Any = SpannerSyncDriver(fast_conn)
        method_adapter: Any = SpannerSyncDriver(method_conn)
        method_adapter._cache_hit_executor = None
        fast_adapter.execute(sql)
        method_adapter.execute(sql)
        fast_res = fast_adapter.execute(sql)
        method_res = method_adapter.execute(sql)
    else:
        fast_adapter = SpannerAsyncDriver(fast_conn)
        method_adapter = SpannerAsyncDriver(method_conn)
        method_adapter._cache_hit_executor = None

        async def _run() -> tuple[Any, Any]:
            await fast_adapter.execute(sql)
            await method_adapter.execute(sql)
            return await fast_adapter.execute(sql), await method_adapter.execute(sql)

        fast_res, method_res = asyncio.run(_run())

    assert fast_res.all() == method_res.all()
    assert fast_res.column_names == method_res.column_names
    assert fast_res.rows_affected == method_res.rows_affected


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_cache_hit_keeps_session_staleness_and_tags(mode: str) -> None:
    """Cached Spanner runs read session staleness and driver_features request_tag dynamically."""
    staleness = StalenessBound.exact_staleness(2500)
    conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=[(1,)],
        description=(("a", None, None, None, None, None, None),),
    )
    if mode == "sync":
        adapter: Any = SpannerSyncDriver(conn, staleness=staleness, driver_features={"request_tag": "first"})
        adapter.execute("SELECT 1 AS a")
        adapter.execute("SELECT 1 AS a")
        first_kwargs = dict(conn.cursor_kwargs[-1])
        adapter.driver_features["request_tag"] = "second"
        adapter.execute("SELECT 1 AS a")
        second_kwargs = dict(conn.cursor_kwargs[-1])
    else:
        adapter = SpannerAsyncDriver(conn, staleness=staleness, driver_features={"request_tag": "first"})

        async def _run() -> tuple[dict[str, Any], dict[str, Any]]:
            await adapter.execute("SELECT 1 AS a")
            await adapter.execute("SELECT 1 AS a")
            first_kw = dict(conn.cursor_kwargs[-1])
            adapter.driver_features["request_tag"] = "second"
            await adapter.execute("SELECT 1 AS a")
            second_kw = dict(conn.cursor_kwargs[-1])
            return first_kw, second_kw

        first_kwargs, second_kwargs = asyncio.run(_run())

    assert first_kwargs["staleness"] == staleness
    assert first_kwargs["request_tag"] == "first"
    assert second_kwargs["staleness"] == staleness
    assert second_kwargs["request_tag"] == "second"


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_cache_hit_read_only_session_refuses_write(mode: str) -> None:
    """A read-only Spanner session refuses cached writes before creating a cursor and still runs reads."""
    conn = _CountingSpannerConnection(
        asynchronous=(mode == "async"),
        rows=[(1,)],
        description=(("a", None, None, None, None, None, None),),
    )
    if mode == "sync":
        adapter: Any = SpannerSyncDriver(conn, read_only=True)
        with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
            adapter._cache_hit_state[1]("UPDATE t SET a = 1", ())
        assert conn.cursor_kwargs == []
        fetched_rows, fetched_names = adapter._cache_hit_state[0]("SELECT 1", ())
    else:
        adapter = SpannerAsyncDriver(conn, read_only=True)

        async def _run() -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            with pytest.raises(ImproperConfigurationError, match="read-only Spanner snapshot"):
                await adapter._cache_hit_state[1]("UPDATE t SET a = 1", ())
            assert conn.cursor_kwargs == []
            return cast(
                "tuple[list[tuple[Any, ...]], tuple[str, ...]]",
                await adapter._cache_hit_state[0]("SELECT 1", ()),
            )

        fetched_rows, fetched_names = asyncio.run(_run())

    assert fetched_rows == [(1,)]
    assert fetched_names == ("a",)


def test_spanner_cache_hit_not_registered_without_cursor() -> None:
    """Async Spanner adapter leaves _cache_hit_executor unset when connection exposes only _sync."""

    class _SyncOnlySpannerConnection:
        def __init__(self) -> None:
            self.config = SpannerConfig(project_id="p", instance_id="i", database_id="d")
            self.dialect = "googlesql"
            self.spanner_dialect = "googlesql"
            self._sync = _CountingSpannerConnection()

    adapter = SpannerAsyncDriver(_SyncOnlySpannerConnection())
    assert adapter._cache_hit_executor is None
    assert adapter._cache_hit_state is None


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_spanner_cache_hit_errors_match_method_path(mode: str) -> None:
    """Native errors raised on a cached Spanner run map to the same SQLSpec exception class with __cause__."""
    native_error = _make_spanner_error("already exists", code=6)
    fast_conn = _CountingSpannerConnection(asynchronous=(mode == "async"), rowcount=1)
    method_conn = _CountingSpannerConnection(asynchronous=(mode == "async"), rowcount=1)
    sql = "INSERT INTO users (id) VALUES (1)"

    if mode == "sync":
        fast_adapter: Any = SpannerSyncDriver(fast_conn)
        method_adapter: Any = SpannerSyncDriver(method_conn)
        method_adapter._cache_hit_executor = None
        fast_adapter.execute(sql)
        method_adapter.execute(sql)
        fast_conn._error = native_error
        method_conn._error = native_error
        with pytest.raises(UniqueViolationError) as fast_exc:
            fast_adapter.execute(sql)
        with pytest.raises(UniqueViolationError) as method_exc:
            method_adapter.execute(sql)
    else:
        fast_adapter = SpannerAsyncDriver(fast_conn)
        method_adapter = SpannerAsyncDriver(method_conn)
        method_adapter._cache_hit_executor = None

        async def _run() -> tuple[BaseException, BaseException]:
            await fast_adapter.execute(sql)
            await method_adapter.execute(sql)
            fast_conn._error = native_error
            method_conn._error = native_error
            with pytest.raises(UniqueViolationError) as f_exc:
                await fast_adapter.execute(sql)
            with pytest.raises(UniqueViolationError) as m_exc:
                await method_adapter.execute(sql)
            return f_exc.value, m_exc.value

        fast_val, method_val = asyncio.run(_run())
        assert type(fast_val) is type(method_val)
        assert fast_val.__cause__ is native_error
        assert fast_conn.cursors[-1].close_calls == 1
        return

    assert type(fast_exc.value) is type(method_exc.value)
    assert fast_exc.value.__cause__ is native_error
    assert fast_conn.cursors[-1].close_calls == 1
