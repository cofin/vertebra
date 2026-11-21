"""Unit tests for the native DuckDB 2.0 driver (`vertebra._duckdb`), savepoints, pipelines, and Arrow SHM."""

from __future__ import annotations

import ast
import asyncio
import datetime
import decimal
import importlib
import math
import threading
import uuid
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any, cast

import msgspec
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest

import vertebra._duckdb as native_duckdb_mod
import vertebra._vertebra as native_core_mod
import vertebra.db.adapters.duckdb._async as duckdb_async_mod
import vertebra.db.adapters.duckdb._sync as duckdb_sync_mod
import vertebra.db.adapters.duckdb.config as duckdb_config_mod
import vertebra.db.adapters.duckdb.core as duckdb_core_mod
import vertebra.db.adapters.duckdb.driver as duckdb_driver_mod
import vertebra.db.adapters.duckdb.pipeline as duckdb_pipeline_mod
import vertebra.db.dbapi as dbapi_mod
from vertebra._duckdb import AsyncDuckDBClient, SyncDuckDBClient
from vertebra.db import (
    ArrowCursor,
    AsyncDatabaseDriver,
    AsyncPipelineCapable,
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    ProgrammingError,
    SyncDatabaseDriver,
    SyncPipelineCapable,
    ThreadOffloadAsyncDatabaseDriver,
)
from vertebra.db.adapters.duckdb import (
    AsyncDuckDBDriver,
    AsyncDuckDBPipeline,
    DuckDBConfig,
    DuckDBCore,
    DuckDBSecretConfig,
    SyncDuckDBDriver,
    SyncDuckDBPipeline,
)
from vertebra.typing import ImproperConfigurationError


class _PipelineAbortError(RuntimeError):
    """Sentinel error used to verify delayed-commit pipeline rollback."""


@pytest.mark.parametrize(
    "module_obj",
    [
        duckdb_config_mod,
        duckdb_core_mod,
        duckdb_driver_mod,
        duckdb_pipeline_mod,
        duckdb_sync_mod,
        duckdb_async_mod,
    ],
    ids=["config", "core", "driver", "pipeline", "sync", "async"],
)
def test_duckdb_adapter_modules_contain_zero_thread_offload_or_legacy_imports(
    module_obj: Any,
) -> None:
    """DuckDB adapter modules contain zero `asyncio.to_thread`, `sqlite3`, or PyPI `duckdb` imports."""
    assert issubclass(SyncDuckDBDriver, SyncDatabaseDriver)
    assert issubclass(AsyncDuckDBDriver, AsyncDatabaseDriver)
    assert not issubclass(AsyncDuckDBDriver, ThreadOffloadAsyncDatabaseDriver)
    assert native_duckdb_mod.GIL_USED is False

    source = Path(module_obj.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_names = [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    imported_from = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ]
    attr_calls = [node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)]
    assert "sqlite3" not in imported_names
    assert "duckdb" not in imported_names
    assert "sqlite3" not in imported_from
    assert "duckdb" not in imported_from
    assert "to_thread" not in attr_calls


@pytest.mark.parametrize(
    ("bad_kwargs", "match_pattern"),
    [
        ({"path": "bad\0path.duckdb"}, "NUL"),
        ({"database": "bad\0db.duckdb"}, "NUL"),
        ({"temp_directory": "/tmp/bad\0dir"}, "NUL"),
        ({"temp_directory": "/tmp/bad\ndir"}, "CR or LF"),
        ({"config": {"custom\0key": "1"}}, "NUL"),
        ({"config": {"threads = 1; DROP TABLE x": "1"}}, "Invalid SQL identifier"),
        ({"config": {"threads": "1\0"}}, "NUL"),
        ({"config": {"threads": "1\nSET bad = '1'"}}, "CR or LF"),
        ({"init_statements": ("SELECT 1\0",)}, "NUL"),
        ({"pool_max_size": 0}, "pool_max_size"),
        ({"pool_max_size": -2}, "pool_max_size"),
    ],
    ids=[
        "nul_path",
        "nul_database",
        "nul_temp_dir",
        "newline_temp_dir",
        "nul_config_key",
        "injected_config_key",
        "nul_config_val",
        "newline_config_val",
        "nul_init_stmt",
        "zero_pool_max",
        "negative_pool_max",
    ],
)
def test_duckdb_config_and_driver_reject_invalid_inputs_pure_in_memory(
    bad_kwargs: dict[str, Any],
    match_pattern: str,
) -> None:
    """`DuckDBConfig`, `SyncDuckDBDriver`, and `AsyncDuckDBDriver` reject invalid identifiers, NUL/CRLF, and non-positive pool sizes."""
    cfg = DuckDBConfig(**bad_kwargs)
    if "pool_max_size" in bad_kwargs:
        with pytest.raises(InterfaceError, match=match_pattern):
            SyncDuckDBDriver(cfg)
        with pytest.raises(InterfaceError, match=match_pattern):
            AsyncDuckDBDriver(cfg)
    else:
        with pytest.raises(InterfaceError, match=match_pattern):
            cfg.bootstrap_statements()


@pytest.mark.parametrize(
    "client_cls",
    [SyncDuckDBClient, AsyncDuckDBClient],
    ids=["sync_client", "async_client"],
)
def test_native_duckdb_client_rejects_nul_bytes_and_zero_connections(
    client_cls: type[Any],
) -> None:
    """`SyncDuckDBClient` and `AsyncDuckDBClient` raise `InterfaceError` on NUL bytes or `max_connections == 0`."""
    with pytest.raises(InterfaceError) as zero_conn_exc:
        client_cls(":memory:", max_connections=0)
    assert zero_conn_exc.value.sqlstate == "HY000"
    assert zero_conn_exc.value.is_transient is False

    with pytest.raises(InterfaceError) as nul_path_exc:
        client_cls("bad\0.duckdb")
    assert nul_path_exc.value.sqlstate == "HY000"

    with pytest.raises(InterfaceError):
        client_cls(":memory:", extensions=["bad\0ext"])


def test_duckdb_secret_redaction_and_fail_loud_extension_bootstrap() -> None:
    """Secrets are redacted in `repr()` and unknown extensions fail loudly during pool bootstrap."""
    secret = DuckDBSecretConfig(
        name="prod_s3",
        secret_type="s3",
        value={
            "key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "region": "us-east-1",
        },
    )
    rendered_repr = repr(secret)
    assert "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" not in rendered_repr
    assert "<redacted>" in rendered_repr

    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(name="bad\0name", value={"secret": "s"}).to_create_secret_sql()

    client = SyncDuckDBClient(":memory:")
    try:
        client.execute(
            "CREATE OR REPLACE SECRET vtb_secret_redaction (TYPE http, BEARER_TOKEN 'super-secret-bearer-value-99')"
        )
        assert "super-secret-bearer-value-99" not in repr(client)
        assert "super-secret-bearer-value-99" not in repr(client.to_async())
    finally:
        client.close()

    with pytest.raises(DatabaseError):
        SyncDuckDBDriver(DuckDBConfig(extensions=("nonexistent_vtb_duckdb_extension_xyz",)))


@pytest.mark.anyio
async def test_duckdb_lossless_column_metadata_on_empty_result_sets() -> None:
    """`execute_with_description` and cursors preserve column names even when `0` rows match."""
    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        sync_driver.execute("CREATE TABLE empty_meta (id BIGINT, label VARCHAR, score DOUBLE)")
        rows, cols = sync_driver.execute_with_description(
            "SELECT id, label, score FROM empty_meta WHERE id > ?",
            (100,),
        )
        assert rows == []
        assert cols == ("id", "label", "score")
        assert sync_driver.execute_tuples("SELECT id, label FROM empty_meta") == []

        cur = sync_driver.cursor()
        try:
            cur.execute("SELECT id, label FROM empty_meta")
            assert cur.fetchall() == []
            assert cur.description is not None
            assert tuple(col[0] for col in cur.description) == ("id", "label")
        finally:
            cur.close()

        await async_driver.execute("CREATE TABLE empty_meta (id BIGINT, label VARCHAR, score DOUBLE)")
        arows, acols = await async_driver.execute_with_description(
            "SELECT id, label, score FROM empty_meta WHERE id > ?",
            (100,),
        )
        assert arows == []
        assert acols == ("id", "label", "score")
        assert await async_driver.execute_tuples("SELECT id, label FROM empty_meta") == []

        acur = async_driver.cursor()
        try:
            await acur.execute("SELECT id, label FROM empty_meta")
            assert await acur.fetchall() == []
            assert acur.description is not None
            assert tuple(col[0] for col in acur.description) == ("id", "label")
        finally:
            await acur.close()
    finally:
        sync_driver.close()
        await async_driver.close()


@pytest.mark.anyio
async def test_duckdb_savepoint_and_nested_transaction_lifecycle() -> None:
    """Sync and async drivers support savepoint creation, partial rollback, release, and nested `transaction()`."""
    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        sync_driver.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, note VARCHAR)")
        with pytest.raises((InterfaceError, ProgrammingError)):
            sync_driver.savepoint("sp_outside_tx")

        with sync_driver.transaction():
            sync_driver.execute("INSERT INTO ledger VALUES (1, 'root')")
            sync_driver.savepoint("sp1")
            sync_driver.execute("INSERT INTO ledger VALUES (2, 'branch_a')")
            sync_driver.rollback_to_savepoint("sp1")
            sync_driver.release_savepoint("sp1")
            with pytest.raises(_PipelineAbortError), sync_driver.transaction():
                sync_driver.execute("INSERT INTO ledger VALUES (3, 'nested_aborted')")
                raise _PipelineAbortError("rollback nested savepoint")
            with sync_driver.transaction():
                sync_driver.execute("INSERT INTO ledger VALUES (4, 'nested_kept')")

        assert sync_driver.execute_tuples("SELECT id, note FROM ledger ORDER BY id") == [
            (1, "root"),
            (4, "nested_kept"),
        ]

        await async_driver.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, note VARCHAR)")
        with pytest.raises((InterfaceError, ProgrammingError)):
            await async_driver.savepoint("sp_outside_tx")

        async with async_driver.transaction():
            await async_driver.execute("INSERT INTO ledger VALUES (10, 'async_root')")
            await async_driver.savepoint("sp_async")
            await async_driver.execute("INSERT INTO ledger VALUES (20, 'async_rolled_back')")
            await async_driver.rollback_to_savepoint("sp_async")
            await async_driver.release_savepoint("sp_async")
            with pytest.raises((NotFoundError, ProgrammingError)):
                await async_driver.rollback_to_savepoint("sp_unknown")
            with pytest.raises(_PipelineAbortError):
                async with async_driver.transaction():
                    await async_driver.execute("INSERT INTO ledger VALUES (30, 'async_nested_aborted')")
                    raise _PipelineAbortError("abort async nested")
            async with async_driver.transaction():
                await async_driver.execute("INSERT INTO ledger VALUES (40, 'async_nested_kept')")

        assert await async_driver.execute_tuples("SELECT id, note FROM ledger ORDER BY id") == [
            (10, "async_root"),
            (40, "async_nested_kept"),
        ]
    finally:
        sync_driver.close()
        await async_driver.close()


@pytest.mark.anyio
async def test_duckdb_delayed_commit_sync_and_async_pipelines() -> None:
    """`SyncDuckDBPipeline` and `AsyncDuckDBPipeline` batch operations, nest via savepoints, and roll back on error."""
    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        assert isinstance(sync_driver, SyncPipelineCapable)
        assert isinstance(async_driver, AsyncPipelineCapable)
        sync_driver.execute("CREATE TABLE pipe_items (id INTEGER PRIMARY KEY, label VARCHAR)")

        with sync_driver.pipeline() as pipe:
            assert isinstance(pipe, SyncDuckDBPipeline)
            pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "one"))
            pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "two"))
            pipe.select("SELECT id, label FROM pipe_items ORDER BY id")
            out = pipe.results()
        assert out == [1, 1, [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}]]

        with sync_driver.transaction():
            sync_driver.execute("INSERT INTO pipe_items VALUES (3, 'outer_kept')")
            with pytest.raises(_PipelineAbortError), sync_driver.pipeline() as nested_pipe:
                nested_pipe.execute("INSERT INTO pipe_items VALUES (4, 'inner_aborted')")
                nested_pipe.results()
                raise _PipelineAbortError("abort inner pipeline")

        assert sync_driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [
            (1, "one"),
            (2, "two"),
            (3, "outer_kept"),
        ]

        await async_driver.execute("CREATE TABLE pipe_items (id INTEGER PRIMARY KEY, label VARCHAR)")
        async with async_driver.pipeline() as apipe:
            assert isinstance(apipe, AsyncDuckDBPipeline)
            apipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (10, "ten"))
            apipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (20, "twenty"))
            apipe.select("SELECT id, label FROM pipe_items ORDER BY id")
            aout = await apipe.results()
        assert aout == [1, 1, [{"id": 10, "label": "ten"}, {"id": 20, "label": "twenty"}]]

        async with async_driver.transaction():
            await async_driver.execute("INSERT INTO pipe_items VALUES (30, 'async_outer_kept')")
            with pytest.raises(_PipelineAbortError):
                async with async_driver.pipeline() as anested_pipe:
                    anested_pipe.execute("INSERT INTO pipe_items VALUES (40, 'async_inner_aborted')")
                    await anested_pipe.results()
                    raise _PipelineAbortError("abort async inner pipeline")

        assert await async_driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [
            (10, "ten"),
            (20, "twenty"),
            (30, "async_outer_kept"),
        ]
    finally:
        sync_driver.close()
        await async_driver.close()


@pytest.mark.parametrize(
    ("setup_sql", "failing_sql", "expected_cls", "expected_sqlstate"),
    [
        (
            "CREATE TABLE u_err (id INTEGER PRIMARY KEY); INSERT INTO u_err VALUES (1)",
            "INSERT INTO u_err VALUES (1)",
            IntegrityError,
            "23000",
        ),
        (
            None,
            "SELECT * FROM nonexistent_table_xyz_duckdb",
            ProgrammingError,
            "42000",
        ),
        (
            None,
            "SELECT CAST('not_an_int' AS INTEGER)",
            DataError,
            "22000",
        ),
    ],
    ids=["unique_violation", "missing_table", "invalid_cast"],
)
def test_duckdb_database_error_attributes(
    setup_sql: str | None,
    failing_sql: str,
    expected_cls: type[DatabaseError],
    expected_sqlstate: str,
) -> None:
    """Raised DuckDB exceptions populate `.sqlstate`, `.vendor_code`, and `.is_transient`."""
    driver = SyncDuckDBDriver()
    try:
        if setup_sql is not None:
            for stmt in setup_sql.split(";"):
                if stmt.strip():
                    driver.execute(stmt.strip())
        with pytest.raises(expected_cls) as exc_info:
            driver.select(failing_sql)
        err = exc_info.value
        assert isinstance(err, DatabaseError)
        assert err.sqlstate == expected_sqlstate
        assert err.vendor_code is None
        assert err.is_transient is False
    finally:
        driver.close()


@pytest.mark.anyio
async def test_duckdb_arrow_c_stream_ipc_bytes_and_dynamic_secret_provider_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`select_to_arrow` exposes `__arrow_c_stream__`, `load_from_arrow` accepts raw IPC bytes, and `value_provider` rotates secrets."""
    token_state = {"generation": 1}

    class _RotatingBearerProvider:
        @property
        def generation(self) -> int:
            return token_state["generation"]

        def __call__(self) -> dict[str, Any]:
            return {"bearer_token": f"bearer-tok-{self.generation}"}

    provider = _RotatingBearerProvider()
    secret = DuckDBSecretConfig(
        name="vtb_http_secret",
        secret_type="http",
        value_provider=provider,
        scope="https://datasets.example.com/",
    )
    sync_driver = SyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    try:
        sync_driver.execute("CREATE TABLE arrow_items (id BIGINT, name VARCHAR)")
        table = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        sink = pa.BufferOutputStream()
        with pa_ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        raw_ipc = sink.getvalue().to_pybytes()

        inserted = sync_driver.load_from_arrow("arrow_items", raw_ipc)
        assert inserted == 3

        with sync_driver.select_to_arrow(
            "SELECT id, name FROM arrow_items ORDER BY id",
            threshold_rows=1,
        ) as cursor:
            assert isinstance(cursor, ArrowCursor)
            assert cursor.used_shm is True
            assert hasattr(cursor, "__arrow_c_stream__")
            imported = pa.Table.from_batches(pa.RecordBatchReader._import_from_c_capsule(cursor.__arrow_c_stream__()))
            assert imported.to_pylist() == [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 3, "name": "c"},
            ]

        with sync_driver.select_to_arrow(
            "SELECT id, name FROM arrow_items ORDER BY id",
            threshold_rows=512,
        ) as inline_cursor:
            assert isinstance(inline_cursor, ArrowCursor)
            assert inline_cursor.used_shm is False
            assert inline_cursor.to_pylist() == [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 3, "name": "c"},
            ]

        visible = sync_driver.select_one_or_none(
            "SELECT name, type, scope FROM duckdb_secrets() WHERE name = 'vtb_http_secret'"
        )
        assert visible is not None
        assert visible["name"] == "vtb_http_secret"
        assert visible["type"] == "http"
        assert visible["scope"] == ["https://datasets.example.com/"]

        token_state["generation"] = 2
        sync_driver.select("SELECT 1 AS ok")
        assert sync_driver._secret_generations["vtb_http_secret"] == 1
        sync_driver.refresh_secrets(force=False)
        assert sync_driver._secret_generations["vtb_http_secret"] == 2
    finally:
        sync_driver.close()

    async_calls = 0

    async def _async_secret_provider() -> dict[str, Any]:
        nonlocal async_calls
        async_calls += 1
        return {"bearer_token": f"async-tok-{async_calls}"}

    async_secret = DuckDBSecretConfig(
        name="vtb_async_http_secret",
        secret_type="http",
        value_provider=_async_secret_provider,
        refresh_interval_seconds=0.001,
    )
    fake_clock = {"now": 100.0}
    monkeypatch.setattr(duckdb_async_mod, "time", SimpleNamespace(monotonic=lambda: fake_clock["now"]))
    async_driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(async_secret,)))
    try:
        await async_driver.select("SELECT 1 AS ok")
        assert async_calls == 1
        fake_clock["now"] += 0.005
        await async_driver.select("SELECT 2 AS ok")
        assert async_calls == 1
        await async_driver.refresh_secrets(force=False)
        assert async_calls == 2
    finally:
        await async_driver.close()


class _PipeRow(msgspec.Struct, frozen=True):
    """Typed row struct for verifying pipeline `schema_type` hydration."""

    id: int
    label: str


@pytest.mark.anyio
async def test_duckdb_secret_escaping_notfound_and_pipeline_schema() -> None:
    """Verify secret SQL escaping, native NotFoundError, and pipeline schema_type."""
    escaped_secret = DuckDBSecretConfig(
        name="s3_escaped",
        secret_type="s3",
        provider="config",
        value={
            "key_id": "kid'quote",
            "secret": "sec'quote",
            "region": "us-east-1",
            "endpoint": "127.0.0.1:9000/a'b",
            "use_ssl": False,
        },
        scope=("s3://bucket-a/", "s3://bucket-b/"),
        persistent=False,
        replace=False,
    ).to_create_secret_sql()
    assert escaped_secret.startswith("CREATE SECRET IF NOT EXISTS s3_escaped (")
    assert "PROVIDER config" in escaped_secret
    assert "KEY_ID 'kid''quote'" in escaped_secret
    assert "SECRET 'sec''quote'" in escaped_secret
    assert "ENDPOINT '127.0.0.1:9000/a''b'" in escaped_secret
    assert "USE_SSL false" in escaped_secret
    assert "SCOPE ['s3://bucket-a/', 's3://bucket-b/']" in escaped_secret

    mapped = DuckDBSecretConfig.from_mapping(
        {
            "name": "mapped_r2",
            "secret_type": "r2",
            "value": {"key_id": "k1", "secret": "s1", "account_id": "acct1"},
            "scope": ["r2://lake/"],
        }
    )
    assert "ACCOUNT_ID 'acct1'" in mapped.to_create_secret_sql()
    assert "SCOPE 'r2://lake/'" in mapped.to_create_secret_sql()

    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(name="bad; DROP TABLE x", value={"secret": "s"}).to_create_secret_sql()
    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(
            name="valid_name",
            secret_type="S3); DROP TABLE x; --",
            value={"secret": "s"},
        ).to_create_secret_sql()
    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(name="valid_name", value={"secret": "line1\nline2"}).to_create_secret_sql()
    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(name="valid_name", value={"bad;key": "v"}).to_create_secret_sql()

    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        assert not hasattr(SyncDuckDBDriver(), "__dict__")
        assert not hasattr(async_driver, "__dict__")
        assert not hasattr(async_driver, "sync_driver")

        sync_driver.execute("CREATE TABLE typed_rows (id INTEGER, label VARCHAR, payload VARCHAR)")
        sync_driver.execute("INSERT INTO typed_rows VALUES (?, ?, ?)", (1, "alpha", (10, 20)))

        assert sync_driver._client is not None
        assert async_driver._client is not None
        with pytest.raises(NotFoundError):
            sync_driver.select_one("SELECT id FROM typed_rows WHERE id = 999")
        with pytest.raises(NotFoundError):
            sync_driver.select_value("SELECT id FROM typed_rows WHERE id = 999")
        with pytest.raises(NotFoundError):
            await async_driver.select_one("SELECT 1 WHERE 1 = 0")
        with pytest.raises(NotFoundError):
            await async_driver.select_value("SELECT 1 WHERE 1 = 0")

        with sync_driver.pipeline() as pipe:
            pipe.select("SELECT id, label FROM typed_rows ORDER BY id", schema_type=_PipeRow)
            typed_sync = pipe.results()
        assert typed_sync == [[_PipeRow(id=1, label="alpha")]]

        await async_driver.execute("CREATE TABLE typed_rows (id INTEGER, label VARCHAR)")
        await async_driver.execute("INSERT INTO typed_rows VALUES (?, ?)", (2, "beta"))
        async with async_driver.pipeline() as apipe:
            apipe.select("SELECT id, label FROM typed_rows ORDER BY id", schema_type=_PipeRow)
            typed_async = await apipe.results()
        assert typed_async == [[_PipeRow(id=2, label="beta")]]

        gen = sync_driver.stream_arrow_batches("SELECT range AS v FROM range(10)", batch_size=2)
        first_batch = next(gen)
        assert len(first_batch) == 2
        gen.close()

        agen = async_driver.stream_arrow_batches("SELECT range AS v FROM range(10)", batch_size=2)
        afirst_batch = await agen.__anext__()
        assert len(afirst_batch) == 2
        await agen.aclose()
    finally:
        sync_driver.close()
        await async_driver.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"persistent": True, "value_provider": lambda: {"bearer_token": "tok"}},
        {"refresh_interval_seconds": True},
        {"refresh_interval_seconds": False},
        {"refresh_interval_seconds": 0.0},
        {"refresh_interval_seconds": -1.0},
        {"refresh_interval_seconds": float("nan")},
        {"refresh_interval_seconds": float("inf")},
    ],
)
def test_duckdb_secret_config_rejects_invalid_persistent_and_refresh_intervals(
    kwargs: dict[str, Any],
) -> None:
    """Verify DuckDBSecretConfig fails closed on persistent dynamic providers and invalid intervals."""
    with pytest.raises(ImproperConfigurationError):
        DuckDBSecretConfig(name="invalid_secret", secret_type="http", **kwargs)
    assert math.isfinite(DuckDBSecretConfig(name="ok_secret", secret_type="http").effective_refresh_interval())


class _FaultInjectingSyncCursor:
    """Real SyncDuckDBCursor proxy raising the owning client's injected error on `execute`."""

    def __init__(self, owner: _FaultInjectingSyncClient, inner: Any) -> None:
        self._owner = owner
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def execute(self, sql: str, params: Any = None) -> Any:
        if self._owner._query_error is not None:
            raise self._owner._query_error
        self._inner.execute(sql, params)
        return self


class _CountingClient:
    """Proxy forwarding attributes to a native client while recording method call names."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def _record(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return attr(*args, **kwargs)

        return _record


class _FaultInjectingSyncClient:
    """Real SyncDuckDBClient proxy injecting an HTTP error on data queries while tracking close()."""

    def __init__(self, inner: SyncDuckDBClient, query_error: Exception | None = None) -> None:
        self._inner = inner
        self._query_error = query_error
        self.close_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def execute(self, sql: str, params: Any = None) -> int:
        if self._query_error is not None and "CREATE" not in sql.upper():
            raise self._query_error
        return int(self._inner.execute(sql, params))

    def fetch_rows(self, sql: str, params: Any = None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        if self._query_error is not None:
            raise self._query_error
        return self._inner.fetch_rows(sql, params)

    def fetch_dicts(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        if self._query_error is not None:
            raise self._query_error
        return self._inner.fetch_dicts(sql, params)

    def fetch_value(self, sql: str, params: Any = None) -> Any:
        if self._query_error is not None:
            raise self._query_error
        return self._inner.fetch_value(sql, params)

    def execute_dml(self, sql: str, params: Any = None) -> tuple[int, int | None]:
        if self._query_error is not None:
            raise self._query_error
        return self._inner.execute_dml(sql, params)

    def cursor(self, *, lazy_begin: bool = False) -> _FaultInjectingSyncCursor:
        return _FaultInjectingSyncCursor(self, self._inner.cursor(lazy_begin=lazy_begin))

    def close(self) -> None:
        self.close_calls += 1
        self._inner.close()


class _FaultInjectingAsyncCursor:
    """Real AsyncDuckDBCursor proxy raising the owning client's injected error on `execute`."""

    def __init__(self, owner: _FaultInjectingAsyncClient, inner: Any) -> None:
        self._owner = owner
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def execute(self, sql: str, params: Any = None) -> Any:
        if self._owner._query_error is not None:
            raise self._owner._query_error
        await self._inner.execute(sql, params)
        return self


class _FaultInjectingAsyncClient:
    """Real AsyncDuckDBClient proxy injecting an HTTP error on data queries while tracking close()."""

    def __init__(self, inner: AsyncDuckDBClient, query_error: Exception | None = None) -> None:
        self._inner = inner
        self._query_error = query_error
        self.close_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def execute(self, sql: str, params: Any = None) -> int:
        if self._query_error is not None and "CREATE" not in sql.upper():
            raise self._query_error
        return int(await self._inner.execute(sql, params))

    async def fetch_rows(self, sql: str, params: Any = None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        if self._query_error is not None:
            raise self._query_error
        return await self._inner.fetch_rows(sql, params)

    async def fetch_dicts(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        if self._query_error is not None:
            raise self._query_error
        return await self._inner.fetch_dicts(sql, params)

    async def fetch_value(self, sql: str, params: Any = None) -> Any:
        if self._query_error is not None:
            raise self._query_error
        return await self._inner.fetch_value(sql, params)

    async def execute_dml(self, sql: str, params: Any = None) -> tuple[int, int | None]:
        if self._query_error is not None:
            raise self._query_error
        return await self._inner.execute_dml(sql, params)

    def cursor(self, *, lazy_begin: bool = False) -> _FaultInjectingAsyncCursor:
        return _FaultInjectingAsyncCursor(self, self._inner.cursor(lazy_begin=lazy_begin))

    async def close(self) -> None:
        self.close_calls += 1
        await self._inner.close()


class _RecordingSecretProvider:
    """Minimal stateful dynamic secret provider advancing generation lazily on fetch after invalidation."""

    def __init__(self) -> None:
        self.generation = 0
        self.invalidated: list[int] = []
        self.calls = 0
        self._stale = True

    def invalidate(self, failed_generation: int) -> None:
        self.invalidated.append(failed_generation)
        if self.generation == failed_generation:
            self._stale = True

    def __call__(self) -> dict[str, str]:
        self.calls += 1
        if self._stale:
            self.generation += 1
            self._stale = False
        return {"bearer_token": f"tok-gen-{self.generation}"}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("error_message", "expected_invalidation"),
    [
        ("HTTP Error: unable to connect (HTTP 401)", True),
        ("HTTP Error: forbidden (HTTP 403)", True),
        ("HTTP Error: not found (HTTP 404)", False),
        ("Parser Error: syntax error at or near 'FROM'", False),
    ],
)
async def test_duckdb_driver_invalidates_only_401_and_403_errors_and_rotates_next_query(
    error_message: str,
    expected_invalidation: bool,
) -> None:
    """Verify sync and async drivers invalidate tracked secret generations only on HTTP 401/403 errors."""
    sync_provider = _RecordingSecretProvider()
    plain_calls = 0

    def plain_callable() -> dict[str, str]:
        nonlocal plain_calls
        plain_calls += 1
        return {"bearer_token": "plain-tok"}

    sync_secrets = (
        DuckDBSecretConfig(name="s_tracked", secret_type="http", value_provider=sync_provider),
        DuckDBSecretConfig(name="s_plain", secret_type="http", value_provider=plain_callable),
    )
    sync_proxy = _FaultInjectingSyncClient(SyncDuckDBClient(":memory:"), query_error=RuntimeError(error_message))
    sync_driver = SyncDuckDBDriver(DuckDBConfig(secrets=sync_secrets), _client=cast("SyncDuckDBClient", sync_proxy))
    try:
        assert sync_provider.calls == 1
        assert plain_calls == 1
        with pytest.raises(RuntimeError, match="Error"):
            sync_driver.select("SELECT * FROM 'https://example.invalid/data.parquet'")
        if expected_invalidation:
            assert sync_provider.invalidated == [1]
        else:
            assert sync_provider.invalidated == []
        assert sync_provider.calls == (2 if expected_invalidation else 1)
        assert plain_calls == (2 if expected_invalidation else 1)
        assert sync_provider.generation == (2 if expected_invalidation else 1)

        sync_proxy._query_error = None
        assert sync_driver.select("SELECT 42 AS answer") == [{"answer": 42}]
        assert sync_provider.calls == (2 if expected_invalidation else 1)
        assert sync_provider.generation == (2 if expected_invalidation else 1)
    finally:
        sync_driver.close()

    async_provider = _RecordingSecretProvider()
    async_secrets = (DuckDBSecretConfig(name="a_tracked", secret_type="http", value_provider=async_provider),)
    async_proxy = _FaultInjectingAsyncClient(AsyncDuckDBClient(":memory:"), query_error=RuntimeError(error_message))
    async_driver = AsyncDuckDBDriver(
        DuckDBConfig(secrets=async_secrets), _client=cast("AsyncDuckDBClient", async_proxy)
    )
    try:
        with pytest.raises(RuntimeError, match="Error"):
            await async_driver.select("SELECT * FROM 'https://example.invalid/data.parquet'")
        if expected_invalidation:
            assert async_provider.invalidated == [1]
        else:
            assert async_provider.invalidated == []
        assert async_provider.calls == (2 if expected_invalidation else 1)
        assert async_provider.generation == (2 if expected_invalidation else 1)

        async_proxy._query_error = None
        assert await async_driver.select("SELECT 7 AS answer") == [{"answer": 7}]
        assert async_provider.calls == (2 if expected_invalidation else 1)
        assert async_provider.generation == (2 if expected_invalidation else 1)
    finally:
        await async_driver.close()


@pytest.mark.anyio
async def test_duckdb_driver_refreshes_plain_callable_secret_without_generation_on_401() -> None:
    """Verify HTTP 401 triggers forced refresh even when the sole dynamic provider has no generation attribute."""
    sync_calls = 0

    def plain_sync_provider() -> dict[str, str]:
        nonlocal sync_calls
        sync_calls += 1
        return {"bearer_token": f"sync-tok-{sync_calls}"}

    sync_proxy = _FaultInjectingSyncClient(
        SyncDuckDBClient(":memory:"), query_error=RuntimeError("HTTP Error: unauthorized (HTTP 401)")
    )
    sync_driver = SyncDuckDBDriver(
        DuckDBConfig(
            secrets=(DuckDBSecretConfig(name="plain_only", secret_type="http", value_provider=plain_sync_provider),)
        ),
        _client=cast("SyncDuckDBClient", sync_proxy),
    )
    try:
        assert sync_calls == 1
        with pytest.raises(RuntimeError, match="HTTP 401"):
            sync_driver.select("SELECT 1")
        assert sync_calls == 2
    finally:
        sync_driver.close()

    async_calls = 0

    def plain_async_provider() -> dict[str, str]:
        nonlocal async_calls
        async_calls += 1
        return {"bearer_token": f"async-tok-{async_calls}"}

    async_proxy = _FaultInjectingAsyncClient(
        AsyncDuckDBClient(":memory:"), query_error=RuntimeError("HTTP Error: forbidden (HTTP 403)")
    )
    async_driver = AsyncDuckDBDriver(
        DuckDBConfig(
            secrets=(DuckDBSecretConfig(name="plain_async", secret_type="http", value_provider=plain_async_provider),)
        ),
        _client=cast("AsyncDuckDBClient", async_proxy),
    )
    try:
        with pytest.raises(RuntimeError, match="HTTP 403"):
            await async_driver.select("SELECT 1")
        assert async_calls == 2
    finally:
        await async_driver.close()


@pytest.mark.anyio
async def test_duckdb_driver_closes_client_when_initial_required_secret_refresh_fails() -> None:
    """Verify failed initial required=True secret refresh closes and clears _client across sync and async drivers."""

    def failing_provider() -> dict[str, str]:
        raise RuntimeError("vault unavailable")

    cfg = DuckDBConfig(
        secrets=(DuckDBSecretConfig(name="broken_secret", secret_type="http", value_provider=failing_provider),)
    )
    sync_proxy = _FaultInjectingSyncClient(SyncDuckDBClient(":memory:"))
    with pytest.raises(RuntimeError, match="vault unavailable"):
        SyncDuckDBDriver(cfg, _client=cast("SyncDuckDBClient", sync_proxy))
    assert sync_proxy.close_calls == 1

    async_proxy = _FaultInjectingAsyncClient(AsyncDuckDBClient(":memory:"))
    async_driver = AsyncDuckDBDriver(cfg, _client=cast("AsyncDuckDBClient", async_proxy))
    with pytest.raises(RuntimeError, match="vault unavailable"):
        await async_driver.select("SELECT 1 AS ok")
    assert async_proxy.close_calls == 1
    assert async_driver._client is None
    await async_driver.close()
    assert async_proxy.close_calls == 1

    tx_proxy = _FaultInjectingAsyncClient(AsyncDuckDBClient(":memory:"))
    tx_driver = AsyncDuckDBDriver(cfg, _client=cast("AsyncDuckDBClient", tx_proxy))
    with pytest.raises(RuntimeError, match="vault unavailable"):
        async with tx_driver.transaction():
            await tx_driver.select("SELECT 1 AS ok")
    assert tx_proxy.close_calls == 1
    assert tx_driver._client is None


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncDuckDBDriver", "vertebra.db.adapters.duckdb", "vertebra.db.adapters.duckdb._sync"),
        ("SyncDuckDBDriver", "vertebra.db.adapters.duckdb.driver", "vertebra.db.adapters.duckdb._sync"),
        ("SyncDuckDBDriver", "vertebra.db.adapters.duckdb._sync", "vertebra.db.adapters.duckdb._sync"),
        ("AsyncDuckDBDriver", "vertebra.db.adapters.duckdb", "vertebra.db.adapters.duckdb._async"),
        ("AsyncDuckDBDriver", "vertebra.db.adapters.duckdb.driver", "vertebra.db.adapters.duckdb._async"),
        ("AsyncDuckDBDriver", "vertebra.db.adapters.duckdb._async", "vertebra.db.adapters.duckdb._async"),
        ("SyncDuckDBPipeline", "vertebra.db.adapters.duckdb", "vertebra.db.adapters.duckdb.pipeline"),
        ("SyncDuckDBPipeline", "vertebra.db.adapters.duckdb.pipeline", "vertebra.db.adapters.duckdb.pipeline"),
        ("AsyncDuckDBPipeline", "vertebra.db.adapters.duckdb", "vertebra.db.adapters.duckdb.pipeline"),
        ("AsyncDuckDBPipeline", "vertebra.db.adapters.duckdb.pipeline", "vertebra.db.adapters.duckdb.pipeline"),
    ],
)
def test_duckdb_driver_and_pipeline_resolution_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify DuckDB sync/async driver and pipeline classes resolve identically across import paths."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


def test_duckdb_sync_secret_provider_and_bootstrap_property_guards() -> None:
    """Verify SyncDuckDBDriver rejects async secret providers and exposes read-only bootstrap statements."""

    async def _async_secret() -> dict[str, str]:
        return {"token": "abc"}

    cfg = DuckDBConfig(
        database=":memory:",
        secrets=(
            DuckDBSecretConfig(
                name="dyn",
                secret_type="bearer",
                value_provider=_async_secret,
                persistent=False,
                required=False,
            ),
        ),
    )
    with pytest.raises(ImproperConfigurationError, match="requires a synchronous `value_provider`"):
        SyncDuckDBDriver(cfg)

    sync_drv = SyncDuckDBDriver(DuckDBConfig(database=":memory:"))
    try:
        assert isinstance(sync_drv.executed_bootstrap_statements, tuple)
        mutable_view: Any = sync_drv
        with pytest.raises(AttributeError):
            mutable_view.executed_bootstrap_statements = ()
    finally:
        sync_drv.close()


def _native_cursor_names(cursor: Any) -> list[str] | None:
    """Return the column names of a cursor description, or `None` without a result set."""
    description = cursor.description
    if description is None:
        return None
    assert all(len(entry) == 7 and entry[1:] == (None,) * 6 for entry in description)
    return [entry[0] for entry in description]


def test_duckdb_native_cursor_fresh_attributes() -> None:
    """A fresh cursor reports no result set and the PEP 249 defaults."""
    with SyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        assert type(cursor).__name__ == "SyncDuckDBCursor"
        assert cursor is not client.cursor()
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert cursor.arraysize == 1


@pytest.mark.parametrize(
    ("sql", "expected_names", "expected_rowcount", "expected_rows"),
    [
        ("SELECT 1 AS a, 'x' AS b", ["a", "b"], -1, [(1, "x")]),
        ("CREATE TABLE nc_ddl (id INTEGER)", None, 0, None),
        ("INSERT INTO nc_seed VALUES (10, 'p'), (11, 'q')", None, 2, None),
        ("INSERT INTO nc_seed VALUES (12, 'r') RETURNING id", ["id"], 1, [(12,)]),
        ("SELECT id, label FROM nc_seed WHERE 1 = 0", ["id", "label"], -1, []),
        ("SELECT id FROM nc_seed ORDER BY id", ["id"], -1, [(1,), (2,), (3,)]),
    ],
    ids=["query", "ddl", "insert", "returning", "empty_result", "seeded_query"],
)
def test_duckdb_native_cursor_statement_outcomes(
    sql: str,
    expected_names: list[str] | None,
    expected_rowcount: int,
    expected_rows: list[tuple[Any, ...]] | None,
) -> None:
    """`execute` returns the cursor and sets `description`, `rowcount`, and the buffered rows."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_seed (id INTEGER, label VARCHAR)")
        client.execute("INSERT INTO nc_seed VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        cursor = client.cursor()
        assert cursor.execute(sql) is cursor
        assert _native_cursor_names(cursor) == expected_names
        assert cursor.rowcount == expected_rowcount
        assert cursor.lastrowid is None
        if expected_rows is None:
            assert cursor.fetchall() == []
        else:
            assert cursor.fetchall() == expected_rows
            assert cursor.fetchall() == []
        assert client.in_transaction is False


def test_duckdb_native_cursor_rich_binds_match_client_execute() -> None:
    """Rich Python parameters bound through the cursor store the values `client.execute` stores."""
    params = (
        decimal.Decimal("12.34"),
        datetime.datetime(2024, 5, 6, 7, 8, 9, 123456),
        datetime.date(2024, 5, 6),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        {"k": [1, 2], "n": None},
    )
    insert_sql = "INSERT INTO nc_rich VALUES (?, ?, ?, ?, ?, ?)"
    select_sql = "SELECT d, ts, dt, u, j FROM nc_rich ORDER BY id"
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_rich (id INTEGER, d DECIMAL(10, 2), ts TIMESTAMP, dt DATE, u UUID, j VARCHAR)")
        client.execute(insert_sql, (1, *params))
        cursor = client.cursor()
        cursor.execute(insert_sql, (2, *params))
        assert cursor.rowcount == 1
        rows = cursor.execute(select_sql).fetchall()
        assert len(rows) == 2
        assert rows[0] == rows[1]
        assert rows == client.cursor().execute(select_sql).fetchall()
        assert rows[1][0] == params[0]
        assert rows[1][1].replace(tzinfo=None) == params[1]
        assert rows[1][2] == params[2]
        assert str(rows[1][3]) == str(params[3])


@pytest.mark.parametrize(
    ("sql", "params", "expected"),
    [
        ("SELECT $v", {"v": 5}, [(5,)]),
        ("SELECT ? + ?", (2, 3), [(5,)]),
        ("SELECT ? + ?", [2, 3], [(5,)]),
        ("SELECT 7", None, [(7,)]),
        ("SELECT 7", (), [(7,)]),
    ],
    ids=["named", "tuple", "list", "none", "empty_tuple"],
)
def test_duckdb_native_cursor_parameter_styles(sql: str, params: Any, expected: list[tuple[Any, ...]]) -> None:
    """`execute` accepts named mappings, positional sequences, `None`, and an empty sequence."""
    with SyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        assert cursor.execute(sql, params).fetchall() == expected


def test_duckdb_native_cursor_fetch_walk_and_discard() -> None:
    """Fetch calls walk the buffer in order, and a new `execute` discards unfetched rows."""
    with SyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        cursor.execute("SELECT i FROM range(3) t(i) ORDER BY i")
        assert cursor.fetchone() == (0,)
        assert cursor.fetchmany(1) == [(1,)]
        assert cursor.fetchall() == [(2,)]
        assert cursor.fetchone() is None
        assert cursor.fetchmany() == []

        cursor.execute("SELECT i FROM range(5) t(i) ORDER BY i")
        cursor.arraysize = 2
        assert cursor.arraysize == 2
        assert cursor.fetchmany() == [(0,), (1,)]
        assert cursor.fetchmany(0) == []
        assert cursor.fetchmany(10) == [(2,), (3,), (4,)]

        cursor.execute("SELECT 'first' AS a")
        cursor.execute("SELECT 'second' AS b")
        assert _native_cursor_names(cursor) == ["b"]
        assert cursor.fetchall() == [("second",)]


def test_duckdb_native_cursor_executemany_counts_rows_and_skips_empty() -> None:
    """`executemany` reports the total changed rows, and an empty sequence reports zero."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_many (id INTEGER, label VARCHAR)")
        cursor = client.cursor()
        assert cursor.executemany("INSERT INTO nc_many VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")]) is cursor
        assert cursor.rowcount == 3
        assert cursor.description is None
        assert cursor.lastrowid is None
        assert cursor.executemany("INSERT INTO nc_many VALUES (?, ?)", []) is cursor
        assert cursor.rowcount == 0
        assert cursor.description is None
        assert client.cursor().execute("SELECT count(*) FROM nc_many").fetchall() == [(3,)]

        lazy = client.cursor(lazy_begin=True)
        lazy.executemany("INSERT INTO nc_many VALUES (?, ?)", [])
        assert lazy.rowcount == 0
        assert client.in_transaction is False


@pytest.mark.parametrize(
    ("setup_sql", "action", "expected_error", "expected_message"),
    [
        ("SELECT 1", "arraysize_zero", "ProgrammingError", "arraysize must be at least 1"),
        ("SELECT 1", "arraysize_negative", "ProgrammingError", "arraysize must be at least 1"),
        ("SELECT 1", "fetchmany_negative", "ProgrammingError", "fetchmany size must not be negative"),
    ],
    ids=[
        "arraysize_zero",
        "arraysize_negative",
        "fetchmany_negative",
    ],
)
def test_duckdb_native_cursor_state_errors(
    setup_sql: str | None,
    action: str,
    expected_error: str,
    expected_message: str,
) -> None:
    """Calls that the cursor state forbids raise the native DB-API error with a fixed message."""
    error_cls = getattr(native_core_mod, expected_error)
    assert getattr(dbapi_mod, expected_error) is error_cls
    with SyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        if setup_sql is not None:
            cursor.execute(setup_sql)
        with pytest.raises(error_cls, match=expected_message):
            if action == "arraysize_zero":
                cursor.arraysize = 0
            elif action == "arraysize_negative":
                cursor.arraysize = -3
            elif action == "fetchmany_negative":
                cursor.fetchmany(-1)
            else:
                getattr(cursor, action)()
        assert cursor.arraysize == 1


def test_duckdb_native_cursor_dml_then_fetch_returns_empty() -> None:
    """A fetch after DML without a returning clause returns an empty list."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_dml (id INTEGER)")
        cursor = client.cursor()
        cursor.execute("INSERT INTO nc_dml VALUES (1)")
        assert cursor.rowcount == 1
        assert cursor.fetchall() == []


@pytest.mark.parametrize(
    "action",
    ["execute", "executemany", "fetchone", "fetchmany", "fetchall"],
)
def test_duckdb_native_cursor_closed_cursor_raises_interface_error(action: str) -> None:
    """After `close`, which is idempotent, every other method raises `InterfaceError`."""
    with SyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        cursor.execute("SELECT 1 AS a")
        cursor.close()
        cursor.close()
        assert cursor.description is None
        assert cursor.rowcount == -1
        with pytest.raises(native_core_mod.InterfaceError, match="cursor is closed") as exc_info:
            if action == "execute":
                cursor.execute("SELECT 1")
            elif action == "executemany":
                cursor.executemany("SELECT ?", [(1,)])
            else:
                getattr(cursor, action)()
        assert isinstance(exc_info.value, dbapi_mod.InterfaceError)
        assert client.cursor().execute("SELECT 2").fetchall() == [(2,)]


def test_duckdb_native_cursor_lazy_begin_opens_and_reopens_transactions() -> None:
    """A lazy-begin cursor opens a transaction before a statement whenever none is active."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_lazy (id INTEGER)")
        cursor = client.cursor(lazy_begin=True)
        assert client.in_transaction is False
        cursor.execute("INSERT INTO nc_lazy VALUES (1)")
        assert client.in_transaction is True
        client.rollback()
        assert client.in_transaction is False
        assert client.cursor().execute("SELECT count(*) FROM nc_lazy").fetchall() == [(0,)]

        cursor.execute("INSERT INTO nc_lazy VALUES (2)")
        assert client.in_transaction is True
        client.commit()
        assert client.in_transaction is False
        cursor.execute("INSERT INTO nc_lazy VALUES (3)")
        assert client.in_transaction is True
        client.rollback()
        assert client.cursor().execute("SELECT id FROM nc_lazy").fetchall() == [(2,)]


def test_duckdb_native_cursor_default_cursor_autocommits() -> None:
    """A default cursor opens no transaction, so its insert survives a later rollback of other work."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_auto (id INTEGER)")
        cursor = client.cursor()
        cursor.execute("INSERT INTO nc_auto VALUES (1)")
        assert client.in_transaction is False
        client.begin()
        client.execute("INSERT INTO nc_auto VALUES (2)")
        client.rollback()
        assert client.cursor().execute("SELECT id FROM nc_auto").fetchall() == [(1,)]


def test_duckdb_native_cursor_two_threads_share_one_cursor() -> None:
    """Two threads calling `execute` on one cursor both return, and one whole result remains."""
    statements = {
        "left": ("SELECT i AS left_col FROM range(200) t(i) ORDER BY i", ["left_col"], [(i,) for i in range(200)]),
        "right": (
            "SELECT i + 1000 AS right_col FROM range(300) t(i) ORDER BY i",
            ["right_col"],
            [(i + 1000,) for i in range(300)],
        ),
    }
    with SyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        barrier = threading.Barrier(2)
        returned: list[Any] = []
        errors: list[BaseException] = []

        def worker(key: str) -> None:
            try:
                barrier.wait(timeout=10)
                returned.append(cursor.execute(statements[key][0]))
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(key,)) for key in statements]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert len(returned) == 2
        assert all(item is cursor for item in returned)
        names = _native_cursor_names(cursor)
        rows = cursor.fetchall()
        assert (names, rows) in [
            (expected_names, expected_rows) for _, expected_names, expected_rows in statements.values()
        ]


@pytest.mark.parametrize(
    ("method", "sql", "params", "expected_error", "expected_sqlstate"),
    [
        ("execute", "SELEC 1", None, "ProgrammingError", "42000"),
        ("execute", "INSERT INTO nc_fail VALUES (1)", None, "IntegrityError", "23000"),
        ("execute", "SELECT ?", object(), "InterfaceError", None),
        ("executemany", "INSERT INTO nc_fail VALUES (?)", [(5,), (5,)], "IntegrityError", "23000"),
        ("executemany", "INSERT INTO nc_fail VALUES (?)", 7, "InterfaceError", None),
    ],
    ids=[
        "execute_syntax",
        "execute_duplicate_key",
        "execute_bad_parameters",
        "executemany_duplicate_key",
        "executemany_bad_parameters",
    ],
)
def test_duckdb_native_cursor_failed_statement_clears_result(
    method: str,
    sql: str,
    params: Any,
    expected_error: str,
    expected_sqlstate: str | None,
) -> None:
    """A failed `execute` or `executemany` raises the mapped error and leaves no result behind."""
    native_cls = getattr(native_core_mod, expected_error)
    dbapi_cls = getattr(dbapi_mod, expected_error)
    assert dbapi_cls is native_cls
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE nc_fail (id INTEGER PRIMARY KEY)")
        client.execute("INSERT INTO nc_fail VALUES (1)")
        cursor = client.cursor()
        cursor.execute("SELECT 1 AS a")
        assert _native_cursor_names(cursor) == ["a"]
        with pytest.raises(native_cls) as exc_info:
            if method == "execute":
                cursor.execute(sql, params)
            else:
                cursor.executemany(sql, params)
        assert isinstance(exc_info.value, dbapi_cls)
        if expected_sqlstate is not None:
            assert exc_info.value.sqlstate == expected_sqlstate
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert cursor.fetchall() == []
        assert cursor.execute("SELECT 3").fetchall() == [(3,)]


@pytest.mark.anyio
async def test_duckdb_async_native_cursor_fresh_attributes() -> None:
    """A fresh async cursor reports no result set and the PEP 249 defaults."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        assert type(cursor).__name__ == "AsyncDuckDBCursor"
        assert cursor is not client.cursor()
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert cursor.arraysize == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql", "expected_names", "expected_rowcount", "expected_rows"),
    [
        ("SELECT 1 AS a", ["a"], -1, [(1,)]),
        ("INSERT INTO anc_seed VALUES (10, 'p'), (11, 'q')", None, 2, None),
        ("INSERT INTO anc_seed VALUES (12, 'r') RETURNING id", ["id"], 1, [(12,)]),
        ("SELECT id, label FROM anc_seed WHERE 1 = 0", ["id", "label"], -1, []),
    ],
    ids=["query", "insert", "returning", "empty_result"],
)
async def test_duckdb_async_native_cursor_statement_outcomes(
    sql: str,
    expected_names: list[str] | None,
    expected_rowcount: int,
    expected_rows: list[tuple[Any, ...]] | None,
) -> None:
    """An awaited `execute` resolves to the cursor and sets `description`, `rowcount`, and the rows."""
    async with AsyncDuckDBClient(":memory:") as client:
        await client.execute("CREATE TABLE anc_seed (id INTEGER, label VARCHAR)")
        await client.execute("INSERT INTO anc_seed VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        cursor = client.cursor()
        assert await cursor.execute(sql) is cursor
        assert _native_cursor_names(cursor) == expected_names
        assert cursor.rowcount == expected_rowcount
        assert cursor.lastrowid is None
        if expected_rows is None:
            assert await cursor.fetchall() == []
        else:
            assert await cursor.fetchall() == expected_rows
            assert await cursor.fetchall() == []
        assert client.in_transaction is False


@pytest.mark.anyio
async def test_duckdb_async_native_cursor_fetch_walk() -> None:
    """`fetchone`, `fetchmany`, and `fetchall` consume the buffered rows in order."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        await cursor.execute("SELECT * FROM (VALUES (1), (2), (3)) AS t(a) ORDER BY a")
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchmany(1) == [(2,)]
        assert await cursor.fetchall() == [(3,)]
        assert await cursor.fetchone() is None
        assert await cursor.fetchmany() == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("param_sets", "expected_rowcount", "expected_ids"),
    [
        ([(1,), (2,), (3,)], 3, [(1,), (2,), (3,)]),
        ([], 0, []),
    ],
    ids=["three_sets", "no_sets"],
)
async def test_duckdb_async_native_cursor_executemany_counts_rows(
    param_sets: list[tuple[int, ...]],
    expected_rowcount: int,
    expected_ids: list[tuple[int, ...]],
) -> None:
    """`executemany` resolves to the cursor and reports the total changed-row count."""
    async with AsyncDuckDBClient(":memory:") as client:
        await client.execute("CREATE TABLE anc_many (id INTEGER)")
        cursor = client.cursor(lazy_begin=True)
        assert await cursor.executemany("INSERT INTO anc_many VALUES (?)", param_sets) is cursor
        assert cursor.rowcount == expected_rowcount
        assert cursor.description is None
        assert client.in_transaction is bool(param_sets)
        if param_sets:
            await client.commit()
        assert await (await client.cursor().execute("SELECT id FROM anc_many ORDER BY id")).fetchall() == expected_ids


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("setup_sql", "action", "expected_message"),
    [
        ("SELECT 1", "arraysize_zero", "arraysize must be at least 1"),
        ("SELECT 1", "fetchmany_negative", "fetchmany size must not be negative"),
    ],
    ids=[
        "arraysize_zero",
        "fetchmany_negative",
    ],
)
async def test_duckdb_async_native_cursor_state_errors_raise_at_call(
    setup_sql: str | None,
    action: str,
    expected_message: str,
) -> None:
    """Calls that the cursor state forbids raise `ProgrammingError` from the call, before any await."""
    assert dbapi_mod.ProgrammingError is native_core_mod.ProgrammingError
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        if setup_sql is not None:
            await cursor.execute(setup_sql)
        with pytest.raises(native_core_mod.ProgrammingError, match=expected_message):
            if action == "arraysize_zero":
                cursor.arraysize = 0
            elif action == "fetchmany_negative":
                cursor.fetchmany(-1)
            else:
                getattr(cursor, action)()
        assert cursor.arraysize == 1


@pytest.mark.anyio
async def test_duckdb_async_native_cursor_fetchmany_zero_returns_no_rows() -> None:
    """`fetchmany(0)` resolves to an empty list and leaves the buffered rows in place."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        await cursor.execute("SELECT 1 AS a")
        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchall() == [(1,)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "action",
    ["execute", "executemany", "fetchone", "fetchmany", "fetchall"],
)
async def test_duckdb_async_native_cursor_closed_cursor_raises_interface_error(action: str) -> None:
    """After `close`, which is idempotent, every other method raises `InterfaceError` at the call."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        await cursor.execute("SELECT 1 AS a")
        await cursor.close()
        await cursor.close()
        assert cursor.description is None
        assert cursor.rowcount == -1
        with pytest.raises(native_core_mod.InterfaceError, match="cursor is closed") as exc_info:
            if action == "execute":
                cursor.execute("SELECT 1")
            elif action == "executemany":
                cursor.executemany("SELECT ?", [(1,)])
            else:
                getattr(cursor, action)()
        assert isinstance(exc_info.value, dbapi_mod.InterfaceError)
        other = client.cursor()
        await other.execute("SELECT 2")
        assert await other.fetchall() == [(2,)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "sql", "params", "expected_error", "expected_sqlstate"),
    [
        ("execute", "INSERT INTO anc_fail VALUES (1)", (), "IntegrityError", "23000"),
        ("execute", "SELECT * FROM anc_missing_table", (), "ProgrammingError", None),
        ("executemany", "INSERT INTO anc_fail VALUES (?)", [(2,), (1,)], "IntegrityError", "23000"),
    ],
    ids=["execute_duplicate_key", "execute_missing_table", "executemany_duplicate_key"],
)
async def test_duckdb_async_native_cursor_failed_statement_clears_result(
    method: str,
    sql: str,
    params: Any,
    expected_error: str,
    expected_sqlstate: str | None,
) -> None:
    """A failed statement raises the mapped error on await and leaves no result behind."""
    native_cls = getattr(native_core_mod, expected_error)
    assert getattr(dbapi_mod, expected_error) is native_cls
    async with AsyncDuckDBClient(":memory:") as client:
        await client.execute("CREATE TABLE anc_fail (id INTEGER PRIMARY KEY)")
        await client.execute("INSERT INTO anc_fail VALUES (1)")
        cursor = client.cursor()
        await cursor.execute("SELECT 1 AS a")
        assert _native_cursor_names(cursor) == ["a"]
        pending = cursor.execute(sql, params) if method == "execute" else cursor.executemany(sql, params)
        with pytest.raises(native_cls) as exc_info:
            await pending
        if expected_sqlstate is not None:
            assert exc_info.value.sqlstate == expected_sqlstate
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        assert await cursor.fetchall() == []
        await cursor.execute("SELECT 3")
        assert await cursor.fetchall() == [(3,)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "params"),
    [("execute", object()), ("executemany", 7)],
    ids=["execute", "executemany"],
)
async def test_duckdb_async_native_cursor_bad_parameters_raise_at_call_and_clear_result(
    method: str,
    params: Any,
) -> None:
    """A parameter conversion error is raised by the call, clears the result, and frees the cursor."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        await cursor.execute("SELECT 1 AS a")
        with pytest.raises(native_core_mod.InterfaceError):
            if method == "execute":
                cursor.execute("SELECT ?", params)
            else:
                cursor.executemany("SELECT ?", params)
        assert cursor.description is None
        assert cursor.rowcount == -1
        await cursor.execute("SELECT 4")
        assert await cursor.fetchall() == [(4,)]


@pytest.mark.anyio
@pytest.mark.parametrize("second", ["execute", "executemany"])
async def test_duckdb_async_native_cursor_rejects_statement_while_one_is_in_flight(second: str) -> None:
    """A second statement call before the first is awaited raises `InterfaceError`; the first completes."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        first = cursor.execute("SELECT 7 AS a")
        with pytest.raises(native_core_mod.InterfaceError, match="cursor is already executing"):
            if second == "execute":
                cursor.execute("SELECT 8")
            else:
                cursor.executemany("SELECT ?", [(8,)])
        assert await first is cursor
        assert await cursor.fetchall() == [(7,)]
        await cursor.execute("SELECT 9")
        assert await cursor.fetchall() == [(9,)]


@pytest.mark.anyio
async def test_duckdb_async_native_cursor_lazy_begin_opens_and_reopens_transactions() -> None:
    """A lazy-begin async cursor opens a transaction before a statement whenever none is active."""
    async with AsyncDuckDBClient(":memory:") as client:
        await client.execute("CREATE TABLE anc_lazy (id INTEGER)")
        cursor = client.cursor(lazy_begin=True)
        assert client.in_transaction is False
        await cursor.execute("INSERT INTO anc_lazy VALUES (1)")
        assert client.in_transaction is True
        await client.rollback()
        assert client.in_transaction is False
        assert await (await client.cursor().execute("SELECT count(*) FROM anc_lazy")).fetchall() == [(0,)]

        await cursor.execute("INSERT INTO anc_lazy VALUES (2)")
        assert client.in_transaction is True
        await client.commit()
        assert client.in_transaction is False
        await cursor.execute("INSERT INTO anc_lazy VALUES (3)")
        assert client.in_transaction is True
        await client.rollback()
        assert await (await client.cursor().execute("SELECT id FROM anc_lazy")).fetchall() == [(2,)]


@pytest.mark.anyio
async def test_duckdb_async_native_cursor_default_cursor_autocommits() -> None:
    """A default async cursor opens no transaction, so its insert survives a later rollback."""
    async with AsyncDuckDBClient(":memory:") as client:
        await client.execute("CREATE TABLE anc_auto (id INTEGER)")
        cursor = client.cursor()
        await cursor.execute("INSERT INTO anc_auto VALUES (1)")
        assert client.in_transaction is False
        await client.begin()
        await client.execute("INSERT INTO anc_auto VALUES (2)")
        await client.rollback()
        assert await (await client.cursor().execute("SELECT id FROM anc_auto")).fetchall() == [(1,)]


def test_duckdb_sync_driver_returns_native_cursor_and_drops_python_cursor() -> None:
    """`SyncDuckDBDriver.cursor()` hands out the native cursor and the Python cursor class is gone."""
    sync_driver = SyncDuckDBDriver()
    try:
        cursor = sync_driver.cursor()
        try:
            assert type(cursor).__name__ == "SyncDuckDBCursor"
            assert isinstance(cursor, native_duckdb_mod.SyncDuckDBCursor)
            assert cursor.execute("SELECT 7 AS n").fetchall() == [(7,)]
        finally:
            cursor.close()
    finally:
        sync_driver.close()
    assert hasattr(duckdb_sync_mod, "_SyncDuckDBCursor") is False
    assert hasattr(duckdb_driver_mod, "_SyncDuckDBCursor") is False


def test_duckdb_sync_driver_queries_take_no_lease_and_cache_no_statement() -> None:
    """Queries through a broker-configured driver acquire no lease and register no statement plan."""
    broker = native_core_mod.LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    sync_driver = SyncDuckDBDriver(broker=broker)
    try:
        assert sync_driver.execute("CREATE TABLE nolease_items (id BIGINT, name VARCHAR)") == 0
        assert sync_driver.execute("INSERT INTO nolease_items VALUES (?, ?)", (1, "a")) == 1
        assert sync_driver.execute_many("INSERT INTO nolease_items VALUES (?, ?)", [(2, "b"), (3, "c")]) == 2
        assert sync_driver.select("SELECT id, name FROM nolease_items ORDER BY id") == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3, "name": "c"},
        ]
        assert sync_driver.execute_tuples("SELECT count(*) FROM nolease_items") == [(3,)]
        assert broker.stats().total_acquired == 0
        statement_cache = broker.statement_cache()
        assert statement_cache is not None
        assert statement_cache.stats().inserts == 0
    finally:
        sync_driver.close()


def test_duckdb_sync_driver_checks_dynamic_secrets_once_at_open() -> None:
    """The dynamic secret provider runs at open and never on the query success path."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="s_once", secret_type="http", value_provider=provider)
    sync_driver = SyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    try:
        assert provider.calls == 1
        for index in range(10):
            assert sync_driver.select("SELECT ? AS n", (index,)) == [{"n": index}]
        assert provider.calls == 1
        assert provider.invalidated == []
    finally:
        sync_driver.close()


@pytest.mark.parametrize(
    ("action", "expected_error"),
    [
        ("select", ProgrammingError),
        ("execute", ProgrammingError),
        ("execute_many", ProgrammingError),
        ("execute_tuples", ProgrammingError),
    ],
)
def test_duckdb_sync_driver_other_errors_do_not_refresh_secrets(
    action: str,
    expected_error: type[Exception],
) -> None:
    """A syntax error raises the mapped database error without touching the secret provider."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="s_quiet", secret_type="http", value_provider=provider)
    sync_driver = SyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    try:
        with pytest.raises(expected_error) as excinfo:
            if action == "execute_many":
                sync_driver.execute_many("SELEC 1 FROM", [(1,)])
            else:
                getattr(sync_driver, action)("SELEC 1 FROM")
        assert isinstance(excinfo.value, DatabaseError)
        assert hasattr(excinfo.value, "sqlstate")
        assert hasattr(excinfo.value, "vendor_code")
        assert excinfo.value.is_transient is False
        assert provider.calls == 1
        assert provider.invalidated == []
        assert sync_driver.select("SELECT 1 AS ok") == [{"ok": 1}]
    finally:
        sync_driver.close()


@pytest.mark.parametrize(
    ("provider_fails_on_refresh", "expected_calls", "expected_notes"),
    [
        (False, 2, []),
        (True, 2, ["DuckDB secret refresh after credential rejection failed: vault unavailable"]),
    ],
    ids=["refresh_succeeds", "refresh_fails"],
)
def test_duckdb_sync_driver_credential_rejection_refreshes_and_reraises_original_error(
    provider_fails_on_refresh: bool,
    expected_calls: int,
    expected_notes: list[str],
) -> None:
    """An HTTP 403 invalidates and refreshes once, re-raising the original error with a note on refresh failure."""
    recorder = _RecordingSecretProvider()
    fail_state = {"armed": False}

    class _Provider:
        @property
        def generation(self) -> int:
            return recorder.generation

        def invalidate(self, failed_generation: int) -> None:
            recorder.invalidate(failed_generation)

        def __call__(self) -> dict[str, str]:
            if fail_state["armed"]:
                recorder.calls += 1
                msg = "vault unavailable"
                raise RuntimeError(msg)
            return recorder()

    secret = DuckDBSecretConfig(name="s_reject", secret_type="http", value_provider=_Provider())
    rejection = RuntimeError("HTTP Error: forbidden (HTTP 403)")
    proxy = _FaultInjectingSyncClient(SyncDuckDBClient(":memory:"), query_error=rejection)
    sync_driver = SyncDuckDBDriver(DuckDBConfig(secrets=(secret,)), _client=cast("SyncDuckDBClient", proxy))
    try:
        assert recorder.calls == 1
        fail_state["armed"] = provider_fails_on_refresh
        with pytest.raises(RuntimeError) as excinfo:
            sync_driver.select("SELECT * FROM 'https://example.invalid/data.parquet'")
        assert excinfo.value is rejection
        assert getattr(excinfo.value, "__notes__", []) == expected_notes
        assert recorder.invalidated == [1]
        assert recorder.calls == expected_calls

        proxy._query_error = None
        assert sync_driver.select("SELECT 42 AS answer") == [{"answer": 42}]
        assert recorder.calls == expected_calls
    finally:
        sync_driver.close()


def test_duckdb_sync_driver_rich_binds_and_execute_on_select() -> None:
    """Rich Python parameters reach the native cursor unchanged and `execute` on a query returns `0`."""
    params = (
        decimal.Decimal("12.34"),
        datetime.datetime(2024, 5, 6, 7, 8, 9, 123456),
        datetime.date(2024, 5, 6),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        {"k": [1, 2], "n": None},
    )
    insert_sql = "INSERT INTO drv_rich VALUES (?, ?, ?, ?, ?, ?)"
    select_sql = "SELECT d, ts, dt, u, j FROM drv_rich ORDER BY id"
    sync_driver = SyncDuckDBDriver()
    try:
        assert sync_driver.execute("SELECT 1") == 0
        sync_driver.execute(
            "CREATE TABLE drv_rich (id INTEGER, d DECIMAL(10, 2), ts TIMESTAMP, dt DATE, u UUID, j VARCHAR)"
        )
        assert sync_driver.execute(insert_sql, (1, *params)) == 1
        assert sync_driver.execute_many(insert_sql, [(2, *params), (3, *params)]) == 2
        with SyncDuckDBClient(":memory:") as reference:
            reference.execute(
                "CREATE TABLE drv_rich (id INTEGER, d DECIMAL(10, 2), ts TIMESTAMP, dt DATE, u UUID, j VARCHAR)"
            )
            reference.execute(insert_sql, (1, *params))
            expected_row = reference.cursor().execute(select_sql).fetchall()[0]
        rows = sync_driver.execute_tuples(select_sql)
        assert rows == [expected_row] * 3
        assert rows[0][0] == params[0]
        assert rows[0][1].replace(tzinfo=None) == params[1]
        assert rows[0][2] == params[2]
        assert str(rows[0][3]) == str(params[3])
        with sync_driver.select_to_arrow(
            "SELECT id FROM drv_rich WHERE d = ? AND dt = ? ORDER BY id", (params[0], params[2])
        ) as arrow:
            assert arrow.to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]
    finally:
        sync_driver.close()


@pytest.mark.anyio
async def test_duckdb_async_driver_returns_native_cursor_and_drops_removed_helpers() -> None:
    """`AsyncDuckDBDriver.cursor()` returns `AsyncDuckDBCursor` and deleted Python helpers are absent."""
    async_driver = AsyncDuckDBDriver()
    try:
        cursor = async_driver.cursor()
        try:
            assert type(cursor).__name__ == "AsyncDuckDBCursor"
            assert isinstance(cursor, native_duckdb_mod.AsyncDuckDBCursor)
            await cursor.execute("SELECT 9 AS n")
            assert await cursor.fetchall() == [(9,)]
        finally:
            await cursor.close()
        assert await async_driver.execute_with_description("CREATE TABLE async_no_rs (id INTEGER)") == ([], ())
    finally:
        await async_driver.close()
    assert hasattr(duckdb_async_mod, "_AsyncDuckDBCursor") is False
    assert hasattr(duckdb_driver_mod, "_AsyncDuckDBCursor") is False
    assert hasattr(duckdb_core_mod, "_MUTATING_SQL_VERBS") is False
    for removed_name in (
        "is_write_statement",
        "normalize_parameters",
        "param_len",
        "_scan_sql_keywords_outside_literals",
    ):
        assert hasattr(DuckDBCore, removed_name) is False


@pytest.mark.anyio
async def test_duckdb_async_driver_queries_take_no_lease_and_cache_no_statement() -> None:
    """Async queries through a broker-configured driver acquire no lease and register no statement plan."""
    broker = native_core_mod.LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    async_driver = AsyncDuckDBDriver(broker=broker)
    try:
        assert await async_driver.execute("CREATE TABLE async_nolease (id BIGINT, name VARCHAR)") == 0
        assert await async_driver.execute("INSERT INTO async_nolease VALUES (?, ?)", (1, "a")) == 1
        assert await async_driver.execute_many("INSERT INTO async_nolease VALUES (?, ?)", [(2, "b"), (3, "c")]) == 2
        assert await async_driver.select("SELECT id, name FROM async_nolease ORDER BY id") == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3, "name": "c"},
        ]
        assert await async_driver.execute_tuples("SELECT count(*) FROM async_nolease") == [(3,)]
        assert broker.stats().total_acquired == 0
        statement_cache = broker.statement_cache()
        assert statement_cache is not None
        assert statement_cache.stats().inserts == 0
    finally:
        await async_driver.close()


@pytest.mark.anyio
async def test_duckdb_async_driver_checks_dynamic_secrets_once_at_open() -> None:
    """The dynamic secret provider runs once at open and never on the async query success path."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="a_once", secret_type="http", value_provider=provider)
    async_driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    try:
        await async_driver.connect()
        assert provider.calls == 1
        for index in range(10):
            assert await async_driver.select("SELECT ? AS n", (index,)) == [{"n": index}]
        assert provider.calls == 1
        assert provider.invalidated == []
    finally:
        await async_driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("action", "expected_error"),
    [
        ("select", ProgrammingError),
        ("execute", ProgrammingError),
        ("execute_many", ProgrammingError),
        ("execute_tuples", ProgrammingError),
    ],
)
async def test_duckdb_async_driver_other_errors_do_not_refresh_secrets(
    action: str,
    expected_error: type[Exception],
) -> None:
    """An async syntax error raises the mapped database error without touching the secret provider."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="a_quiet", secret_type="http", value_provider=provider)
    async_driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    try:
        await async_driver.connect()
        with pytest.raises(expected_error) as excinfo:
            if action == "execute_many":
                await async_driver.execute_many("SELEC 1 FROM", [(1,)])
            else:
                await getattr(async_driver, action)("SELEC 1 FROM")
        assert isinstance(excinfo.value, DatabaseError)
        assert hasattr(excinfo.value, "sqlstate")
        assert hasattr(excinfo.value, "vendor_code")
        assert excinfo.value.is_transient is False
        assert provider.calls == 1
        assert provider.invalidated == []
        assert await async_driver.select("SELECT 1 AS ok") == [{"ok": 1}]
    finally:
        await async_driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("provider_fails_on_refresh", "expected_calls", "expected_notes"),
    [
        (False, 2, []),
        (True, 2, ["DuckDB secret refresh after credential rejection failed: vault unavailable"]),
    ],
    ids=["refresh_succeeds", "refresh_fails"],
)
async def test_duckdb_async_driver_credential_rejection_refreshes_and_reraises_original_error(
    provider_fails_on_refresh: bool,
    expected_calls: int,
    expected_notes: list[str],
) -> None:
    """An async HTTP 401 invalidates and refreshes once, re-raising the original error with a note on refresh failure."""
    recorder = _RecordingSecretProvider()
    fail_state = {"armed": False}

    class _AsyncProvider:
        @property
        def generation(self) -> int:
            return recorder.generation

        def invalidate(self, failed_generation: int) -> None:
            recorder.invalidate(failed_generation)

        async def __call__(self) -> dict[str, str]:
            if fail_state["armed"]:
                recorder.calls += 1
                msg = "vault unavailable"
                raise RuntimeError(msg)
            return recorder()

    secret = DuckDBSecretConfig(name="a_reject", secret_type="http", value_provider=_AsyncProvider())
    rejection = RuntimeError("HTTP Error: unable to connect (HTTP 401)")
    proxy = _FaultInjectingAsyncClient(AsyncDuckDBClient(":memory:"), query_error=rejection)
    async_driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(secret,)), _client=cast("AsyncDuckDBClient", proxy))
    try:
        await async_driver.connect()
        assert recorder.calls == 1
        fail_state["armed"] = provider_fails_on_refresh
        with pytest.raises(RuntimeError) as excinfo:
            await async_driver.select("SELECT * FROM 'https://example.invalid/data.parquet'")
        assert excinfo.value is rejection
        assert getattr(excinfo.value, "__notes__", []) == expected_notes
        assert recorder.invalidated == [1]
        assert recorder.calls == expected_calls

        proxy._query_error = None
        assert await async_driver.select("SELECT 42 AS answer") == [{"answer": 42}]
        assert recorder.calls == expected_calls
    finally:
        await async_driver.close()


def test_duckdb_sync_single_call_select_dicts_empty_and_no_result() -> None:
    """Single-call read methods match cursor output for selects, empty results, and DDL statements."""
    with SyncDuckDBClient(":memory:") as client:
        select_sql = "SELECT 1 AS a, 'x' AS b"
        cursor = client.cursor()
        cursor.execute(select_sql)
        expected_rows = cursor.fetchall()
        assert cursor.description is not None
        expected_names = tuple(col[0] for col in cursor.description)

        rows, names = client.fetch_rows(select_sql)
        assert (rows, names) == (expected_rows, expected_names)

        dicts = client.fetch_dicts("SELECT i AS id, 'v' || CAST(i AS VARCHAR) AS label FROM range(2) t(i) ORDER BY i")
        assert dicts == [{"id": 0, "label": "v0"}, {"id": 1, "label": "v1"}]

        assert client.fetch_dicts("SELECT 1 AS a, 2 AS a") == [{"a": 2}]

        empty_sql = "SELECT 1 AS a WHERE false"
        assert client.fetch_rows(empty_sql) == ([], ("a",))
        assert client.fetch_dicts(empty_sql) == []
        with pytest.raises(NotFoundError):
            client.fetch_value(empty_sql)

        assert client.fetch_rows("CREATE TABLE sc_t1 (id INTEGER)") == ([], ())
        assert client.fetch_dicts("CREATE TABLE sc_t2 (id INTEGER)") == []
        with pytest.raises(NotFoundError):
            client.fetch_value("CREATE TABLE sc_t3 (id INTEGER)")
        assert client.fetch_value("SELECT count(*) FROM sc_t1") == 0
        assert client.fetch_value("SELECT count(*) FROM sc_t2") == 0
        assert client.fetch_value("SELECT count(*) FROM sc_t3") == 0

        assert client.fetch_value("SELECT 42") == 42
        assert client.fetch_value("SELECT NULL") is None


def test_duckdb_sync_single_call_dml_returning_and_no_transaction() -> None:
    """`execute_dml` and `fetch_rows` handle DML, RETURNING clauses, and leave no open transaction."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE sc_dml (id INTEGER)")
        assert client.execute_dml("INSERT INTO sc_dml VALUES (1), (2)") == (2, None)
        assert client.in_transaction is False
        assert client.execute_dml("UPDATE sc_dml SET id = 99 WHERE id = 999") == (0, None)
        assert client.execute_dml("SELECT id FROM sc_dml") == (0, None)

        assert client.fetch_rows("INSERT INTO sc_dml VALUES (9) RETURNING id") == ([(9,)], ("id",))
        assert client.execute_dml("INSERT INTO sc_dml VALUES (10) RETURNING id") == (1, None)
        assert client.in_transaction is False


def test_duckdb_sync_single_call_rich_values_match_cursor() -> None:
    """Rich Python parameter values bound via `fetch_rows` match the cursor output."""
    params = (
        decimal.Decimal("12.34"),
        datetime.datetime(2024, 5, 6, 7, 8, 9, 123456),
        datetime.date(2024, 5, 6),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        {"k": [1, 2], "n": None},
    )
    insert_sql = "INSERT INTO sc_rich VALUES (?, ?, ?, ?, ?, ?)"
    select_sql = "SELECT d, ts, dt, u, j FROM sc_rich ORDER BY id"
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE sc_rich (id INTEGER, d DECIMAL(10, 2), ts TIMESTAMP, dt DATE, u UUID, j VARCHAR)")
        client.cursor().execute(insert_sql, (1, *params))
        assert client.execute_dml(insert_sql, (2, *params)) == (1, None)
        cursor_rows = client.cursor().execute(select_sql).fetchall()
        rows, names = client.fetch_rows(select_sql)
        assert names == ("d", "ts", "dt", "u", "j")
        assert rows == cursor_rows
        assert rows[0] == rows[1]

        single_rows, _ = client.fetch_rows(
            "SELECT ?::DECIMAL(10, 2), ?::TIMESTAMP, ?::DATE, ?::UUID, ?::VARCHAR",
            params,
        )
        direct_cursor_rows = (
            client.cursor()
            .execute(
                "SELECT ?::DECIMAL(10, 2), ?::TIMESTAMP, ?::DATE, ?::UUID, ?::VARCHAR",
                params,
            )
            .fetchall()
        )
        assert single_rows == direct_cursor_rows


@pytest.mark.parametrize(
    ("sql", "params"),
    [
        ("SELECT ? AS v", (7,)),
        ("SELECT ? AS v", [7]),
        ("SELECT $1 AS v", (7,)),
        ("SELECT $1 AS v", [7]),
        ("SELECT $v AS v", {"v": 7}),
        ("SELECT :v AS v", {"v": 7}),
        ("SELECT $v AS v", MappingProxyType({"v": 7})),
        ("SELECT :v AS v", MappingProxyType({"v": 7})),
    ],
    ids=[
        "qmark_tuple",
        "qmark_list",
        "dollar_pos_tuple",
        "dollar_pos_list",
        "dollar_named_dict",
        "colon_named_dict",
        "dollar_named_proxy",
        "colon_named_proxy",
    ],
)
def test_duckdb_sync_single_call_parameter_styles(sql: str, params: Any) -> None:
    """Positional sequences, dicts, and non-dict mappings match cursor results and bind by name."""
    with SyncDuckDBClient(":memory:") as client:
        cursor_rows = client.cursor().execute(sql, params).fetchall()
        rows, names = client.fetch_rows(sql, params)
        assert rows == cursor_rows == [(7,)]
        assert names == ("v",)


@pytest.mark.parametrize(
    "empty_params",
    [(), [], {}, None],
    ids=["empty_tuple", "empty_list", "empty_dict", "none"],
)
def test_duckdb_sync_single_call_empty_parameters(empty_params: Any) -> None:
    """Empty parameter containers and `None` behave identically to omitting parameters."""
    with SyncDuckDBClient(":memory:") as client:
        client.execute("CREATE TABLE sc_empty (id INTEGER)")
        assert client.fetch_rows("SELECT 1 AS a", empty_params) == client.fetch_rows("SELECT 1 AS a")
        assert client.fetch_dicts("SELECT 1 AS a", empty_params) == client.fetch_dicts("SELECT 1 AS a")
        assert client.fetch_value("SELECT 1 AS a", empty_params) == client.fetch_value("SELECT 1 AS a")
        assert client.execute_dml("INSERT INTO sc_empty VALUES (1)", empty_params) == (1, None)


@pytest.mark.parametrize(
    ("setup_sql", "bad_sql"),
    [
        ("CREATE TABLE sc_pk (id INTEGER PRIMARY KEY); INSERT INTO sc_pk VALUES (1)", "INSERT INTO sc_pk VALUES (1)"),
        (None, "SELEC 1"),
        (None, "SELECT * FROM sc_missing_table"),
    ],
    ids=["duplicate_key", "syntax_error", "missing_table"],
)
def test_duckdb_sync_single_call_error_classes_match_cursor(setup_sql: str | None, bad_sql: str) -> None:
    """Single-call methods raise the same error class and `sqlstate` as the cursor."""
    with SyncDuckDBClient(":memory:") as client:
        if setup_sql is not None:
            for stmt in setup_sql.split(";"):
                client.execute(stmt.strip())

        with pytest.raises(DatabaseError) as cursor_exc:
            client.cursor().execute(bad_sql)

        for call in (
            lambda: client.fetch_rows(bad_sql),
            lambda: client.fetch_dicts(bad_sql),
            lambda: client.fetch_value(bad_sql),
            lambda: client.execute_dml(bad_sql),
        ):
            with pytest.raises(type(cursor_exc.value)) as single_exc:
                call()
            assert single_exc.value.sqlstate == cursor_exc.value.sqlstate


def test_duckdb_sync_single_call_closed_client_matches_cursor() -> None:
    """All four single-call methods on a closed client raise the same error class as the cursor."""
    client = SyncDuckDBClient(":memory:")
    cursor = client.cursor()
    client.close()

    with pytest.raises(DatabaseError) as cursor_exc:
        cursor.execute("SELECT 1")

    for call in (
        lambda: client.fetch_rows("SELECT 1"),
        lambda: client.fetch_dicts("SELECT 1"),
        lambda: client.fetch_value("SELECT 1"),
        lambda: client.execute_dml("SELECT 1"),
    ):
        with pytest.raises(type(cursor_exc.value)):
            call()


@pytest.mark.anyio
async def test_duckdb_async_single_call_results_match_sync() -> None:
    """All four async single-call methods return the same values as the sync client across reads, DDL, DML, and RETURNING."""
    with SyncDuckDBClient(":memory:") as sync_client:
        async with AsyncDuckDBClient(":memory:") as async_client:
            select_sql = "SELECT 1 AS a, 'x' AS b"
            assert await async_client.fetch_rows(select_sql) == sync_client.fetch_rows(select_sql)

            dicts_sql = "SELECT i AS id, 'v' || CAST(i AS VARCHAR) AS label FROM range(2) t(i) ORDER BY i"
            assert await async_client.fetch_dicts(dicts_sql) == sync_client.fetch_dicts(dicts_sql)

            dup_sql = "SELECT 1 AS a, 2 AS a"
            assert await async_client.fetch_dicts(dup_sql) == sync_client.fetch_dicts(dup_sql)

            empty_sql = "SELECT 1 AS a WHERE false"
            assert await async_client.fetch_rows(empty_sql) == sync_client.fetch_rows(empty_sql)
            assert await async_client.fetch_dicts(empty_sql) == sync_client.fetch_dicts(empty_sql)

            assert await async_client.fetch_rows("CREATE TABLE asc_t1 (id INTEGER)") == sync_client.fetch_rows(
                "CREATE TABLE asc_t1 (id INTEGER)"
            )
            assert await async_client.fetch_dicts("CREATE TABLE asc_t2 (id INTEGER)") == sync_client.fetch_dicts(
                "CREATE TABLE asc_t2 (id INTEGER)"
            )
            assert await async_client.fetch_value("SELECT count(*) FROM asc_t1") == 0
            assert await async_client.fetch_value("SELECT count(*) FROM asc_t2") == 0

            assert await async_client.fetch_value("SELECT 42") == sync_client.fetch_value("SELECT 42")
            assert await async_client.fetch_value("SELECT NULL") is sync_client.fetch_value("SELECT NULL") is None

            await async_client.execute("CREATE TABLE asc_dml (id INTEGER)")
            sync_client.execute("CREATE TABLE asc_dml (id INTEGER)")
            assert await async_client.execute_dml("INSERT INTO asc_dml VALUES (1), (2)") == sync_client.execute_dml(
                "INSERT INTO asc_dml VALUES (1), (2)"
            )
            assert async_client.in_transaction is False
            assert await async_client.execute_dml(
                "UPDATE asc_dml SET id = 99 WHERE id = 999"
            ) == sync_client.execute_dml("UPDATE asc_dml SET id = 99 WHERE id = 999")
            assert await async_client.execute_dml("SELECT id FROM asc_dml") == sync_client.execute_dml(
                "SELECT id FROM asc_dml"
            )

            assert await async_client.fetch_rows(
                "INSERT INTO asc_dml VALUES (9) RETURNING id"
            ) == sync_client.fetch_rows("INSERT INTO asc_dml VALUES (9) RETURNING id")
            assert await async_client.execute_dml(
                "INSERT INTO asc_dml VALUES (10) RETURNING id"
            ) == sync_client.execute_dml("INSERT INTO asc_dml VALUES (10) RETURNING id")
            assert async_client.in_transaction is False


@pytest.mark.anyio
async def test_duckdb_async_single_call_one_awaitable() -> None:
    """A single `fetch_rows` call returns one awaitable resolving directly to `(rows, names)`."""
    async with AsyncDuckDBClient(":memory:") as client:
        aw = client.fetch_rows("SELECT 1 AS a")
        assert await aw == ([(1,)], ("a",))


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method",
    ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"],
)
async def test_duckdb_async_single_call_bind_error_raises_at_call(method: str) -> None:
    """Parameter conversion errors raise `InterfaceError` synchronously at the call, before any await."""
    async with AsyncDuckDBClient(":memory:") as client:
        with pytest.raises(InterfaceError):
            getattr(client, method)("SELECT ?", 5)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method",
    ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"],
)
async def test_duckdb_async_single_call_engine_error_raises_on_await(method: str) -> None:
    """An engine error does not raise when building the awaitable and raises the cursor's error class on await."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        with pytest.raises(DatabaseError) as cursor_exc:
            await cursor.execute("SELEC 1")

        aw = getattr(client, method)("SELEC 1")
        with pytest.raises(type(cursor_exc.value)) as single_exc:
            await aw
        assert single_exc.value.sqlstate == cursor_exc.value.sqlstate


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sql",
    ["SELECT 1 WHERE false", "CREATE TABLE asc_nf (id INTEGER)"],
    ids=["empty_select", "ddl_no_result"],
)
async def test_duckdb_async_single_call_fetch_value_not_found_raises_on_await(sql: str) -> None:
    """`fetch_value` raises `NotFoundError` on await when the statement produces no rows."""
    async with AsyncDuckDBClient(":memory:") as client:
        aw = client.fetch_value(sql)
        with pytest.raises(NotFoundError):
            await aw


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "args", "expected"),
    [
        ("fetchone", (), (0,)),
        ("fetchmany", (2,), [(0,), (1,)]),
        ("fetchall", (), [(0,), (1,), (2,)]),
        ("close", (), None),
    ],
    ids=["fetchone", "fetchmany", "fetchall", "close"],
)
async def test_duckdb_async_cursor_ready_fetch_does_not_iterate_loop(
    method: str,
    args: tuple[Any, ...],
    expected: Any,
) -> None:
    """Awaiting `fetchone`, `fetchmany`, `fetchall`, or `close` completes inline without running queued loop callbacks."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        await cursor.execute("SELECT i FROM range(3) t(i) ORDER BY i")

        loop = asyncio.get_running_loop()
        ran: list[int] = []
        loop.call_soon(ran.append, 1)

        result = await getattr(cursor, method)(*args)
        assert result == expected
        assert ran == []

        await asyncio.sleep(0)
        assert ran == [1]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "args", "expected"),
    [
        ("fetchone", (), (0,)),
        ("fetchmany", (2,), [(0,), (1,)]),
        ("fetchall", (), [(0,), (1,), (2,)]),
        ("close", (), None),
    ],
    ids=["fetchone", "fetchmany", "fetchall", "close"],
)
async def test_duckdb_async_cursor_ready_awaitable_type_and_reawait(
    method: str,
    args: tuple[Any, ...],
    expected: Any,
) -> None:
    """Cursor fetch and close methods return a local `ReadyAwaitable` that resolves to the same object on repeated awaits."""
    async with AsyncDuckDBClient(":memory:") as client:
        cursor = client.cursor()
        await cursor.execute("SELECT i FROM range(3) t(i) ORDER BY i")

        aw = getattr(cursor, method)(*args)
        assert type(aw).__name__ == "ReadyAwaitable"
        assert not isinstance(aw, native_core_mod.ReadyAwaitable)

        first = await aw
        second = await aw
        assert first == expected
        assert second is first


class _SingleCallRow(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Typed row schema for single-call DuckDB driver hydration tests."""

    id: int
    name: str


def test_duckdb_sync_driver_single_call_counts_and_schema_hydration() -> None:
    """Every sync query method invokes exactly one native single-call client method."""
    counter = _CountingClient(SyncDuckDBClient(":memory:"))
    driver = SyncDuckDBDriver(_client=cast("SyncDuckDBClient", counter))
    try:
        counter.calls.clear()
        assert driver.execute("CREATE TABLE sc_drv (id INTEGER, name VARCHAR)") == 0
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert driver.execute_dml("INSERT INTO sc_drv VALUES (?, ?), (?, ?)", (1, "a", 2, "b")) == (2, None)
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert driver.select("SELECT id, name FROM sc_drv ORDER BY id") == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
        ]
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select_one("SELECT id, name FROM sc_drv WHERE id = ?", (1,)) == {"id": 1, "name": "a"}
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select_one_or_none("SELECT id, name FROM sc_drv WHERE id = ?", (2,)) == {"id": 2, "name": "b"}
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert driver.select_value("SELECT count(*) FROM sc_drv") == 2
        assert counter.calls == ["fetch_value"]

        counter.calls.clear()
        assert driver.execute_with_description("SELECT id, name FROM sc_drv ORDER BY id") == (
            [(1, "a"), (2, "b")],
            ("id", "name"),
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert driver.execute_tuples("SELECT id, name FROM sc_drv ORDER BY id") == [(1, "a"), (2, "b")]
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        typed_rows = driver.select("SELECT id, name FROM sc_drv ORDER BY id", schema_type=_SingleCallRow)
        assert typed_rows == [_SingleCallRow(id=1, name="a"), _SingleCallRow(id=2, name="b")]
        assert all(isinstance(row, _SingleCallRow) for row in typed_rows)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        typed_one = driver.select_one("SELECT id, name FROM sc_drv WHERE id = ?", (1,), schema_type=_SingleCallRow)
        assert typed_one == _SingleCallRow(id=1, name="a")
        assert isinstance(typed_one, _SingleCallRow)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        typed_opt = driver.select_one_or_none(
            "SELECT id, name FROM sc_drv WHERE id = ?",
            (2,),
            schema_type=_SingleCallRow,
        )
        assert typed_opt == _SingleCallRow(id=2, name="b")
        assert isinstance(typed_opt, _SingleCallRow)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert (
            driver.select_one_or_none(
                "SELECT id, name FROM sc_drv WHERE id = ?",
                (99,),
                schema_type=_SingleCallRow,
            )
            is None
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        with pytest.raises(NotFoundError):
            driver.select_one("SELECT id, name FROM sc_drv WHERE id = ?", (99,), schema_type=_SingleCallRow)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert driver.select_with_total("SELECT id, name FROM sc_drv ORDER BY id") == (
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            2,
        )
        assert counter.calls == ["fetch_dicts"]
    finally:
        driver.close()


@pytest.mark.anyio
async def test_duckdb_async_driver_single_call_counts_and_schema_hydration() -> None:
    """Every async query method invokes exactly one native single-call client method."""
    counter = _CountingClient(AsyncDuckDBClient(":memory:"))
    driver = AsyncDuckDBDriver(_client=cast("AsyncDuckDBClient", counter))
    try:
        counter.calls.clear()
        assert await driver.execute("CREATE TABLE sc_async_drv (id INTEGER, name VARCHAR)") == 0
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert await driver.execute_dml("INSERT INTO sc_async_drv VALUES (?, ?), (?, ?)", (1, "a", 2, "b")) == (2, None)
        assert counter.calls == ["execute_dml"]

        counter.calls.clear()
        assert await driver.select("SELECT id, name FROM sc_async_drv ORDER BY id") == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
        ]
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select_one("SELECT id, name FROM sc_async_drv WHERE id = ?", (1,)) == {"id": 1, "name": "a"}
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select_one_or_none("SELECT id, name FROM sc_async_drv WHERE id = ?", (2,)) == {
            "id": 2,
            "name": "b",
        }
        assert counter.calls == ["fetch_dicts"]

        counter.calls.clear()
        assert await driver.select_value("SELECT count(*) FROM sc_async_drv") == 2
        assert counter.calls == ["fetch_value"]

        counter.calls.clear()
        assert await driver.execute_with_description("SELECT id, name FROM sc_async_drv ORDER BY id") == (
            [(1, "a"), (2, "b")],
            ("id", "name"),
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert await driver.execute_tuples("SELECT id, name FROM sc_async_drv ORDER BY id") == [(1, "a"), (2, "b")]
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        typed_rows = await driver.select("SELECT id, name FROM sc_async_drv ORDER BY id", schema_type=_SingleCallRow)
        assert typed_rows == [_SingleCallRow(id=1, name="a"), _SingleCallRow(id=2, name="b")]
        assert all(isinstance(row, _SingleCallRow) for row in typed_rows)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        typed_one = await driver.select_one(
            "SELECT id, name FROM sc_async_drv WHERE id = ?",
            (1,),
            schema_type=_SingleCallRow,
        )
        assert typed_one == _SingleCallRow(id=1, name="a")
        assert isinstance(typed_one, _SingleCallRow)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        typed_opt = await driver.select_one_or_none(
            "SELECT id, name FROM sc_async_drv WHERE id = ?",
            (2,),
            schema_type=_SingleCallRow,
        )
        assert typed_opt == _SingleCallRow(id=2, name="b")
        assert isinstance(typed_opt, _SingleCallRow)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert (
            await driver.select_one_or_none(
                "SELECT id, name FROM sc_async_drv WHERE id = ?",
                (99,),
                schema_type=_SingleCallRow,
            )
            is None
        )
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        with pytest.raises(NotFoundError):
            await driver.select_one("SELECT id, name FROM sc_async_drv WHERE id = ?", (99,), schema_type=_SingleCallRow)
        assert counter.calls == ["fetch_rows"]

        counter.calls.clear()
        assert await driver.select_with_total("SELECT id, name FROM sc_async_drv ORDER BY id") == (
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            2,
        )
        assert counter.calls == ["fetch_dicts"]
    finally:
        await driver.close()


@pytest.mark.parametrize(
    "method_name",
    [
        "select",
        "select_one",
        "select_one_or_none",
        "select_value",
        "execute_with_description",
        "execute_tuples",
        "execute",
        "execute_dml",
    ],
)
def test_duckdb_sync_driver_rejection_refreshes_across_all_single_call_methods(method_name: str) -> None:
    """Every sync single-call method invalidates and refreshes dynamic secrets once on HTTP 403 and re-raises the original error."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="s_each", secret_type="http", value_provider=provider)
    rejection = RuntimeError("HTTP Error: forbidden (HTTP 403)")
    proxy = _FaultInjectingSyncClient(SyncDuckDBClient(":memory:"), query_error=rejection)
    driver = SyncDuckDBDriver(DuckDBConfig(secrets=(secret,)), _client=cast("SyncDuckDBClient", proxy))
    try:
        assert provider.calls == 1
        with pytest.raises(RuntimeError) as excinfo:
            getattr(driver, method_name)("SELECT 1 AS ok")
        assert excinfo.value is rejection
        assert provider.invalidated == [1]
        assert provider.calls == 2
        assert provider.generation == 2
    finally:
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method_name",
    [
        "select",
        "select_one",
        "select_one_or_none",
        "select_value",
        "execute_with_description",
        "execute_tuples",
        "execute",
        "execute_dml",
    ],
)
async def test_duckdb_async_driver_rejection_refreshes_across_all_single_call_methods(method_name: str) -> None:
    """Every async single-call method invalidates and refreshes dynamic secrets once on HTTP 403 and re-raises the original error."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="a_each", secret_type="http", value_provider=provider)
    rejection = RuntimeError("HTTP Error: forbidden (HTTP 403)")
    proxy = _FaultInjectingAsyncClient(AsyncDuckDBClient(":memory:"), query_error=rejection)
    driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(secret,)), _client=cast("AsyncDuckDBClient", proxy))
    try:
        await driver.connect()
        assert provider.calls == 1
        with pytest.raises(RuntimeError) as excinfo:
            await getattr(driver, method_name)("SELECT 1 AS ok")
        assert excinfo.value is rejection
        assert provider.invalidated == [1]
        assert provider.calls == 2
        assert provider.generation == 2
    finally:
        await driver.close()


@pytest.mark.parametrize(
    "method_name",
    [
        "select",
        "select_one",
        "select_one_or_none",
        "select_value",
        "execute_with_description",
        "execute_tuples",
        "execute",
        "execute_dml",
    ],
)
def test_duckdb_sync_driver_closed_raises_without_refreshing_secrets(method_name: str) -> None:
    """After `close()`, every sync query method raises `InterfaceError` without touching secret providers."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="s_closed", secret_type="http", value_provider=provider)
    driver = SyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    assert provider.calls == 1
    driver.close()
    driver.connect()
    with pytest.raises(InterfaceError, match="DuckDB driver is closed"):
        getattr(driver, method_name)("SELECT 1 AS ok")
    assert provider.calls == 1
    assert provider.invalidated == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method_name",
    [
        "select",
        "select_one",
        "select_one_or_none",
        "select_value",
        "execute_with_description",
        "execute_tuples",
        "execute",
        "execute_dml",
    ],
)
async def test_duckdb_async_driver_closed_raises_without_refreshing_secrets(method_name: str) -> None:
    """After `close()`, every async query method raises `InterfaceError` without touching secret providers."""
    provider = _RecordingSecretProvider()
    secret = DuckDBSecretConfig(name="a_closed", secret_type="http", value_provider=provider)
    driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(secret,)))
    await driver.connect()
    assert provider.calls == 1
    await driver.close()
    with pytest.raises(InterfaceError, match="DuckDB driver is closed"):
        await getattr(driver, method_name)("SELECT 1 AS ok")
    assert provider.calls == 1
    assert provider.invalidated == []


@pytest.mark.anyio
async def test_duckdb_drivers_single_call_results_and_no_result_set_semantics() -> None:
    """Verify DDL, `execute_dml`, `execute` on select, `select_value`, `schema_type=dict`, and no-result statements."""
    sync_driver = SyncDuckDBDriver()
    try:
        assert sync_driver.execute("CREATE TABLE sc_res (id INTEGER, label VARCHAR)") == 0
        assert sync_driver.execute_dml("INSERT INTO sc_res VALUES (?, ?), (?, ?)", (1, "x", 2, "y")) == (2, None)
        assert sync_driver.execute("SELECT id FROM sc_res") == 0
        assert sync_driver.select_value("SELECT count(*) FROM sc_res") == 2
        assert sync_driver.select_value("SELECT count(*) FROM sc_res", value_type=str) == "2"
        dict_rows = sync_driver.select("SELECT id, label FROM sc_res ORDER BY id", schema_type=dict)
        assert dict_rows == [{"id": 1, "label": "x"}, {"id": 2, "label": "y"}]
        assert all(type(row) is dict for row in dict_rows)
        assert sync_driver.select("CREATE TABLE sc_no_rs1 (id INTEGER)") == []
        assert sync_driver.execute_with_description("CREATE TABLE sc_no_rs2 (id INTEGER)") == ([], ())
        with pytest.raises(NotFoundError):
            sync_driver.select_one("CREATE TABLE sc_no_rs3 (id INTEGER)")
        with pytest.raises(NotFoundError):
            sync_driver.select_value("CREATE TABLE sc_no_rs4 (id INTEGER)")
        assert sync_driver.select_one_or_none("CREATE TABLE sc_no_rs5 (id INTEGER)") is None
    finally:
        sync_driver.close()

    async_driver = AsyncDuckDBDriver()
    try:
        assert await async_driver.execute("CREATE TABLE sc_ares (id INTEGER, label VARCHAR)") == 0
        assert await async_driver.execute_dml("INSERT INTO sc_ares VALUES (?, ?), (?, ?)", (1, "x", 2, "y")) == (
            2,
            None,
        )
        assert await async_driver.execute("SELECT id FROM sc_ares") == 0
        assert await async_driver.select_value("SELECT count(*) FROM sc_ares") == 2
        assert await async_driver.select_value("SELECT count(*) FROM sc_ares", value_type=str) == "2"
        adict_rows = await async_driver.select("SELECT id, label FROM sc_ares ORDER BY id", schema_type=dict)
        assert adict_rows == [{"id": 1, "label": "x"}, {"id": 2, "label": "y"}]
        assert all(type(row) is dict for row in adict_rows)
        assert await async_driver.select("CREATE TABLE sc_ano_rs1 (id INTEGER)") == []
        assert await async_driver.execute_with_description("CREATE TABLE sc_ano_rs2 (id INTEGER)") == ([], ())
        with pytest.raises(NotFoundError):
            await async_driver.select_one("CREATE TABLE sc_ano_rs3 (id INTEGER)")
        with pytest.raises(NotFoundError):
            await async_driver.select_value("CREATE TABLE sc_ano_rs4 (id INTEGER)")
        assert await async_driver.select_one_or_none("CREATE TABLE sc_ano_rs5 (id INTEGER)") is None
    finally:
        await async_driver.close()


@pytest.mark.anyio
async def test_duckdb_async_driver_failed_startup_binds_closed_callables() -> None:
    """After a failed initial required secret refresh, every async single-call query raises `InterfaceError`."""

    def failing_provider() -> dict[str, str]:
        msg = "vault unavailable"
        raise RuntimeError(msg)

    cfg = DuckDBConfig(
        secrets=(DuckDBSecretConfig(name="broken_single", secret_type="http", value_provider=failing_provider),)
    )
    proxy = _FaultInjectingAsyncClient(AsyncDuckDBClient(":memory:"))
    driver = AsyncDuckDBDriver(cfg, _client=cast("AsyncDuckDBClient", proxy))
    with pytest.raises(RuntimeError, match="vault unavailable"):
        await driver.select("SELECT 1")
    assert driver._client is None
    assert proxy.close_calls == 1
    with pytest.raises(InterfaceError, match="DuckDB driver is closed"):
        await driver.select("SELECT 1")
    with pytest.raises(InterfaceError, match="DuckDB driver is closed"):
        await driver.execute_dml("SELECT 1")
