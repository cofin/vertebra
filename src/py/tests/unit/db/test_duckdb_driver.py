"""Unit tests for the native DuckDB 2.0 driver (`vertebra._duckdb`), savepoints, pipelines, and Arrow SHM."""

from __future__ import annotations

import ast
import asyncio
import math
from pathlib import Path
from typing import Any, cast

import msgspec
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest

import vertebra._duckdb as native_duckdb_mod
import vertebra.db.adapters.duckdb.config as duckdb_config_mod
import vertebra.db.adapters.duckdb.core as duckdb_core_mod
import vertebra.db.adapters.duckdb.driver as duckdb_driver_mod
import vertebra.db.adapters.duckdb.pipeline as duckdb_pipeline_mod
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
    ],
    ids=["config", "core", "driver", "pipeline"],
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

        with sync_driver.cursor() as cur:
            cur.execute("SELECT id, label FROM empty_meta")
            assert cur.fetchall() == []
            assert cur.description is not None
            assert tuple(col[0] for col in cur.description) == ("id", "label")

        await async_driver.execute("CREATE TABLE empty_meta (id BIGINT, label VARCHAR, score DOUBLE)")
        arows, acols = await async_driver.execute_with_description(
            "SELECT id, label, score FROM empty_meta WHERE id > ?",
            (100,),
        )
        assert arows == []
        assert acols == ("id", "label", "score")
        assert await async_driver.execute_tuples("SELECT id, label FROM empty_meta") == []

        async with async_driver.cursor() as acur:
            await acur.execute("SELECT id, label FROM empty_meta")
            assert await acur.fetchall() == []
            assert acur.description is not None
            assert tuple(col[0] for col in acur.description) == ("id", "label")
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
async def test_duckdb_arrow_c_stream_ipc_bytes_and_dynamic_secret_provider_rotation() -> None:
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
    async_driver = AsyncDuckDBDriver(DuckDBConfig(secrets=(async_secret,)))
    try:
        await async_driver.select("SELECT 1 AS ok")
        assert async_calls == 1
        await asyncio.sleep(0.005)
        await async_driver.select("SELECT 2 AS ok")
        assert async_calls == 2
    finally:
        await async_driver.close()


class _PipeRow(msgspec.Struct, frozen=True):
    """Typed row struct for verifying pipeline `schema_type` hydration."""

    id: int
    label: str


@pytest.mark.anyio
async def test_duckdb_secret_escaping_core_write_detection_notfound_and_pipeline_schema() -> None:
    """Verify secret SQL escaping, comment/CTE write classification, native NotFoundError, and pipeline schema_type."""
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

    assert DuckDBCore.is_write_statement("/* header */ INSERT INTO t VALUES (1)") is True
    assert DuckDBCore.is_write_statement("-- lead\nWITH c AS (SELECT 1) DELETE FROM t") is True
    assert DuckDBCore.is_write_statement("WITH c AS (SELECT 'DELETE' AS verb) SELECT * FROM c") is False

    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        assert not hasattr(SyncDuckDBDriver(), "__dict__")
        assert not hasattr(async_driver, "__dict__")
        assert not hasattr(async_driver, "sync_driver")

        sync_driver.execute("CREATE TABLE typed_rows (id INTEGER, label VARCHAR, payload VARCHAR)")
        sync_driver.execute("INSERT INTO typed_rows VALUES (?, ?, ?)", (1, "alpha", (10, 20)))

        with pytest.raises(NotFoundError):
            sync_driver._client.fetch_one("SELECT id FROM typed_rows WHERE id = 999")
        with pytest.raises(NotFoundError):
            sync_driver._client.select_value("SELECT id FROM typed_rows WHERE id = 999")
        with pytest.raises(NotFoundError):
            await async_driver._client.fetch_one("SELECT 1 WHERE 1 = 0")
        with pytest.raises(NotFoundError):
            await async_driver._client.select_value("SELECT 1 WHERE 1 = 0")

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

    def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        if self._query_error is not None:
            raise self._query_error
        rows, cols = self._inner.execute_with_description(sql, params)
        return list(rows), tuple(str(col) for col in cols)

    def close(self) -> None:
        self.close_calls += 1
        self._inner.close()


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

    async def execute_with_description(
        self, sql: str, params: Any = None
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        if self._query_error is not None:
            raise self._query_error
        rows, cols = await self._inner.execute_with_description(sql, params)
        return list(rows), tuple(str(col) for col in cols)

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
        assert async_provider.calls == 1
        if expected_invalidation:
            assert async_provider.invalidated == [1]
        else:
            assert async_provider.invalidated == []

        async_proxy._query_error = None
        assert await async_driver.select("SELECT 7 AS answer") == [{"answer": 7}]
        assert async_provider.calls == (2 if expected_invalidation else 1)
        assert async_provider.generation == (2 if expected_invalidation else 1)
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
