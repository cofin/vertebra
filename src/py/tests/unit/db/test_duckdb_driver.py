"""Unit tests for the native DuckDB 2.0 driver (`vertebra._duckdb`), savepoints, pipelines, and Arrow SHM."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

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
from vertebra.storage import SyncGcpTokenCache


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
        ({"config": {"custom\0key": "1"}}, "NUL"),
        ({"config": {"threads": "1\0"}}, "NUL"),
        ({"init_statements": ("SELECT 1\0",)}, "NUL"),
        ({"pool_max_size": 0}, "pool_max_size"),
        ({"pool_max_size": -2}, "pool_max_size"),
    ],
    ids=[
        "nul_path",
        "nul_database",
        "nul_temp_dir",
        "nul_config_key",
        "nul_config_val",
        "nul_init_stmt",
        "zero_pool_max",
        "negative_pool_max",
    ],
)
def test_duckdb_config_and_driver_reject_invalid_inputs_pure_in_memory(
    bad_kwargs: dict[str, Any],
    match_pattern: str,
) -> None:
    """`DuckDBConfig`, `SyncDuckDBDriver`, and `AsyncDuckDBDriver` reject NUL bytes and non-positive pool sizes."""
    cfg = DuckDBConfig(**bad_kwargs)
    with pytest.raises(InterfaceError, match=match_pattern):
        if "pool_max_size" in bad_kwargs:
            SyncDuckDBDriver(cfg)
        else:
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
        secret_type="S3",
        key_id="AKIAIOSFODNN7EXAMPLE",
        secret="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    rendered_repr = repr(secret)
    assert "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" not in rendered_repr
    assert "<redacted>" in rendered_repr

    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(name="bad\0name", secret="s").to_create_secret_sql()

    client = SyncDuckDBClient(":memory:")
    try:
        client.apply_gcs_secret(
            secret_name="vtb_secret_redaction",
            token="super-secret-bearer-value-99",
            generation=1,
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
async def test_duckdb_arrow_c_stream_ipc_bytes_and_capsule_token_rotation() -> None:
    """`select_to_arrow` exposes `__arrow_c_stream__`, `load_from_arrow` accepts raw IPC bytes, and capsule tokens rotate."""
    sync_driver = SyncDuckDBDriver()
    try:
        sync_driver.execute("CREATE TABLE arrow_items (id BIGINT, name VARCHAR)")
        table = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        sink = pa.BufferOutputStream()
        with pa_ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        raw_ipc = sink.getvalue().to_pybytes()

        inserted = sync_driver.load_from_arrow("arrow_items", raw_ipc)
        assert inserted == 3

        with sync_driver.select_to_arrow("SELECT id, name FROM arrow_items ORDER BY id") as cursor:
            assert isinstance(cursor, ArrowCursor)
            assert cursor.used_shm is True
            assert hasattr(cursor, "__arrow_c_stream__")
            imported = pa.Table.from_batches(pa.RecordBatchReader._import_from_c_capsule(cursor.__arrow_c_stream__()))
            assert imported.to_pylist() == [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 3, "name": "c"},
            ]

        cache = SyncGcpTokenCache(
            {"access_token": "capsule-bearer-token-1", "quota_project_id": "proj-1"},
            allow_ambient_adc=False,
        )
        cache.get_token()
        capsule = cache._capsule()
        rotated, applied_gen = sync_driver._client.apply_gcs_secret(
            secret_name="vtb_capsule_secret",
            token_cache=capsule,
            scope="gs://analytics-bucket/",
        )
        assert rotated is True
        assert applied_gen == 1
        assert sync_driver.applied_token_generation == 1
        assert "SCOPE 'gs://analytics-bucket/'" in sync_driver.last_gcs_secret_sql

        ddl = DuckDBCore.build_gcs_secret_sql(
            "vtb_core_secret",
            bearer_token="tok'1",
            scope="gs://bucket/prefix",
        )
        assert ddl == (
            "CREATE OR REPLACE SECRET vtb_core_secret (TYPE gcs, BEARER_TOKEN 'tok''1', SCOPE 'gs://bucket/prefix')"
        )
    finally:
        sync_driver.close()
