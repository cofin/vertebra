"""Native DuckDB analytical driver integration suite covering VTBARROW SHM, pipelines, savepoints, and GCS bearer rotation."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from vertebra.db.adapters.duckdb import (
    AsyncDuckDBDriver,
    DuckDBConfig,
    DuckDBSecretConfig,
    SyncDuckDBDriver,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import DatabaseError
from vertebra.storage import (
    AsyncGcsBearerSecretProvider,
    GCSConfig,
    SyncGcsBearerSecretProvider,
)
from vertebra.utils.module_loader import ensure_module

_HTTPFS_ENABLED = os.environ.get("VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS") == "1"
_HTTPFS_MARKS = [pytest.mark.driver_engine("duckdb")] if _HTTPFS_ENABLED else []


class _PipelineRollbackError(RuntimeError):
    """Sentinel error raised inside a DuckDB pipeline or savepoint block."""


class _LoopbackOAuthServer:
    """In-process HTTP server issuing sequential OAuth2 bearer tokens for GCS rotation tests."""

    def __init__(self) -> None:
        self.token_calls = 0
        self._lock = threading.Lock()
        parent = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                _ = (format, args)

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                if length > 0:
                    _ = self.rfile.read(length)
                with parent._lock:
                    parent.token_calls += 1
                    idx = parent.token_calls
                body = json.dumps(
                    {
                        "access_token": f"vtb-duckdb-oauth-token-{idx}",
                        "expires_in": 3600,
                        "token_type": "Bearer",
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.port = int(self._server.server_address[1])
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def token_uri(self) -> str:
        """Return the loopback token endpoint URL."""
        return f"http://127.0.0.1:{self.port}/token"

    def close(self) -> None:
        """Shut down the loopback HTTP server."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2.0)


class _SyncFaultInjectingDuckDBClient:
    """Delegating synchronous DuckDB client wrapper that can raise a synthetic HTTP 401/403 error."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fail_with_status: int | None = None

    @property
    def in_transaction(self) -> bool:
        """Return whether the underlying client is inside an explicit transaction."""
        return bool(self._inner.in_transaction)

    def execute(self, sql: str, params: Any = None) -> int:
        """Delegate DDL/DML execution to the native DuckDB client."""
        return int(self._inner.execute(sql, params))

    def execute_with_description(
        self,
        sql: str,
        params: Any = None,
    ) -> tuple[list[Any], tuple[str, ...]]:
        """Raise a simulated HTTP status rejection when armed, else delegate to the native client."""
        if self.fail_with_status is not None:
            status = self.fail_with_status
            self.fail_with_status = None
            msg = f"HTTP Error: Unable to connect to URL (HTTP {status})"
            raise RuntimeError(msg)
        rows, cols = self._inner.execute_with_description(sql, params)
        return list(rows), tuple(str(c) for c in cols)

    def close(self) -> None:
        """Close the underlying native DuckDB client."""
        self._inner.close()


class _AsyncFaultInjectingDuckDBClient:
    """Delegating asynchronous DuckDB client wrapper that can raise a synthetic HTTP 401/403 error."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fail_with_status: int | None = None

    @property
    def in_transaction(self) -> bool:
        """Return whether the underlying client is inside an explicit transaction."""
        return bool(self._inner.in_transaction)

    async def execute(self, sql: str, params: Any = None) -> int:
        """Delegate DDL/DML execution to the native async DuckDB client."""
        return int(await self._inner.execute(sql, params))

    async def execute_with_description(
        self,
        sql: str,
        params: Any = None,
    ) -> tuple[list[Any], tuple[str, ...]]:
        """Raise a simulated HTTP status rejection when armed, else delegate to the native client."""
        if self.fail_with_status is not None:
            status = self.fail_with_status
            self.fail_with_status = None
            msg = f"HTTP Error: Unable to connect to URL (HTTP {status})"
            raise RuntimeError(msg)
        rows, cols = await self._inner.execute_with_description(sql, params)
        return list(rows), tuple(str(c) for c in cols)

    async def close(self) -> None:
        """Close the underlying native async DuckDB client."""
        await self._inner.close()


def _shm_arrow_segments() -> set[str]:
    """Return active ``/dev/shm`` segment names matching Vertebra Arrow shared-memory prefixes."""
    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        return set()
    return {entry.name for entry in shm_dir.iterdir() if "vtb" in entry.name.lower() or "arrow" in entry.name.lower()}


@pytest.fixture
def oauth_server() -> Iterator[_LoopbackOAuthServer]:
    """Start and tear down a loopback OAuth2 token server."""
    server = _LoopbackOAuthServer()
    try:
        yield server
    finally:
        server.close()


@pytest.mark.driver_engine("duckdb")
@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_duckdb_native_arrow_shm_batches_and_bulk_load(mode: str) -> None:
    """Verify embedded DuckDB ``select_to_arrow`` SHM handoff, ``stream_arrow_batches``, and ``load_from_arrow``."""
    ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")
    target_rows = max(ARROW_SHM_THRESHOLD_ROWS, 512) + 32
    before_shm = _shm_arrow_segments()

    if mode == "sync":
        with SyncDuckDBDriver(DuckDBConfig(database=":memory:")) as driver:
            driver.execute("CREATE TABLE vtb_duck_arrow (id BIGINT, label VARCHAR)")
            source_table = pa.Table.from_pydict(
                {
                    "id": list(range(1, target_rows + 1)),
                    "label": [f"row-{idx}" for idx in range(1, target_rows + 1)],
                }
            )
            inserted = driver.load_from_arrow("vtb_duck_arrow", source_table)
            assert inserted == target_rows

            with driver.select_to_arrow(
                "SELECT id, label FROM vtb_duck_arrow ORDER BY id",
                threshold_rows=ARROW_SHM_THRESHOLD_ROWS,
            ) as cursor:
                assert cursor.row_count == target_rows
                assert cursor.used_shm is True
                arrow_table = cursor.to_pyarrow()
                assert arrow_table.num_rows == target_rows
                assert tuple(c.lower() for c in arrow_table.column_names) == ("id", "label")

            batches = list(
                driver.stream_arrow_batches(
                    "SELECT id, label FROM vtb_duck_arrow ORDER BY id",
                    batch_size=128,
                )
            )
            assert len(batches) == 5
            assert (
                sum(int(batch.num_rows) if hasattr(batch, "num_rows") else batch.row_count for batch in batches)
                == target_rows
            )
    else:
        async with AsyncDuckDBDriver(DuckDBConfig(database=":memory:")) as driver:
            await driver.execute("CREATE TABLE vtb_duck_arrow (id BIGINT, label VARCHAR)")
            source_table = pa.Table.from_pydict(
                {
                    "id": list(range(1, target_rows + 1)),
                    "label": [f"row-{idx}" for idx in range(1, target_rows + 1)],
                }
            )
            inserted = await driver.load_from_arrow("vtb_duck_arrow", source_table)
            assert inserted == target_rows

            with await driver.select_to_arrow(
                "SELECT id, label FROM vtb_duck_arrow ORDER BY id",
                threshold_rows=ARROW_SHM_THRESHOLD_ROWS,
            ) as cursor:
                assert cursor.row_count == target_rows
                assert cursor.used_shm is True
                arrow_table = cursor.to_pyarrow()
                assert arrow_table.num_rows == target_rows
                assert tuple(c.lower() for c in arrow_table.column_names) == ("id", "label")

            async_batches = [
                batch
                async for batch in driver.stream_arrow_batches(
                    "SELECT id, label FROM vtb_duck_arrow ORDER BY id",
                    batch_size=128,
                )
            ]
            assert len(async_batches) == 5
            assert (
                sum(int(batch.num_rows) if hasattr(batch, "num_rows") else batch.row_count for batch in async_batches)
                == target_rows
            )

    after_shm = _shm_arrow_segments()
    assert after_shm - before_shm == set()


@pytest.mark.driver_engine("duckdb")
@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_duckdb_native_pipeline_rollback_and_savepoints(mode: str) -> None:
    """Verify DuckDB transactional ``pipeline()`` rollback on mid-batch error and nested savepoints."""
    ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")
    if mode == "sync":
        with SyncDuckDBDriver(DuckDBConfig(database=":memory:")) as driver:
            driver.execute("CREATE TABLE vtb_duck_tx (id BIGINT PRIMARY KEY, label VARCHAR NOT NULL)")
            driver.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (1, "seed"))

            with pytest.raises(DatabaseError), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (2, "transient"))
                pipe.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (1, "duplicate-pk"))
                _ = pipe.results()

            assert driver.select_value("SELECT COUNT(*) FROM vtb_duck_tx", value_type=int) == 1

            with driver.transaction():
                driver.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (2, "outer-kept"))
                with pytest.raises(_PipelineRollbackError), driver.transaction():
                    driver.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (3, "inner-rolled-back"))
                    raise _PipelineRollbackError("revert nested savepoint")

            rows = driver.select("SELECT id, label FROM vtb_duck_tx ORDER BY id")
            assert rows == [
                {"id": 1, "label": "seed"},
                {"id": 2, "label": "outer-kept"},
            ]
    else:
        async with AsyncDuckDBDriver(DuckDBConfig(database=":memory:")) as driver:
            await driver.execute("CREATE TABLE vtb_duck_tx (id BIGINT PRIMARY KEY, label VARCHAR NOT NULL)")
            await driver.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (1, "seed"))

            with pytest.raises(DatabaseError):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (2, "transient"))
                    pipe.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (1, "duplicate-pk"))
                    _ = await pipe.results()

            assert await driver.select_value("SELECT COUNT(*) FROM vtb_duck_tx", value_type=int) == 1

            async with driver.transaction():
                await driver.execute("INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)", (2, "outer-kept"))
                with pytest.raises(_PipelineRollbackError):
                    async with driver.transaction():
                        await driver.execute(
                            "INSERT INTO vtb_duck_tx (id, label) VALUES (?, ?)",
                            (3, "inner-rolled-back"),
                        )
                        raise _PipelineRollbackError("revert nested async savepoint")

            rows = await driver.select("SELECT id, label FROM vtb_duck_tx ORDER BY id")
            assert rows == [
                {"id": 1, "label": "seed"},
                {"id": 2, "label": "outer-kept"},
            ]


@pytest.mark.driver_engine("duckdb")
@pytest.mark.anyio
@pytest.mark.parametrize(("mode", "status_code"), [("sync", 401), ("async", 403)])
async def test_duckdb_loopback_gcs_bearer_secret_rotation_on_auth_rejection(
    oauth_server: _LoopbackOAuthServer,
    tmp_path: Path,
    mode: str,
    status_code: int,
) -> None:
    """Verify loopback OAuth2 bearer rotation across ``SyncGcsBearerSecretProvider`` and ``AsyncGcsBearerSecretProvider``."""
    native_mod = ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")
    adc_file = tmp_path / "authorized_user.json"
    adc_file.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "vtb-test-client",
                "client_secret": "vtb-test-secret",
                "refresh_token": "vtb-test-refresh",
                "token_uri": oauth_server.token_uri,
            }
        ),
        encoding="utf-8",
    )
    gcs_cfg = GCSConfig(bucket="vtb-lake", application_credentials=str(adc_file))

    if mode == "sync":
        provider = SyncGcsBearerSecretProvider.from_config(gcs_cfg, allow_ambient_adc=False)
        proxy = _SyncFaultInjectingDuckDBClient(native_mod.SyncDuckDBClient(":memory:"))
        driver = SyncDuckDBDriver(
            DuckDBConfig(
                database=":memory:",
                secrets=(
                    DuckDBSecretConfig(
                        name="gcs_lake",
                        secret_type="http",
                        value_provider=provider,
                        scope="http://127.0.0.1/",
                        refresh_interval_seconds=3600.0,
                    ),
                ),
            ),
            _client=cast("Any", proxy),
        )
        try:
            assert provider.generation == 1
            assert oauth_server.token_calls == 1
            assert driver.select("SELECT name FROM duckdb_secrets() WHERE name = 'gcs_lake'") == [{"name": "gcs_lake"}]
            proxy.fail_with_status = status_code
            with pytest.raises(RuntimeError, match=f"HTTP {status_code}"):
                driver.select("SELECT 1 AS ok")
            assert provider.needs_refresh() is True

            assert driver.select("SELECT 1 AS ok") == [{"ok": 1}]
            assert provider.generation == 2
            assert oauth_server.token_calls == 2
            assert provider.needs_refresh() is False
        finally:
            driver.close()
    else:
        async_provider = AsyncGcsBearerSecretProvider.from_config(gcs_cfg, allow_ambient_adc=False)
        async_proxy = _AsyncFaultInjectingDuckDBClient(native_mod.AsyncDuckDBClient(":memory:"))
        async_driver = AsyncDuckDBDriver(
            DuckDBConfig(
                database=":memory:",
                secrets=(
                    DuckDBSecretConfig(
                        name="gcs_lake",
                        secret_type="http",
                        value_provider=async_provider,
                        scope="http://127.0.0.1/",
                        refresh_interval_seconds=3600.0,
                    ),
                ),
            ),
            _client=cast("Any", async_proxy),
        )
        try:
            assert async_provider.generation == 0
            assert await async_driver.select("SELECT name FROM duckdb_secrets() WHERE name = 'gcs_lake'") == [
                {"name": "gcs_lake"}
            ]
            assert async_provider.generation == 1
            assert oauth_server.token_calls == 1
            async_proxy.fail_with_status = status_code
            with pytest.raises(RuntimeError, match=f"HTTP {status_code}"):
                await async_driver.select("SELECT 1 AS ok")
            assert async_provider.needs_refresh() is True

            assert await async_driver.select("SELECT 1 AS ok") == [{"ok": 1}]
            assert async_provider.generation == 2
            assert oauth_server.token_calls == 2
            assert async_provider.needs_refresh() is False
        finally:
            await async_driver.close()


@pytest.mark.parametrize("mark_trigger", [pytest.param("httpfs", marks=_HTTPFS_MARKS)])
def test_duckdb_live_httpfs_gcs_bearer_rotation(
    oauth_server: _LoopbackOAuthServer,
    tmp_path: Path,
    mark_trigger: str,
) -> None:
    """Verify live ``httpfs`` extension ``TYPE GCS`` secret registration when ``VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1``."""
    _ = mark_trigger
    if not _HTTPFS_ENABLED:
        pytest.skip("Set VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1 to run live DuckDB httpfs extension test")

    ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")
    adc_file = tmp_path / "authorized_user_httpfs.json"
    adc_file.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "vtb-httpfs-client",
                "client_secret": "vtb-httpfs-secret",
                "refresh_token": "vtb-httpfs-refresh",
                "token_uri": oauth_server.token_uri,
            }
        ),
        encoding="utf-8",
    )
    provider = SyncGcsBearerSecretProvider.from_config(
        GCSConfig(bucket="vtb-httpfs-lake", application_credentials=str(adc_file)),
        allow_ambient_adc=False,
    )
    cfg = DuckDBConfig(
        database=":memory:",
        extensions=("httpfs",),
        secrets=(
            DuckDBSecretConfig(
                name="gcs_httpfs_secret",
                secret_type="gcs",
                value_provider=provider,
                scope="gs://vtb-httpfs-lake/",
                refresh_interval_seconds=3600.0,
            ),
        ),
    )
    with SyncDuckDBDriver(cfg) as driver:
        rows = driver.select("SELECT name, type FROM duckdb_secrets() WHERE name = 'gcs_httpfs_secret'")
        assert len(rows) == 1
        assert rows[0]["name"] == "gcs_httpfs_secret"
        assert str(rows[0]["type"]).lower() == "gcs"
        assert provider.generation == 1
        provider.invalidate(1)
        assert provider.needs_refresh() is True
        assert driver.select_value("SELECT 1", value_type=int) == 1
        assert provider.generation == 2
