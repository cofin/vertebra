"""Synchronous and asynchronous Oracle drivers, multi-pool registry, and Arrow zero-copy bridge."""

from __future__ import annotations

import array
import importlib
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from vertebra._typing import MissingDependencyError
from vertebra.db.oracle.config import OracleConfig
from vertebra.utils.serializers import encode_json

if TYPE_CHECKING:
    from vertebra.db.oracle.wallet import OracleWallet

__all__ = (
    "AsyncOracleDriver",
    "OracleDataFrame",
    "OraclePoolRegistry",
    "SyncOracleDriver",
)


class OracleDataFrame:
    """Tabular result wrapper exposing the Arrow C Data Interface PyCapsule protocol."""

    __slots__ = ("_columns", "_rows")

    def __init__(self, columns: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
        self._columns = columns
        self._rows = rows

    @property
    def columns(self) -> tuple[str, ...]:
        """Return column names."""
        return self._columns

    @property
    def rows(self) -> list[tuple[Any, ...]]:
        """Return row tuples."""
        return self._rows

    def __len__(self) -> int:
        return len(self._rows)

    def to_dicts(self) -> list[dict[str, Any]]:
        """Convert rows into a list of column-keyed dicts."""
        cols = self._columns
        return [dict(zip(cols, row, strict=True)) for row in self._rows]

    def to_arrow_table(self) -> Any:
        """Materialize rows into a pyarrow.Table when PyArrow is installed."""
        pa: Any = importlib.import_module("pyarrow")
        if not self._columns:
            return pa.table({})
        col_arrays: dict[str, list[Any]] = {col: [] for col in self._columns}
        for row in self._rows:
            for idx, col in enumerate(self._columns):
                col_arrays[col].append(row[idx] if idx < len(row) else None)
        return pa.table(col_arrays)

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Export the result set as an Arrow C Stream PyCapsule for zero-copy consumers."""
        table = self.to_arrow_table()
        return table.__arrow_c_stream__(requested_schema=requested_schema)


class SyncOracleDriver:
    """Synchronous Oracle Database driver with OSON, VECTOR, and zero-Instant-Client wallet support."""

    __slots__ = ("_closed", "_config", "_pool", "_wallet")

    def __init__(self, config: OracleConfig | None = None, *, pool: Any = None) -> None:
        self._config = config or OracleConfig()
        self._wallet: OracleWallet | None = self._config.resolve_wallet()
        self._pool = pool
        self._closed: bool = False

    @property
    def config(self) -> OracleConfig:
        """Return the active Oracle configuration."""
        return self._config

    @property
    def wallet(self) -> OracleWallet | None:
        """Return the extracted wallet metadata if configured."""
        return self._wallet

    @staticmethod
    def encode_vector(
        values: Sequence[float | int],
        *,
        type_code: str = "f",
        dtype: str | None = None,
    ) -> array.array[Any]:
        """Encode a float or int sequence into an array.array suitable for Oracle 26ai DB_TYPE_VECTOR binds."""
        resolved = dtype if dtype is not None else type_code
        if resolved in ("float64", "d"):
            return array.array("d", (float(v) for v in values))
        if resolved in ("int8", "b"):
            return array.array("b", (int(v) for v in values))
        if resolved in ("float32", "f"):
            return array.array("f", (float(v) for v in values))
        msg = f"Unsupported Oracle vector type code or dtype: {resolved!r}"
        raise ValueError(msg)

    def prepare_bind_params(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Prepare bind parameters with automatic VECTOR array.array and JSON serialization."""
        if not params:
            return {}
        prepared: dict[str, Any] = {}
        for key, value in params.items():
            if isinstance(value, list) and value and all(isinstance(item, (int, float)) for item in value):
                prepared[key] = self.encode_vector(value)
            elif isinstance(value, dict):
                prepared[key] = encode_json(value)
            else:
                prepared[key] = value
        return prepared

    def _ensure_pool(self) -> Any:
        """Lazily initialize oracledb connection pool or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        try:
            oracledb: Any = importlib.import_module("oracledb")
        except ImportError as exc:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb", extra="oracle") from exc

        if self._config.thick_mode_lib_dir is not None:
            oracledb.init_oracle_client(lib_dir=self._config.thick_mode_lib_dir)

        params = self._config.to_oracledb_connect_params()
        self._pool = oracledb.create_pool(**params)
        return self._pool

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement and return plain tuples and column description."""
        if self._closed:
            msg = "SyncOracleDriver is closed"
            raise RuntimeError(msg)
        pool = self._ensure_pool()
        prepared_params = self.prepare_bind_params(dict(parameters)) if isinstance(parameters, Mapping) else parameters
        if hasattr(pool, "execute_with_description"):
            res: tuple[
                list[tuple[Any, ...]],
                tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
            ] = pool.execute_with_description(sql, prepared_params)
            return res
        if hasattr(pool, "acquire"):
            with pool.acquire() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(sql, prepared_params if prepared_params else None)
                    rows = cur.fetchall() if hasattr(cur, "fetchall") else []
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                        if getattr(cur, "description", None)
                        else None
                    )
                    return [tuple(r) for r in rows], desc
                finally:
                    if hasattr(cur, "close"):
                        cur.close()
        if hasattr(pool, "connection"):
            with pool.connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(sql, prepared_params if prepared_params else None)
                    rows = cur.fetchall() if hasattr(cur, "fetchall") else []
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                        if getattr(cur, "description", None)
                        else None
                    )
                    return [tuple(r) for r in rows], desc
                finally:
                    if hasattr(cur, "close"):
                        cur.close()
        if hasattr(pool, "execute"):
            exec_res = pool.execute(sql, prepared_params if prepared_params else None)
            rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
            desc = (
                tuple((d[0], d[1], None, None, None, None, None) for d in exec_res.description)
                if getattr(exec_res, "description", None)
                else None
            )
            return [tuple(r) for r in rows], desc
        return [], None

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def execute_df(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> OracleDataFrame:
        """Execute a SQL statement and return an Arrow-compatible OracleDataFrame."""
        rows, desc = self.execute_with_description(sql, parameters)
        cols = tuple(d[0] for d in desc) if desc else ()
        return OracleDataFrame(cols, rows)

    def stage_to_dbms_cloud(
        self,
        store: Any,
        object_path: str,
        target_table: str,
        *,
        credential_name: str = "OBJ_STORE_CRED",
        file_uri_list: str | None = None,
        format_json: dict[str, Any] | None = None,
    ) -> None:
        """Stage an ObjectStore dataset into an Oracle table via DBMS_CLOUD.COPY_DATA."""
        if file_uri_list is None:
            base_url = getattr(store, "url", "memory://").rstrip("/")
            clean_path = object_path.lstrip("/")
            file_uri_list = f"{base_url}/{clean_path}"
        if format_json is not None:
            format_val = encode_json(format_json)
        elif object_path.endswith(".csv"):
            format_val = '{"type": "csv", "skipheaders": "1"}'
        elif object_path.endswith((".json", ".jsonl")):
            format_val = '{"type": "json"}'
        else:
            format_val = '{"type": "parquet"}'

        copy_sql = (
            "BEGIN "
            "DBMS_CLOUD.COPY_DATA("
            "table_name => :table_name, "
            "credential_name => :credential_name, "
            "file_uri_list => :file_uri_list, "
            "format => :format"
            "); "
            "END;"
        )
        binds = {
            "table_name": target_table,
            "credential_name": credential_name,
            "file_uri_list": file_uri_list,
            "format": format_val,
        }
        self.execute_tuples(copy_sql, binds)

    def commit(self) -> None:
        """Commit the active transaction."""
        if self._pool is not None:
            if hasattr(self._pool, "commit"):
                self._pool.commit()
            elif hasattr(self._pool, "conn") and hasattr(self._pool.conn, "commit"):
                self._pool.conn.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        if self._pool is not None:
            if hasattr(self._pool, "rollback"):
                self._pool.rollback()
            elif hasattr(self._pool, "conn") and hasattr(self._pool.conn, "rollback"):
                self._pool.conn.rollback()

    def close(self) -> None:
        """Close the driver and release the connection pool."""
        self._closed = True
        if self._pool is not None:
            if hasattr(self._pool, "close"):
                self._pool.close()
            if hasattr(self._pool, "conn") and hasattr(self._pool.conn, "close"):
                self._pool.conn.close()

    def fetch_df(self, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> OracleDataFrame:
        """Construct an Arrow-compatible OracleDataFrame from columns and rows."""
        return OracleDataFrame(tuple(columns), [tuple(row) for row in rows])


class AsyncOracleDriver:
    """Asynchronous Oracle Database driver with OSON, VECTOR, and Arrow PyCapsule streaming."""

    __slots__ = ("_closed", "_config", "_pool", "_sync", "_wallet")

    def __init__(self, config: OracleConfig | None = None, *, pool: Any = None) -> None:
        self._config = config or OracleConfig()
        self._sync = SyncOracleDriver(self._config, pool=pool)
        self._wallet: OracleWallet | None = self._sync.wallet
        self._pool = pool
        self._closed: bool = False

    @property
    def config(self) -> OracleConfig:
        """Return the active Oracle configuration."""
        return self._config

    @property
    def wallet(self) -> OracleWallet | None:
        """Return the extracted wallet metadata if configured."""
        return self._wallet

    async def _ensure_pool(self) -> Any:
        """Lazily initialize async oracledb connection pool or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        try:
            oracledb: Any = importlib.import_module("oracledb")
        except ImportError as exc:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb", extra="oracle") from exc

        if self._config.thick_mode_lib_dir is not None:
            oracledb.init_oracle_client(lib_dir=self._config.thick_mode_lib_dir)

        params = self._config.to_oracledb_connect_params()
        self._pool = oracledb.create_pool_async(**params)
        return self._pool

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement and return rows and column descriptions."""
        if self._closed:
            msg = "AsyncOracleDriver is closed"
            raise RuntimeError(msg)
        pool = await self._ensure_pool()
        prepared_params = (
            self._sync.prepare_bind_params(dict(parameters)) if isinstance(parameters, Mapping) else parameters
        )
        if hasattr(pool, "execute_with_description"):
            raw_res = pool.execute_with_description(sql, prepared_params)
            if hasattr(raw_res, "__await__"):
                raw_res = await raw_res
            res: tuple[
                list[tuple[Any, ...]],
                tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
            ] = raw_res
            return res
        if hasattr(pool, "acquire"):
            async with pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, prepared_params if prepared_params else None)
                rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                desc = (
                    tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                    if getattr(cur, "description", None)
                    else None
                )
                return [tuple(r) for r in rows], desc
        if hasattr(pool, "connection"):
            conn = pool.connection()
            if hasattr(conn, "__aenter__"):
                async with conn as active_conn, active_conn.cursor() as cur:
                    await cur.execute(sql, prepared_params if prepared_params else None)
                    rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                        if getattr(cur, "description", None)
                        else None
                    )
                    return [tuple(r) for r in rows], desc
        return self._sync.execute_with_description(sql, parameters)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        rows, _ = await self.execute_with_description(sql, parameters)
        return rows

    async def execute_df(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> OracleDataFrame:
        """Asynchronously execute a SQL statement and return an Arrow-compatible OracleDataFrame."""
        rows, desc = await self.execute_with_description(sql, parameters)
        cols = tuple(d[0] for d in desc) if desc else ()
        return OracleDataFrame(cols, rows)

    async def stage_to_dbms_cloud(
        self,
        store: Any,
        object_path: str,
        target_table: str,
        *,
        credential_name: str = "OBJ_STORE_CRED",
        file_uri_list: str | None = None,
        format_json: dict[str, Any] | None = None,
    ) -> None:
        """Stage an ObjectStore dataset asynchronously into an Oracle table via DBMS_CLOUD.COPY_DATA."""
        self._sync.stage_to_dbms_cloud(
            store,
            object_path,
            target_table,
            credential_name=credential_name,
            file_uri_list=file_uri_list,
            format_json=format_json,
        )

    def fetch_df(self, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> OracleDataFrame:
        """Construct an Arrow-compatible OracleDataFrame synchronously."""
        return self._sync.fetch_df(columns, rows)

    async def close(self) -> None:
        """Close the async driver pool."""
        self._closed = True
        if self._pool is not None and hasattr(self._pool, "close"):
            res = self._pool.close()
            if hasattr(res, "__await__"):
                await res
        self._sync.close()


class OraclePoolRegistry:
    """Multi-pool registry isolating OLTP, analytics, and vector connection pools without state mutation."""

    __slots__ = ("_base_config", "_pools")

    def __init__(self, base_config: OracleConfig) -> None:
        self._base_config = base_config
        self._pools: dict[str, AsyncOracleDriver] = {}

    @property
    def pool_names(self) -> tuple[str, ...]:
        """Return registered pool names."""
        return tuple(self._pools.keys())

    def register_pool(
        self,
        name: str,
        *,
        config: OracleConfig | None = None,
        min_size: int = 1,
        max_size: int = 4,
        driver_features: Mapping[str, Any] | None = None,
    ) -> AsyncOracleDriver:
        """Register and return an isolated named Oracle pool driver."""
        source_cfg = config or self._base_config
        derived_cfg = source_cfg.derive_pool(
            bind_key=name,
            min_size=min_size,
            max_size=max_size,
            driver_features=driver_features,
        )
        driver = AsyncOracleDriver(derived_cfg)
        self._pools[name] = driver
        return driver

    def get_pool(self, name: str = "default") -> AsyncOracleDriver:
        """Retrieve a registered pool by name, lazily initializing default if needed."""
        if name not in self._pools:
            if name == "default":
                return self.register_pool("default")
            msg = f"Oracle pool {name!r} is not registered"
            raise KeyError(msg)
        return self._pools[name]
