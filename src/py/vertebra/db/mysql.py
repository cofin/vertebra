"""MySQL and MariaDB driver and Cloud SQL (:3307) configuration."""

from __future__ import annotations

import collections
import importlib
import urllib.parse
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._typing import MissingDependencyError
from vertebra.db.dialer.cloudsql import CloudSqlConfig, CloudSqlDialer

if TYPE_CHECKING:
    import ssl

__all__ = (
    "AsyncMysqlDriver",
    "MysqlConfig",
    "SyncMysqlDriver",
)


class MysqlConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for MySQL 8.0+/9.0+ and MariaDB connections."""

    dsn: str = "mysql://localhost:3306/mysql"
    min_size: int = 1
    max_size: int = 10
    cloud_sql_instance: str | None = None
    enable_iam_auth: bool = False
    charset: str = "utf8mb4"
    autocommit: bool = False

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)

    def to_cloud_dialer_config(self) -> CloudSqlConfig | None:
        """Return CloudSqlConfig when `cloud_sql_instance` is set."""
        if self.cloud_sql_instance:
            return CloudSqlConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None

    def parse_dsn_params(self) -> dict[str, Any]:
        """Parse DSN connection string into connection parameters for driver pools."""
        parsed = urllib.parse.urlparse(self.dsn)
        host = parsed.hostname or "localhost"
        port = parsed.port or 3306
        user = parsed.username or ""
        password = parsed.password or ""
        database = parsed.path.lstrip("/") if parsed.path else ""
        query = urllib.parse.parse_qs(parsed.query)
        charset = query.get("charset", [self.charset])[0]
        autocommit_val = query.get("autocommit", [str(self.autocommit).lower()])[0]
        autocommit = autocommit_val.lower() in ("true", "1", "yes")
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "charset": charset,
            "autocommit": autocommit,
        }


class SyncMysqlDriver:
    """Synchronous MySQL driver facade with bounded connection pooling."""

    __slots__ = ("_closed", "_config", "_connect_factory", "_dialer", "_idle_conns")

    def __init__(
        self,
        config: MysqlConfig | None = None,
        *,
        dialer: CloudSqlDialer | None = None,
        connect_factory: Any = None,
    ) -> None:
        self._config = config or MysqlConfig()
        self._dialer = dialer
        if self._dialer is None:
            csql_cfg = self._config.to_cloud_dialer_config()
            if csql_cfg is not None:
                self._dialer = CloudSqlDialer(csql_cfg)
        self._connect_factory = connect_factory
        self._idle_conns: collections.deque[Any] = collections.deque(maxlen=self._config.max_size)
        self._closed: bool = False

    @property
    def config(self) -> MysqlConfig:
        """Return the active MysqlConfig."""
        return self._config

    @property
    def dialer(self) -> CloudSqlDialer | None:
        """Return the attached CloudSqlDialer if configured."""
        return self._dialer

    def _create_connection(self) -> Any:
        """Create a new database connection or raise MissingDependencyError."""
        params = self._config.parse_dsn_params()
        tls_sock: ssl.SSLSocket | None = None
        if self._dialer is not None:
            tls_sock = self._dialer.connect_sync(host=params["host"])

        if self._connect_factory is not None:
            if tls_sock is not None:
                try:
                    return self._connect_factory(sock=tls_sock)
                except TypeError:
                    return self._connect_factory()
            return self._connect_factory()

        try:
            pymysql: Any = importlib.import_module("pymysql")
        except ImportError as exc:
            msg = "pymysql"
            raise MissingDependencyError(msg, install_package="pymysql", extra="mysql") from exc

        connect_kwargs: dict[str, Any] = {
            "host": params["host"],
            "port": params["port"],
            "user": params["user"],
            "password": params["password"],
            "database": params["database"],
            "charset": params["charset"],
            "autocommit": params["autocommit"],
        }
        if tls_sock is not None:
            connect_kwargs["sock"] = tls_sock
        return pymysql.connect(**connect_kwargs)

    def _acquire(self) -> Any:
        """Acquire an idle connection or create a new connection."""
        if self._closed:
            msg = "SyncMysqlDriver is closed"
            raise RuntimeError(msg)
        if self._idle_conns:
            return self._idle_conns.pop()
        return self._create_connection()

    def _release(self, conn: Any) -> None:
        """Release a connection back to the idle pool or close it."""
        if self._closed or len(self._idle_conns) >= self._config.max_size:
            if hasattr(conn, "close"):
                conn.close()
        else:
            self._idle_conns.append(conn)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement and return plain tuples and column description."""
        conn = self._acquire()
        try:
            if hasattr(conn, "execute_with_description"):
                res: tuple[
                    list[tuple[Any, ...]],
                    tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                ] = conn.execute_with_description(sql, parameters)
                return res
            if hasattr(conn, "cursor"):
                cur = conn.cursor()
                try:
                    cur.execute(sql, parameters if parameters else None)
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
            if hasattr(conn, "execute"):
                exec_res = conn.execute(sql, parameters if parameters else None)
                rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
                desc = (
                    tuple((d[0], d[1], None, None, None, None, None) for d in exec_res.description)
                    if getattr(exec_res, "description", None)
                    else None
                )
                return [tuple(r) for r in rows], desc
            return [], None
        finally:
            self._release(conn)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def commit(self) -> None:
        """Commit the active transaction across active connections."""
        for conn in list(self._idle_conns):
            if hasattr(conn, "commit"):
                conn.commit()
        if hasattr(self._connect_factory, "commit"):
            self._connect_factory.commit()

    def rollback(self) -> None:
        """Roll back the active transaction across active connections."""
        for conn in list(self._idle_conns):
            if hasattr(conn, "rollback"):
                conn.rollback()
        if hasattr(self._connect_factory, "rollback"):
            self._connect_factory.rollback()

    def close(self) -> None:
        """Close the driver and release all pooled connections."""
        self._closed = True
        while self._idle_conns:
            conn = self._idle_conns.pop()
            if hasattr(conn, "close"):
                conn.close()
        if hasattr(self._connect_factory, "close"):
            self._connect_factory.close()


class AsyncMysqlDriver:
    """Asynchronous MySQL driver facade backed by asyncmy."""

    __slots__ = ("_closed", "_config", "_dialer", "_pool")

    def __init__(
        self,
        config: MysqlConfig | None = None,
        *,
        dialer: CloudSqlDialer | None = None,
        pool: Any = None,
    ) -> None:
        self._config = config or MysqlConfig()
        self._dialer = dialer
        if self._dialer is None:
            csql_cfg = self._config.to_cloud_dialer_config()
            if csql_cfg is not None:
                self._dialer = CloudSqlDialer(csql_cfg)
        self._pool = pool
        self._closed: bool = False

    @property
    def config(self) -> MysqlConfig:
        """Return the active MysqlConfig."""
        return self._config

    @property
    def dialer(self) -> CloudSqlDialer | None:
        """Return the attached CloudSqlDialer if configured."""
        return self._dialer

    async def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        try:
            asyncmy: Any = importlib.import_module("asyncmy")
        except ImportError as exc:
            msg = "asyncmy"
            raise MissingDependencyError(msg, install_package="asyncmy", extra="mysql") from exc
        params = self._config.parse_dsn_params()
        pool_kwargs: dict[str, Any] = {
            "host": params["host"],
            "port": params["port"],
            "user": params["user"],
            "password": params["password"],
            "db": params["database"],
            "charset": params["charset"],
            "autocommit": params["autocommit"],
            "minsize": self._config.min_size,
            "maxsize": self._config.max_size,
        }
        self._pool = await asyncmy.create_pool(**pool_kwargs)
        return self._pool

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement and return rows and column descriptions."""
        if self._closed:
            msg = "AsyncMysqlDriver is closed"
            raise RuntimeError(msg)
        pool = await self._ensure_pool()
        if hasattr(pool, "execute_with_description"):
            raw_res = pool.execute_with_description(sql, parameters)
            if hasattr(raw_res, "__await__"):
                raw_res = await raw_res
            res: tuple[
                list[tuple[Any, ...]],
                tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
            ] = raw_res
            return res
        if hasattr(pool, "acquire"):
            async with pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, parameters if parameters else None)
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
                    await cur.execute(sql, parameters if parameters else None)
                    rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                        if getattr(cur, "description", None)
                        else None
                    )
                    return [tuple(r) for r in rows], desc
        return [], None

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        rows, _ = await self.execute_with_description(sql, parameters)
        return rows

    async def close(self) -> None:
        """Close the async driver pool."""
        self._closed = True
        if self._pool is not None:
            if hasattr(self._pool, "close"):
                res = self._pool.close()
                if hasattr(res, "__await__"):
                    await res
            if hasattr(self._pool, "wait_closed"):
                await self._pool.wait_closed()
