"""SQLSpec and Advanced Alchemy ``WorldDriver`` implementations for benchmark apps.

Supports both ``vertebra`` (Vertebra SQLSpec adapters and SQLAlchemy ``+vtb`` /
``+vtb_async`` dialects with ``BrokerPool`` / ``AsyncBrokerPool``) and ``upstream``
(upstream ``sqlspec`` and vanilla SQLAlchemy) flavors across SQLite and PostgreSQL.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import weakref
from typing import Any, Final

from tools.benchmarks.apps._data_backend import WORLD_ROW_COUNT, DataBackendConfig, WorldRow
from tools.benchmarks.apps._db_drivers import (
    POSTGRES_BENCH_INSTALL_HINT,
    WorldDriver,
    ensure_task_context,
    normalize_sqlite_target,
    record_to_world_row,
    require_postgres_dsn,
    resolve_async_broker_transport,
    resolve_benchmark_broker,
    resolve_sync_broker_transport,
    seed_postgres_world_async,
    seed_postgres_world_sync,
    seed_sqlite_world_sync,
    world_seed_rows,
)
from vertebra.utils.module_loader import import_optional

__all__ = (
    "AdvancedAlchemyWorldDriver",
    "SQLSpecWorldDriver",
    "build_orm_world_driver",
    "rewrite_sqlalchemy_url",
)

_SQLITE_SELECT_SQL: Final[str] = "SELECT id, randomnumber FROM world WHERE id = ?"
_SQLITE_UPDATE_SQL: Final[str] = "UPDATE world SET randomnumber = ? WHERE id = ?"
_PG_SELECT_DOLLAR_SQL: Final[str] = "SELECT id, randomnumber FROM world WHERE id = $1"
_PG_UPDATE_DOLLAR_SQL: Final[str] = "UPDATE world SET randomnumber = $1 WHERE id = $2"
_PG_SELECT_PERCENT_SQL: Final[str] = "SELECT id, randomnumber FROM world WHERE id = %s"
_PG_UPDATE_PERCENT_SQL: Final[str] = "UPDATE world SET randomnumber = %s WHERE id = %s"

_SQLITE_CREATE_WORLD_SQL: Final[str] = (
    "CREATE TABLE IF NOT EXISTS world (id INTEGER PRIMARY KEY, randomnumber INTEGER NOT NULL)"
)
_SQLITE_INSERT_WORLD_SQL: Final[str] = "INSERT OR IGNORE INTO world (id, randomnumber) VALUES (?, ?)"


def rewrite_sqlalchemy_url(
    backend: str,
    dsn: str,
    *,
    flavor: str,
    is_async: bool,
) -> str:
    """Rewrite a SQLite path or PostgreSQL DSN into the target SQLAlchemy dialect URL."""
    norm_backend = backend.strip().lower()
    norm_flavor = flavor.strip().lower()
    if norm_backend == "sqlite":
        target = normalize_sqlite_target(dsn)
        if norm_flavor == "vertebra":
            scheme = "sqlite+vtb_async" if is_async else "sqlite+vtb"
        else:
            scheme = "sqlite+aiosqlite" if is_async else "sqlite"
        if target == ":memory:":
            return f"{scheme}:///:memory:"
        return f"{scheme}:///{target}"

    if norm_backend == "postgres":
        cleaned = require_postgres_dsn(dsn)
        _, remainder = cleaned.split("://", 1) if "://" in cleaned else ("postgresql", cleaned)
        if norm_flavor == "vertebra":
            scheme = "postgresql+vtb_async" if is_async else "postgresql+vtb"
        else:
            scheme = "postgresql+asyncpg" if is_async else "postgresql+psycopg"
        return f"{scheme}://{remainder}"

    msg = f"Unsupported SQLAlchemy backend: {backend!r}"
    raise ValueError(msg)


def _seed_sqlite_session_sqlspec_sync(session: Any) -> None:
    """Seed the 10,000-row ``world`` table inside an open sync SQLSpec SQLite session."""
    session.execute(_SQLITE_CREATE_WORLD_SQL)
    row = session.select_one("SELECT COUNT(*) AS cnt FROM world")
    existing = int(next(iter(row.values()))) if isinstance(row, dict) else int(row[0])
    if existing < WORLD_ROW_COUNT:
        for row_id, random_num in world_seed_rows():
            session.execute(_SQLITE_INSERT_WORLD_SQL, (row_id, random_num))
        commit_fn = getattr(session, "commit", None)
        if callable(commit_fn):
            commit_fn()


async def _seed_sqlite_session_sqlspec_async(session: Any) -> None:
    """Seed the 10,000-row ``world`` table inside an open async SQLSpec SQLite session."""
    await session.execute(_SQLITE_CREATE_WORLD_SQL)
    row = await session.select_one("SELECT COUNT(*) AS cnt FROM world")
    existing = int(next(iter(row.values()))) if isinstance(row, dict) else int(row[0])
    if existing < WORLD_ROW_COUNT:
        for row_id, random_num in world_seed_rows():
            await session.execute(_SQLITE_INSERT_WORLD_SQL, (row_id, random_num))
        commit_fn = getattr(session, "commit", None)
        if callable(commit_fn):
            result = commit_fn()
            if asyncio.iscoroutine(result):
                await result


class SQLSpecWorldDriver:
    """SQLSpec ``WorldDriver`` supporting ``vertebra`` and ``upstream`` SQLite/PostgreSQL adapters."""

    def __init__(self, config: DataBackendConfig, *, pool_size: int) -> None:
        if import_optional("sqlspec") is None:
            msg = "sqlspec is required when VTB_BENCH_DB_ADAPTER=sqlspec"
            raise RuntimeError(msg)

        self.config = config
        self.backend = config.db_backend.strip().lower()
        self.flavor = config.db_adapter_flavor.strip().lower()
        self.pool_size = max(1, pool_size)
        self.share_pool = config.broker_mode.strip().lower() == "shm_broker"
        self._sync_lock = threading.Lock()
        self._loop_lock = threading.Lock()
        self._sync_cfg: Any = None
        self._async_cfgs: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any] = weakref.WeakKeyDictionary()
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._pg_seeded = False

        if self.backend == "sqlite":
            self.db_target = normalize_sqlite_target(config.db_dsn)
            self.is_memory = self.db_target == ":memory:" or "mode=memory" in self.db_target
            if not self.is_memory:
                seed_sqlite_world_sync(self.db_target)
        elif self.backend == "postgres":
            self.db_target = require_postgres_dsn(config.db_dsn)
            self.is_memory = False
        else:
            msg = f"SQLSpecWorldDriver supports sqlite or postgres (got {config.db_backend!r})"
            raise ValueError(msg)

    def _build_sync_config(self) -> Any:
        if self.backend == "sqlite":
            if self.flavor == "vertebra":
                mod = import_optional("vertebra.extensions.sqlspec.adapters.sqlite")
                if mod is None:
                    msg = "vertebra.extensions.sqlspec.adapters.sqlite is required for Vertebra SQLSpec SQLite"
                    raise RuntimeError(msg)
                return mod.SQLiteSyncConfig(
                    connection_config={"database": self.db_target},
                    share_pool=self.share_pool,
                )
            upstream_mod = import_optional("sqlspec.adapters.sqlite")
            if upstream_mod is None or not hasattr(upstream_mod, "SqliteConfig"):
                msg = "sqlspec.adapters.sqlite.SqliteConfig is required for upstream SQLSpec SQLite"
                raise RuntimeError(msg)
            return upstream_mod.SqliteConfig(connection_config={"database": self.db_target})

        if self.flavor == "vertebra":
            pg_mod = import_optional("vertebra.extensions.sqlspec.adapters.postgres")
            if pg_mod is None:
                msg = "vertebra.extensions.sqlspec.adapters.postgres is required for Vertebra SQLSpec PostgreSQL"
                raise RuntimeError(msg)
            broker = resolve_benchmark_broker(self.config.broker_mode, pool_size=self.pool_size)
            return pg_mod.PostgresSyncConfig(
                connection_config={
                    "dsn": self.db_target,
                    "min_size": 1,
                    "max_size": self.pool_size,
                },
                share_pool=self.share_pool,
                broker=broker,
            )

        if import_optional("psycopg") is None:
            msg = f"psycopg is required for upstream sync SQLSpec PostgreSQL ({POSTGRES_BENCH_INSTALL_HINT})"
            raise RuntimeError(msg)
        psycopg_sqlspec = import_optional("sqlspec.adapters.psycopg")
        if psycopg_sqlspec is None or not hasattr(psycopg_sqlspec, "PsycopgSyncConfig"):
            msg = "sqlspec.adapters.psycopg.PsycopgSyncConfig is required for upstream sync SQLSpec PostgreSQL"
            raise RuntimeError(msg)
        return psycopg_sqlspec.PsycopgSyncConfig(
            connection_config={"conninfo": self.db_target, "min_size": 1, "max_size": self.pool_size}
        )

    def _build_async_config(self) -> Any:
        if self.backend == "sqlite":
            if self.flavor == "vertebra":
                mod = import_optional("vertebra.extensions.sqlspec.adapters.sqlite")
                if mod is None:
                    msg = "vertebra.extensions.sqlspec.adapters.sqlite is required for Vertebra SQLSpec SQLite"
                    raise RuntimeError(msg)
                return mod.SQLiteAsyncConfig(
                    connection_config={"database": self.db_target},
                    share_pool=self.share_pool,
                )
            if import_optional("aiosqlite") is None:
                msg = "aiosqlite is required for upstream async SQLSpec SQLite"
                raise RuntimeError(msg)
            aio_mod = import_optional("sqlspec.adapters.aiosqlite")
            if aio_mod is None or not hasattr(aio_mod, "AiosqliteConfig"):
                msg = "sqlspec.adapters.aiosqlite.AiosqliteConfig is required for upstream async SQLSpec SQLite"
                raise RuntimeError(msg)
            return aio_mod.AiosqliteConfig(connection_config={"database": self.db_target})

        if self.flavor == "vertebra":
            pg_mod = import_optional("vertebra.extensions.sqlspec.adapters.postgres")
            if pg_mod is None:
                msg = "vertebra.extensions.sqlspec.adapters.postgres is required for Vertebra SQLSpec PostgreSQL"
                raise RuntimeError(msg)
            async_broker = resolve_async_broker_transport(self.config.broker_mode, pool_size=self.pool_size)
            return pg_mod.PostgresAsyncConfig(
                connection_config={
                    "dsn": self.db_target,
                    "min_size": 1,
                    "max_size": self.pool_size,
                },
                share_pool=self.share_pool,
                broker=async_broker,
            )

        if import_optional("asyncpg") is None:
            msg = f"asyncpg is required for upstream async SQLSpec PostgreSQL ({POSTGRES_BENCH_INSTALL_HINT})"
            raise RuntimeError(msg)
        asyncpg_sqlspec = import_optional("sqlspec.adapters.asyncpg")
        if asyncpg_sqlspec is None or not hasattr(asyncpg_sqlspec, "AsyncpgConfig"):
            msg = "sqlspec.adapters.asyncpg.AsyncpgConfig is required for upstream async SQLSpec PostgreSQL"
            raise RuntimeError(msg)
        return asyncpg_sqlspec.AsyncpgConfig(
            connection_config={"dsn": self.db_target, "min_size": 1, "max_size": self.pool_size}
        )

    def _ensure_sync_cfg(self) -> Any:
        with self._sync_lock:
            if self._sync_cfg is None:
                self._sync_cfg = self._build_sync_config()
            if self.backend == "postgres" and not self._pg_seeded:
                seed_postgres_world_sync(self.db_target)
                self._pg_seeded = True
            return self._sync_cfg

    def _loop_init_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._loop_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._async_locks[loop] = lock
            return lock

    async def _ensure_async_cfg(self) -> Any:
        loop = asyncio.get_running_loop()
        existing = self._async_cfgs.get(loop)
        if existing is not None:
            return existing
        async with self._loop_init_lock(loop):
            existing = self._async_cfgs.get(loop)
            if existing is not None:
                return existing
            cfg = self._build_async_config()
            if self.backend == "postgres" and not self._pg_seeded:
                await seed_postgres_world_async(self.db_target)
                self._pg_seeded = True
            self._async_cfgs[loop] = cfg
            return cfg

    def _select_sql(self, *, is_async: bool) -> str:
        if self.backend == "sqlite":
            return _SQLITE_SELECT_SQL
        if self.flavor == "upstream" and not is_async:
            return _PG_SELECT_PERCENT_SQL
        return _PG_SELECT_DOLLAR_SQL

    def _update_sql(self, *, is_async: bool) -> str:
        if self.backend == "sqlite":
            return _SQLITE_UPDATE_SQL
        if self.flavor == "upstream" and not is_async:
            return _PG_UPDATE_PERCENT_SQL
        return _PG_UPDATE_DOLLAR_SQL

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        cfg = self._ensure_sync_cfg()
        sql = self._select_sql(is_async=False)
        with cfg.provide_session() as session:
            if self.is_memory:
                _seed_sqlite_session_sqlspec_sync(session)
            rec = session.select_one(sql, (row_id,))
        return record_to_world_row(rec, row_id)

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        cfg = self._ensure_sync_cfg()
        sql = self._select_sql(is_async=False)
        with cfg.provide_session() as session:
            if self.is_memory:
                _seed_sqlite_session_sqlspec_sync(session)
            return [record_to_world_row(session.select_one(sql, (row_id,)), row_id) for row_id in row_ids]

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        cfg = self._ensure_sync_cfg()
        select_sql = self._select_sql(is_async=False)
        update_sql = self._update_sql(is_async=False)
        updated: list[WorldRow] = []
        with cfg.provide_session() as session:
            if self.is_memory:
                _seed_sqlite_session_sqlspec_sync(session)
            for row_id, new_value in updates:
                _ = session.select_one(select_sql, (row_id,))
                session.execute(update_sql, (new_value, row_id))
                updated.append(WorldRow(id=row_id, randomNumber=new_value))
            commit_fn = getattr(session, "commit", None)
            if callable(commit_fn):
                commit_fn()
        return updated

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        if self.flavor == "upstream":
            await ensure_task_context()
        cfg = await self._ensure_async_cfg()
        sql = self._select_sql(is_async=True)
        async with cfg.provide_session() as session:
            if self.is_memory:
                await _seed_sqlite_session_sqlspec_async(session)
            rec = await session.select_one(sql, (row_id,))
        return record_to_world_row(rec, row_id)

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        if self.flavor == "upstream":
            await ensure_task_context()
        cfg = await self._ensure_async_cfg()
        sql = self._select_sql(is_async=True)
        rows: list[WorldRow] = []
        async with cfg.provide_session() as session:
            if self.is_memory:
                await _seed_sqlite_session_sqlspec_async(session)
            for row_id in row_ids:
                rec = await session.select_one(sql, (row_id,))
                rows.append(record_to_world_row(rec, row_id))
        return rows

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        if self.flavor == "upstream":
            await ensure_task_context()
        cfg = await self._ensure_async_cfg()
        select_sql = self._select_sql(is_async=True)
        update_sql = self._update_sql(is_async=True)
        updated: list[WorldRow] = []
        async with cfg.provide_session() as session:
            if self.is_memory:
                await _seed_sqlite_session_sqlspec_async(session)
            for row_id, new_value in updates:
                _ = await session.select_one(select_sql, (row_id,))
                await session.execute(update_sql, (new_value, row_id))
                updated.append(WorldRow(id=row_id, randomNumber=new_value))
            commit_fn = getattr(session, "commit", None)
            if callable(commit_fn):
                res = commit_fn()
                if asyncio.iscoroutine(res):
                    await res
        return updated

    def close(self) -> None:
        with self._sync_lock:
            if self._sync_cfg is not None:
                close_pool = getattr(self._sync_cfg, "close_pool", None)
                if callable(close_pool):
                    with contextlib.suppress(Exception):
                        close_pool()
                self._sync_cfg = None
        with self._loop_lock:
            self._async_cfgs.clear()

    async def aclose(self) -> None:
        with self._loop_lock:
            cfgs = list(self._async_cfgs.values())
            self._async_cfgs.clear()
        for cfg in cfgs:
            close_pool = getattr(cfg, "close_pool", None)
            if callable(close_pool):
                with contextlib.suppress(Exception):
                    res = close_pool()
                    if asyncio.iscoroutine(res):
                        await res
        self.close()


_AA_MODEL_CACHE: dict[str, tuple[Any, Any, Any, Any, Any]] = {}
_AA_MODEL_LOCK = threading.Lock()


def _get_aa_types() -> tuple[Any, Any, Any, Any, Any]:
    """Lazily define declarative ``BenchWorldModel`` and Advanced Alchemy repository/service classes."""
    with _AA_MODEL_LOCK:
        cached = _AA_MODEL_CACHE.get("bundle")
        if cached is not None:
            return cached

        sa_mod = import_optional("sqlalchemy")
        sa_orm_mod = import_optional("sqlalchemy.orm")
        aa_repo_mod = import_optional("advanced_alchemy.repository")
        aa_svc_mod = import_optional("advanced_alchemy.service")
        if sa_mod is None or sa_orm_mod is None or aa_repo_mod is None or aa_svc_mod is None:
            msg = "sqlalchemy and advanced_alchemy are required when VTB_BENCH_DB_ADAPTER=advanced_alchemy"
            raise RuntimeError(msg)

        base_cls = type("_BenchWorldBase", (sa_orm_mod.DeclarativeBase,), {})
        model_cls = type(
            "BenchWorldModel",
            (base_cls,),
            {
                "__tablename__": "world",
                "id": sa_orm_mod.mapped_column(sa_mod.Integer, primary_key=True, autoincrement=False),
                "randomnumber": sa_orm_mod.mapped_column(sa_mod.Integer, nullable=False),
            },
        )
        sync_repo_cls = type("WorldSyncRepository", (aa_repo_mod.SQLAlchemySyncRepository,), {"model_type": model_cls})
        async_repo_cls = type(
            "WorldAsyncRepository",
            (aa_repo_mod.SQLAlchemyAsyncRepository,),
            {"model_type": model_cls},
        )
        sync_svc_cls = type(
            "WorldSyncService",
            (aa_svc_mod.SQLAlchemySyncRepositoryService,),
            {"repository_type": sync_repo_cls},
        )
        async_svc_cls = type(
            "WorldAsyncService",
            (aa_svc_mod.SQLAlchemyAsyncRepositoryService,),
            {"repository_type": async_repo_cls},
        )

        bundle: tuple[Any, Any, Any, Any, Any] = (base_cls, model_cls, sync_svc_cls, async_svc_cls, sa_mod)
        _AA_MODEL_CACHE["bundle"] = bundle
        return bundle


class AdvancedAlchemyWorldDriver:
    """Advanced Alchemy ``WorldDriver`` supporting Vertebra ``+vtb``/``+vtb_async`` and vanilla SQLAlchemy."""

    def __init__(self, config: DataBackendConfig, *, pool_size: int) -> None:
        self._base_cls, self._model_cls, self._sync_service_cls, self._async_service_cls, self._sa = _get_aa_types()
        self.config = config
        self.backend = config.db_backend.strip().lower()
        self.flavor = config.db_adapter_flavor.strip().lower()
        self.pool_size = max(1, pool_size)
        self.use_broker_pool = self.flavor == "vertebra" and config.broker_mode.strip().lower() == "shm_broker"
        self._sync_lock = threading.Lock()
        self._loop_lock = threading.Lock()
        self._sync_engine: Any = None
        self._sync_sessionmaker: Any = None
        self._async_engines: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, tuple[Any, Any]] = (
            weakref.WeakKeyDictionary()
        )
        self._async_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._pg_seeded = False

        if self.backend == "sqlite":
            self.db_target = normalize_sqlite_target(config.db_dsn)
            self.is_memory = self.db_target == ":memory:" or "mode=memory" in self.db_target
            if not self.is_memory:
                seed_sqlite_world_sync(self.db_target)
        elif self.backend == "postgres":
            self.db_target = require_postgres_dsn(config.db_dsn)
            self.is_memory = False
        else:
            msg = f"AdvancedAlchemyWorldDriver supports sqlite or postgres (got {config.db_backend!r})"
            raise ValueError(msg)

        if self.flavor == "vertebra" and import_optional("vertebra.extensions.sqlalchemy") is None:
            msg = "vertebra.extensions.sqlalchemy is required for Vertebra Advanced Alchemy benchmarks"
            raise RuntimeError(msg)

    def _seed_sqlite_engine_sync(self, engine: Any) -> None:
        self._base_cls.metadata.create_all(engine)
        with engine.begin() as conn:
            existing = int(conn.execute(self._sa.text("SELECT COUNT(*) FROM world")).scalar_one())
            if existing < WORLD_ROW_COUNT:
                conn.execute(
                    self._sa.text("INSERT OR IGNORE INTO world (id, randomnumber) VALUES (:id, :rn)"),
                    [{"id": row_id, "rn": rn} for row_id, rn in world_seed_rows()],
                )

    async def _seed_sqlite_engine_async(self, engine: Any) -> None:
        async with engine.begin() as conn:
            await conn.run_sync(self._base_cls.metadata.create_all)
            existing = int((await conn.execute(self._sa.text("SELECT COUNT(*) FROM world"))).scalar_one())
            if existing < WORLD_ROW_COUNT:
                await conn.execute(
                    self._sa.text("INSERT OR IGNORE INTO world (id, randomnumber) VALUES (:id, :rn)"),
                    [{"id": row_id, "rn": rn} for row_id, rn in world_seed_rows()],
                )

    def _ensure_sync_sessionmaker(self) -> Any:
        with self._sync_lock:
            if self._sync_sessionmaker is not None:
                return self._sync_sessionmaker
            if self.backend == "postgres" and self.flavor == "upstream" and import_optional("psycopg") is None:
                msg = (
                    f"psycopg is required for upstream sync Advanced Alchemy PostgreSQL ({POSTGRES_BENCH_INSTALL_HINT})"
                )
                raise RuntimeError(msg)
            sa_orm = import_optional("sqlalchemy.orm")
            assert sa_orm is not None
            url = rewrite_sqlalchemy_url(self.backend, self.db_target, flavor=self.flavor, is_async=False)
            engine_kwargs: dict[str, Any] = {}
            if self.flavor == "vertebra":
                engine_kwargs["connect_args"] = {"share_pool": False}
            if self.use_broker_pool:
                vtb_pool = import_optional("vertebra.extensions.sqlalchemy.pool")
                assert vtb_pool is not None
                sync_transport = resolve_sync_broker_transport(self.config.broker_mode, pool_size=self.pool_size)
                assert sync_transport is not None
                engine_kwargs["poolclass"] = vtb_pool.BrokerPool.bind_class(
                    broker=sync_transport,
                    url=url,
                )
                engine_kwargs["pool_size"] = self.pool_size
            elif not self.is_memory:
                engine_kwargs["pool_size"] = self.pool_size
            self._sync_engine = self._sa.create_engine(url, **engine_kwargs)
            if self.is_memory:
                self._seed_sqlite_engine_sync(self._sync_engine)
            elif self.backend == "postgres" and not self._pg_seeded:
                seed_postgres_world_sync(self.db_target)
                self._pg_seeded = True
            self._sync_sessionmaker = sa_orm.sessionmaker(bind=self._sync_engine, expire_on_commit=False)
            return self._sync_sessionmaker

    def _loop_init_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._loop_lock:
            lock = self._async_locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._async_locks[loop] = lock
            return lock

    async def _ensure_async_sessionmaker(self) -> Any:
        loop = asyncio.get_running_loop()
        existing = self._async_engines.get(loop)
        if existing is not None:
            return existing[1]
        async with self._loop_init_lock(loop):
            existing = self._async_engines.get(loop)
            if existing is not None:
                return existing[1]
            if self.backend == "sqlite" and self.flavor == "upstream" and import_optional("aiosqlite") is None:
                msg = "aiosqlite is required for upstream async Advanced Alchemy SQLite"
                raise RuntimeError(msg)
            if self.backend == "postgres" and self.flavor == "upstream" and import_optional("asyncpg") is None:
                msg = f"asyncpg is required for upstream async Advanced Alchemy PostgreSQL ({POSTGRES_BENCH_INSTALL_HINT})"
                raise RuntimeError(msg)
            sa_async = import_optional("sqlalchemy.ext.asyncio")
            assert sa_async is not None
            url = rewrite_sqlalchemy_url(self.backend, self.db_target, flavor=self.flavor, is_async=True)
            engine_kwargs: dict[str, Any] = {}
            if self.flavor == "vertebra":
                engine_kwargs["connect_args"] = {"share_pool": False}
            if self.use_broker_pool:
                vtb_pool = import_optional("vertebra.extensions.sqlalchemy.pool")
                assert vtb_pool is not None
                async_transport = resolve_async_broker_transport(self.config.broker_mode, pool_size=self.pool_size)
                assert async_transport is not None
                engine_kwargs["poolclass"] = vtb_pool.AsyncBrokerPool.bind_class(
                    broker=async_transport,
                    url=url,
                )
                engine_kwargs["pool_size"] = self.pool_size
            elif not self.is_memory:
                engine_kwargs["pool_size"] = self.pool_size
            engine = sa_async.create_async_engine(url, **engine_kwargs)
            if self.is_memory:
                await self._seed_sqlite_engine_async(engine)
            elif self.backend == "postgres" and not self._pg_seeded:
                await seed_postgres_world_async(self.db_target)
                self._pg_seeded = True
            sessionmaker = sa_async.async_sessionmaker(bind=engine, expire_on_commit=False)
            self._async_engines[loop] = (engine, sessionmaker)
            return sessionmaker

    def fetch_world_sync(self, row_id: int) -> WorldRow:
        session_factory = self._ensure_sync_sessionmaker()
        with session_factory() as session:
            service = self._sync_service_cls(session=session)
            model = service.get(row_id)
            return record_to_world_row(model, row_id)

    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]:
        session_factory = self._ensure_sync_sessionmaker()
        with session_factory() as session:
            service = self._sync_service_cls(session=session)
            return [record_to_world_row(service.get(row_id), row_id) for row_id in row_ids]

    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        session_factory = self._ensure_sync_sessionmaker()
        updated: list[WorldRow] = []
        with session_factory() as session:
            service = self._sync_service_cls(session=session)
            for row_id, new_value in updates:
                model = service.get(row_id)
                model.randomnumber = new_value
                updated.append(WorldRow(id=row_id, randomNumber=new_value))
            session.commit()
        return updated

    async def fetch_world_async(self, row_id: int) -> WorldRow:
        await ensure_task_context()
        session_factory = await self._ensure_async_sessionmaker()
        async with session_factory() as session:
            service = self._async_service_cls(session=session)
            model = await service.get(row_id)
            return record_to_world_row(model, row_id)

    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]:
        await ensure_task_context()
        session_factory = await self._ensure_async_sessionmaker()
        rows: list[WorldRow] = []
        async with session_factory() as session:
            service = self._async_service_cls(session=session)
            for row_id in row_ids:
                model = await service.get(row_id)
                rows.append(record_to_world_row(model, row_id))
        return rows

    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]:
        await ensure_task_context()
        session_factory = await self._ensure_async_sessionmaker()
        updated: list[WorldRow] = []
        async with session_factory() as session:
            service = self._async_service_cls(session=session)
            for row_id, new_value in updates:
                model = await service.get(row_id)
                model.randomnumber = new_value
                updated.append(WorldRow(id=row_id, randomNumber=new_value))
            await session.commit()
        return updated

    def close(self) -> None:
        with self._sync_lock:
            if self._sync_engine is not None:
                with contextlib.suppress(Exception):
                    self._sync_engine.dispose()
                self._sync_engine = None
            self._sync_sessionmaker = None
        with self._loop_lock:
            self._async_engines.clear()

    async def aclose(self) -> None:
        with self._loop_lock:
            entries = list(self._async_engines.values())
            self._async_engines.clear()
        for engine, _ in entries:
            with contextlib.suppress(Exception):
                await engine.dispose()
        self.close()


def build_orm_world_driver(config: DataBackendConfig, *, pool_size: int) -> WorldDriver:
    """Build the SQLSpec or Advanced Alchemy ``WorldDriver`` for ``config``."""
    adapter = config.db_adapter.strip().lower()
    if adapter == "sqlspec":
        return SQLSpecWorldDriver(config, pool_size=pool_size)
    if adapter == "advanced_alchemy":
        return AdvancedAlchemyWorldDriver(config, pool_size=pool_size)
    msg = f"Unsupported ORM db_adapter: {config.db_adapter!r}"
    raise ValueError(msg)
