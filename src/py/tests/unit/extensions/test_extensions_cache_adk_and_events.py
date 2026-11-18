"""Unit tests for Alchemy cache regions, SQLSpec ADK stores, CDC/Events backends, and reconcile stores."""

from __future__ import annotations

import ast
import asyncio
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from advanced_alchemy.cache import CacheManager
from advanced_alchemy.cache._null import NO_VALUE
from sqlspec.config import ADKConfig
from sqlspec.core import StatementConfig
from sqlspec.data_dictionary import VersionInfo
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import MigrationError
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.litestar.store import CompanionIndex
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import load_reconcile_callable, load_reconcile_downgrade_callable
from sqlspec.migrations.schema import DropStatement
from sqlspec.utils.serializers import from_json

from vertebra.db.cdc import CDCConfig, ChangeFeed
from vertebra.extensions.alchemy.cache import (
    AsyncCacheRegion,
    SyncCacheRegion,
    create_cache_config,
)
from vertebra.extensions.sqlspec.adapters import (
    PostgresAsyncConfig,
    PostgresSyncConfig,
)
from vertebra.extensions.sqlspec.adapters.duckdb import (
    DuckDBAsyncADKArtifactStore,
    DuckDBAsyncADKMemoryStore,
    DuckDBAsyncConfig,
    DuckDBSyncADKArtifactStore,
    DuckDBSyncADKMemoryStore,
    DuckDBSyncConfig,
)
from vertebra.extensions.sqlspec.adapters.duckdb.adk import _row_to_memory as _duckdb_row_to_memory
from vertebra.extensions.sqlspec.adapters.oracle import (
    OracleAsyncADKArtifactStore,
    OracleAsyncADKMemoryStore,
    OracleAsyncADKStore,
    OracleAsyncConfig,
    OracleAsyncEventsBackend,
    OracleEventQueueStore,
    OracleLitestarStore,
    OracleSyncADKArtifactStore,
    OracleSyncADKMemoryStore,
    OracleSyncADKStore,
    OracleSyncConfig,
    OracleSyncEventsBackend,
)
from vertebra.extensions.sqlspec.adapters.oracle import adk as oracle_adk_module
from vertebra.extensions.sqlspec.adapters.oracle import reconcile as oracle_reconcile
from vertebra.extensions.sqlspec.adapters.oracle.adk import OracleADKConfig
from vertebra.extensions.sqlspec.adapters.postgres import (
    PostgresAsyncADKArtifactStore,
    PostgresAsyncADKMemoryStore,
    PostgresAsyncADKStore,
    PostgresAsyncEventsBackend,
    PostgresEventQueueStore,
    PostgresLitestarStore,
    PostgresSyncADKArtifactStore,
    PostgresSyncADKMemoryStore,
    PostgresSyncADKStore,
    PostgresSyncEventsBackend,
)
from vertebra.extensions.sqlspec.adapters.postgres import adk as postgres_adk_module
from vertebra.extensions.sqlspec.adapters.postgres import reconcile as postgres_reconcile
from vertebra.extensions.sqlspec.adapters.postgres.adk import PostgresADKConfig
from vertebra.extensions.sqlspec.adapters.postgres.adk import _row_to_stored_memory as _pg_row_to_stored_memory
from vertebra.extensions.sqlspec.adapters.sqlite import (
    SQLiteAsyncADKMemoryStore,
    SQLiteAsyncConfig,
    SQLiteSyncADKMemoryStore,
    SQLiteSyncConfig,
)
from vertebra.extensions.sqlspec.events import ChangeFeedAsyncEventBackend, ChangeFeedSyncEventBackend


@pytest.mark.anyio
async def test_alchemy_cache_region_roundtrip_and_manager_invalidation(tmp_path: Path) -> None:
    """Alchemy cache regions preserve bytes vs str, wire CacheManager over file/memory/shm, and reject legacy classes."""
    from vertebra.cache import AsyncCacheStore, SyncCacheStore

    raw_entity_bytes = b'{"id": 1, "name": "Alice"}'
    raw_entity_str = '{"id": 1, "name": "Alice"}'

    file_region = SyncCacheRegion(SyncCacheStore(f"file://{tmp_path / 'l2_store'}"))
    file_region.set("entity:bytes", raw_entity_bytes)
    file_region.set("entity:str", raw_entity_str)
    file_region.set("entity:dict", {"id": 1, "ok": True})
    fetched_bytes = file_region.get("entity:bytes")
    fetched_str = file_region.get("entity:str")
    assert isinstance(fetched_bytes, bytes) and fetched_bytes == raw_entity_bytes
    assert isinstance(fetched_str, str) and fetched_str == raw_entity_str
    assert file_region.get("entity:dict") == {"id": 1, "ok": True}

    async_region = AsyncCacheRegion(AsyncCacheStore(f"file://{tmp_path / 'async_only_store'}"))
    await async_region.set("async:bytes", raw_entity_bytes)
    roundtripped = await async_region.get("async:bytes")
    assert isinstance(roundtripped, bytes) and roundtripped == raw_entity_bytes
    assert await async_region.get("missing:key") is NO_VALUE

    mem_cfg = create_cache_config("memory://")
    assert mem_cfg.region_factory is not None
    manager = CacheManager(mem_cfg)
    manager.set_sync("k1", {"ok": True})
    assert manager.get_sync("k1") == {"ok": True}
    manager.invalidate_all_sync()
    assert manager.get_sync("k1") is NO_VALUE


@pytest.mark.anyio
async def test_sqlspec_adk_tuple_mapping_artifact_versions_session_scope_and_search_errors() -> None:
    """ADK stores map positional memory tuples accurately, return StoredArtifact dicts, scope Oracle keys, and propagate search errors."""
    ts = datetime(2026, 11, 16, 8, 0, tzinfo=UTC)
    ins_ts = datetime(2026, 11, 16, 8, 5, tzinfo=UTC)

    duck_tuple = (
        "m-1",
        "s-1",
        "app",
        "u-1",
        "user",
        "ev-1",
        "assistant",
        ts,
        '{"text": "hi"}',
        "hi",
        '{"tag": "v1"}',
        [0.25, 0.75],
        ins_ts,
    )
    duck_mem = _duckdb_row_to_memory(duck_tuple, deserializer=from_json)
    assert duck_mem["metadata_json"] == {"tag": "v1"}
    assert duck_mem["embedding"] == [0.25, 0.75]
    assert duck_mem["inserted_at"] == ins_ts

    pg_tuple = (
        "m-2",
        "s-2",
        "app",
        "u-2",
        "user",
        "ev-2",
        "assistant",
        ts,
        '{"text": "pg"}',
        "pg",
        '{"k": "v"}',
        ins_ts,
        "[0.5, -0.5]",
    )
    pg_mem = _pg_row_to_stored_memory(pg_tuple)
    assert pg_mem["timestamp"] == ts
    assert pg_mem["inserted_at"] == ins_ts
    assert pg_mem["embedding"] == [0.5, -0.5]

    sample_art_rows = [
        {
            "app_name": "app",
            "user_id": "u1",
            "session_id": "s1",
            "filename": "report.pdf",
            "version": 1,
            "mime_type": "application/pdf",
            "canonical_uri": "memory://bucket/report_v1.pdf",
            "custom_metadata": '{"rev": 1}',
            "size": 128,
            "updated": ts,
            "signed_url": None,
            "created_at": ts,
        }
    ]

    class _StubSyncSessionDriver:
        def __init__(self, rows: list[dict[str, Any]], captured: list[tuple[str, Any]]) -> None:
            self._rows = rows
            self._captured = captured

        def select(self, query: str, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            self._captured.append((query, args or kwargs))
            return list(self._rows)

    class _StubAsyncSessionDriver:
        def __init__(self, rows: list[dict[str, Any]], captured: list[tuple[str, Any]]) -> None:
            self._rows = rows
            self._captured = captured

        async def select(self, query: str, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            self._captured.append((query, args or kwargs))
            return list(self._rows)

    class _StubSyncCfg:
        def __init__(self, rows: list[dict[str, Any]], *, fail_sql: str | None = None) -> None:
            self.extension_config: dict[str, Any] = {"adk": {"use_fts": True, "memory_use_fts": True}}
            self.rows = rows
            self.fail_sql = fail_sql
            self.captured: list[tuple[str, Any]] = []

        def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
            cfg = self

            class _Ctx:
                def __enter__(self) -> Any:
                    if cfg.fail_sql is not None:

                        class _FailingDriver:
                            def execute(self, *_a: Any, **_kw: Any) -> int:
                                return 0

                            def select(self, query: str, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
                                if cfg.fail_sql and cfg.fail_sql in query:
                                    raise RuntimeError("fts engine failure")
                                return []

                        return _FailingDriver()
                    return _StubSyncSessionDriver(cfg.rows, cfg.captured)

                def __exit__(self, *_exc: Any) -> None:
                    return None

            return _Ctx()

    class _StubAsyncCfg:
        def __init__(self, rows: list[dict[str, Any]], *, fail_sql: str | None = None) -> None:
            self.extension_config: dict[str, Any] = {"adk": {"use_fts": True, "memory_use_fts": True}}
            self.rows = rows
            self.fail_sql = fail_sql
            self.captured: list[tuple[str, Any]] = []

        def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
            cfg = self

            class _AsyncCtx:
                async def __aenter__(self) -> Any:
                    if cfg.fail_sql is not None:

                        class _FailingAsyncDriver:
                            async def execute(self, *_a: Any, **_kw: Any) -> int:
                                return 0

                            async def select(self, query: str, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
                                if cfg.fail_sql and cfg.fail_sql in query:
                                    raise RuntimeError("fts engine failure")
                                return []

                        return _FailingAsyncDriver()
                    return _StubAsyncSessionDriver(cfg.rows, cfg.captured)

                async def __aexit__(self, *_exc: Any) -> None:
                    return None

            return _AsyncCtx()

    duck_sync_art = DuckDBSyncADKArtifactStore(cast(DuckDBSyncConfig, _StubSyncCfg(sample_art_rows)))
    sync_versions = duck_sync_art.list_artifact_versions("app", "u1", "report.pdf", session_id="s1")
    assert len(sync_versions) == 1
    assert isinstance(sync_versions[0], dict)
    assert sync_versions[0]["canonical_uri"] == "memory://bucket/report_v1.pdf"

    duck_async_art = DuckDBAsyncADKArtifactStore(cast(DuckDBAsyncConfig, _StubAsyncCfg(sample_art_rows)))
    async_versions = await duck_async_art.list_artifact_versions("app", "u1", "report.pdf", session_id="s1")
    assert len(async_versions) == 1
    assert isinstance(async_versions[0], dict)
    assert async_versions[0]["version"] == 1

    ora_sync_cfg = _StubSyncCfg([{"filename": "a.txt"}, {"filename": "user:b.txt"}])
    ora_sync_art = OracleSyncADKArtifactStore(cast(OracleSyncConfig, ora_sync_cfg))
    assert ora_sync_art.list_artifact_keys("app", "u1", session_id="sess-9") == ["a.txt", "user:b.txt"]
    assert "session_id IN ('', :session_id)" in ora_sync_cfg.captured[-1][0]
    ora_sync_art.list_artifact_keys("app", "u1", session_id=None)
    assert "session_id = ''" in ora_sync_cfg.captured[-1][0]

    ora_async_cfg = _StubAsyncCfg([{"filename": "a.txt"}])
    ora_async_art = OracleAsyncADKArtifactStore(cast(OracleAsyncConfig, ora_async_cfg))
    assert await ora_async_art.list_artifact_keys("app", "u1", session_id="sess-9") == ["a.txt"]
    assert "session_id IN ('', :session_id)" in ora_async_cfg.captured[-1][0]
    await ora_async_art.list_artifact_keys("app", "u1", session_id=None)
    assert "session_id = ''" in ora_async_cfg.captured[-1][0]

    duck_fail_sync = DuckDBSyncADKMemoryStore(cast(DuckDBSyncConfig, _StubSyncCfg([], fail_sql="match_bm25")))
    with pytest.raises(RuntimeError, match="fts engine failure"):
        duck_fail_sync.search_entries("needle", "app", "u1")

    duck_fail_async = DuckDBAsyncADKMemoryStore(cast(DuckDBAsyncConfig, _StubAsyncCfg([], fail_sql="match_bm25")))
    with pytest.raises(RuntimeError, match="fts engine failure"):
        await duck_fail_async.search_entries("needle", "app", "u1")

    sqlite_fail_sync = SQLiteSyncADKMemoryStore(cast(SQLiteSyncConfig, _StubSyncCfg([], fail_sql="MATCH")))
    with pytest.raises(RuntimeError, match="fts engine failure"):
        sqlite_fail_sync.search_entries("needle", "app", "u1")

    sqlite_fail_async = SQLiteAsyncADKMemoryStore(cast(SQLiteAsyncConfig, _StubAsyncCfg([], fail_sql="MATCH")))
    with pytest.raises(RuntimeError, match="fts engine failure"):
        await sqlite_fail_async.search_entries("needle", "app", "u1")


@pytest.mark.anyio
async def test_sqlspec_events_async_pump_survives_timeout_sync_poll_interval_and_oracle_skip_locked() -> None:
    """Async event backends keep source.listen() alive across poll timeouts, honor sync poll_interval, and use SKIP LOCKED in Oracle."""

    class _FakeNotifyFrame:
        def __init__(self, channel: str, payload: str) -> None:
            self.channel = channel
            self.payload = payload

    class _PumpedAsyncNotifySource:
        def __init__(self) -> None:
            self.queue: asyncio.Queue[_FakeNotifyFrame] = asyncio.Queue()
            self.listen_calls = 0
            self.closed_by_cancel = False
            self.subscribed: list[str] = []

        async def subscribe(self, channels: list[str]) -> None:
            self.subscribed.extend(channels)

        async def unsubscribe(self, _channels: list[str]) -> None:
            return None

        async def publish(self, channel: str, payload: str | bytes) -> None:
            text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
            await self.queue.put(_FakeNotifyFrame(channel, text))

        async def close(self) -> None:
            return None

        async def listen(self) -> AsyncIterator[_FakeNotifyFrame]:
            self.listen_calls += 1
            try:
                while True:
                    item = await self.queue.get()
                    yield item
            except asyncio.CancelledError:
                self.closed_by_cancel = True
                raise

    pg_source = _PumpedAsyncNotifySource()
    pg_cfg = PostgresAsyncConfig(dsn="postgresql://localhost:5432/app", share_pool=False)
    pg_events = PostgresAsyncEventsBackend(pg_cfg, source=cast(Any, pg_source))
    try:
        assert await pg_events.dequeue("orders", poll_interval=0.01) is None
        assert await pg_events.dequeue("orders", poll_interval=0.01) is None
        assert pg_source.listen_calls == 1
        assert pg_source.closed_by_cancel is False

        ev_id = await pg_events.publish("orders", {"order_id": 101})
        msg = await pg_events.dequeue("orders", poll_interval=0.2)
        assert msg is not None
        assert msg.event_id == ev_id
        assert msg.payload == {"order_id": 101}
    finally:
        await pg_events.shutdown()

    ora_source = _PumpedAsyncNotifySource()
    ora_cfg = OracleAsyncConfig(dsn="oracle://scott:tiger@localhost:1521/FREEPDB1", share_pool=False)
    ora_events = OracleAsyncEventsBackend(ora_cfg, source=cast(Any, ora_source))
    try:
        assert await ora_events.dequeue("alerts", poll_interval=0.01) is None
        assert ora_source.listen_calls == 1
        assert ora_source.closed_by_cancel is False
        ora_ev_id = await ora_events.publish("alerts", {"severity": "high"})
        ora_msg = await ora_events.dequeue("alerts", poll_interval=0.2)
        assert ora_msg is not None
        assert ora_msg.event_id == ora_ev_id
        assert ora_msg.payload == {"severity": "high"}
    finally:
        await ora_events.shutdown()

    cf_source = _PumpedAsyncNotifySource()
    cf_async = ChangeFeedAsyncEventBackend(
        ChangeFeed(CDCConfig(channel="cdc_async")),
        notify_source=cast(Any, cf_source),
    )
    try:
        assert await cf_async.dequeue("stream_a", poll_interval=0.01) is None
        assert cf_source.listen_calls == 1
        assert cf_source.closed_by_cancel is False
        await cf_async.publish("stream_b", {"target": "b"})
        await cf_async.publish("stream_a", {"target": "a"})
        msg_a = await cf_async.dequeue("stream_a", poll_interval=0.2)
        msg_b = await cf_async.dequeue("stream_b", poll_interval=0.2)
        assert msg_a is not None and msg_a.payload == {"target": "a"}
        assert msg_b is not None and msg_b.payload == {"target": "b"}
    finally:
        await cf_async.shutdown()

    cf_sync = ChangeFeedSyncEventBackend(ChangeFeed(CDCConfig(channel="cdc_sync")))
    try:
        t0 = time.monotonic()
        assert cf_sync.dequeue("jobs", poll_interval=0.03) is None
        assert (time.monotonic() - t0) >= 0.02

        waiting = threading.Event()
        orig_wait = cf_sync._cond.wait

        def _signaling_wait(timeout: float | None = None) -> bool:
            waiting.set()
            return orig_wait(timeout=timeout)

        cast(Any, cf_sync._cond).wait = _signaling_wait

        def _delayed_pub() -> None:
            assert waiting.wait(timeout=5.0)
            cf_sync.publish("jobs", {"job_id": 7})

        pub_thread = threading.Thread(target=_delayed_pub)
        pub_thread.start()
        try:
            woken = cf_sync.dequeue("jobs", poll_interval=0.5)
        finally:
            cast(Any, cf_sync._cond).wait = orig_wait
        pub_thread.join()
        assert woken is not None and woken.payload == {"job_id": 7}
    finally:
        cf_sync.shutdown()

    executed_oracle_sql: list[str] = []

    class _OracleTableFallbackSyncCfg:
        extension_config: dict[str, Any] = {}

        def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
            class _Ctx:
                def __enter__(self) -> Any:
                    class _Drv:
                        def select_one_or_none(self, query: str, _params: Any = None) -> Any:
                            executed_oracle_sql.append(query)
                            return {
                                "event_id": "ev-ora-1",
                                "channel": "billing",
                                "payload_json": '{"ok": true}',
                                "metadata_json": None,
                                "attempts": 0,
                                "available_at": datetime.now(UTC),
                                "created_at": datetime.now(UTC),
                            }

                        def execute(self, query: str, _params: Any = None) -> int:
                            executed_oracle_sql.append(query)
                            return 1

                        def commit(self) -> None:
                            return None

                        def rollback(self) -> None:
                            return None

                    return _Drv()

                def __exit__(self, *_exc: Any) -> None:
                    return None

            return _Ctx()

    ora_table_sync = OracleSyncEventsBackend(cast(OracleSyncConfig, _OracleTableFallbackSyncCfg()))
    claimed = ora_table_sync.dequeue("billing", poll_interval=0.01)
    assert claimed is not None and claimed.event_id == "ev-ora-1"
    assert any("FOR UPDATE SKIP LOCKED" in q for q in executed_oracle_sql)
    assert any("AND status = 'pending'" in q for q in executed_oracle_sql)


class _PgMissingTableError(RuntimeError):
    """Simulated PostgreSQL 42P01 missing relation error."""

    sqlstate = "42P01"


class _PgExecResult:
    """Minimal execute result exposing affected row count."""

    __slots__ = ("rows_affected",)

    def __init__(self, rows_affected: int = 1) -> None:
        self.rows_affected = rows_affected


class _InMemoryPostgresADKState:
    """Stateful PostgreSQL ADK statement double recording queries and preserving scoped state."""

    def __init__(self, *, missing_table: bool = False) -> None:
        self.missing_table = missing_table
        self.sessions: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.app_states: dict[str, dict[str, Any]] = {}
        self.user_states: dict[tuple[str, str], dict[str, Any]] = {}
        self.memory_rows: list[dict[str, Any]] = []
        self.recorded: list[tuple[str, tuple[Any, ...]]] = []
        self.scripts: list[str] = []
        self.commits = 0

    def execute_script(self, script: str) -> None:
        self.scripts.append(script)

    def commit(self) -> None:
        self.commits += 1

    def select_one_or_none(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.recorded.append((query, args))
        if self.missing_table:
            raise _PgMissingTableError("relation does not exist")
        now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
        if query.startswith('INSERT INTO "adk_session"'):
            session_id, app_name, user_id = str(args[0]), str(args[1]), str(args[2])
            state_json = str(args[-1])
            row = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_json,
                "create_time": now,
                "update_time": now,
            }
            self.sessions[(app_name, user_id, session_id)] = row
            return dict(row)
        if query.startswith('UPDATE "adk_session" SET state ='):
            state_json, app_name, user_id, session_id = str(args[0]), str(args[1]), str(args[2]), str(args[3])
            key = (app_name, user_id, session_id)
            existing = self.sessions.get(key)
            if existing is None:
                return None
            existing["state"] = state_json
            existing["update_time"] = now
            return dict(existing)
        if 'FROM "adk_session"' in query:
            app_name, user_id, session_id = str(args[0]), str(args[1]), str(args[2])
            found_row = self.sessions.get((app_name, user_id, session_id))
            return dict(found_row) if found_row is not None else None
        if 'FROM "adk_app_state"' in query:
            app_name = str(args[0])
            app_row = self.app_states.get(app_name)
            return dict(app_row) if app_row is not None else None
        if 'FROM "adk_user_state"' in query:
            app_name, user_id = str(args[0]), str(args[1])
            user_row = self.user_states.get((app_name, user_id))
            return dict(user_row) if user_row is not None else None
        return None

    def select(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.recorded.append((query, args))
        if self.missing_table:
            raise _PgMissingTableError("relation does not exist")
        return []

    def execute(self, query: str, *args: Any) -> _PgExecResult:
        self.recorded.append((query, args))
        if self.missing_table:
            raise _PgMissingTableError("relation does not exist")
        if query.startswith('INSERT INTO "adk_app_state"') and "ON CONFLICT" in query:
            app_name, state_json = str(args[0]), str(args[1])
            if "DO NOTHING" in query:
                self.app_states.setdefault(app_name, {"state": state_json})
            else:
                self.app_states[app_name] = {"state": state_json}
            return _PgExecResult(1)
        if query.startswith('UPDATE "adk_app_state"'):
            state_json, app_name = str(args[0]), str(args[1])
            self.app_states[app_name] = {"state": state_json}
            return _PgExecResult(1)
        if query.startswith('INSERT INTO "adk_user_state"') and "ON CONFLICT" in query:
            app_name, user_id, state_json = str(args[0]), str(args[1]), str(args[2])
            if "DO NOTHING" in query:
                self.user_states.setdefault((app_name, user_id), {"state": state_json})
            else:
                self.user_states[(app_name, user_id)] = {"state": state_json}
            return _PgExecResult(1)
        if query.startswith('UPDATE "adk_user_state"'):
            state_json, app_name, user_id = str(args[0]), str(args[1]), str(args[2])
            self.user_states[(app_name, user_id)] = {"state": state_json}
            return _PgExecResult(1)
        if query.startswith('INSERT INTO "adk_event"'):
            self.events.append({"id": args[0], "app_name": args[1], "user_id": args[2], "session_id": args[3]})
            return _PgExecResult(1)
        if query.startswith('DELETE FROM "adk_memory" WHERE session_id = $1 AND app_name = $2 AND user_id = $3'):
            session_id, app_name, user_id = str(args[0]), str(args[1]), str(args[2])
            before = len(self.memory_rows)
            self.memory_rows = [
                row
                for row in self.memory_rows
                if not (row["session_id"] == session_id and row["app_name"] == app_name and row["user_id"] == user_id)
            ]
            return _PgExecResult(before - len(self.memory_rows))
        return _PgExecResult(0)


class _InMemoryPgADKSyncConfig:
    """Sync config double providing sessions over ``_InMemoryPostgresADKState``."""

    def __init__(self, state: _InMemoryPostgresADKState, adk_cfg: Mapping[str, Any] | None = None) -> None:
        self.state = state
        self.extension_config: dict[str, Any] = {"adk": dict(adk_cfg or {})}
        self.driver_features: dict[str, Any] = {}

    def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
        state = self.state

        class _Ctx:
            def __enter__(self) -> _InMemoryPostgresADKState:
                return state

            def __exit__(self, *_exc: Any) -> None:
                return None

        return _Ctx()


class _InMemoryPgADKAsyncConfig:
    """Async config double providing sessions over ``_InMemoryPostgresADKState``."""

    def __init__(self, state: _InMemoryPostgresADKState, adk_cfg: Mapping[str, Any] | None = None) -> None:
        self.state = state
        self.extension_config: dict[str, Any] = {"adk": dict(adk_cfg or {})}
        self.driver_features: dict[str, Any] = {}

    def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
        state = self.state

        class _AsyncDriver:
            async def execute_script(self, script: str) -> None:
                state.execute_script(script)

            async def commit(self) -> None:
                state.commit()

            async def select_one_or_none(self, query: str, *args: Any) -> dict[str, Any] | None:
                return state.select_one_or_none(query, *args)

            async def select(self, query: str, *args: Any) -> list[dict[str, Any]]:
                return state.select(query, *args)

            async def execute(self, query: str, *args: Any) -> _PgExecResult:
                return state.execute(query, *args)

        class _Ctx:
            async def __aenter__(self) -> _AsyncDriver:
                return _AsyncDriver()

            async def __aexit__(self, *_exc: Any) -> None:
                return None

        return _Ctx()


def test_postgres_adk_stores_subclass_upstream_and_require_google_adk() -> None:
    """PostgreSQL ADK stores subclass upstream bases, declare __slots__, align config keys, and fail loud without google-adk."""
    assert issubclass(PostgresSyncADKStore, BaseSyncADKStore)
    assert issubclass(PostgresAsyncADKStore, BaseAsyncADKStore)
    assert issubclass(PostgresSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(PostgresAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(PostgresSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(PostgresAsyncADKArtifactStore, BaseAsyncADKArtifactStore)
    assert not hasattr(postgres_adk_module, "_FallbackADKMemoryStoreCommon")
    assert not hasattr(postgres_adk_module, "_FallbackADKArtifactStoreCommon")

    for store_cls in (
        PostgresSyncADKStore,
        PostgresAsyncADKStore,
        PostgresSyncADKMemoryStore,
        PostgresAsyncADKMemoryStore,
        PostgresSyncADKArtifactStore,
        PostgresAsyncADKArtifactStore,
    ):
        assert "__slots__" in store_cls.__dict__

    expected_keys = {
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "fillfactor",
        "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_scale_factor",
        "vector_index_type",
        "vector_dimensions",
        "enable_bm25",
        "scann_num_leaves",
        "scann_quantizer",
        "memory_table",
        "artifact_table",
    }
    config_keys = set(PostgresADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    missing_script = (
        "import sys\n"
        "from vertebra.typing import MissingDependencyError\n"
        "sys.modules['google.adk'] = None\n"
        "try:\n"
        "    import vertebra.extensions.sqlspec.adapters.postgres.adk\n"
        "except MissingDependencyError as exc:\n"
        "    assert exc.install_package == 'vertebra-core[adk]'\n"
        "else:\n"
        "    raise AssertionError('Expected MissingDependencyError when google.adk is absent')\n"
    )
    subprocess.run([sys.executable, "-c", missing_script], check=True)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_postgres_adk_session_deltas_merged_state_and_missing_tables(is_async_mode: bool) -> None:
    """create_session and append_event_and_update_state apply scoped deltas atomically and return merged state."""
    state = _InMemoryPostgresADKState()
    ts = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    event_record: Any = {
        "id": "ev-1",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-1",
        "timestamp": ts,
        "event_data": {"author": "agent"},
    }
    bad_event_record: Any = {**event_record, "user_id": "other-user"}

    if not is_async_mode:
        sync_store = PostgresSyncADKStore(cast(PostgresSyncConfig, _InMemoryPgADKSyncConfig(state)))
        assert not hasattr(sync_store, "__dict__")
        created = sync_store.create_session(
            "sess-1",
            "app-1",
            "user-1",
            {"local": 1},
            app_state_delta={"app:theme": "dark"},
            user_state_delta={"user:lang": "en"},
        )
        assert created["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}

        updated = sync_store.append_event_and_update_state(
            event_record,
            "app-1",
            "user-1",
            "sess-1",
            {"local": 2},
            app_state_delta={"app:count": 5},
            user_state_delta={"user:tier": "pro"},
        )
        assert updated["state"] == {
            "local": 2,
            "app:theme": "dark",
            "app:count": 5,
            "user:lang": "en",
            "user:tier": "pro",
        }
        with pytest.raises(ValueError, match="event_record scope"):
            sync_store.append_event_and_update_state(bad_event_record, "app-1", "user-1", "sess-1", {})

        missing_store = PostgresSyncADKStore(
            cast(PostgresSyncConfig, _InMemoryPgADKSyncConfig(_InMemoryPostgresADKState(missing_table=True)))
        )
        assert missing_store.get_session("app-1", "user-1", "sess-1") is None
        assert missing_store.list_sessions("app-1", "user-1") == []
        assert missing_store.get_events("app-1", "user-1", "sess-1") == []
        assert missing_store.get_app_state("app-1") is None
        assert missing_store.get_user_state("app-1", "user-1") is None
        assert missing_store.get_metadata("k") is None
        return

    async_store = PostgresAsyncADKStore(cast(PostgresAsyncConfig, _InMemoryPgADKAsyncConfig(state)))
    assert not hasattr(async_store, "__dict__")
    created_async = await async_store.create_session(
        "sess-1",
        "app-1",
        "user-1",
        {"local": 1},
        app_state_delta={"app:theme": "dark"},
        user_state_delta={"user:lang": "en"},
    )
    assert created_async["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}

    updated_async = await async_store.append_event_and_update_state(
        event_record,
        "app-1",
        "user-1",
        "sess-1",
        {"local": 2},
        app_state_delta={"app:count": 5},
        user_state_delta={"user:tier": "pro"},
    )
    assert updated_async["state"] == {
        "local": 2,
        "app:theme": "dark",
        "app:count": 5,
        "user:lang": "en",
        "user:tier": "pro",
    }
    with pytest.raises(ValueError, match="event_record scope"):
        await async_store.append_event_and_update_state(bad_event_record, "app-1", "user-1", "sess-1", {})

    missing_async = PostgresAsyncADKStore(
        cast(PostgresAsyncConfig, _InMemoryPgADKAsyncConfig(_InMemoryPostgresADKState(missing_table=True)))
    )
    assert await missing_async.get_session("app-1", "user-1", "sess-1") is None
    assert await missing_async.list_sessions("app-1", "user-1") == []
    assert await missing_async.get_events("app-1", "user-1", "sess-1") == []
    assert await missing_async.get_app_state("app-1") is None
    assert await missing_async.get_user_state("app-1", "user-1") is None
    assert await missing_async.get_metadata("k") is None


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_postgres_adk_memory_delete_by_session_owner_scoping_and_ddl(is_async_mode: bool) -> None:
    """delete_entries_by_session scopes by session_id, app_name, and user_id; memory/artifact missing tables return empty."""
    state = _InMemoryPostgresADKState()
    state.memory_rows = [
        {"id": "1", "session_id": "s1", "app_name": "app-a", "user_id": "u1"},
        {"id": "2", "session_id": "s1", "app_name": "app-a", "user_id": "u2"},
        {"id": "3", "session_id": "s1", "app_name": "app-b", "user_id": "u1"},
    ]
    adk_cfg: PostgresADKConfig = {
        "vector_index_type": "scann",
        "vector_dimensions": 256,
        "scann_num_leaves": 64,
        "scann_quantizer": "SQ8",
        "enable_bm25": True,
        "memory_use_fts": True,
    }

    if not is_async_mode:
        sync_mem = PostgresSyncADKMemoryStore(cast(PostgresSyncConfig, _InMemoryPgADKSyncConfig(state, adk_cfg)))
        assert not hasattr(sync_mem, "__dict__")
        sync_mem.create_tables()
        assert "VECTOR(256)" in state.scripts[-1]
        assert "USING scann (embedding)" in state.scripts[-1]
        assert "USING bm25 (content_text)" in state.scripts[-1]
        deleted = sync_mem.delete_entries_by_session("s1", "app-a", "u1")
        assert deleted == 1
        assert [row["id"] for row in state.memory_rows] == ["2", "3"]
        del_query, del_params = state.recorded[-1]
        assert "WHERE session_id = $1 AND app_name = $2 AND user_id = $3" in del_query
        assert del_params == ("s1", "app-a", "u1")

        missing_state = _InMemoryPostgresADKState(missing_table=True)
        missing_mem = PostgresSyncADKMemoryStore(cast(PostgresSyncConfig, _InMemoryPgADKSyncConfig(missing_state)))
        assert missing_mem.search_entries("hello", "app-a", "u1") == []
        missing_art = PostgresSyncADKArtifactStore(cast(PostgresSyncConfig, _InMemoryPgADKSyncConfig(missing_state)))
        assert missing_art.get_artifact("app-a", "u1", "f.txt") is None
        assert missing_art.list_artifact_keys("app-a", "u1") == []
        assert missing_art.list_artifact_versions("app-a", "u1", "f.txt") == []
        return

    async_mem = PostgresAsyncADKMemoryStore(cast(PostgresAsyncConfig, _InMemoryPgADKAsyncConfig(state, adk_cfg)))
    assert not hasattr(async_mem, "__dict__")
    await async_mem.create_tables()
    assert "VECTOR(256)" in state.scripts[-1]
    assert "USING scann (embedding)" in state.scripts[-1]
    deleted_async = await async_mem.delete_entries_by_session("s1", "app-a", "u1")
    assert deleted_async == 1
    assert [row["id"] for row in state.memory_rows] == ["2", "3"]
    del_query, del_params = state.recorded[-1]
    assert "WHERE session_id = $1 AND app_name = $2 AND user_id = $3" in del_query
    assert del_params == ("s1", "app-a", "u1")

    missing_state = _InMemoryPostgresADKState(missing_table=True)
    missing_async_mem = PostgresAsyncADKMemoryStore(cast(PostgresAsyncConfig, _InMemoryPgADKAsyncConfig(missing_state)))
    assert await missing_async_mem.search_entries("hello", "app-a", "u1") == []
    missing_async_art = PostgresAsyncADKArtifactStore(
        cast(PostgresAsyncConfig, _InMemoryPgADKAsyncConfig(missing_state))
    )
    assert await missing_async_art.get_artifact("app-a", "u1", "f.txt") is None
    assert await missing_async_art.list_artifact_keys("app-a", "u1") == []
    assert await missing_async_art.list_artifact_versions("app-a", "u1", "f.txt") == []


class _OracleMissingTableError(RuntimeError):
    """Simulated Oracle ORA-00942 table-or-view-does-not-exist error."""


class _InMemoryOracleADKState:
    """Stateful Oracle ADK statement double supporting named-bind dicts and ORA-00942 checks."""

    def __init__(self, *, missing_table: bool = False) -> None:
        self.missing_table = missing_table
        self.sessions: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.app_states: dict[str, dict[str, Any]] = {}
        self.user_states: dict[tuple[str, str], dict[str, Any]] = {}
        self.memory_rows: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.recorded: list[tuple[str, dict[str, Any]]] = []
        self.scripts: list[str] = []
        self.commits = 0

    def execute_script(self, script: str) -> None:
        self.scripts.append(script)

    def commit(self) -> None:
        self.commits += 1

    def select_one_or_none(self, query: str, params: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        bound = dict(params or {})
        self.recorded.append((query, bound))
        if self.missing_table:
            raise _OracleMissingTableError("ORA-00942: table or view does not exist")
        if "FROM adk_session" in query:
            key = (str(bound["app_name"]), str(bound["user_id"]), str(bound["id"]))
            found = self.sessions.get(key)
            return dict(found) if found is not None else None
        if "FROM adk_app_state" in query:
            found_app = self.app_states.get(str(bound["app_name"]))
            return dict(found_app) if found_app is not None else None
        if "FROM adk_user_state" in query:
            found_user = self.user_states.get((str(bound["app_name"]), str(bound["user_id"])))
            return dict(found_user) if found_user is not None else None
        if "FROM adk_artifact" in query:
            matches = [
                row
                for row in self.artifacts
                if row["app_name"] == bound["app_name"]
                and row["user_id"] == bound["user_id"]
                and row["session_id"] == bound["session_id"]
                and row["filename"] == bound["filename"]
                and ("version" not in bound or row["version"] == bound["version"])
            ]
            if not matches:
                return None
            matches.sort(key=lambda item: int(item["version"]), reverse=True)
            return dict(matches[0])
        return None

    def select(self, query: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        bound = dict(params or {})
        self.recorded.append((query, bound))
        if self.missing_table:
            raise _OracleMissingTableError("ORA-00942: table or view does not exist")
        if "FROM adk_artifact" in query and "DISTINCT filename" in query:
            allowed = {"", str(bound["session_id"])} if "session_id IN ('', :session_id)" in query else {""}
            names = sorted(
                {
                    str(row["filename"])
                    for row in self.artifacts
                    if row["app_name"] == bound["app_name"]
                    and row["user_id"] == bound["user_id"]
                    and row["session_id"] in allowed
                }
            )
            return [{"filename": name} for name in names]
        if "FROM adk_artifact" in query:
            matches = [
                dict(row)
                for row in self.artifacts
                if row["app_name"] == bound["app_name"]
                and row["user_id"] == bound["user_id"]
                and row["session_id"] == bound["session_id"]
                and row["filename"] == bound["filename"]
            ]
            matches.sort(key=lambda item: int(item["version"]))
            return matches
        return []

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> _PgExecResult:
        bound = dict(params or {})
        self.recorded.append((query, bound))
        if self.missing_table:
            raise _OracleMissingTableError("ORA-00942: table or view does not exist")
        now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
        if query.startswith("INSERT INTO adk_session"):
            key = (str(bound["app_name"]), str(bound["user_id"]), str(bound["id"]))
            self.sessions[key] = {
                "id": bound["id"],
                "app_name": bound["app_name"],
                "user_id": bound["user_id"],
                "state": bound["state"],
                "create_time": now,
                "update_time": now,
            }
            return _PgExecResult(1)
        if query.startswith("UPDATE adk_session SET state ="):
            key = (str(bound["app_name"]), str(bound["user_id"]), str(bound["id"]))
            existing = self.sessions.get(key)
            if existing is None:
                return _PgExecResult(0)
            existing["state"] = bound["state"]
            existing["update_time"] = now
            return _PgExecResult(1)
        if query.startswith("INSERT INTO adk_app_state") or query.startswith("UPDATE adk_app_state"):
            self.app_states[str(bound["app_name"])] = {"state": bound["state"]}
            return _PgExecResult(1)
        if query.startswith("INSERT INTO adk_user_state") or query.startswith("UPDATE adk_user_state"):
            self.user_states[(str(bound["app_name"]), str(bound["user_id"]))] = {"state": bound["state"]}
            return _PgExecResult(1)
        if query.startswith("INSERT INTO adk_event"):
            self.events.append(bound)
            return _PgExecResult(1)
        if query.startswith(
            "DELETE FROM adk_memory WHERE session_id = :session_id AND app_name = :app_name AND user_id = :user_id"
        ):
            before = len(self.memory_rows)
            self.memory_rows = [
                row
                for row in self.memory_rows
                if not (
                    row["session_id"] == bound["session_id"]
                    and row["app_name"] == bound["app_name"]
                    and row["user_id"] == bound["user_id"]
                )
            ]
            return _PgExecResult(before - len(self.memory_rows))
        if query.startswith("MERGE INTO adk_artifact"):
            key_fields = ("app_name", "user_id", "session_id", "filename", "version")
            self.artifacts = [row for row in self.artifacts if any(row[k] != bound[k] for k in key_fields)]
            self.artifacts.append(
                {
                    "app_name": bound["app_name"],
                    "user_id": bound["user_id"],
                    "session_id": bound["session_id"],
                    "filename": bound["filename"],
                    "version": bound["version"],
                    "mime_type": bound.get("mime_type"),
                    "canonical_uri": bound["canonical_uri"],
                    "custom_metadata": bound.get("custom_metadata"),
                    "created_at": bound.get("created_at") or now,
                }
            )
            return _PgExecResult(1)
        if query.startswith("DELETE FROM adk_artifact WHERE"):
            before = len(self.artifacts)
            self.artifacts = [
                row
                for row in self.artifacts
                if not (
                    row["app_name"] == bound["app_name"]
                    and row["user_id"] == bound["user_id"]
                    and row["session_id"] == bound["session_id"]
                    and row["filename"] == bound["filename"]
                )
            ]
            return _PgExecResult(before - len(self.artifacts))
        return _PgExecResult(0)


class _InMemoryOracleADKSyncConfig:
    """Sync config double providing sessions over ``_InMemoryOracleADKState``."""

    def __init__(self, state: _InMemoryOracleADKState, adk_cfg: Mapping[str, Any] | None = None) -> None:
        self.state = state
        self.extension_config: dict[str, Any] = {"adk": dict(adk_cfg or {})}
        self.driver_features: dict[str, Any] = {}

    def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
        state = self.state

        class _Ctx:
            def __enter__(self) -> _InMemoryOracleADKState:
                return state

            def __exit__(self, *_exc: Any) -> None:
                return None

        return _Ctx()


class _InMemoryOracleADKAsyncConfig:
    """Async config double providing sessions over ``_InMemoryOracleADKState``."""

    def __init__(self, state: _InMemoryOracleADKState, adk_cfg: Mapping[str, Any] | None = None) -> None:
        self.state = state
        self.extension_config: dict[str, Any] = {"adk": dict(adk_cfg or {})}
        self.driver_features: dict[str, Any] = {}

    def provide_session(self, *_args: Any, **_kwargs: Any) -> Any:
        state = self.state

        class _AsyncDriver:
            async def execute_script(self, script: str) -> None:
                state.execute_script(script)

            async def commit(self) -> None:
                state.commit()

            async def select_one_or_none(
                self, query: str, params: Mapping[str, Any] | None = None
            ) -> dict[str, Any] | None:
                return state.select_one_or_none(query, params)

            async def select(self, query: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
                return state.select(query, params)

            async def execute(self, query: str, params: Mapping[str, Any] | None = None) -> _PgExecResult:
                return state.execute(query, params)

        class _Ctx:
            async def __aenter__(self) -> _AsyncDriver:
                return _AsyncDriver()

            async def __aexit__(self, *_exc: Any) -> None:
                return None

        return _Ctx()


def test_oracle_adk_stores_subclass_upstream_and_require_google_adk() -> None:
    """Oracle ADK stores subclass upstream bases, declare __slots__, align config keys, and fail loud without google-adk."""
    assert issubclass(OracleSyncADKStore, BaseSyncADKStore)
    assert issubclass(OracleAsyncADKStore, BaseAsyncADKStore)
    assert issubclass(OracleSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(OracleAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(OracleSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(OracleAsyncADKArtifactStore, BaseAsyncADKArtifactStore)
    assert not hasattr(oracle_adk_module, "_FallbackADKMemoryStoreCommon")
    assert not hasattr(oracle_adk_module, "_FallbackADKArtifactStoreCommon")

    for store_cls in (
        OracleSyncADKStore,
        OracleAsyncADKStore,
        OracleSyncADKMemoryStore,
        OracleAsyncADKMemoryStore,
        OracleSyncADKArtifactStore,
        OracleAsyncADKArtifactStore,
        oracle_adk_module._OracleADKSessionSQLMixin,
        oracle_adk_module._OracleADKMemorySQLMixin,
        oracle_adk_module._OracleADKArtifactSQLMixin,
    ):
        assert "__slots__" in store_cls.__dict__

    expected_keys = {
        "compress_events",
        "retention_days",
        "memory_table",
        "artifact_table",
        "use_fts",
        "vector_index_type",
        "vector_dimensions",
        "inmemory",
        "in_memory",
        "json_storage",
        "compression",
        "partitioning",
        "session_table_options",
        "events_table_options",
        "app_state_table_options",
        "user_state_table_options",
        "memory_table_options",
    }
    config_keys = set(OracleADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    missing_script = (
        "import sys\n"
        "from vertebra.typing import MissingDependencyError\n"
        "sys.modules['google.adk'] = None\n"
        "try:\n"
        "    import vertebra.extensions.sqlspec.adapters.oracle.adk\n"
        "except MissingDependencyError as exc:\n"
        "    assert exc.install_package == 'vertebra-core[adk]'\n"
        "else:\n"
        "    raise AssertionError('Expected MissingDependencyError when google.adk is absent')\n"
    )
    subprocess.run([sys.executable, "-c", missing_script], check=True)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_oracle_adk_session_deltas_merged_state_and_missing_tables(is_async_mode: bool) -> None:
    """Oracle ADK create_session and append_event_and_update_state merge scoped deltas atomically and tolerate ORA-00942."""
    state = _InMemoryOracleADKState()
    ts = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    event_record: Any = {
        "id": "ev-1",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-1",
        "timestamp": ts,
        "event_data": {"author": "agent"},
    }
    bad_event_record: Any = {**event_record, "user_id": "other-user"}
    adk_cfg: OracleADKConfig = {
        "compress_events": True,
        "inmemory": True,
        "json_storage": "json",
        "retention_days": 30,
        "vector_index_type": "hnsw",
        "vector_dimensions": 256,
        "use_fts": True,
    }

    if not is_async_mode:
        real_cfg = OracleSyncConfig(dsn="oracle://user:pass@localhost:1521/freepdb1", extension_config={"adk": adk_cfg})
        configured_store = OracleSyncADKStore(real_cfg)
        assert not hasattr(configured_store, "__dict__")

        sync_store = OracleSyncADKStore(cast(OracleSyncConfig, _InMemoryOracleADKSyncConfig(state, adk_cfg)))
        sync_store.create_tables()
        assert any("INMEMORY" in script for script in state.scripts)
        assert any("ROW STORE COMPRESS ADVANCED" in script for script in state.scripts)

        created = sync_store.create_session(
            "sess-1",
            "app-1",
            "user-1",
            {"local": 1},
            app_state_delta={"app:theme": "dark"},
            user_state_delta={"user:lang": "en"},
        )
        assert created["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}

        updated = sync_store.append_event_and_update_state(
            event_record,
            "app-1",
            "user-1",
            "sess-1",
            {"local": 2},
            app_state_delta={"app:count": 5},
            user_state_delta={"user:tier": "pro"},
        )
        assert updated["state"] == {
            "local": 2,
            "app:theme": "dark",
            "app:count": 5,
            "user:lang": "en",
            "user:tier": "pro",
        }
        with pytest.raises(ValueError, match="event_record scope"):
            sync_store.append_event_and_update_state(bad_event_record, "app-1", "user-1", "sess-1", {})

        missing_store = OracleSyncADKStore(
            cast(OracleSyncConfig, _InMemoryOracleADKSyncConfig(_InMemoryOracleADKState(missing_table=True)))
        )
        assert missing_store.get_session("app-1", "user-1", "sess-1") is None
        assert missing_store.list_sessions("app-1", "user-1") == []
        assert missing_store.get_events("app-1", "user-1", "sess-1") == []
        assert missing_store.get_app_state("app-1") is None
        assert missing_store.get_user_state("app-1", "user-1") is None
        assert missing_store.get_metadata("k") is None
        return

    real_async_cfg = OracleAsyncConfig(
        dsn="oracle://user:pass@localhost:1521/freepdb1", extension_config={"adk": adk_cfg}
    )
    configured_async = OracleAsyncADKStore(real_async_cfg)
    assert not hasattr(configured_async, "__dict__")

    async_store = OracleAsyncADKStore(cast(OracleAsyncConfig, _InMemoryOracleADKAsyncConfig(state, adk_cfg)))
    await async_store.create_tables()
    assert any("INMEMORY" in script for script in state.scripts)
    assert any("ROW STORE COMPRESS ADVANCED" in script for script in state.scripts)

    created_async = await async_store.create_session(
        "sess-1",
        "app-1",
        "user-1",
        {"local": 1},
        app_state_delta={"app:theme": "dark"},
        user_state_delta={"user:lang": "en"},
    )
    assert created_async["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}

    updated_async = await async_store.append_event_and_update_state(
        event_record,
        "app-1",
        "user-1",
        "sess-1",
        {"local": 2},
        app_state_delta={"app:count": 5},
        user_state_delta={"user:tier": "pro"},
    )
    assert updated_async["state"] == {
        "local": 2,
        "app:theme": "dark",
        "app:count": 5,
        "user:lang": "en",
        "user:tier": "pro",
    }
    with pytest.raises(ValueError, match="event_record scope"):
        await async_store.append_event_and_update_state(bad_event_record, "app-1", "user-1", "sess-1", {})

    missing_async = OracleAsyncADKStore(
        cast(OracleAsyncConfig, _InMemoryOracleADKAsyncConfig(_InMemoryOracleADKState(missing_table=True)))
    )
    assert await missing_async.get_session("app-1", "user-1", "sess-1") is None
    assert await missing_async.list_sessions("app-1", "user-1") == []
    assert await missing_async.get_events("app-1", "user-1", "sess-1") == []
    assert await missing_async.get_app_state("app-1") is None
    assert await missing_async.get_user_state("app-1", "user-1") is None
    assert await missing_async.get_metadata("k") is None


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_oracle_adk_memory_delete_by_session_owner_scoping_and_artifacts(is_async_mode: bool) -> None:
    """Oracle memory delete_entries_by_session scopes by owner and artifact stores preserve empty-session sentinel and versions."""
    state = _InMemoryOracleADKState()
    state.memory_rows = [
        {"id": "1", "session_id": "s1", "app_name": "app-a", "user_id": "u1"},
        {"id": "2", "session_id": "s1", "app_name": "app-a", "user_id": "u2"},
        {"id": "3", "session_id": "s1", "app_name": "app-b", "user_id": "u1"},
    ]
    adk_cfg: OracleADKConfig = {
        "vector_dimensions": 256,
        "use_fts": True,
        "json_storage": "json",
        "retention_days": 14,
    }
    ts = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    art_v0: Any = {
        "app_name": "app-a",
        "user_id": "u1",
        "session_id": None,
        "filename": "user_pref.json",
        "version": 0,
        "mime_type": "application/json",
        "canonical_uri": "memory://bucket/user_pref_v0.json",
        "custom_metadata": None,
        "created_at": ts,
    }
    art_v1: Any = {
        "app_name": "app-a",
        "user_id": "u1",
        "session_id": "s1",
        "filename": "report.pdf",
        "version": 1,
        "mime_type": "application/pdf",
        "canonical_uri": "memory://bucket/report_v1.pdf",
        "custom_metadata": {"rev": 1},
        "created_at": ts,
    }

    if not is_async_mode:
        sync_mem = OracleSyncADKMemoryStore(cast(OracleSyncConfig, _InMemoryOracleADKSyncConfig(state, adk_cfg)))
        assert not hasattr(sync_mem, "__dict__")
        sync_mem.create_tables()
        assert "VECTOR(256, FLOAT32)" in state.scripts[-1]
        assert "CTXSYS.CONTEXT" in state.scripts[-1]
        deleted = sync_mem.delete_entries_by_session("s1", "app-a", "u1")
        assert deleted == 1
        assert [row["id"] for row in state.memory_rows] == ["2", "3"]
        del_query, del_params = state.recorded[-1]
        assert "WHERE session_id = :session_id AND app_name = :app_name AND user_id = :user_id" in del_query
        assert del_params == {"session_id": "s1", "app_name": "app-a", "user_id": "u1"}

        sync_art = OracleSyncADKArtifactStore(cast(OracleSyncConfig, _InMemoryOracleADKSyncConfig(state, adk_cfg)))
        sync_art.insert_artifact(art_v0)
        sync_art.insert_artifact(art_v1)
        assert sync_art.list_artifact_keys("app-a", "u1", session_id="s1") == ["report.pdf", "user_pref.json"]
        assert sync_art.list_artifact_keys("app-a", "u1", session_id=None) == ["user_pref.json"]
        versions = sync_art.list_artifact_versions("app-a", "u1", "report.pdf", session_id="s1")
        assert len(versions) == 1
        assert versions[0]["canonical_uri"] == "memory://bucket/report_v1.pdf"
        removed = sync_art.delete_artifact("app-a", "u1", "report.pdf", session_id="s1")
        assert len(removed) == 1
        assert sync_art.list_artifact_keys("app-a", "u1", session_id="s1") == ["user_pref.json"]

        missing_state = _InMemoryOracleADKState(missing_table=True)
        missing_mem = OracleSyncADKMemoryStore(cast(OracleSyncConfig, _InMemoryOracleADKSyncConfig(missing_state)))
        assert missing_mem.search_entries("hello", "app-a", "u1") == []
        assert missing_mem.delete_entries_by_session("s1", "app-a", "u1") == 0
        missing_art = OracleSyncADKArtifactStore(cast(OracleSyncConfig, _InMemoryOracleADKSyncConfig(missing_state)))
        assert missing_art.get_artifact("app-a", "u1", "f.txt") is None
        assert missing_art.list_artifact_keys("app-a", "u1") == []
        assert missing_art.list_artifact_versions("app-a", "u1", "f.txt") == []
        return

    async_mem = OracleAsyncADKMemoryStore(cast(OracleAsyncConfig, _InMemoryOracleADKAsyncConfig(state, adk_cfg)))
    assert not hasattr(async_mem, "__dict__")
    await async_mem.create_tables()
    assert "VECTOR(256, FLOAT32)" in state.scripts[-1]
    assert "CTXSYS.CONTEXT" in state.scripts[-1]
    deleted_async = await async_mem.delete_entries_by_session("s1", "app-a", "u1")
    assert deleted_async == 1
    assert [row["id"] for row in state.memory_rows] == ["2", "3"]
    del_query, del_params = state.recorded[-1]
    assert "WHERE session_id = :session_id AND app_name = :app_name AND user_id = :user_id" in del_query
    assert del_params == {"session_id": "s1", "app_name": "app-a", "user_id": "u1"}

    async_art = OracleAsyncADKArtifactStore(cast(OracleAsyncConfig, _InMemoryOracleADKAsyncConfig(state, adk_cfg)))
    await async_art.insert_artifact(art_v0)
    await async_art.insert_artifact(art_v1)
    assert await async_art.list_artifact_keys("app-a", "u1", session_id="s1") == ["report.pdf", "user_pref.json"]
    assert await async_art.list_artifact_keys("app-a", "u1", session_id=None) == ["user_pref.json"]
    versions_async = await async_art.list_artifact_versions("app-a", "u1", "report.pdf", session_id="s1")
    assert len(versions_async) == 1
    assert versions_async[0]["canonical_uri"] == "memory://bucket/report_v1.pdf"
    removed_async = await async_art.delete_artifact("app-a", "u1", "report.pdf", session_id="s1")
    assert len(removed_async) == 1
    assert await async_art.list_artifact_keys("app-a", "u1", session_id="s1") == ["user_pref.json"]

    missing_state = _InMemoryOracleADKState(missing_table=True)
    missing_async_mem = OracleAsyncADKMemoryStore(cast(OracleAsyncConfig, _InMemoryOracleADKAsyncConfig(missing_state)))
    assert await missing_async_mem.search_entries("hello", "app-a", "u1") == []
    assert await missing_async_mem.delete_entries_by_session("s1", "app-a", "u1") == 0
    missing_async_art = OracleAsyncADKArtifactStore(
        cast(OracleAsyncConfig, _InMemoryOracleADKAsyncConfig(missing_state))
    )
    assert await missing_async_art.get_artifact("app-a", "u1", "f.txt") is None
    assert await missing_async_art.list_artifact_keys("app-a", "u1") == []
    assert await missing_async_art.list_artifact_versions("app-a", "u1", "f.txt") == []


class _RecordingBaseDataDictionary:
    def __init__(
        self,
        *,
        tables: list[dict[str, Any]] | None = None,
        indexes: list[dict[str, Any]] | None = None,
        columns: dict[str, list[dict[str, Any]]] | None = None,
        version: VersionInfo | None = None,
    ) -> None:
        self.tables: list[dict[str, Any]] = list(tables or [])
        self.indexes: list[dict[str, Any]] = list(indexes or [])
        self.columns: dict[str, list[dict[str, Any]]] = dict(columns or {})
        self.version = version or VersionInfo(23, 0, 0)
        self.calls: list[tuple[str, Any]] = []

    def _get_tables(self, schema: str | None = None) -> list[dict[str, Any]]:
        self.calls.append(("get_tables", schema))
        return list(self.tables)

    def _get_columns(self, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        self.calls.append(("get_columns", (table, schema)))
        return list(self.columns.get(table, []))

    def _get_indexes(self, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        self.calls.append(("get_indexes", (table, schema)))
        return list(self.indexes)

    def _get_version(self) -> VersionInfo:
        self.calls.append(("get_version", None))
        return self.version


class _RecordingMigrationDataDictionary(_RecordingBaseDataDictionary):
    def get_tables(self, _driver: Any, *, schema: str | None = None) -> list[dict[str, Any]]:
        return self._get_tables(schema)

    def get_columns(self, _driver: Any, table: str, *, schema: str | None = None) -> list[dict[str, Any]]:
        return self._get_columns(table, schema)

    def get_indexes(self, _driver: Any, *, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        return self._get_indexes(table, schema)

    def get_version(self, _driver: Any) -> VersionInfo:
        return self._get_version()


class _RecordingAsyncMigrationDataDictionary(_RecordingBaseDataDictionary):
    async def get_tables(self, _driver: Any, *, schema: str | None = None) -> list[dict[str, Any]]:
        return self._get_tables(schema)

    async def get_columns(self, _driver: Any, table: str, *, schema: str | None = None) -> list[dict[str, Any]]:
        return self._get_columns(table, schema)

    async def get_indexes(self, _driver: Any, *, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        return self._get_indexes(table, schema)

    async def get_version(self, _driver: Any) -> VersionInfo:
        return self._get_version()


class _RecordingSyncMigrationDriver(SyncDriverAdapterBase):
    def __init__(
        self,
        data_dictionary: _RecordingMigrationDataDictionary | None = None,
        *,
        select_rows: dict[str, list[Any]] | None = None,
        execute_error: Exception | None = None,
    ) -> None:
        super().__init__(connection=None, statement_config=StatementConfig())
        self._data_dictionary = data_dictionary or _RecordingMigrationDataDictionary()
        self.scripts: list[str] = []
        self.executed: list[tuple[str, Any]] = []
        self.selects: list[tuple[str, Any]] = []
        self._select_rows = dict(select_rows or {})
        self._execute_error = execute_error

    @property
    def data_dictionary(self) -> Any:
        return self._data_dictionary

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def dispatch_execute(self, cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise NotImplementedError

    def dispatch_execute_many(self, cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise NotImplementedError

    def dispatch_special_handling(self, cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        return None

    def handle_database_exceptions(self) -> Any:
        raise NotImplementedError

    def with_cursor(self, connection: Any) -> Any:
        _ = connection
        raise NotImplementedError

    def execute_script(self, sql_text: str, *parameters: Any, **kwargs: Any) -> Any:
        _ = (parameters, kwargs)
        self.scripts.append(sql_text)

    def execute(self, sql_text: Any, *parameters: Any, **kwargs: Any) -> Any:
        _ = kwargs
        param_arg = parameters[0] if parameters else None
        self.executed.append((str(sql_text), param_arg))
        if self._execute_error is not None:
            raise self._execute_error

    def select(self, sql_text: Any, *parameters: Any, **kwargs: Any) -> list[Any]:
        _ = kwargs
        param_arg = parameters[0] if parameters else None
        sql_str = str(sql_text)
        self.selects.append((sql_str, param_arg))
        for marker, rows in self._select_rows.items():
            if marker in sql_str:
                return list(rows)
        return []


class _RecordingAsyncMigrationDriver(AsyncDriverAdapterBase):
    def __init__(
        self,
        data_dictionary: _RecordingAsyncMigrationDataDictionary | None = None,
        *,
        select_rows: dict[str, list[Any]] | None = None,
        execute_error: Exception | None = None,
    ) -> None:
        super().__init__(connection=None, statement_config=StatementConfig())
        self._data_dictionary = data_dictionary or _RecordingAsyncMigrationDataDictionary()
        self.scripts: list[str] = []
        self.executed: list[tuple[str, Any]] = []
        self.selects: list[tuple[str, Any]] = []
        self._select_rows = dict(select_rows or {})
        self._execute_error = execute_error

    @property
    def data_dictionary(self) -> Any:
        return self._data_dictionary

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def dispatch_execute(self, cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise NotImplementedError

    async def dispatch_execute_many(self, cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        raise NotImplementedError

    async def dispatch_special_handling(self, cursor: Any, statement: Any) -> Any:
        _ = (cursor, statement)
        return None

    def handle_database_exceptions(self) -> Any:
        raise NotImplementedError

    def with_cursor(self, connection: Any) -> Any:
        _ = connection
        raise NotImplementedError

    async def execute_script(self, sql_text: str, *parameters: Any, **kwargs: Any) -> Any:
        _ = (parameters, kwargs)
        self.scripts.append(sql_text)

    async def execute(self, sql_text: Any, *parameters: Any, **kwargs: Any) -> Any:
        _ = kwargs
        param_arg = parameters[0] if parameters else None
        self.executed.append((str(sql_text), param_arg))
        if self._execute_error is not None:
            raise self._execute_error

    async def select(self, sql_text: Any, *parameters: Any, **kwargs: Any) -> list[Any]:
        _ = kwargs
        param_arg = parameters[0] if parameters else None
        sql_str = str(sql_text)
        self.selects.append((sql_str, param_arg))
        for marker, rows in self._select_rows.items():
            if marker in sql_str:
                return list(rows)
        return []


@pytest.mark.parametrize(
    "cls",
    [
        PostgresEventQueueStore,
        PostgresSyncEventsBackend,
        PostgresAsyncEventsBackend,
        PostgresLitestarStore,
        OracleEventQueueStore,
        OracleSyncEventsBackend,
        OracleAsyncEventsBackend,
        OracleLitestarStore,
    ],
)
def test_postgres_and_oracle_event_litestar_stores_declare_slots(cls: type[Any]) -> None:
    """Every Postgres and Oracle event/Litestar store and backend declares __slots__ and future annotations."""
    assert "__slots__" in cls.__dict__
    module = sys.modules[cls.__module__]
    assert module.__file__ is not None
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    future_imports = [node for node in tree.body if isinstance(node, ast.ImportFrom) and node.module == "__future__"]
    assert any(alias.name == "annotations" for node in future_imports for alias in node.names)


@pytest.mark.parametrize(
    ("engine", "store_cls", "ext_key", "table_key", "table_name", "expected_schema", "expected_table"),
    [
        (
            "postgres",
            PostgresEventQueueStore,
            "events",
            "queue_table",
            "sqlspec_event_queue",
            None,
            "sqlspec_event_queue",
        ),
        ("postgres", PostgresEventQueueStore, "events", "queue_table", "app_schema.event_q", "app_schema", "event_q"),
        ("postgres", PostgresLitestarStore, "litestar", "session_table", "litestar_session", None, "litestar_session"),
        (
            "postgres",
            PostgresLitestarStore,
            "litestar",
            "session_table",
            "app_schema.sess_tbl",
            "app_schema",
            "sess_tbl",
        ),
        ("oracle", OracleEventQueueStore, "events", "queue_table", "sqlspec_event_queue", None, "sqlspec_event_queue"),
        ("oracle", OracleEventQueueStore, "events", "queue_table", "app_schema.event_q", "app_schema", "event_q"),
        ("oracle", OracleLitestarStore, "litestar", "session_table", "litestar_session", None, "litestar_session"),
        ("oracle", OracleLitestarStore, "litestar", "session_table", "app_schema.sess_tbl", "app_schema", "sess_tbl"),
    ],
)
def test_postgres_and_oracle_store_schema_targets_companions_and_drops(
    engine: str,
    store_cls: type[Any],
    ext_key: str,
    table_key: str,
    table_name: str,
    expected_schema: str | None,
    expected_table: str,
) -> None:
    """Verify get_schema_targets, CompanionIndex definitions, and downgrade_drops for bare and qualified tables."""
    if engine == "postgres":
        cfg: Any = PostgresSyncConfig(
            dsn="postgresql://localhost:5432/app",
            share_pool=False,
            extension_config={ext_key: {table_key: table_name}},
        )
    else:
        cfg = OracleSyncConfig(
            dsn="oracle://scott:tiger@localhost:1521/FREEPDB1",
            share_pool=False,
            extension_config={ext_key: {table_key: table_name}},
        )

    store = store_cls(cfg)
    targets = store.get_schema_targets()
    assert len(targets) == 1
    assert targets[0].table_name == expected_table
    assert targets[0].schema == expected_schema

    companions = store.get_companion_indexes()
    assert len(companions) == 1
    assert isinstance(companions[0], CompanionIndex)
    assert companions[0].table == expected_table
    assert companions[0].schema == expected_schema
    if engine == "postgres":
        expected_predicate = "WHERE expires_at IS NOT NULL" if ext_key == "litestar" else "WHERE status = 'pending'"
        assert expected_predicate in companions[0].ddl
    else:
        assert "IF NOT EXISTS" not in companions[0].ddl
        assert "SQLCODE NOT IN (-955, -1408)" in companions[0].ddl

    drops = store.downgrade_drops()
    assert len(drops) == 2
    assert all(isinstance(item, DropStatement) for item in drops)
    assert "DROP INDEX" in drops[0].statement
    assert "DROP TABLE" in drops[1].statement


def test_oracle_event_queue_store_native_backend_targets() -> None:
    """Verify OracleEventQueueStore validates and expands aq / txeventq native targets."""
    default_cfg = OracleSyncConfig(
        dsn="oracle://scott:tiger@localhost:1521/FREEPDB1",
        share_pool=False,
        extension_config={"events": {"backend": "aq"}},
    )
    assert OracleEventQueueStore(default_cfg).native_backend_targets("poll_queue") == ()
    assert OracleEventQueueStore(default_cfg).native_backend_targets("aq") == ("SQLSPEC_EVENTS_QUEUE",)

    templated_cfg = OracleSyncConfig(
        dsn="oracle://scott:tiger@localhost:1521/FREEPDB1",
        share_pool=False,
        extension_config={
            "events": {
                "backend": "txeventq",
                "aq_queue": "vtb_q_{channel}",
                "aq_channels": ["orders", "alerts", "orders"],
            }
        },
    )
    assert OracleEventQueueStore(templated_cfg).native_backend_targets("txeventq") == (
        "VTB_Q_ORDERS",
        "VTB_Q_ALERTS",
    )

    missing_channels_cfg = OracleSyncConfig(
        dsn="oracle://scott:tiger@localhost:1521/FREEPDB1",
        share_pool=False,
        extension_config={"events": {"backend": "aq", "aq_queue": "vtb_q_{channel}"}},
    )
    with pytest.raises(MigrationError, match="aq_channels"):
        OracleEventQueueStore(missing_channels_cfg).native_backend_targets("aq")

    qualified_queue_cfg = OracleSyncConfig(
        dsn="oracle://scott:tiger@localhost:1521/FREEPDB1",
        share_pool=False,
        extension_config={"events": {"backend": "aq", "aq_queue": "schema.bad_queue"}},
    )
    with pytest.raises(MigrationError, match="bare unquoted"):
        OracleEventQueueStore(qualified_queue_cfg).native_backend_targets("aq")

    old_oracle_cfg = OracleSyncConfig(
        dsn="oracle://scott:tiger@localhost:1521/FREEPDB1",
        share_pool=False,
        extension_config={"events": {"backend": "aq"}},
    )
    old_oracle_cfg._oracle_version_cache.set(VersionInfo(19, 3, 0))
    with pytest.raises(MigrationError, match="Oracle 21c"):
        OracleEventQueueStore(old_oracle_cfg).native_backend_targets("aq")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("engine", "is_async"),
    [
        ("postgres", False),
        ("postgres", True),
        ("oracle", False),
        ("oracle", True),
    ],
)
async def test_postgres_and_oracle_reconcile_roundtrip_sync_and_async(engine: str, is_async: bool) -> None:
    """Verify reconcile() and downgrade()/down() round-trips across Litestar, Events, and ADK."""
    reconciler = postgres_reconcile if engine == "postgres" else oracle_reconcile

    def _make_cfg(ext_cfg: dict[str, Any]) -> Any:
        if engine == "postgres":
            return (
                PostgresAsyncConfig(dsn="postgresql://localhost:5432/app", share_pool=False, extension_config=ext_cfg)
                if is_async
                else PostgresSyncConfig(
                    dsn="postgresql://localhost:5432/app", share_pool=False, extension_config=ext_cfg
                )
            )
        return (
            OracleAsyncConfig(
                dsn="oracle://scott:tiger@localhost:1521/FREEPDB1", share_pool=False, extension_config=ext_cfg
            )
            if is_async
            else OracleSyncConfig(
                dsn="oracle://scott:tiger@localhost:1521/FREEPDB1", share_pool=False, extension_config=ext_cfg
            )
        )

    def _make_driver(
        dd: _RecordingMigrationDataDictionary | None = None,
        *,
        select_rows: dict[str, list[Any]] | None = None,
        execute_error: Exception | None = None,
    ) -> Any:
        if is_async:
            async_dd = _RecordingAsyncMigrationDataDictionary(
                tables=dd.tables if dd else None,
                indexes=dd.indexes if dd else None,
                columns=dd.columns if dd else None,
                version=dd.version if dd else None,
            )
            return _RecordingAsyncMigrationDriver(async_dd, select_rows=select_rows, execute_error=execute_error)
        return _RecordingSyncMigrationDriver(dd, select_rows=select_rows, execute_error=execute_error)

    async def _run(fn: Any, ctx: MigrationContext) -> None:
        res = fn(ctx)
        if is_async:
            assert res is not None
            await res
        else:
            assert res is None

    ls_cfg = _make_cfg({"litestar": {"session_table": "custom_sessions"}})
    ls_driver = _make_driver()
    await _run(reconciler.reconcile, MigrationContext(config=ls_cfg, driver=ls_driver))
    assert any("CREATE TABLE" in stmt and "custom_sessions" in stmt for stmt in ls_driver.scripts)
    assert any("expires_at" in stmt for stmt in ls_driver.scripts)

    ls_existing_dd = _RecordingMigrationDataDictionary(
        tables=[{"table_name": "custom_sessions"}],
        columns={
            "custom_sessions": [
                {"column_name": "session_id"},
                {"column_name": "data"},
                {"column_name": "expires_at"},
                {"column_name": "created_at"},
                {"column_name": "updated_at"},
            ]
        },
        indexes=[
            {
                "table_name": "custom_sessions",
                "index_name": ("idx_custom_sessions_expires_at" if engine == "postgres" else "idx_custom_sessions_exp"),
            }
        ],
    )
    ls_noop_driver = _make_driver(ls_existing_dd)
    await _run(reconciler.reconcile, MigrationContext(config=ls_cfg, driver=ls_noop_driver))
    assert ls_noop_driver.scripts == []

    await _run(reconciler.down, MigrationContext(config=ls_cfg, driver=ls_driver))
    assert any("DROP INDEX" in stmt for stmt in ls_driver.scripts)
    assert any("DROP TABLE" in stmt and "custom_sessions" in stmt for stmt in ls_driver.scripts)

    if engine == "postgres":
        notify_cfg = _make_cfg({"events": {"backend": "notify"}})
        notify_driver = _make_driver()
        await _run(reconciler.reconcile, MigrationContext(config=notify_cfg, driver=notify_driver))
        assert notify_driver.scripts == []

        ev_cfg = _make_cfg({"events": {"backend": "notify_queue", "queue_table": "vtb_events"}})
        ev_driver = _make_driver()
        await _run(reconciler.reconcile, MigrationContext(config=ev_cfg, driver=ev_driver))
        assert any("CREATE TABLE" in stmt and "vtb_events" in stmt for stmt in ev_driver.scripts)
        assert any("WHERE status = 'pending'" in stmt for stmt in ev_driver.scripts)
        await _run(reconciler.downgrade, MigrationContext(config=ev_cfg, driver=ev_driver))
        assert any("DROP TABLE" in stmt and "vtb_events" in stmt for stmt in ev_driver.scripts)
    else:
        aq_cfg = _make_cfg({"events": {"backend": "aq", "aq_queue": "vtb_aq_q"}})
        aq_driver = _make_driver()
        await _run(reconciler.reconcile, MigrationContext(config=aq_cfg, driver=aq_driver))
        assert any("CREATE TABLE" in stmt and "sqlspec_event_queue" in stmt for stmt in aq_driver.scripts)
        assert any("CREATE_QUEUE_TABLE" in sql_text for sql_text, _ in aq_driver.executed)
        assert any("CREATE_QUEUE(" in sql_text for sql_text, _ in aq_driver.executed)
        assert any("START_QUEUE(" in sql_text for sql_text, _ in aq_driver.executed)

        txq_cfg = _make_cfg({"events": {"backend": "txeventq", "aq_queue": "vtb_txq"}})
        txq_driver = _make_driver()
        await _run(reconciler.reconcile, MigrationContext(config=txq_cfg, driver=txq_driver))
        assert any("CREATE_TRANSACTIONAL_EVENT_QUEUE" in sql_text for sql_text, _ in txq_driver.executed)

        no_create_cfg = _make_cfg({"events": {"backend": "aq", "aq_queue": "vtb_aq_q", "create_schema": False}})
        existing_ev_dd = _RecordingMigrationDataDictionary(
            tables=[{"table_name": "sqlspec_event_queue"}],
            columns={
                "sqlspec_event_queue": [
                    {"column_name": "event_id"},
                    {"column_name": "channel"},
                    {"column_name": "payload_json"},
                    {"column_name": "metadata_json"},
                    {"column_name": "status"},
                    {"column_name": "available_at"},
                    {"column_name": "lease_expires_at"},
                    {"column_name": "attempts"},
                    {"column_name": "created_at"},
                ]
            },
            indexes=[{"table_name": "sqlspec_event_queue", "index_name": "idx_sqlspec_event_queue_ch_st"}],
        )
        with pytest.raises(MigrationError, match="create_schema=False"):
            await _run(
                reconciler.reconcile, MigrationContext(config=no_create_cfg, driver=_make_driver(existing_ev_dd))
            )

        priv_driver = _make_driver(execute_error=RuntimeError("ORA-01031: insufficient privileges"))
        with pytest.raises(MigrationError, match="DBMS_AQADM"):
            await _run(reconciler.reconcile, MigrationContext(config=aq_cfg, driver=priv_driver))

    adk_cfg = _make_cfg({"adk": {"enable_sessions": True, "enable_memory": True}})
    adk_driver = _make_driver()
    await _run(reconciler.reconcile, MigrationContext(config=adk_cfg, driver=adk_driver))
    assert any("adk_session" in stmt for stmt in adk_driver.scripts)
    assert any("adk_memory" in stmt for stmt in adk_driver.scripts)
    await _run(reconciler.downgrade, MigrationContext(config=adk_cfg, driver=adk_driver))
    drop_scripts = [stmt for stmt in adk_driver.scripts if "DROP TABLE" in stmt]
    assert len(drop_scripts) >= 6
    assert "adk_memory" in drop_scripts[0]


@pytest.mark.parametrize("engine", ["postgres", "oracle"])
def test_reconcile_module_isolation_and_error_semantics(engine: str) -> None:
    """Importing reconcile.py and running Litestar/Events never imports ADK stores or google.adk, and validates inputs."""
    cfg_cls_name = "PostgresSyncConfig" if engine == "postgres" else "OracleSyncConfig"
    dsn_literal = repr(
        "postgresql://localhost:5432/app" if engine == "postgres" else "oracle://scott:tiger@localhost:1521/FREEPDB1"
    )
    probe_script = (
        "import sys\n"
        f"import vertebra.extensions.sqlspec.adapters.{engine}.reconcile as rec\n"
        "from sqlspec.core import StatementConfig\n"
        "from sqlspec.driver import SyncDriverAdapterBase\n"
        "from sqlspec.migrations.context import MigrationContext\n"
        f"from vertebra.extensions.sqlspec.adapters.{engine} import {cfg_cls_name}\n"
        "assert 'google.adk' not in sys.modules\n"
        f"assert 'vertebra.extensions.sqlspec.adapters.{engine}.adk' not in sys.modules\n"
        "class _DD:\n"
        "    def get_tables(self, _d, *, schema=None): return []\n"
        "    def get_columns(self, _d, _t, *, schema=None): return []\n"
        "    def get_indexes(self, _d, *, table, schema=None): return []\n"
        "    def get_version(self, _d): return None\n"
        "class _Drv(SyncDriverAdapterBase):\n"
        "    def __init__(self):\n"
        "        super().__init__(connection=None, statement_config=StatementConfig())\n"
        "        self._dd = _DD()\n"
        "    @property\n"
        "    def data_dictionary(self): return self._dd\n"
        "    def begin(self): pass\n"
        "    def commit(self): pass\n"
        "    def rollback(self): pass\n"
        "    def dispatch_execute(self, _c, _s): raise NotImplementedError\n"
        "    def dispatch_execute_many(self, _c, _s): raise NotImplementedError\n"
        "    def dispatch_special_handling(self, _c, _s): return None\n"
        "    def handle_database_exceptions(self): raise NotImplementedError\n"
        "    def with_cursor(self, _c): raise NotImplementedError\n"
        "    def execute_script(self, _s): pass\n"
        "    def execute(self, _s, _p=None): pass\n"
        "    def select(self, _s, _p=None): return []\n"
        f"cfg = {cfg_cls_name}(dsn={dsn_literal}, share_pool=False, extension_config={{'litestar': {{}}, 'events': {{'backend': 'poll_queue'}}}})\n"
        "rec.reconcile(MigrationContext(config=cfg, driver=_Drv()))\n"
        "assert 'google.adk' not in sys.modules\n"
        f"assert 'vertebra.extensions.sqlspec.adapters.{engine}.adk' not in sys.modules\n"
        "print('ISOLATED_OK')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe_script],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "ISOLATED_OK" in proc.stdout

    reconciler = postgres_reconcile if engine == "postgres" else oracle_reconcile
    sync_driver = _RecordingSyncMigrationDriver()
    async_driver = _RecordingAsyncMigrationDriver()

    with pytest.raises(MigrationError, match="context.config"):
        reconciler.reconcile(MigrationContext(config=None, driver=cast(Any, sync_driver)))

    sync_cfg: Any = (
        PostgresSyncConfig(dsn="postgresql://localhost:5432/app", share_pool=False)
        if engine == "postgres"
        else OracleSyncConfig(dsn="oracle://scott:tiger@localhost:1521/FREEPDB1", share_pool=False)
    )
    async_cfg: Any = (
        PostgresAsyncConfig(dsn="postgresql://localhost:5432/app", share_pool=False)
        if engine == "postgres"
        else OracleAsyncConfig(dsn="oracle://scott:tiger@localhost:1521/FREEPDB1", share_pool=False)
    )

    with pytest.raises(MigrationError, match="context.driver"):
        reconciler.reconcile(MigrationContext(config=sync_cfg, driver=None))

    with pytest.raises(MigrationError, match="sync-async mismatch"):
        reconciler.reconcile(MigrationContext(config=sync_cfg, driver=cast(Any, async_driver)))

    with pytest.raises(MigrationError, match="sync-async mismatch"):
        reconciler.reconcile(MigrationContext(config=async_cfg, driver=cast(Any, sync_driver)))

    with pytest.raises(MigrationError, match="migration_mode"):
        reconciler.reconcile(
            MigrationContext(
                config=sync_cfg,
                driver=cast(Any, sync_driver),
                extension_config={"litestar": {"migration_mode": "invalid_mode"}},
            )
        )


@pytest.mark.parametrize(
    ("engine", "use_path_spec"),
    [
        ("postgres", False),
        ("postgres", True),
        ("oracle", False),
        ("oracle", True),
    ],
)
def test_postgres_and_oracle_add_extension_migrations_discovery(engine: str, use_path_spec: bool) -> None:
    """Verify add_extension_migrations discovers and executes adapter reconcile.py for vertebra_* adapters."""
    reconcile_mod = postgres_reconcile if engine == "postgres" else oracle_reconcile
    assert reconcile_mod.__file__ is not None
    spec: str | Path = (
        Path(reconcile_mod.__file__).parent if use_path_spec else f"vertebra.extensions.sqlspec:adapters/{engine}"
    )
    cfg: Any = (
        PostgresSyncConfig(dsn="postgresql://localhost:5432/app", share_pool=False)
        if engine == "postgres"
        else OracleSyncConfig(dsn="oracle://scott:tiger@localhost:1521/FREEPDB1", share_pool=False)
    )
    cfg.add_extension_migrations("litestar", spec, settings={"session_table": "discovered_sessions"})
    cfg.add_extension_migrations("events", spec, settings={"backend": "poll_queue", "queue_table": "discovered_events"})

    commands = cfg.get_migration_commands()
    reconciler_names = [desc.name for desc in commands.extension_reconcilers]
    assert reconciler_names == ["litestar", "events"]

    driver = _RecordingSyncMigrationDriver()
    for desc in commands.extension_reconcilers:
        up_fn = load_reconcile_callable(desc.module_path, extension_name=desc.name)
        down_fn = load_reconcile_downgrade_callable(desc.module_path, extension_name=desc.name)
        assert callable(down_fn)
        ctx = MigrationContext.from_config(cfg)
        ctx.driver = cast(Any, driver)
        ctx.extension_config = dict(cfg.extension_config[desc.name])
        assert up_fn(ctx) is None
        assert down_fn(ctx) is None

    assert any("discovered_sessions" in stmt for stmt in driver.scripts)
    assert any("discovered_events" in stmt for stmt in driver.scripts)
