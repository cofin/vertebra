# ruff: noqa: S608
"""SQLite ADK store for Vertebra."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, MemoryRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

SECONDS_PER_DAY = 86400.0
JULIAN_EPOCH = 2440587.5

__all__ = ("SqliteADKMemoryStore", "SqliteADKStore", "SyncSqliteADKMemoryStore", "SyncSqliteADKStore")


def _datetime_to_julian(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta_days = (dt - epoch).total_seconds() / SECONDS_PER_DAY
    return JULIAN_EPOCH + delta_days


def _julian_to_datetime(julian: float) -> datetime:
    days_since_epoch = julian - JULIAN_EPOCH
    timestamp = days_since_epoch * SECONDS_PER_DAY
    return datetime.fromtimestamp(timestamp, tz=UTC)


def _to_sqlite_bool(value: bool | None) -> int | None:
    if value is None:
        return None
    return 1 if value else 0


def _from_sqlite_bool(value: int | None) -> bool | None:
    if value is None:
        return None
    return bool(value)


class SqliteADKStore(BaseAsyncADKStore["VertebraSqliteAsyncConfig"]):
    """SQLite ADK store for Vertebra.

    Mirrors sqlspec.adapters.aiosqlite.adk.store.AiosqliteADKStore but adapted
    for Vertebra's broker-backed SQLite driver.
    """

    __slots__ = ()

    async def _get_create_sessions_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time REAL NOT NULL,
            update_time REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            invocation_id TEXT NOT NULL,
            author TEXT NOT NULL,
            actions BLOB NOT NULL,
            long_running_tool_ids_json TEXT,
            branch TEXT,
            timestamp REAL NOT NULL,
            content TEXT,
            grounding_metadata TEXT,
            custom_metadata TEXT,
            partial INTEGER,
            turn_complete INTEGER,
            interrupted INTEGER,
            error_code TEXT,
            error_message TEXT,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        async with self.config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        now = datetime.now(UTC)
        now_julian = _datetime_to_julian(now)
        state_json = to_json(state) if state else "{}"

        async with await self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table}
                (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                await conn.execute(sql, [session_id, app_name, user_id, owner_id, state_json, now_julian, now_julian])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                await conn.execute(sql, [session_id, app_name, user_id, state_json, now_julian, now_julian])

        record = await self.get_session(session_id)
        if record is None:
            msg = f"Failed to create session {session_id}"
            raise RuntimeError(msg)
        return record

    async def get_session(self, session_id: str) -> SessionRecord | None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = ?
        """

        async with self.config.provide_session() as driver:
            result = await driver.execute(sql, [session_id])
            row = result.one_or_none()

            if row is None:
                return None

            return SessionRecord(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=from_json(row["state"]) if row["state"] else {},
                create_time=_julian_to_datetime(row["create_time"]),
                update_time=_julian_to_datetime(row["update_time"]),
            )

    async def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        now_julian = _datetime_to_julian(datetime.now(UTC))
        state_json = to_json(state) if state else "{}"

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE id = ?
        """

        async with await self.config.provide_connection() as conn:
            await conn.execute(sql, [state_json, now_julian, session_id])

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = ?"

        async with await self.config.provide_connection() as conn:
            await conn.execute(sql, [session_id])

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ?
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ? AND user_id = ?
            ORDER BY update_time DESC
            """
            params = [app_name, user_id]

        async with self.config.provide_session() as driver:
            result = await driver.execute(sql, params)
            rows = result.all()

            return [
                SessionRecord(
                    id=row["id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    state=from_json(row["state"]) if row["state"] else {},
                    create_time=_julian_to_datetime(row["create_time"]),
                    update_time=_julian_to_datetime(row["update_time"]),
                )
                for row in rows
            ]

    async def append_event(self, event_record: EventRecord) -> None:
        timestamp_julian = _datetime_to_julian(event_record["timestamp"])

        content_json = to_json(event_record.get("content")) if event_record.get("content") else None
        grounding_metadata_json = (
            to_json(event_record.get("grounding_metadata")) if event_record.get("grounding_metadata") else None
        )
        custom_metadata_json = (
            to_json(event_record.get("custom_metadata")) if event_record.get("custom_metadata") else None
        )

        partial_int = _to_sqlite_bool(event_record.get("partial"))
        turn_complete_int = _to_sqlite_bool(event_record.get("turn_complete"))
        interrupted_int = _to_sqlite_bool(event_record.get("interrupted"))

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        async with await self.config.provide_connection() as conn:
            await conn.execute(
                sql,
                [
                    event_record["id"],
                    event_record["session_id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["actions"],
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    timestamp_julian,
                    content_json,
                    grounding_metadata_json,
                    custom_metadata_json,
                    partial_int,
                    turn_complete_int,
                    interrupted_int,
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ],
            )

    async def get_events(
        self, session_id: str, after_timestamp: datetime | None = None, limit: int | None = None
    ) -> list[EventRecord]:
        where_clauses = ["session_id = ?"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > ?")
            params.append(_datetime_to_julian(after_timestamp))

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        async with self.config.provide_session() as driver:
            result = await driver.execute(sql, params)
            rows = result.all()

            return [
                EventRecord(
                    id=row["id"],
                    session_id=row["session_id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    invocation_id=row["invocation_id"],
                    author=row["author"],
                    actions=bytes(cast("bytes", row["actions"])),
                    long_running_tool_ids_json=row["long_running_tool_ids_json"],
                    branch=row["branch"],
                    timestamp=_julian_to_datetime(row["timestamp"]),
                    content=from_json(row["content"]) if row["content"] else None,
                    grounding_metadata=from_json(row["grounding_metadata"]) if row["grounding_metadata"] else None,
                    custom_metadata=from_json(row["custom_metadata"]) if row["custom_metadata"] else None,
                    partial=_from_sqlite_bool(row["partial"]),
                    turn_complete=_from_sqlite_bool(row["turn_complete"]),
                    interrupted=_from_sqlite_bool(row["interrupted"]),
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]


class SqliteADKMemoryStore(BaseAsyncADKMemoryStore["VertebraSqliteAsyncConfig"]):
    """SQLite ADK memory store for Vertebra."""

    __slots__ = ()

    async def _get_create_memory_table_sql(self) -> str:
        fts_table = ""
        if self._use_fts:
            fts_table = f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {self._memory_table}_fts USING fts5(
            content_text,
            content={self._memory_table},
            content_rowid=rowid
        );

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ai AFTER INSERT ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ad AFTER DELETE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_au AFTER UPDATE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            author TEXT,
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_table}
        """

    def _get_drop_memory_table_sql(self) -> list[str]:
        statements = [f"DROP TABLE IF EXISTS {self._memory_table}"]
        if self._use_fts:
            statements.extend([
                f"DROP TABLE IF EXISTS {self._memory_table}_fts",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ai",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ad",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_au",
            ])
        return statements

    async def create_tables(self) -> None:
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: list[MemoryRecord], owner_id: object | None = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        async with await self._config.provide_connection() as conn:
            for entry in entries:
                params: list[Any]
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = [
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        _datetime_to_julian(entry["timestamp"]),
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        _datetime_to_julian(entry["inserted_at"]),
                    ]
                else:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = [
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        _datetime_to_julian(entry["timestamp"]),
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        _datetime_to_julian(entry["inserted_at"]),
                    ]
                rowcount = await conn.execute(sql, params)
                if rowcount:
                    inserted_count += rowcount
        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: int | None = None
    ) -> list[MemoryRecord]:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        if self._use_fts:
            sql = f"""
            SELECT m.* FROM {self._memory_table} AS m
            JOIN {self._memory_table}_fts AS fts ON m.rowid = fts.rowid
            WHERE m.app_name = ? AND user_id = ? AND fts.content_text MATCH ?
            ORDER BY m.timestamp DESC
            LIMIT ?
            """
            params = [app_name, user_id, query, limit_value]
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = ? AND user_id = ? AND content_text LIKE ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
            params = [app_name, user_id, f"%{query}%", limit_value]

        async with self._config.provide_session() as driver:
            result = await driver.execute(sql, params)
            records: list[MemoryRecord] = []
            for row in result.all():
                raw = dict(row)
                raw["timestamp"] = _julian_to_datetime(raw["timestamp"])
                raw["inserted_at"] = _julian_to_datetime(raw["inserted_at"])
                raw["content_json"] = from_json(raw["content_json"])
                raw["metadata_json"] = from_json(raw["metadata_json"]) if raw["metadata_json"] else None
                records.append(cast("MemoryRecord", raw))
            return records

    async def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        async with await self._config.provide_connection() as conn:
            result = await conn.execute(sql, [session_id])
            return cast("int", result or 0)

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        cutoff = _datetime_to_julian(datetime.now(UTC)) - days
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        async with await self._config.provide_connection() as conn:
            result = await conn.execute(sql, [cutoff])
            return cast("int", result or 0)


class SyncSqliteADKStore(BaseSyncADKStore["VertebraSqliteSyncConfig"]):
    """Synchronous SQLite ADK store for Vertebra."""

    __slots__ = ()

    def _get_create_sessions_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time REAL NOT NULL,
            update_time REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);
        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);
        """

    def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            invocation_id TEXT NOT NULL,
            author TEXT NOT NULL,
            actions BLOB NOT NULL,
            long_running_tool_ids_json TEXT,
            branch TEXT,
            timestamp REAL NOT NULL,
            content TEXT,
            grounding_metadata TEXT,
            custom_metadata TEXT,
            partial INTEGER,
            turn_complete INTEGER,
            interrupted INTEGER,
            error_code TEXT,
            error_message TEXT,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def create_tables(self) -> None:
        with self.config.provide_session() as driver:
            driver.execute_script(self._get_create_sessions_table_sql())
            driver.execute_script(self._get_create_events_table_sql())

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        now = datetime.now(UTC)
        now_julian = _datetime_to_julian(now)
        state_json = to_json(state) if state else "{}"

        with self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table}
                (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                conn._execute(sql, [session_id, app_name, user_id, owner_id, state_json, now_julian, now_julian])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                conn._execute(sql, [session_id, app_name, user_id, state_json, now_julian, now_julian])

        record = self.get_session(session_id)
        if record is None:
            msg = f"Failed to create session {session_id}"
            raise RuntimeError(msg)
        return record

    def get_session(self, session_id: str) -> SessionRecord | None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = ?
        """

        with self.config.provide_session() as driver:
            result = driver.execute(sql, [session_id])
            row = result.one_or_none()

            if row is None:
                return None

            return SessionRecord(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=from_json(row["state"]) if row["state"] else {},
                create_time=_julian_to_datetime(row["create_time"]),
                update_time=_julian_to_datetime(row["update_time"]),
            )

    def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        now_julian = _datetime_to_julian(datetime.now(UTC))
        state_json = to_json(state) if state else "{}"

        sql = f"""
        UPDATE {self._session_table}
        SET state = ?, update_time = ?
        WHERE id = ?
        """

        with self.config.provide_connection() as conn:
            conn._execute(sql, [state_json, now_julian, session_id])

    def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = ?"

        with self.config.provide_connection() as conn:
            conn._execute(sql, [session_id])

    def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ?
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ? AND user_id = ?
            ORDER BY update_time DESC
            """
            params = [app_name, user_id]

        with self.config.provide_session() as driver:
            result = driver.execute(sql, params)
            rows = result.all()

            return [
                SessionRecord(
                    id=row["id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    state=from_json(row["state"]) if row["state"] else {},
                    create_time=_julian_to_datetime(row["create_time"]),
                    update_time=_julian_to_datetime(row["update_time"]),
                )
                for row in rows
            ]

    def create_event(
        self,
        event_id: str,
        session_id: str,
        app_name: str,
        user_id: str,
        author: str | None = None,
        actions: bytes | None = None,
        content: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> EventRecord:
        timestamp = kwargs.get("timestamp") or datetime.now(UTC)
        timestamp_julian = _datetime_to_julian(timestamp)

        content_json = to_json(content) if content else None
        grounding_metadata_json = (
            to_json(kwargs.get("grounding_metadata")) if kwargs.get("grounding_metadata") else None
        )
        custom_metadata_json = to_json(kwargs.get("custom_metadata")) if kwargs.get("custom_metadata") else None

        partial_int = _to_sqlite_bool(kwargs.get("partial"))
        turn_complete_int = _to_sqlite_bool(kwargs.get("turn_complete"))
        interrupted_int = _to_sqlite_bool(kwargs.get("interrupted"))
        invocation_id = kwargs.get("invocation_id") or ""

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        with self.config.provide_connection() as conn:
            conn._execute(
                sql,
                [
                    event_id,
                    session_id,
                    app_name,
                    user_id,
                    invocation_id,
                    author or "",
                    actions or b"",
                    kwargs.get("long_running_tool_ids_json"),
                    kwargs.get("branch"),
                    timestamp_julian,
                    content_json,
                    grounding_metadata_json,
                    custom_metadata_json,
                    partial_int,
                    turn_complete_int,
                    interrupted_int,
                    kwargs.get("error_code"),
                    kwargs.get("error_message"),
                ],
            )

        return EventRecord(
            id=event_id,
            session_id=session_id,
            app_name=app_name,
            user_id=user_id,
            invocation_id=invocation_id,
            author=author or "",
            actions=actions or b"",
            long_running_tool_ids_json=kwargs.get("long_running_tool_ids_json"),
            branch=kwargs.get("branch"),
            timestamp=cast("datetime", timestamp),
            content=content,
            grounding_metadata=kwargs.get("grounding_metadata"),
            custom_metadata=kwargs.get("custom_metadata"),
            partial=kwargs.get("partial"),
            turn_complete=kwargs.get("turn_complete"),
            interrupted=kwargs.get("interrupted"),
            error_code=kwargs.get("error_code"),
            error_message=kwargs.get("error_message"),
        )

    def list_events(self, session_id: str) -> list[EventRecord]:
        sql = f"""
        SELECT * FROM {self._events_table}
        WHERE session_id = ?
        ORDER BY timestamp ASC
        """

        with self.config.provide_session() as driver:
            result = driver.execute(sql, [session_id])
            rows = result.all()

            return [
                EventRecord(
                    id=row["id"],
                    session_id=row["session_id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    invocation_id=row["invocation_id"],
                    author=row["author"],
                    actions=bytes(cast("bytes", row["actions"])),
                    long_running_tool_ids_json=row["long_running_tool_ids_json"],
                    branch=row["branch"],
                    timestamp=_julian_to_datetime(row["timestamp"]),
                    content=from_json(row["content"]) if row["content"] else None,
                    grounding_metadata=from_json(row["grounding_metadata"]) if row["grounding_metadata"] else None,
                    custom_metadata=from_json(row["custom_metadata"]) if row["custom_metadata"] else None,
                    partial=_from_sqlite_bool(row["partial"]),
                    turn_complete=_from_sqlite_bool(row["turn_complete"]),
                    interrupted=_from_sqlite_bool(row["interrupted"]),
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]


class SyncSqliteADKMemoryStore(BaseSyncADKMemoryStore["VertebraSqliteSyncConfig"]):
    """Synchronous SQLite ADK memory store for Vertebra."""

    __slots__ = ()

    def _get_create_memory_table_sql(self) -> str:
        # Same as async
        fts_table = ""
        if self._use_fts:
            fts_table = f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {self._memory_table}_fts USING fts5(
            content_text,
            content={self._memory_table},
            content_rowid=rowid
        );

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ai AFTER INSERT ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_ad AFTER DELETE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS {self._memory_table}_au AFTER UPDATE ON {self._memory_table} BEGIN
            INSERT INTO {self._memory_table}_fts({self._memory_table}_fts, rowid, content_text)
            VALUES('delete', old.rowid, old.content_text);
            INSERT INTO {self._memory_table}_fts(rowid, content_text) VALUES (new.rowid, new.content_text);
        END;
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            author TEXT,
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_table}
        """

    def _get_drop_memory_table_sql(self) -> list[str]:
        statements = [f"DROP TABLE IF EXISTS {self._memory_table}"]
        if self._use_fts:
            statements.extend([
                f"DROP TABLE IF EXISTS {self._memory_table}_fts",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ai",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_ad",
                f"DROP TRIGGER IF EXISTS {self._memory_table}_au",
            ])
        return statements

    def create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_memory_table_sql())

    def insert_memory_entries(self, entries: list[MemoryRecord], owner_id: object | None = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        with self._config.provide_connection() as conn:
            for entry in entries:
                params: list[Any]
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = [
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        _datetime_to_julian(entry["timestamp"]),
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        _datetime_to_julian(entry["inserted_at"]),
                    ]
                else:
                    sql = f"""
                    INSERT OR IGNORE INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = [
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        _datetime_to_julian(entry["timestamp"]),
                        to_json(entry["content_json"]),
                        entry["content_text"],
                        to_json(entry["metadata_json"]),
                        _datetime_to_julian(entry["inserted_at"]),
                    ]
                rowcount = conn._execute(sql, params)
                if rowcount:
                    inserted_count += rowcount.get("rowcount", 0)
        return inserted_count

    def search_entries(self, query: str, app_name: str, user_id: str, limit: int | None = None) -> list[MemoryRecord]:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        if self._use_fts:
            sql = f"""
            SELECT m.* FROM {self._memory_table} AS m
            JOIN {self._memory_table}_fts AS fts ON m.rowid = fts.rowid
            WHERE m.app_name = ? AND user_id = ? AND fts.content_text MATCH ?
            ORDER BY m.timestamp DESC
            LIMIT ?
            """
            params = [app_name, user_id, query, limit_value]
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = ? AND user_id = ? AND content_text LIKE ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
            params = [app_name, user_id, f"%{query}%", limit_value]

        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            records: list[MemoryRecord] = []
            for row in result.all():
                raw = dict(row)
                raw["timestamp"] = _julian_to_datetime(raw["timestamp"])
                raw["inserted_at"] = _julian_to_datetime(raw["inserted_at"])
                raw["content_json"] = from_json(raw["content_json"])
                raw["metadata_json"] = from_json(raw["metadata_json"]) if raw["metadata_json"] else None
                records.append(cast("MemoryRecord", raw))
            return records

    def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        with self._config.provide_connection() as conn:
            result = conn._execute(sql, [session_id])
            return result.get("rowcount", 0)

    def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        cutoff = _datetime_to_julian(datetime.now(UTC)) - days
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < ?"
        with self._config.provide_connection() as conn:
            result = conn._execute(sql, [cutoff])
            return result.get("rowcount", 0)
