# ruff: noqa: S608
"""PostgreSQL ADK store for Vertebra."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, MemoryRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import datetime


__all__ = ("PostgresADKMemoryStore", "PostgresADKStore", "SyncPostgresADKMemoryStore", "SyncPostgresADKStore")


class PostgresADKStore(BaseAsyncADKStore["VertebraPostgresAsyncConfig"]):
    """PostgreSQL ADK store for Vertebra.

    Mirrors sqlspec.adapters.asyncpg.adk.store.AsyncpgADKStore but adapted
    for Vertebra's broker-backed PostgreSQL driver.
    """

    __slots__ = ()

    async def _get_create_sessions_table_sql(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            author VARCHAR(256),
            actions BYTEA,
            long_running_tool_ids_json JSONB,
            branch VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSONB,
            grounding_metadata JSONB,
            custom_metadata JSONB,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
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
        async with await self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table}
                (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await conn.execute(sql, [session_id, app_name, user_id, owner_id, state])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await conn.execute(sql, [session_id, app_name, user_id, state])

        record = await self.get_session(session_id)
        if record is None:
            msg = f"Failed to create session {session_id}"
            raise RuntimeError(msg)
        return record

    async def get_session(self, session_id: str) -> SessionRecord | None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = $1
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
                state=row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

    async def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE id = $2
        """

        async with await self.config.provide_connection() as conn:
            await conn.execute(sql, [state, session_id])

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = $1"

        async with await self.config.provide_connection() as conn:
            await conn.execute(sql, [session_id])

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2
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
                    state=from_json(row["state"]) if isinstance(row["state"], str) else row["state"],
                    create_time=row["create_time"],
                    update_time=row["update_time"],
                )
                for row in rows
            ]

    async def append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
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
                    event_record.get("invocation_id") or "",
                    event_record.get("author") or "",
                    event_record.get("actions") or b"",
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    event_record["timestamp"],
                    event_record.get("content"),
                    event_record.get("grounding_metadata"),
                    event_record.get("custom_metadata"),
                    event_record.get("partial"),
                    event_record.get("turn_complete"),
                    event_record.get("interrupted"),
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ],
            )

    async def get_events(
        self, session_id: str, after_timestamp: datetime | None = None, limit: int | None = None
    ) -> list[EventRecord]:
        where_clauses = ["session_id = $1"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append(f"timestamp > ${len(params) + 1}")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT ${len(params) + 1}" if limit else ""
        if limit:
            params.append(limit)

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
                    timestamp=row["timestamp"],
                    content=row["content"],
                    grounding_metadata=row["grounding_metadata"],
                    custom_metadata=row["custom_metadata"],
                    partial=row["partial"],
                    turn_complete=row["turn_complete"],
                    interrupted=row["interrupted"],
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]


class PostgresADKMemoryStore(BaseAsyncADKMemoryStore["VertebraPostgresAsyncConfig"]):
    """PostgreSQL ADK memory store for Vertebra."""

    __slots__ = ()

    async def _get_create_memory_table_sql(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

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
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    rowcount = await conn.execute(
                        sql,
                        [
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            owner_id,
                            entry["timestamp"],
                            to_json(entry["content_json"]),
                            entry["content_text"],
                            to_json(entry["metadata_json"]),
                            entry["inserted_at"],
                        ],
                    )
                else:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    rowcount = await conn.execute(
                        sql,
                        [
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            entry["timestamp"],
                            to_json(entry["content_json"]),
                            entry["content_text"],
                            to_json(entry["metadata_json"]),
                            entry["inserted_at"],
                        ],
                    )
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
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', $3)
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = [app_name, user_id, query, limit_value]
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2 AND content_text ILIKE $3
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = [app_name, user_id, f"%{query}%", limit_value]

        async with self._config.provide_session() as driver:
            result = await driver.execute(sql, params)
            return cast("list[MemoryRecord]", [dict(row) for row in result.all()])

    async def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = $1"
        async with await self.config.provide_connection() as conn:
            result = await conn.execute(sql, [session_id])
            return result or 0

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 day'))
        """
        async with await self.config.provide_connection() as conn:
            result = await conn.execute(sql, [days])
            return result or 0


class SyncPostgresADKStore(BaseSyncADKStore["VertebraPostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK store for Vertebra."""

    __slots__ = ()

    def _get_create_sessions_table_sql(self) -> str:
        # Same SQL as async version
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            author VARCHAR(256),
            actions BYTEA,
            long_running_tool_ids_json JSONB,
            branch VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSONB,
            grounding_metadata JSONB,
            custom_metadata JSONB,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
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
        with self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table}
                (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                conn._execute(sql, [session_id, app_name, user_id, owner_id, state])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                conn._execute(sql, [session_id, app_name, user_id, state])

        record = self.get_session(session_id)
        if record is None:
            msg = f"Failed to create session {session_id}"
            raise RuntimeError(msg)
        return record

    def get_session(self, session_id: str) -> SessionRecord | None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = $1
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
                state=row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

    def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE id = $2
        """

        with self.config.provide_connection() as conn:
            conn._execute(sql, [state, session_id])

    def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = $1"

        with self.config.provide_connection() as conn:
            conn._execute(sql, [session_id])

    def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2
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
                    state=from_json(row["state"]) if isinstance(row["state"], str) else row["state"],
                    create_time=row["create_time"],
                    update_time=row["update_time"],
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
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
        )
        """

        timestamp = kwargs.get("timestamp")
        invocation_id = kwargs.get("invocation_id") or ""
        long_running_tool_ids_json = kwargs.get("long_running_tool_ids_json")
        branch = kwargs.get("branch")
        grounding_metadata = kwargs.get("grounding_metadata")
        custom_metadata = kwargs.get("custom_metadata")
        partial = kwargs.get("partial")
        turn_complete = kwargs.get("turn_complete")
        interrupted = kwargs.get("interrupted")
        error_code = kwargs.get("error_code")
        error_message = kwargs.get("error_message")

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
                    long_running_tool_ids_json,
                    branch,
                    timestamp,
                    content,
                    grounding_metadata,
                    custom_metadata,
                    partial,
                    turn_complete,
                    interrupted,
                    error_code,
                    error_message,
                ],
            )

        # Return a dummy or fetch? Base class expects return.
        # For sync, we'll just construct the record from inputs for now to avoid extra fetch.
        return EventRecord(
            id=event_id,
            session_id=session_id,
            app_name=app_name,
            user_id=user_id,
            invocation_id=invocation_id,
            author=author or "",
            actions=actions or b"",
            long_running_tool_ids_json=long_running_tool_ids_json,
            branch=branch,
            timestamp=cast("datetime", timestamp),
            content=content,
            grounding_metadata=grounding_metadata,
            custom_metadata=custom_metadata,
            partial=partial,
            turn_complete=turn_complete,
            interrupted=interrupted,
            error_code=error_code,
            error_message=error_message,
        )

    def list_events(self, session_id: str) -> list[EventRecord]:
        sql = f"""
        SELECT * FROM {self._events_table}
        WHERE session_id = $1
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
                    actions=bytes(row["actions"]) if row["actions"] else b"",
                    long_running_tool_ids_json=row["long_running_tool_ids_json"],
                    branch=row["branch"],
                    timestamp=row["timestamp"],
                    content=row["content"],
                    grounding_metadata=row["grounding_metadata"],
                    custom_metadata=row["custom_metadata"],
                    partial=row["partial"],
                    turn_complete=row["turn_complete"],
                    interrupted=row["interrupted"],
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]


class SyncPostgresADKMemoryStore(BaseSyncADKMemoryStore["VertebraPostgresSyncConfig"]):
    """Synchronous PostgreSQL ADK memory store for Vertebra."""

    __slots__ = ()

    def _get_create_memory_table_sql(self) -> str:
        # Same as async
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

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
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    rowcount = conn._execute(
                        sql,
                        [
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            owner_id,
                            entry["timestamp"],
                            to_json(entry["content_json"]),
                            entry["content_text"],
                            to_json(entry["metadata_json"]),
                            entry["inserted_at"],
                        ],
                    )
                else:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    rowcount = conn._execute(
                        sql,
                        [
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            entry["timestamp"],
                            to_json(entry["content_json"]),
                            entry["content_text"],
                            to_json(entry["metadata_json"]),
                            entry["inserted_at"],
                        ],
                    )
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
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', $3)
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = [app_name, user_id, query, limit_value]
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2 AND content_text ILIKE $3
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = [app_name, user_id, f"%{query}%", limit_value]

        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            return cast("list[MemoryRecord]", [dict(row) for row in result.all()])

    def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = $1"
        with self.config.provide_connection() as conn:
            result = conn._execute(sql, [session_id])
            return result.get("rowcount", 0)

    def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 day'))
        """
        with self.config.provide_connection() as conn:
            result = conn._execute(sql, [days])
            return result.get("rowcount", 0)
