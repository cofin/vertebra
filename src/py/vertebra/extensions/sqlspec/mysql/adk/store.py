"MySQL ADK store for Vertebra."

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import datetime
    from vertebra.extensions.sqlspec.mysql.config import MysqlConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("MysqlADKMemoryStore", "MysqlADKStore", "SyncMysqlADKMemoryStore", "SyncMysqlADKStore")


def _parse_owner_id_column_for_mysql(column_ddl: str) -> tuple[str, str]:
    """Parse owner ID column DDL for MySQL FOREIGN KEY syntax."""
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)

    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"

    return (col_def, fk_constraint)


class MysqlADKStore(BaseAsyncADKStore["MysqlConfig"]):
    """MySQL ADK store for Vertebra.

    Mirrors sqlspec.adapters.asyncmy.adk.store.AsyncmyADKStore but adapted
    for Vertebra's broker-backed MySQL driver.
    """

    __slots__ = ()

    async def _get_create_sessions_table_sql(self) -> str:
        owner_id_col = ""
        fk_constraint = ""

        if self._owner_id_column_ddl:
            col_def, fk_def = _parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_col = f"{col_def},"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            {owner_id_col}
            state JSON NOT NULL,
            create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_{self._session_table}_app_user (app_name, user_id),
            INDEX idx_{self._session_table}_update_time (update_time DESC){fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            actions BLOB NOT NULL,
            long_running_tool_ids_json JSON,
            branch VARCHAR(256),
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content JSON,
            grounding_metadata JSON,
            custom_metadata JSON,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE,
            INDEX idx_{self._events_table}_session (session_id, timestamp ASC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_tables_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        async with self.config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())

    async def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None
    ) -> SessionRecord:
        state_json = to_json(state)

        async with self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """
                await conn.execute(sql, [session_id, app_name, user_id, owner_id, state_json])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """
                await conn.execute(sql, [session_id, app_name, user_id, state_json])

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(self, session_id: str) -> SessionRecord | None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        async with self.config.provide_session() as driver:
            result = await driver.execute(sql, [session_id])
            row = result.fetchone()

            if row is None:
                return None

            return SessionRecord(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=from_json(row["state"]) if isinstance(row["state"], str) else row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

    async def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        async with self.config.provide_connection() as conn:
            await conn.execute(sql, [state_json, session_id])

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        async with self.config.provide_connection() as conn:
            await conn.execute(sql, [session_id])

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """
            params = [app_name, user_id]

        async with self.config.provide_session() as driver:
            result = await driver.execute(sql, params)
            rows = result.fetchall()

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
        content_json = to_json(event_record.get("content")) if event_record.get("content") else None
        grounding_metadata_json = (
            to_json(event_record.get("grounding_metadata")) if event_record.get("grounding_metadata") else None
        )
        custom_metadata_json = (
            to_json(event_record.get("custom_metadata")) if event_record.get("custom_metadata") else None
        )

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        async with self.config.provide_connection() as conn:
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
                    event_record["timestamp"],
                    content_json,
                    grounding_metadata_json,
                    custom_metadata_json,
                    event_record.get("partial"),
                    event_record.get("turn_complete"),
                    event_record.get("interrupted"),
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ],
            )

    async def get_events(
        self,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None
    ) -> list[EventRecord]:
        where_clauses = ["session_id = %s"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

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
            rows = result.fetchall()

            return [
                EventRecord(
                    id=row["id"],
                    session_id=row["session_id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    invocation_id=row["invocation_id"],
                    author=row["author"],
                    actions=bytes(row["actions"]),
                    long_running_tool_ids_json=row["long_running_tool_ids_json"],
                    branch=row["branch"],
                    timestamp=row["timestamp"],
                    content=from_json(row["content"]) if row["content"] and isinstance(row["content"], str) else row["content"],
                    grounding_metadata=from_json(row["grounding_metadata"]) if row["grounding_metadata"] and isinstance(row["grounding_metadata"], str) else row["grounding_metadata"],
                    custom_metadata=from_json(row["custom_metadata"]) if row["custom_metadata"] and isinstance(row["custom_metadata"], str) else row["custom_metadata"],
                    partial=row["partial"],
                    turn_complete=row["turn_complete"],
                    interrupted=row["interrupted"],
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]


class MysqlADKMemoryStore(BaseAsyncADKMemoryStore["MysqlConfig"]):
    """MySQL ADK memory store for Vertebra."""

    __slots__ = ()

    async def _get_create_memory_table_sql(self) -> str:
        owner_id_line = ""
        fk_constraint = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = _parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_line = f",\n            {col_def}"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        fts_index = ""
        if self._use_fts:
            fts_index = f",\n            FULLTEXT INDEX idx_{self._memory_table}_fts (content_text)"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            INDEX idx_{self._memory_table}_app_user_time (app_name, user_id, timestamp),
            INDEX idx_{self._memory_table}_session (session_id){fts_index}{fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
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
        if self._owner_id_column_name:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        async with self._config.provide_connection() as conn:
            for entry in entries:
                params: list[Any]
                if self._owner_id_column_name:
                    params = [
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
                    ]
                else:
                    params = [
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
                    ]
                rowcount = await conn.execute(sql, params)
                if rowcount:
                    inserted_count += rowcount
        return inserted_count

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None
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
            WHERE app_name = %s AND user_id = %s
              AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = [app_name, user_id, query, limit_value]
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s AND content_text LIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = [app_name, user_id, f"%{query}%", limit_value]

        async with self._config.provide_session() as driver:
            result = await driver.execute(sql, params)
            return [cast("MemoryRecord", dict(row)) for row in result.fetchall()]

    async def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        async with self._config.provide_connection() as conn:
            return await conn.execute(sql, [session_id]) or 0

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (UTC_TIMESTAMP(6) - INTERVAL %s DAY)
        """
        async with self._config.provide_connection() as conn:
            return await conn.execute(sql, [days]) or 0


class SyncMysqlADKStore(BaseSyncADKStore["MysqlConfig"]):
    """Synchronous MySQL ADK store for Vertebra."""

    __slots__ = ()

    def _get_create_sessions_table_sql(self) -> str:
        # Same SQL
        owner_id_col = ""
        fk_constraint = ""

        if self._owner_id_column_ddl:
            col_def, fk_def = _parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_col = f"{col_def},"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            {owner_id_col}
            state JSON NOT NULL,
            create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_{self._session_table}_app_user (app_name, user_id),
            INDEX idx_{self._session_table}_update_time (update_time DESC){fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            actions BLOB NOT NULL,
            long_running_tool_ids_json JSON,
            branch VARCHAR(256),
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content JSON,
            grounding_metadata JSON,
            custom_metadata JSON,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE,
            INDEX idx_{self._events_table}_session (session_id, timestamp ASC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_tables_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def create_tables(self) -> None:
        with self.config.provide_session() as driver:
            driver.execute_script(self._get_create_sessions_table_sql())
            driver.execute_script(self._get_create_events_table_sql())

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None
    ) -> SessionRecord:
        state_json = to_json(state)

        with self.config.provide_connection() as conn:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """
                conn.execute(sql, [session_id, app_name, user_id, owner_id, state_json])
            else:
                sql = f"""
                INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """
                conn.execute(sql, [session_id, app_name, user_id, state_json])

        return self.get_session(session_id)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> SessionRecord | None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        with self.config.provide_session() as driver:
            result = driver.execute(sql, [session_id])
            row = result.fetchone()

            if row is None:
                return None

            return SessionRecord(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=from_json(row["state"]) if isinstance(row["state"], str) else row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

    def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        with self.config.provide_connection() as conn:
            conn.execute(sql, [state_json, session_id])

    def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        with self.config.provide_connection() as conn:
            conn.execute(sql, [session_id])

    def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """
            params = [app_name]
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """
            params = [app_name, user_id]

        with self.config.provide_session() as driver:
            result = driver.execute(sql, params)
            rows = result.fetchall()

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
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        timestamp = kwargs.get("timestamp")
        invocation_id = kwargs.get("invocation_id")
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
            conn.execute(
                sql,
                [
                    event_id,
                    session_id,
                    app_name,
                    user_id,
                    invocation_id,
                    author,
                    actions,
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

        return EventRecord(
            id=event_id,
            session_id=session_id,
            app_name=app_name,
            user_id=user_id,
            invocation_id=invocation_id,
            author=author,
            actions=actions,
            long_running_tool_ids_json=long_running_tool_ids_json,
            branch=branch,
            timestamp=timestamp,
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
        WHERE session_id = %s
        ORDER BY timestamp ASC
        """

        with self.config.provide_session() as driver:
            result = driver.execute(sql, [session_id])
            rows = result.fetchall()

            return [
                EventRecord(
                    id=row["id"],
                    session_id=row["session_id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    invocation_id=row["invocation_id"],
                    author=row["author"],
                    actions=bytes(row["actions"]),
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


class SyncMysqlADKMemoryStore(BaseSyncADKMemoryStore["MysqlConfig"]):
    """Synchronous MySQL ADK memory store for Vertebra."""

    __slots__ = ()

    def _get_create_memory_table_sql(self) -> str:
        # Same SQL
        owner_id_line = ""
        fk_constraint = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = _parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_line = f",\n            {col_def}"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        fts_index = ""
        if self._use_fts:
            fts_index = f",\n            FULLTEXT INDEX idx_{self._memory_table}_fts (content_text)"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            INDEX idx_{self._memory_table}_app_user_time (app_name, user_id, timestamp),
            INDEX idx_{self._memory_table}_session (session_id){fts_index}{fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
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
        if self._owner_id_column_name:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        with self._config.provide_connection() as conn:
            for entry in entries:
                params: list[Any]
                if self._owner_id_column_name:
                    params = [
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
                    ]
                else:
                    params = [
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
                    ]
                rowcount = conn.execute(sql, params)
                if rowcount:
                    inserted_count += rowcount
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
            WHERE app_name = %s AND user_id = %s
              AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = [app_name, user_id, query, limit_value]
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s AND content_text LIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = [app_name, user_id, f"%{query}%", limit_value]

        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            return [cast("MemoryRecord", dict(row)) for row in result.fetchall()]

    def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        with self._config.provide_connection() as conn:
            return conn.execute(sql, [session_id]) or 0

    def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (UTC_TIMESTAMP(6) - INTERVAL %s DAY)
        """
        with self._config.provide_connection() as conn:
            return conn.execute(sql, [days]) or 0
