"""SQLSpec provider host for broker-registered adapters."""

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any, cast

from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.core.result import ArrowResult, SQLResult
from sqlspec.utils.correlation import CorrelationContext

from vertebra._vertebra import ShmRegion
from vertebra.resources.provider import ProviderConfig, ProviderHost
from vertebra.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    import pyarrow as pa  # pyright: ignore[reportMissingImports]
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

logger = logging.getLogger(__name__)


class _SyncSessionState:
    __slots__ = ("ctx", "driver")

    def __init__(self, ctx: Any, driver: "SyncDriverAdapterBase") -> None:
        self.ctx = ctx
        self.driver = driver


class _AsyncSessionState:
    __slots__ = ("ctx", "driver")

    def __init__(self, ctx: Any, driver: "AsyncDriverAdapterBase") -> None:
        self.ctx = ctx
        self.driver = driver


class SqlSpecProvider:
    """Provider host that executes SQLSpec adapter calls for the broker."""

    def __init__(
        self,
        adapter_config: "SyncDatabaseConfig[Any, Any, Any] | AsyncDatabaseConfig[Any, Any, Any]",
        provider_config: ProviderConfig,
    ) -> None:
        self._config = provider_config
        self._adapter_config = adapter_config
        self._host = ProviderHost(provider_config, self._handle)
        self._is_async = bool(getattr(adapter_config, "is_async", False))
        self._sync_sessions: dict[str, _SyncSessionState] = {}
        self._async_sessions: dict[str, _AsyncSessionState] = {}
        self._arrow_regions: dict[str, ShmRegion] = {}

    async def register(self) -> None:
        await self._host.register()

    async def serve(self) -> None:
        await self._host.serve()

    async def _handle(self, request: "dict[str, object]") -> "dict[str, object]":
        if self._is_async:
            return await self._handle_async(request)
        return await asyncio.to_thread(self._handle_sync, request)

    def _handle_sync(self, request: "dict[str, object]") -> "dict[str, object]":  # noqa: PLR0911
        kind = str(request.get("kind") or "")
        try:
            if kind == "acquire":
                return self._acquire_sync()
            if kind == "release":
                return self._release_sync(str(request.get("token") or ""))
            if kind == "execute":
                return self._execute_sync(request)
            if kind == "execute_many":
                return self._execute_many_sync(request)
            if kind == "command":
                return self._command_sync(request)
            if kind == "begin":
                return self._begin_sync(request)
            if kind == "commit":
                return self._commit_sync(request)
            if kind == "rollback":
                return self._rollback_sync(request)
            if kind == "arrow_release":
                return self._arrow_release(str(request.get("name") or ""))
            if kind == "health":
                return {"ok": True}
        except Exception as exc:  # noqa: BLE001 - IPC must not propagate exceptions
            return {"ok": False, "error": {"message": str(exc)}}
        return {"ok": False, "error": {"message": f"Unknown provider request: {kind}"}}

    async def _handle_async(self, request: "dict[str, object]") -> "dict[str, object]":  # noqa: PLR0911
        kind = str(request.get("kind") or "")
        try:
            if kind == "acquire":
                return await self._acquire_async()
            if kind == "release":
                return await self._release_async(str(request.get("token") or ""))
            if kind == "execute":
                return await self._execute_async(request)
            if kind == "execute_many":
                return await self._execute_many_async(request)
            if kind == "command":
                return await self._command_async(request)
            if kind == "begin":
                return await self._begin_async(request)
            if kind == "commit":
                return await self._commit_async(request)
            if kind == "rollback":
                return await self._rollback_async(request)
            if kind == "arrow_release":
                return self._arrow_release(str(request.get("name") or ""))
            if kind == "health":
                return {"ok": True}
        except Exception as exc:  # noqa: BLE001 - IPC must not propagate exceptions
            return {"ok": False, "error": {"message": str(exc)}}
        return {"ok": False, "error": {"message": f"Unknown provider request: {kind}"}}

    def _ensure_capacity(self) -> None:
        max_size = self._config.max_size
        in_use = len(self._async_sessions) if self._is_async else len(self._sync_sessions)
        if max_size > 0 and in_use >= max_size:
            msg = "Provider is at capacity"
            raise RuntimeError(msg)

    def _acquire_sync(self) -> "dict[str, object]":
        self._ensure_capacity()
        if not isinstance(self._adapter_config, SyncDatabaseConfig):
            msg = "Expected SyncDatabaseConfig"
            raise TypeError(msg)
        ctx = self._adapter_config.provide_session()
        driver = ctx.__enter__()
        token = uuid.uuid4().hex

        self._sync_sessions[token] = _SyncSessionState(ctx, cast("SyncDriverAdapterBase", driver))
        return {"ok": True, "token": token}

    async def _acquire_async(self) -> "dict[str, object]":
        self._ensure_capacity()
        if not isinstance(self._adapter_config, AsyncDatabaseConfig):
            msg = "Expected AsyncDatabaseConfig"
            raise TypeError(msg)
        # Note: provide_session itself is sync, but __aenter__ is async
        ctx = self._adapter_config.provide_session()
        driver = await ctx.__aenter__()
        token = uuid.uuid4().hex

        self._async_sessions[token] = _AsyncSessionState(ctx, cast("AsyncDriverAdapterBase", driver))
        return {"ok": True, "token": token}

    def _release_sync(self, token: str) -> "dict[str, object]":
        state = self._sync_sessions.pop(token, None)
        if state is None:
            return {"ok": True}
        state.ctx.__exit__(None, None, None)
        return {"ok": True}

    async def _release_async(self, token: str) -> "dict[str, object]":
        state = self._async_sessions.pop(token, None)
        if state is None:
            return {"ok": True}
        await state.ctx.__aexit__(None, None, None)
        return {"ok": True}

    def _execute_sync(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._sync_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        sql = str(request.get("sql") or "")
        params = request.get("params")
        if params is None:
            params = []
        correlation_id = request.get("correlation_id")
        with CorrelationContext.context(str(correlation_id) if correlation_id else None):
            result = self._execute_driver_sync(state.driver, sql, params)
        return self._encode_result(result)

    def _execute_many_sync(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._sync_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        sql = str(request.get("sql") or "")
        params = request.get("params")
        if not isinstance(params, (list, tuple)):
            params = []
        correlation_id = request.get("correlation_id")
        with CorrelationContext.context(str(correlation_id) if correlation_id else None):
            result = state.driver.execute_many(sql, cast("Any", params))
        return self._encode_result(result)

    def _command_sync(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._sync_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        command = str(request.get("command") or "")
        args = request.get("args")
        if args is None:
            args = []
        correlation_id = request.get("correlation_id")
        with CorrelationContext.context(str(correlation_id) if correlation_id else None):
            result = self._execute_driver_sync(state.driver, command, args)
        return self._encode_result(result)

    def _begin_sync(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._sync_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        state.driver.begin()
        return {"ok": True}

    def _commit_sync(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._sync_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        state.driver.commit()
        return {"ok": True}

    def _rollback_sync(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._sync_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        state.driver.rollback()
        return {"ok": True}

    async def _execute_async(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._async_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        sql = str(request.get("sql") or "")
        params = request.get("params")
        if params is None:
            params = []
        correlation_id = request.get("correlation_id")
        with CorrelationContext.context(str(correlation_id) if correlation_id else None):
            result = await self._execute_driver_async(state.driver, sql, params)
        return self._encode_result(result)

    async def _execute_many_async(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._async_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        sql = str(request.get("sql") or "")
        params = request.get("params")
        if not isinstance(params, (list, tuple)):
            params = []
        correlation_id = request.get("correlation_id")
        with CorrelationContext.context(str(correlation_id) if correlation_id else None):
            result = await state.driver.execute_many(sql, cast("Any", params))
        return self._encode_result(result)

    async def _command_async(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._async_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        command = str(request.get("command") or "")
        args = request.get("args")
        if args is None:
            args = []
        correlation_id = request.get("correlation_id")
        with CorrelationContext.context(str(correlation_id) if correlation_id else None):
            result = await self._execute_driver_async(state.driver, command, args)
        return self._encode_result(result)

    async def _begin_async(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._async_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        await state.driver.begin()
        return {"ok": True}

    async def _commit_async(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._async_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        await state.driver.commit()
        return {"ok": True}

    async def _rollback_async(self, request: "dict[str, object]") -> "dict[str, object]":
        state = self._async_sessions.get(str(request.get("token") or ""))
        if state is None:
            msg = "Unknown provider session"
            raise RuntimeError(msg)
        await state.driver.rollback()
        return {"ok": True}

    def _execute_driver_sync(
        self, driver: "SyncDriverAdapterBase", sql: str, params: object
    ) -> "SQLResult | ArrowResult":
        if isinstance(params, dict):
            return driver.execute(sql, **params)
        if isinstance(params, (list, tuple)):
            return driver.execute(sql, *params)
        return driver.execute(sql, params)

    async def _execute_driver_async(
        self, driver: "AsyncDriverAdapterBase", sql: str, params: object
    ) -> "SQLResult | ArrowResult":
        if isinstance(params, dict):
            return await driver.execute(sql, **params)
        if isinstance(params, (list, tuple)):
            return await driver.execute(sql, *params)
        return await driver.execute(sql, params)

    def _encode_result(self, result: "SQLResult | ArrowResult") -> "dict[str, object]":
        if isinstance(result, ArrowResult):
            return self._encode_arrow_result(result)
        # result is SQLResult after exhaustive ArrowResult check
        data = result.data or []
        columns = list(result.column_names or [])
        if not columns and data:
            first = data[0]
            if isinstance(first, dict):
                columns = list(first.keys())
        rows = self._normalize_rows(data, columns)
        rowcount = int(result.rows_affected or len(rows))
        payload = self._maybe_encode_arrow(rows, columns, rowcount)
        if payload is not None:
            payload["ok"] = True
            return payload
        return {"ok": True, "columns": columns, "rows": rows, "rowcount": rowcount}

    def _encode_arrow_result(self, result: "ArrowResult") -> "dict[str, object]":
        table = result.data
        if table is None:
            return {"ok": True, "rowcount": int(result.rows_affected or 0)}
        payload = self._write_arrow_region(table)
        payload["rowcount"] = int(result.rows_affected or getattr(table, "num_rows", 0))
        payload["ok"] = True
        return payload

    def _maybe_encode_arrow(
        self, rows: "list[list[object]]", columns: "list[str]", rowcount: int
    ) -> "dict[str, object] | None":
        if not rows:
            return None
        estimated_bytes = len(rows) * max(1, len(columns)) * 8
        if rowcount < self._config.arrow_threshold_rows and estimated_bytes < self._config.arrow_threshold_bytes:
            return None

        # Use lazy flag check - only import pyarrow if installed
        if not PYARROW_INSTALLED:
            logger.debug("pyarrow not installed - falling back to MessagePack encoding")
            return None

        import pyarrow as pa  # pyright: ignore[reportMissingImports]

        records = [dict(zip(columns, row, strict=False)) for row in rows]
        table = pa.Table.from_pylist(records)
        payload = self._write_arrow_region(table)
        payload["rowcount"] = rowcount
        return payload

    def _write_arrow_region(self, table: "pa.Table") -> "dict[str, object]":
        import pyarrow as pa  # pyright: ignore[reportMissingImports]

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buffer = sink.getvalue().to_pybytes()
        name = f"/{self._config.name}.provider.arrow.{uuid.uuid4().hex}"
        region = ShmRegion.create_named(name, len(buffer))
        memoryview(region)[: len(buffer)] = buffer
        self._arrow_regions[name] = region
        return {"arrow": {"name": name, "size": len(buffer)}}

    def _normalize_rows(self, data: object, columns: "list[str]") -> "list[list[object]]":
        rows: list[list[object]] = []
        if not hasattr(data, "__iter__"):
            return rows
        for row in cast("Any", data):
            if isinstance(row, dict):
                rows.append([row.get(column) for column in columns])
            elif isinstance(row, (list, tuple)):
                rows.append(list(row))
            else:
                rows.append([row])
        return rows

    def _arrow_release(self, name: str) -> "dict[str, object]":
        region = self._arrow_regions.pop(name, None)
        if region is None:
            return {"ok": False, "error": {"message": "Arrow region not found"}}
        region.close()
        return {"ok": True}


__all__ = ("SqlSpecProvider",)
