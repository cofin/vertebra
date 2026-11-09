"""Transactional single-Sync PostgreSQL pipeline context managers."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Literal, Protocol, Self, runtime_checkable
from uuid import UUID

from vertebra.db.adapters.postgres.types import (
    PgArray,
    PgBitString,
    PgComposite,
    PgInterval,
    PgJson,
    PgParameter,
    PgTemporal,
    PgVector,
    PostgresJsonValue,
    PostgresPipelineOperation,
    PostgresPipelineOutcome,
    PostgresValue,
    _project_public_row,
)
from vertebra.db.driver import DataError, InterfaceError

__all__ = (
    "AsyncPipelineClientProtocol",
    "AsyncPostgresPipeline",
    "PostgresPipelineParameters",
    "PreparePostgresQuery",
    "SyncPipelineClientProtocol",
    "SyncPostgresPipeline",
)

type PostgresPipelineParameters = (
    Sequence[PostgresValue | PgParameter] | Mapping[str, PostgresValue | PgParameter] | None
)
type PreparePostgresQuery = Callable[
    [str, PostgresPipelineParameters],
    tuple[str, tuple[PostgresValue | PgParameter, ...]],
]


@runtime_checkable
class SyncPipelineClientProtocol(Protocol):
    """Structural protocol satisfied by ``SyncPostgresClient`` and deterministic test doubles."""

    def pipeline_begin(self) -> int:
        """Open a retained pipeline transaction or savepoint scope and return its integer identifier."""
        ...

    def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> tuple[PostgresPipelineOutcome, ...]:
        """Execute queued operations inside ``scope_id`` as one single-Sync extended batch."""
        ...

    def pipeline_finish(self, scope_id: int, success: bool) -> None:
        """Commit/release or roll back ``scope_id``."""
        ...


@runtime_checkable
class AsyncPipelineClientProtocol(Protocol):
    """Structural protocol satisfied by ``AsyncPostgresClient`` and deterministic test doubles."""

    def pipeline_begin(self) -> Awaitable[int]:
        """Open a retained pipeline transaction or savepoint scope asynchronously."""
        ...

    def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> Awaitable[tuple[PostgresPipelineOutcome, ...]]:
        """Execute queued operations asynchronously inside ``scope_id`` as one single-Sync batch."""
        ...

    def pipeline_finish(self, scope_id: int, success: bool) -> Awaitable[None]:
        """Commit/release or roll back ``scope_id`` asynchronously."""
        ...


def _default_prepare_query(
    sql: str,
    parameters: PostgresPipelineParameters,
) -> tuple[str, tuple[PostgresValue | PgParameter, ...]]:
    """Validate positional ``$N`` SQL parameters without rewriting ``%s`` or ``%(name)s``."""
    if not isinstance(sql, str):
        msg = "Pipeline SQL statement must be a str"
        raise TypeError(msg)
    if parameters is None:
        return sql, ()
    if isinstance(parameters, Mapping):
        msg = "Mapping parameters require a named-parameter bind compiler; pass a positional Sequence for $N SQL"
        raise InterfaceError(msg)
    if isinstance(parameters, (str, bytes, bytearray, memoryview)) or not isinstance(parameters, Sequence):
        msg = "Pipeline parameters must be a non-string Sequence or None"
        raise InterfaceError(msg)
    return sql, tuple(parameters)


def _snapshot_json_tree(value: object, ancestors: frozenset[int]) -> PostgresJsonValue:
    """Recursively copy a cycle-free RFC 8259 JSON tree into immutable/unaliased containers."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value) if type(value) is not int else value
    if isinstance(value, float):
        fval = float(value) if type(value) is not float else value
        if not math.isfinite(fval):
            msg = "PgJson float values must be finite"
            raise DataError(msg)
        return fval
    if isinstance(value, str):
        return str(value) if type(value) is not str else value
    if isinstance(value, list):
        obj_id = id(value)
        if obj_id in ancestors:
            msg = "Pipeline JSON parameter contains a reference cycle"
            raise DataError(msg)
        next_ancestors = ancestors | {obj_id}
        return [_snapshot_json_tree(item, next_ancestors) for item in value]
    if isinstance(value, dict):
        return _snapshot_json_object(value, ancestors)
    msg = f"Unsupported PostgreSQL JSON parameter value type: {type(value).__name__}"
    raise DataError(msg)


def _snapshot_json_object(
    value: dict[Any, Any],
    ancestors: frozenset[int],
) -> dict[str, PostgresJsonValue]:
    """Recursively copy a JSON object mapping with cycle detection and ``str`` key validation."""
    obj_id = id(value)
    if obj_id in ancestors:
        msg = "Pipeline JSON parameter contains a reference cycle"
        raise DataError(msg)
    next_ancestors = ancestors | {obj_id}
    copied: dict[str, PostgresJsonValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            msg = "JSON object parameter keys must be str"
            raise DataError(msg)
        norm_key = str(key) if type(key) is not str else key
        copied[norm_key] = _snapshot_json_tree(item, next_ancestors)
    return copied


def _snapshot_sql_value(value: object, ancestors: frozenset[int]) -> PostgresValue:
    """Recursively snapshot a ``PostgresValue`` (including mutable buffer conveniences) at enqueue time."""
    if isinstance(value, PgParameter):
        msg = "PgParameter cannot be nested inside another wrapper or container"
        raise DataError(msg)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value) if type(value) is not int else value
    if isinstance(value, float):
        return float(value) if type(value) is not float else value
    if isinstance(value, str):
        return str(value) if type(value) is not str else value
    if isinstance(value, bytes):
        return bytes(value) if type(value) is not bytes else value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(
        value,
        (Decimal, UUID, date, datetime, time, PgInterval, PgBitString, PgVector, PgTemporal),
    ):
        return value
    if isinstance(value, PgJson):
        obj_id = id(value)
        if obj_id in ancestors:
            msg = "Pipeline PgJson parameter contains a reference cycle"
            raise DataError(msg)
        return PgJson(_snapshot_json_tree(value.value, ancestors | {obj_id}))
    if isinstance(value, PgArray):
        obj_id = id(value)
        if obj_id in ancestors:
            msg = "Pipeline PgArray parameter contains a reference cycle"
            raise DataError(msg)
        next_ancestors = ancestors | {obj_id}
        return PgArray(
            values=tuple(_snapshot_sql_value(item, next_ancestors) for item in value.values),
            dimensions=value.dimensions,
            lower_bounds=value.lower_bounds,
            element_oid=value.element_oid,
        )
    if isinstance(value, PgComposite):
        obj_id = id(value)
        if obj_id in ancestors:
            msg = "Pipeline PgComposite parameter contains a reference cycle"
            raise DataError(msg)
        next_ancestors = ancestors | {obj_id}
        return PgComposite(
            type_oid=value.type_oid,
            field_names=value.field_names,
            field_oids=value.field_oids,
            values=tuple(_snapshot_sql_value(item, next_ancestors) for item in value.values),
        )
    if isinstance(value, list):
        obj_id = id(value)
        if obj_id in ancestors:
            msg = "Pipeline list parameter contains a reference cycle"
            raise DataError(msg)
        next_ancestors = ancestors | {obj_id}
        return [_snapshot_sql_value(item, next_ancestors) for item in value]
    if isinstance(value, tuple):
        obj_id = id(value)
        if obj_id in ancestors:
            msg = "Pipeline tuple parameter contains a reference cycle"
            raise DataError(msg)
        next_ancestors = ancestors | {obj_id}
        return tuple(_snapshot_sql_value(item, next_ancestors) for item in value)
    if isinstance(value, dict):
        return _snapshot_json_object(value, ancestors)
    msg = f"Unsupported PostgreSQL pipeline parameter value type: {type(value).__name__}"
    raise DataError(msg)


def _snapshot_outer_parameter(param: object) -> PostgresValue | PgParameter:
    """Snapshot one outer query parameter while preserving any ``PgParameter`` OID wrapper."""
    if isinstance(param, PgParameter):
        snap_inner = _snapshot_sql_value(param.value, frozenset({id(param)}))
        return PgParameter(value=snap_inner, type_oid=param.type_oid)
    return _snapshot_sql_value(param, frozenset())


def _build_queued_operation(
    prepare: PreparePostgresQuery,
    kind: Literal["execute", "select"],
    sql: str,
    parameters: PostgresPipelineParameters,
) -> PostgresPipelineOperation:
    """Compile and deep-snapshot one queued pipeline operation at enqueue time."""
    prepared_sql, prepared_params = prepare(sql, parameters)
    if not isinstance(prepared_sql, str):
        msg = "prepare callback must return (str, tuple[...])"
        raise InterfaceError(msg)
    if isinstance(prepared_params, (str, bytes, bytearray, memoryview)) or not isinstance(
        prepared_params,
        Sequence,
    ):
        msg = "prepare callback parameters must be a non-string Sequence"
        raise InterfaceError(msg)
    snap_params = tuple(_snapshot_outer_parameter(p) for p in prepared_params)
    return PostgresPipelineOperation(sql=prepared_sql, parameters=snap_params, kind=kind)


def _project_pipeline_outcomes(
    operations: Sequence[PostgresPipelineOperation],
    outcomes: Sequence[PostgresPipelineOutcome],
) -> list[Any]:
    """Validate outcome cardinality/ordering and project DML counts and ``SELECT`` dict rows."""
    if len(outcomes) != len(operations):
        msg = f"Pipeline returned {len(outcomes)} outcomes for {len(operations)} queued operations"
        raise InterfaceError(msg)
    projected: list[Any] = []
    for expected_idx, (op, outcome) in enumerate(zip(operations, outcomes, strict=True)):
        if outcome.index != expected_idx:
            msg = f"Pipeline outcome index {outcome.index} did not match expected position {expected_idx}"
            raise InterfaceError(msg)
        if outcome.status != "result" or outcome.result is None:
            msg = f"Pipeline outcome at index {expected_idx} returned unexpected status {outcome.status!r} without raising"
            raise InterfaceError(msg)
        result = outcome.result
        if op.kind == "execute":
            projected.append(int(result.affected_rows))
        else:
            col_names = tuple(col.name for col in result.columns)
            rows_out = [dict(zip(col_names, _project_public_row(row), strict=True)) for row in result.rows]
            projected.append(rows_out)
    return projected


class SyncPostgresPipeline:
    """Synchronous transactional single-Sync PostgreSQL pipeline context manager."""

    __slots__ = (
        "_client",
        "_closed",
        "_entered",
        "_failed",
        "_operations",
        "_prepare",
        "_release",
        "_released",
        "_scope_id",
        "_submitted",
    )

    def __init__(
        self,
        client: SyncPipelineClientProtocol,
        *,
        prepare: PreparePostgresQuery | None = None,
        release: Callable[[], None] | None = None,
    ) -> None:
        """Bind a synchronous pipeline context to ``client`` without performing I/O."""
        self._client = client
        self._prepare: PreparePostgresQuery = _default_prepare_query if prepare is None else prepare
        self._release = release
        self._scope_id: int | None = None
        self._operations: list[PostgresPipelineOperation] = []
        self._entered = False
        self._submitted = False
        self._failed = False
        self._closed = False
        self._released = False

    def _run_release_once(self) -> None:
        """Invoke the optional managed lease release hook at most once."""
        if self._released or self._release is None:
            return
        self._released = True
        self._release()

    def __enter__(self) -> Self:
        """Open a retained pipeline transaction or savepoint scope on the bound client."""
        if self._entered or self._closed:
            msg = "SyncPostgresPipeline cannot be entered more than once"
            raise InterfaceError(msg)
        self._entered = True
        try:
            self._scope_id = self._client.pipeline_begin()
        except BaseException as exc:
            self._closed = True
            self._failed = True
            try:
                self._run_release_once()
            except Exception as release_err:
                exc.add_note(f"pipeline release cleanup failed: {release_err!r}")
            raise
        return self

    def execute(self, sql: str, parameters: PostgresPipelineParameters = None) -> None:
        """Snapshot and enqueue a DML/DDL statement whose result projects to an affected-row ``int``."""
        self._enqueue("execute", sql, parameters)

    def select(self, sql: str, parameters: PostgresPipelineParameters = None) -> None:
        """Snapshot and enqueue a ``SELECT``/``RETURNING`` query whose result projects to ``list[dict[str, Any]]``."""
        self._enqueue("select", sql, parameters)

    def _enqueue(
        self,
        kind: Literal["execute", "select"],
        sql: str,
        parameters: PostgresPipelineParameters,
    ) -> None:
        """Validate open/unsubmitted state and append an immutable queued operation snapshot."""
        if not self._entered or self._closed or self._scope_id is None:
            msg = "SyncPostgresPipeline must be entered before queueing operations"
            raise InterfaceError(msg)
        if self._submitted:
            self._failed = True
            msg = "Cannot enqueue pipeline operations after results() has already been called"
            raise InterfaceError(msg)
        try:
            op = _build_queued_operation(self._prepare, kind, sql, parameters)
        except BaseException:
            self._failed = True
            raise
        self._operations.append(op)

    def results(self) -> list[Any]:
        """Submit queued operations in one single-Sync batch and return projected results inside the open scope."""
        if not self._entered or self._closed or self._scope_id is None:
            msg = "SyncPostgresPipeline must be active when calling results()"
            raise InterfaceError(msg)
        if self._submitted:
            self._failed = True
            msg = "results() may only be called once per SyncPostgresPipeline scope"
            raise InterfaceError(msg)
        if self._failed:
            msg = "Cannot execute pipeline results() after an earlier preparation or parameter failure"
            raise InterfaceError(msg)
        self._submitted = True
        if not self._operations:
            return []
        try:
            outcomes = self._client.pipeline_results(self._scope_id, tuple(self._operations))
            return _project_pipeline_outcomes(self._operations, outcomes)
        except BaseException:
            self._failed = True
            raise

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit/release or roll back the retained scope before invoking ``release`` once."""
        if self._closed:
            return
        self._closed = True
        scope_id = self._scope_id
        self._scope_id = None
        commit_ok = exc_type is None and exc is None and not self._failed
        finish_err: Exception | None = None
        release_err: Exception | None = None

        if scope_id is not None:
            try:
                self._client.pipeline_finish(scope_id, commit_ok)
            except Exception as err:
                if exc is not None:
                    exc.add_note(f"pipeline_finish cleanup failed: {err!r}")
                else:
                    finish_err = err

        try:
            self._run_release_once()
        except Exception as err:
            if exc is not None:
                exc.add_note(f"pipeline release cleanup failed: {err!r}")
            else:
                release_err = err

        if exc is not None:
            return
        if finish_err is not None and release_err is not None:
            finish_err.add_note(f"pipeline release cleanup failed: {release_err!r}")
            raise finish_err from release_err
        if finish_err is not None:
            raise finish_err
        if release_err is not None:
            raise release_err


class AsyncPostgresPipeline:
    """Asynchronous transactional single-Sync PostgreSQL pipeline context manager."""

    __slots__ = (
        "_client",
        "_closed",
        "_entered",
        "_failed",
        "_operations",
        "_prepare",
        "_release",
        "_released",
        "_scope_id",
        "_submitted",
    )

    def __init__(
        self,
        client: AsyncPipelineClientProtocol,
        *,
        prepare: PreparePostgresQuery | None = None,
        release: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Bind an asynchronous pipeline context to ``client`` without performing I/O."""
        self._client = client
        self._prepare: PreparePostgresQuery = _default_prepare_query if prepare is None else prepare
        self._release = release
        self._scope_id: int | None = None
        self._operations: list[PostgresPipelineOperation] = []
        self._entered = False
        self._submitted = False
        self._failed = False
        self._closed = False
        self._released = False

    async def _run_release_once(self) -> None:
        """Invoke the optional async managed lease release hook at most once."""
        if self._released or self._release is None:
            return
        self._released = True
        await self._release()

    async def __aenter__(self) -> Self:
        """Open a retained pipeline transaction or savepoint scope asynchronously on the bound client."""
        if self._entered or self._closed:
            msg = "AsyncPostgresPipeline cannot be entered more than once"
            raise InterfaceError(msg)
        self._entered = True
        try:
            self._scope_id = await self._client.pipeline_begin()
        except BaseException as exc:
            self._closed = True
            self._failed = True
            try:
                await self._run_release_once()
            except Exception as release_err:
                exc.add_note(f"pipeline release cleanup failed: {release_err!r}")
            raise
        return self

    def execute(self, sql: str, parameters: PostgresPipelineParameters = None) -> None:
        """Snapshot and enqueue a DML/DDL statement whose result projects to an affected-row ``int``."""
        self._enqueue("execute", sql, parameters)

    def select(self, sql: str, parameters: PostgresPipelineParameters = None) -> None:
        """Snapshot and enqueue a ``SELECT``/``RETURNING`` query whose result projects to ``list[dict[str, Any]]``."""
        self._enqueue("select", sql, parameters)

    def _enqueue(
        self,
        kind: Literal["execute", "select"],
        sql: str,
        parameters: PostgresPipelineParameters,
    ) -> None:
        """Validate open/unsubmitted state and append an immutable queued operation snapshot."""
        if not self._entered or self._closed or self._scope_id is None:
            msg = "AsyncPostgresPipeline must be entered before queueing operations"
            raise InterfaceError(msg)
        if self._submitted:
            self._failed = True
            msg = "Cannot enqueue pipeline operations after results() has already been called"
            raise InterfaceError(msg)
        try:
            op = _build_queued_operation(self._prepare, kind, sql, parameters)
        except BaseException:
            self._failed = True
            raise
        self._operations.append(op)

    async def results(self) -> list[Any]:
        """Submit queued operations asynchronously in one single-Sync batch and return projected results."""
        if not self._entered or self._closed or self._scope_id is None:
            msg = "AsyncPostgresPipeline must be active when calling results()"
            raise InterfaceError(msg)
        if self._submitted:
            self._failed = True
            msg = "results() may only be called once per AsyncPostgresPipeline scope"
            raise InterfaceError(msg)
        if self._failed:
            msg = "Cannot execute pipeline results() after an earlier preparation or parameter failure"
            raise InterfaceError(msg)
        self._submitted = True
        if not self._operations:
            return []
        try:
            outcomes = await self._client.pipeline_results(self._scope_id, tuple(self._operations))
            return _project_pipeline_outcomes(self._operations, outcomes)
        except BaseException:
            self._failed = True
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit/release or roll back the retained scope asynchronously before invoking ``release`` once."""
        if self._closed:
            return
        self._closed = True
        scope_id = self._scope_id
        self._scope_id = None
        commit_ok = exc_type is None and exc is None and not self._failed
        finish_err: Exception | None = None
        release_err: Exception | None = None

        if scope_id is not None:
            try:
                await self._client.pipeline_finish(scope_id, commit_ok)
            except Exception as err:
                if exc is not None:
                    exc.add_note(f"pipeline_finish cleanup failed: {err!r}")
                else:
                    finish_err = err

        try:
            await self._run_release_once()
        except Exception as err:
            if exc is not None:
                exc.add_note(f"pipeline release cleanup failed: {err!r}")
            else:
                release_err = err

        if exc is not None:
            return
        if finish_err is not None and release_err is not None:
            finish_err.add_note(f"pipeline release cleanup failed: {release_err!r}")
            raise finish_err from release_err
        if finish_err is not None:
            raise finish_err
        if release_err is not None:
            raise release_err
