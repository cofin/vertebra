"""DuckDB synchronous and asynchronous SQLSpec driver adapters over `vertebra._duckdb`."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import ParameterStyle, StatementConfig
from sqlspec.driver import validate_savepoint_name

from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.duckdb.core import (
    build_duckdb_copy_sql,
    default_statement_config,
    description_has_arrow_divergent_types,
    duckdb_copy_filename_follows_query,
    map_duckdb_exception,
    order_duckdb_copy_parameters,
)
from vertebra.extensions.sqlspec.adapters.duckdb.data_dictionary import (
    DuckDBAsyncDataDictionary,
    DuckDBSyncDataDictionary,
)

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.duckdb._typing import DuckDBDriverFeatures

__all__ = ("DuckDBAsyncDriver", "DuckDBSyncDriver")


class DuckDBSyncDriver(BaseSyncDriver):
    """Synchronous DuckDB SQLSpec driver backed by Vertebra `SyncDuckDBDriver`."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "duckdb"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: DuckDBDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous DuckDB driver adapter preserving native Python `fetchall()` fidelity."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: DuckDBSyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> DuckDBSyncDataDictionary:
        """Return lazily instantiated synchronous DuckDB data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = DuckDBSyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default DuckDB `StatementConfig` supporting `?` and `$1` placeholders."""
        return default_statement_config()

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the client-bound callables the native driver holds, or `None` to register no executor."""
        if getattr(self.native_driver, "_dynamic_secrets", ()):
            return self._single_call_pair(self.native_driver, "execute_with_description", "execute_dml")
        return self._single_call_pair(self.native_driver, "_fetch_rows", "_run_dml")

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_duckdb_exception(exc)

    def create_savepoint(self, name: str) -> None:
        """Create a DuckDB transactional savepoint."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"SAVEPOINT {valid}")

    def release_savepoint(self, name: str) -> None:
        """Release a DuckDB transactional savepoint."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a DuckDB transactional savepoint."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    def prepare_copy_to_query(
        self,
        statement: Any,
        *parameters: Any,
        destination: str,
        file_format: str = "parquet",
        filename_follows_query: bool | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> tuple[str, tuple[Any, ...]]:
        """Compile `statement` into DuckDB `COPY (<query>) TO ?` with DuckDB 2.0+ lexical bind ordering."""
        cfg = statement_config or self.statement_config
        prepared = self.prepare_statement(statement, parameters, statement_config=cfg, kwargs=kwargs)
        compiled_sql, compiled_params = self._compiled_sql(prepared, cfg)
        copy_sql = build_duckdb_copy_sql(compiled_sql, file_format)
        if copy_sql is None:
            msg = f"Cannot wrap DuckDB COPY statement for format={file_format!r}: {compiled_sql!r}"
            raise ValueError(msg)
        seq_params: Sequence[Any]
        if compiled_params is None:
            seq_params = ()
        elif isinstance(compiled_params, Mapping):
            seq_params = tuple(compiled_params.values())
        elif isinstance(compiled_params, Sequence) and not isinstance(compiled_params, (str, bytes)):
            seq_params = compiled_params
        else:
            seq_params = (compiled_params,)
        ordered = order_duckdb_copy_parameters(
            seq_params,
            destination,
            filename_follows_query=(
                duckdb_copy_filename_follows_query() if filename_follows_query is None else filename_follows_query
            ),
        )
        return copy_sql, ordered

    def should_bypass_arrow_materialization(self, description: Sequence[Any] | None) -> bool:
        """Return True when DuckDB's Arrow export diverges from native Python `fetchall()` fidelity."""
        return description_has_arrow_divergent_types(description)


class DuckDBAsyncDriver(BaseAsyncDriver):
    """Asynchronous DuckDB SQLSpec driver backed by Vertebra `AsyncDuckDBDriver`."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "duckdb"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: DuckDBDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous DuckDB driver adapter preserving native Python `fetchall()` fidelity."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: DuckDBAsyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> DuckDBAsyncDataDictionary:
        """Return lazily instantiated asynchronous DuckDB data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = DuckDBAsyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default DuckDB `StatementConfig` supporting `?` and `$1` placeholders."""
        return default_statement_config()

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the client-bound callables the native async driver holds, or `None` to register no executor."""
        if getattr(self.native_driver, "_dynamic_secrets", ()):
            return BaseSyncDriver._single_call_pair(self.native_driver, "execute_with_description", "execute_dml")
        return BaseSyncDriver._single_call_pair(self.native_driver, "_fetch_rows", "_run_dml")

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_duckdb_exception(exc)

    async def create_savepoint(self, name: str) -> None:
        """Create a DuckDB transactional savepoint asynchronously."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "savepoint", None)
        if callable(fn):
            await cast("Any", fn)(valid)
            return
        await self.connection.execute(f"SAVEPOINT {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Release a DuckDB transactional savepoint asynchronously."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            await cast("Any", fn)(valid)
            return
        await self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a DuckDB transactional savepoint asynchronously."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            await cast("Any", fn)(valid)
            return
        await self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    def prepare_copy_to_query(
        self,
        statement: Any,
        *parameters: Any,
        destination: str,
        file_format: str = "parquet",
        filename_follows_query: bool | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> tuple[str, tuple[Any, ...]]:
        """Compile `statement` into DuckDB `COPY (<query>) TO ?` with DuckDB 2.0+ lexical bind ordering."""
        cfg = statement_config or self.statement_config
        prepared = self.prepare_statement(statement, parameters, statement_config=cfg, kwargs=kwargs)
        compiled_sql, compiled_params = self._compiled_sql(prepared, cfg)
        copy_sql = build_duckdb_copy_sql(compiled_sql, file_format)
        if copy_sql is None:
            msg = f"Cannot wrap DuckDB COPY statement for format={file_format!r}: {compiled_sql!r}"
            raise ValueError(msg)
        seq_params: Sequence[Any]
        if compiled_params is None:
            seq_params = ()
        elif isinstance(compiled_params, Mapping):
            seq_params = tuple(compiled_params.values())
        elif isinstance(compiled_params, Sequence) and not isinstance(compiled_params, (str, bytes)):
            seq_params = compiled_params
        else:
            seq_params = (compiled_params,)
        ordered = order_duckdb_copy_parameters(
            seq_params,
            destination,
            filename_follows_query=(
                duckdb_copy_filename_follows_query() if filename_follows_query is None else filename_follows_query
            ),
        )
        return copy_sql, ordered

    def should_bypass_arrow_materialization(self, description: Sequence[Any] | None) -> bool:
        """Return True when DuckDB's Arrow export diverges from native Python `fetchall()` fidelity."""
        return description_has_arrow_divergent_types(description)
