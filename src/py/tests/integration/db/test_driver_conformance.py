"""Cross-engine database, analytical, and cache driver contract conformance suite."""

from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest
from msgspec import structs

import vertebra.db.adapters.datafusion.driver as datafusion_driver_mod
from vertebra._vertebra import BigQueryError, SpannerError
from vertebra.db import (
    ArrowCursor,
    AsyncDatabaseDriver,
    AsyncPipelineCapable,
    BaseDatabaseConfig,
    DatabaseError,
    DatabaseRegistry,
    IntegrityError,
    NotFoundError,
    OperationalError,
    ProgrammingError,
    SyncDatabaseDriver,
    SyncPipelineCapable,
    provide_driver,
)
from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver,
    BigQueryConfig,
    BigQueryCore,
    SyncBigQueryDriver,
)
from vertebra.db.adapters.datafusion import (
    DataFusionConfig,
    DataFusionEngine,
    TableRegistration,
)
from vertebra.db.adapters.duckdb import (
    AsyncDuckDBDriver,
    DuckDBConfig,
    SyncDuckDBDriver,
)
from vertebra.db.adapters.mssql import (
    AsyncMSSQLDriver,
    MSSQLConfig,
    SyncMSSQLDriver,
)
from vertebra.db.adapters.mysql import (
    AsyncMySQLDriver,
    MySQLConfig,
    SyncMySQLDriver,
)
from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    OracleConfig,
    SyncOracleDriver,
)
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PgJson,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.adapters.redis import (
    AsyncRedisDriver,
    RedisConfig,
    SyncRedisDriver,
)
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    SpannerConfig,
    SpannerCore,
    SyncSpannerDriver,
)
from vertebra.db.adapters.sqlite import (
    AsyncSQLiteDriver,
    SQLiteConfig,
    SyncSQLiteDriver,
)
from vertebra.db.adapters.valkey import (
    AsyncValkeyDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)
from vertebra.typing import FrozenBaseStruct, MissingDependencyError, reset_dependency_cache
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import decode_json, encode_json

_DUCKDB_SKIP_REASON = "duckdb publishes no free-threaded wheel; DuckDB tests run on GIL interpreters"


class _RollbackSentinelError(RuntimeError):
    """Sentinel exception used to verify transaction rollback semantics."""


class ConformanceItemRow(FrozenBaseStruct, frozen=True, gc=False):
    """Typed row schema used to validate `schema_type` hydration across SQL drivers."""

    id: int
    """Primary key identifier."""
    label: str
    """Human-readable item label."""
    amount: int | None = None
    """Optional integer quantity."""


_ITEMS_DDL = "CREATE TABLE vtb_conformance_items (id BIGINT PRIMARY KEY, label TEXT NOT NULL, amount BIGINT)"

_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id BIGINT PRIMARY KEY, "
    "val_int BIGINT, "
    "val_float DOUBLE PRECISION, "
    "val_decimal TEXT, "
    "val_bool INTEGER, "
    "val_text TEXT, "
    "val_bytes BLOB, "
    "val_ts TEXT, "
    "val_json TEXT"
    ")"
)

_DUCKDB_ITEMS_DDL = "CREATE TABLE vtb_conformance_items (id BIGINT PRIMARY KEY, label VARCHAR NOT NULL, amount BIGINT)"

_DUCKDB_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id BIGINT PRIMARY KEY, "
    "val_int BIGINT, "
    "val_float DOUBLE, "
    "val_decimal DECIMAL(18, 4), "
    "val_bool BOOLEAN, "
    "val_text VARCHAR, "
    "val_bytes BLOB, "
    "val_ts TIMESTAMP, "
    "val_json VARCHAR"
    ")"
)

_POSTGRES_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id BIGINT PRIMARY KEY, "
    "val_int BIGINT, "
    "val_float DOUBLE PRECISION, "
    "val_decimal NUMERIC(18, 4), "
    "val_bool BOOLEAN, "
    "val_text TEXT, "
    "val_bytes BYTEA, "
    "val_ts TIMESTAMPTZ, "
    "val_json JSONB"
    ")"
)

_MYSQL_ITEMS_DDL = (
    "CREATE TABLE vtb_conformance_items (id BIGINT PRIMARY KEY, label VARCHAR(255) NOT NULL, amount BIGINT)"
)

_MYSQL_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id BIGINT PRIMARY KEY, "
    "val_int BIGINT, "
    "val_float DOUBLE, "
    "val_decimal DECIMAL(20, 4), "
    "val_bool BOOLEAN, "
    "val_text TEXT, "
    "val_bytes BLOB, "
    "val_ts DATETIME(6), "
    "val_json JSON"
    ")"
)

_BIGQUERY_ITEMS_DDL = "CREATE TABLE vtb_conformance_items (id INT64, label STRING NOT NULL, amount INT64)"

_BIGQUERY_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id INT64, "
    "val_int INT64, "
    "val_float FLOAT64, "
    "val_decimal NUMERIC, "
    "val_bool BOOL, "
    "val_text STRING, "
    "val_bytes BYTES, "
    "val_ts TIMESTAMP, "
    "val_json JSON"
    ")"
)

_SPANNER_ITEMS_DDL = (
    "CREATE TABLE vtb_conformance_items (id INT64 NOT NULL, label STRING(MAX) NOT NULL, amount INT64) PRIMARY KEY (id)"
)

_SPANNER_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id INT64 NOT NULL, "
    "val_int INT64, "
    "val_float FLOAT64, "
    "val_decimal NUMERIC, "
    "val_bool BOOL, "
    "val_text STRING(MAX), "
    "val_bytes BYTES(MAX), "
    "val_ts TIMESTAMP, "
    "val_json JSON"
    ") PRIMARY KEY (id)"
)

_MSSQL_ITEMS_DDL = (
    "CREATE TABLE vtb_conformance_items (id BIGINT PRIMARY KEY, label NVARCHAR(255) NOT NULL, amount BIGINT)"
)

_MSSQL_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id BIGINT PRIMARY KEY, "
    "val_int BIGINT, "
    "val_float FLOAT, "
    "val_decimal DECIMAL(38, 4), "
    "val_bool BIT, "
    "val_text NVARCHAR(MAX), "
    "val_bytes VARBINARY(MAX), "
    "val_ts DATETIMEOFFSET, "
    "val_json NVARCHAR(MAX)"
    ")"
)

_ORACLE_ITEMS_DDL = (
    "CREATE TABLE vtb_conformance_items (id NUMBER(19) PRIMARY KEY, label VARCHAR2(255) NOT NULL, amount NUMBER(19))"
)

_ORACLE_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id NUMBER(19) PRIMARY KEY, "
    "val_int NUMBER(19), "
    "val_float BINARY_DOUBLE, "
    "val_decimal NUMBER(18, 6), "
    "val_bool BOOLEAN, "
    "val_text CLOB, "
    "val_bytes RAW(2000), "
    "val_ts TIMESTAMP WITH TIME ZONE, "
    "val_json JSON"
    ")"
)


class EngineConformanceSpec(FrozenBaseStruct, frozen=True, gc=False):
    """Declarative specification for a synchronous/asynchronous SQL or analytical adapter pair."""

    engine: str
    """Canonical adapter identifier (`sqlite`, `postgres`, `oracle`, `mysql`, `mssql`, `duckdb`, `bigquery`, `spanner`)."""
    config_cls: type[BaseDatabaseConfig]
    """Adapter configuration struct subclassing `BaseDatabaseConfig`."""
    sync_driver_cls: type[SyncDatabaseDriver[Any]]
    """Paired synchronous driver subclassing `SyncDatabaseDriver`."""
    async_driver_cls: type[AsyncDatabaseDriver[Any]]
    """Paired asynchronous driver subclassing `AsyncDatabaseDriver`."""
    sample_url: str
    """Canonical DSN URL parsed by `DatabaseRegistry.parse_url` and `config_cls.from_url`."""
    probe_sql: str = "SELECT 1"
    """Connectivity probe statement returning integer `1`."""
    placeholder_style: str = "qmark"
    """Positional placeholder syntax (`qmark`, `dollar`, `colon`, `at_p`, `at_upper_p`)."""
    items_ddl: str = _ITEMS_DDL
    """Engine-specific DDL statement provisioning `vtb_conformance_items`."""
    types_ddl: str = _TYPES_DDL
    """Engine-specific DDL statement provisioning `vtb_conformance_types`."""
    supports_pipeline: bool = False
    """Whether the driver pair implements `SyncPipelineCapable` and `AsyncPipelineCapable`."""
    duplicate_or_constraint_sql: tuple[str, tuple[Any, ...]] | None = None
    """Optional `(sql, params)` statement triggering a constraint error when primary keys are unenforced."""
    constraint_error_types: tuple[type[DatabaseError], ...] = (IntegrityError,)
    """Typed `DatabaseError` subclasses expected on primary-key or `NOT NULL` constraint violations."""
    invalid_sql_error_types: tuple[type[DatabaseError], ...] = (ProgrammingError, OperationalError)
    """Typed `DatabaseError` subclasses expected when querying a nonexistent relation."""
    native_extension_module: str | None = None
    """Compiled native Rust PyO3 extension module required for live wire execution, if any."""
    pending_flow_id: str | None = None
    """Owning `native-data-platform` Flow spec ID when native wire implementation is still pending."""
    env_dsn_vars: tuple[str, ...] = ()
    """Environment variable names providing live external or emulator DSNs."""
    container_fixture: str | None = None
    """Optional `pytest-databases` configuration fixture in `conftest.py` used when `VERTEBRA_CONFORMANCE_DOCKER=1`."""

    def bind_scalar_case(self, type_case: ScalarTypeCase) -> Any:
        """Encode `type_case.input_value` for engines requiring explicit `JSON` parameter descriptors."""
        if self.engine in {"bigquery", "spanner"} and type_case.comparison_kind == "json":
            return {"type": "JSON", "value": encode_json(type_case.input_value)}
        if self.engine == "postgres" and type_case.comparison_kind == "json":
            return PgJson(type_case.input_value)
        return type_case.input_value

    def typed_null_int64(self) -> Any:
        """Return an engine-appropriate `NULL` parameter value targeting a nullable `INT64` / `BIGINT` column."""
        if self.engine in {"bigquery", "spanner"}:
            return {"type": "INT64", "value": None}
        return None

    def format_sql(self, sql: str) -> str:
        """Rewrite `?` placeholders outside quoted literals into this engine's `placeholder_style`."""
        if self.placeholder_style == "qmark":
            return sql
        out: list[str] = []
        param_index = 0
        idx = 0
        length = len(sql)
        while idx < length:
            ch = sql[idx]
            if ch in {"'", '"'}:
                quote = ch
                out.append(ch)
                idx += 1
                while idx < length:
                    cur = sql[idx]
                    out.append(cur)
                    if cur == quote:
                        if idx + 1 < length and sql[idx + 1] == quote:
                            out.append(sql[idx + 1])
                            idx += 2
                            continue
                        idx += 1
                        break
                    idx += 1
                continue
            if ch == "?":
                param_index += 1
                if self.placeholder_style == "dollar":
                    out.append(f"${param_index}")
                elif self.placeholder_style == "colon":
                    out.append(f":{param_index}")
                elif self.placeholder_style == "at_p":
                    out.append(f"@p{param_index}")
                elif self.placeholder_style == "at_upper_p":
                    out.append(f"@P{param_index}")
                else:
                    msg = f"Unsupported placeholder_style: {self.placeholder_style!r}"
                    raise ValueError(msg)
                idx += 1
                continue
            out.append(ch)
            idx += 1
        return "".join(out)


class ScalarTypeCase(FrozenBaseStruct, frozen=True, gc=False):
    """Parameterized scalar or structured bind-and-decode test case."""

    name: str
    """Short pytest parameter identifier."""
    column: str
    """Target column in `vtb_conformance_types`."""
    input_value: Any
    """Python value bound as a query parameter."""
    value_type: type[Any] | None
    """Optional `value_type` passed to `select_value`."""
    comparison_kind: str
    """Assertion mode (`null`, `exact`, `float`, `bool`, `bytes`, `datetime`, `json`)."""


class CacheConformanceSpec(FrozenBaseStruct, frozen=True, gc=False):
    """Declarative specification for a RESP3 cache adapter pair."""

    engine: str
    """Cache adapter identifier (`redis` or `valkey`)."""
    config_cls: type[RedisConfig]
    """Configuration class (`RedisConfig` or `ValkeyConfig`)."""
    sync_driver_cls: type[SyncRedisDriver]
    """Synchronous RESP3 driver class."""
    async_driver_cls: type[AsyncRedisDriver]
    """Asynchronous RESP3 driver class."""
    tcp_url: str
    """Canonical TCP URL used to verify URL parsing and shared pool keys."""
    env_dsn_vars: tuple[str, ...] = ()
    """Environment variables holding a live server URL, checked in order."""
    container_fixture: str | None = None
    """Optional `pytest-databases` configuration fixture in `conftest.py` used when `VERTEBRA_CONFORMANCE_DOCKER=1`."""


class _StubDataFusionFrame:
    """Minimal DataFusion DataFrame stand-in returning a registered `pyarrow.Table`."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_arrow_table(self) -> pa.Table:
        """Return the underlying `pyarrow.Table`."""
        return self._table


class _StubDataFusionSessionContext:
    """In-process session context double exercising `DataFusionEngine` table registration and Arrow execution."""

    def __init__(self) -> None:
        self.registered: dict[str, pa.Table] = {}

    def register_table(self, name: str, table: pa.Table) -> None:
        """Record a normalized `pyarrow.Table` under `name`."""
        self.registered[name] = table

    def sql(self, query: str) -> _StubDataFusionFrame:
        """Return the first registered table referenced in `query`."""
        for name, table in self.registered.items():
            if name in query:
                return _StubDataFusionFrame(table)
        msg = f"No registered table matched query: {query!r}"
        raise KeyError(msg)


ENGINE_SPECS: tuple[EngineConformanceSpec, ...] = (
    EngineConformanceSpec(
        engine="sqlite",
        config_cls=SQLiteConfig,
        sync_driver_cls=SyncSQLiteDriver,
        async_driver_cls=AsyncSQLiteDriver,
        sample_url="sqlite:////tmp/vtb_conformance_sample.sqlite3",
        placeholder_style="qmark",
    ),
    EngineConformanceSpec(
        engine="postgres",
        config_cls=PostgreSQLConfig,
        sync_driver_cls=SyncPostgreSQLDriver,
        async_driver_cls=AsyncPostgreSQLDriver,
        sample_url="postgresql://vtb:secret@127.0.0.1:5432/vtb",
        placeholder_style="dollar",
        types_ddl=_POSTGRES_TYPES_DDL,
        supports_pipeline=True,
        native_extension_module="vertebra._postgres",
        pending_flow_id=None,
        env_dsn_vars=("VERTEBRA_CONFORMANCE_POSTGRES_DSN",),
        container_fixture="vtb_postgres_config",
    ),
    EngineConformanceSpec(
        engine="oracle",
        config_cls=OracleConfig,
        sync_driver_cls=SyncOracleDriver,
        async_driver_cls=AsyncOracleDriver,
        sample_url="oracle://system:secret@127.0.0.1:1521/FREEPDB1",
        probe_sql="SELECT 1 FROM DUAL",
        placeholder_style="colon",
        items_ddl=_ORACLE_ITEMS_DDL,
        types_ddl=_ORACLE_TYPES_DDL,
        supports_pipeline=True,
        native_extension_module="vertebra._oracle",
        pending_flow_id=None,
        env_dsn_vars=("VERTEBRA_CONFORMANCE_ORACLE_DSN",),
        container_fixture="vtb_oracle_config",
    ),
    EngineConformanceSpec(
        engine="mysql",
        config_cls=MySQLConfig,
        sync_driver_cls=SyncMySQLDriver,
        async_driver_cls=AsyncMySQLDriver,
        sample_url="mysql://vtb:secret@127.0.0.1:3306/vtb",
        placeholder_style="qmark",
        items_ddl=_MYSQL_ITEMS_DDL,
        types_ddl=_MYSQL_TYPES_DDL,
        supports_pipeline=True,
        native_extension_module="vertebra._mysql",
        pending_flow_id=None,
        env_dsn_vars=("VERTEBRA_CONFORMANCE_MYSQL_DSN",),
        container_fixture="vtb_mysql_config",
    ),
    EngineConformanceSpec(
        engine="mssql",
        config_cls=MSSQLConfig,
        sync_driver_cls=SyncMSSQLDriver,
        async_driver_cls=AsyncMSSQLDriver,
        sample_url="mssql://sa:secret@127.0.0.1:1433/master",
        placeholder_style="at_upper_p",
        items_ddl=_MSSQL_ITEMS_DDL,
        types_ddl=_MSSQL_TYPES_DDL,
        supports_pipeline=True,
        native_extension_module="vertebra._mssql",
        pending_flow_id=None,
        env_dsn_vars=("VERTEBRA_CONFORMANCE_MSSQL_DSN",),
        container_fixture="vtb_mssql_config",
    ),
    EngineConformanceSpec(
        engine="duckdb",
        config_cls=DuckDBConfig,
        sync_driver_cls=SyncDuckDBDriver,
        async_driver_cls=AsyncDuckDBDriver,
        sample_url="duckdb:////tmp/vtb_conformance_sample.duckdb",
        placeholder_style="qmark",
        items_ddl=_DUCKDB_ITEMS_DDL,
        types_ddl=_DUCKDB_TYPES_DDL,
        supports_pipeline=True,
        native_extension_module="vertebra._duckdb",
        env_dsn_vars=("VERTEBRA_CONFORMANCE_DUCKDB_DSN",),
    ),
    EngineConformanceSpec(
        engine="bigquery",
        config_cls=BigQueryConfig,
        sync_driver_cls=SyncBigQueryDriver,
        async_driver_cls=AsyncBigQueryDriver,
        sample_url="bigquery://test-project/test_dataset?location=US",
        placeholder_style="qmark",
        items_ddl=_BIGQUERY_ITEMS_DDL,
        types_ddl=_BIGQUERY_TYPES_DDL,
        duplicate_or_constraint_sql=(
            "INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)",
            (1, None, 20),
        ),
        constraint_error_types=(BigQueryError,),
        invalid_sql_error_types=(BigQueryError,),
        native_extension_module="vertebra._bigquery",
        env_dsn_vars=("VERTEBRA_CONFORMANCE_BIGQUERY_DSN",),
    ),
    EngineConformanceSpec(
        engine="spanner",
        config_cls=SpannerConfig,
        sync_driver_cls=SyncSpannerDriver,
        async_driver_cls=AsyncSpannerDriver,
        sample_url="spanner://projects/test-project/instances/test-instance/databases/test-db",
        placeholder_style="at_p",
        items_ddl=_SPANNER_ITEMS_DDL,
        types_ddl=_SPANNER_TYPES_DDL,
        constraint_error_types=(SpannerError,),
        invalid_sql_error_types=(SpannerError,),
        native_extension_module="vertebra._spanner",
        env_dsn_vars=("VERTEBRA_CONFORMANCE_SPANNER_DSN", "SPANNER_EMULATOR_HOST"),
    ),
)

SCALAR_TYPE_CASES: tuple[ScalarTypeCase, ...] = (
    ScalarTypeCase(
        name="null",
        column="val_text",
        input_value=None,
        value_type=None,
        comparison_kind="null",
    ),
    ScalarTypeCase(
        name="int64",
        column="val_int",
        input_value=(1 << 40) + 7,
        value_type=int,
        comparison_kind="exact",
    ),
    ScalarTypeCase(
        name="float64",
        column="val_float",
        input_value=3.141592653589793,
        value_type=float,
        comparison_kind="float",
    ),
    ScalarTypeCase(
        name="decimal",
        column="val_decimal",
        input_value=Decimal("12345.6789"),
        value_type=Decimal,
        comparison_kind="exact",
    ),
    ScalarTypeCase(
        name="bool_true",
        column="val_bool",
        input_value=True,
        value_type=bool,
        comparison_kind="bool",
    ),
    ScalarTypeCase(
        name="bool_false",
        column="val_bool",
        input_value=False,
        value_type=bool,
        comparison_kind="bool",
    ),
    ScalarTypeCase(
        name="utf8_text",
        column="val_text",
        input_value="Vertebra 脊椎 🚀 café",
        value_type=str,
        comparison_kind="exact",
    ),
    ScalarTypeCase(
        name="binary_bytes",
        column="val_bytes",
        input_value=b"\x00\x01\xfe\xffvtb",
        value_type=bytes,
        comparison_kind="bytes",
    ),
    ScalarTypeCase(
        name="tz_datetime",
        column="val_ts",
        input_value=datetime(2026, 11, 4, 20, 30, 15, tzinfo=UTC),
        value_type=datetime,
        comparison_kind="datetime",
    ),
    ScalarTypeCase(
        name="json_dict",
        column="val_json",
        input_value={"nested": [1, True], "tag": "vtb"},
        value_type=None,
        comparison_kind="json",
    ),
    ScalarTypeCase(
        name="json_list",
        column="val_json",
        input_value=["alpha", 42, {"ok": True}],
        value_type=None,
        comparison_kind="json",
    ),
)

CACHE_ENGINE_SPECS: tuple[CacheConformanceSpec, ...] = (
    CacheConformanceSpec(
        engine="redis",
        config_cls=RedisConfig,
        sync_driver_cls=SyncRedisDriver,
        async_driver_cls=AsyncRedisDriver,
        tcp_url="redis://127.0.0.1:6379/2",
        env_dsn_vars=("VERTEBRA_CONFORMANCE_REDIS_DSN",),
        container_fixture="vtb_redis_config",
    ),
    CacheConformanceSpec(
        engine="valkey",
        config_cls=ValkeyConfig,
        sync_driver_cls=SyncValkeyDriver,
        async_driver_cls=AsyncValkeyDriver,
        tcp_url="valkey://127.0.0.1:6379/2",
        env_dsn_vars=("VERTEBRA_CONFORMANCE_VALKEY_DSN",),
        container_fixture="vtb_valkey_config",
    ),
)


_CONFORMANCE_DOCKER_ENV = "VERTEBRA_CONFORMANCE_DOCKER"
POSTGRES_CONFORMANCE_VERSIONS: tuple[str, ...] = ("14", "15", "16", "17", "18")


def _parse_conformance_env_filter(env: Mapping[str, str], key: str) -> tuple[str, ...]:
    """Return stripped non-empty comma-separated filter tokens from ``env[key]``."""
    return tuple(token.strip() for token in env.get(key, "").split(",") if token.strip())


def _expand_live_engine_variants(
    specs: Sequence[EngineConformanceSpec],
    *,
    env: Mapping[str, str] | None = None,
) -> list[tuple[str, EngineConformanceSpec]]:
    """Expand ``postgres`` across selected major versions `14`–`18` and honor env engine filters."""
    resolved_env = os.environ if env is None else env
    selected_engines = _parse_conformance_env_filter(resolved_env, "VERTEBRA_CONFORMANCE_ENGINES")
    selected_pg_versions = _parse_conformance_env_filter(resolved_env, "VERTEBRA_CONFORMANCE_POSTGRES_VERSION")
    unknown_versions = [v for v in selected_pg_versions if v not in POSTGRES_CONFORMANCE_VERSIONS]
    if unknown_versions:
        msg = f"Unsupported VERTEBRA_CONFORMANCE_POSTGRES_VERSION entries: {unknown_versions}"
        raise ValueError(msg)

    variants: list[tuple[str, EngineConformanceSpec]] = []
    for spec in specs:
        if selected_engines and spec.engine not in selected_engines:
            continue
        if spec.engine != "postgres":
            variants.append((spec.engine, spec))
            continue
        versions = (
            tuple(v for v in POSTGRES_CONFORMANCE_VERSIONS if v in selected_pg_versions)
            if selected_pg_versions
            else POSTGRES_CONFORMANCE_VERSIONS
        )
        for version in versions:
            versioned_spec = structs.replace(
                spec,
                env_dsn_vars=(f"VERTEBRA_CONFORMANCE_POSTGRES_{version}_DSN", *spec.env_dsn_vars),
                container_fixture=f"vtb_postgres_{version}_config",
            )
            variants.append((f"postgres-{version}", versioned_spec))
    return variants


def _build_live_engine_params(
    specs: Sequence[EngineConformanceSpec] = ENGINE_SPECS,
    *,
    env: Mapping[str, str] | None = None,
) -> list[Any]:
    """Build fixture parameters, marking pending engines strict ``xfail`` only when their native module imports.

    Args:
        specs: Engine specs to parametrize.
        env: Optional environment mapping overriding ``VERTEBRA_CONFORMANCE_ENGINES`` and PostgreSQL version filters.

    Returns:
        One ``pytest.param`` per spec variant with stable parameter IDs.
    """
    params: list[Any] = []
    for param_id, spec in _expand_live_engine_variants(specs, env=env):
        if spec.pending_flow_id is not None and not _native_module_missing(spec):
            params.append(
                pytest.param(
                    spec,
                    id=param_id,
                    marks=pytest.mark.xfail(strict=True, reason=f"Owned by {spec.pending_flow_id}"),
                )
            )
        else:
            params.append(pytest.param(spec, id=param_id))
    return params


def _native_module_missing(spec: EngineConformanceSpec) -> bool:
    """Return True when ``spec`` names a native module that does not import."""
    return spec.native_extension_module is not None and import_optional(spec.native_extension_module) is None


def _require_native_module(spec: EngineConformanceSpec) -> None:
    """Skip the case, or fail a non-pending case under Docker conformance, when the native module is missing.

    Args:
        spec: Engine spec whose ``native_extension_module`` is checked.

    Raises:
        pytest.skip.Exception: When the module is missing and either the spec is pending or
            ``VERTEBRA_CONFORMANCE_DOCKER`` is not ``"1"``.
        pytest.fail.Exception: When the module is missing, the spec is not pending, and
            ``VERTEBRA_CONFORMANCE_DOCKER`` is ``"1"``.
    """
    if spec.native_extension_module is None:
        return
    try:
        ensure_module(spec.native_extension_module)
    except MissingDependencyError:
        reason = f"{spec.engine} conformance requires the {spec.native_extension_module} native module"
        if spec.pending_flow_id is None and os.environ.get(_CONFORMANCE_DOCKER_ENV) == "1":
            pytest.fail(reason, pytrace=False)
        pytest.skip(reason)


def _resolve_live_config(
    spec: EngineConformanceSpec,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> BaseDatabaseConfig:
    """Resolve a live configuration for ``spec``, checking its native module before credential skips."""
    _require_native_module(spec)
    if spec.engine == "sqlite":
        db_path = tmp_path / "vtb_conformance.sqlite3"
        return SQLiteConfig.from_url(f"sqlite:///{db_path}")
    if spec.engine == "duckdb":
        db_path = tmp_path / "vtb_conformance.duckdb"
        return DuckDBConfig.from_url(f"duckdb:///{db_path}")
    if spec.engine == "bigquery":
        dsn = os.environ.get("VERTEBRA_CONFORMANCE_BIGQUERY_DSN", "").strip()
        if not dsn:
            pytest.skip("BigQuery conformance requires VERTEBRA_CONFORMANCE_BIGQUERY_DSN")
        return BigQueryConfig.from_url(dsn)
    if spec.engine == "spanner":
        dsn = os.environ.get("VERTEBRA_CONFORMANCE_SPANNER_DSN", "").strip()
        emulator = os.environ.get("SPANNER_EMULATOR_HOST", "").strip()
        if not dsn and not emulator:
            pytest.skip("Spanner conformance requires VERTEBRA_CONFORMANCE_SPANNER_DSN or SPANNER_EMULATOR_HOST")
        resolved_dsn = (
            dsn
            if dsn
            else (
                f"spanner://projects/test-project/instances/test-instance/databases/test-db?endpoint=http://{emulator}"
            )
        )
        return SpannerConfig.from_url(resolved_dsn)
    for env_key in spec.env_dsn_vars:
        env_dsn = os.environ.get(env_key, "").strip()
        if env_dsn:
            return spec.config_cls.from_url(env_dsn)
    if spec.container_fixture is not None and os.environ.get(_CONFORMANCE_DOCKER_ENV) == "1":
        fixture_cfg = request.getfixturevalue(spec.container_fixture)
        assert isinstance(fixture_cfg, BaseDatabaseConfig)
        return fixture_cfg
    if spec.engine == "mysql":
        mysql_mod = ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")
        loopback_server = mysql_mod.MySQLWireTestServer()
        request.addfinalizer(loopback_server.close)
        return MySQLConfig.from_url(loopback_server.dsn)
    if spec.engine == "mssql":
        pytest.skip("MSSQL conformance requires VERTEBRA_CONFORMANCE_MSSQL_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")
    if spec.engine == "oracle":
        pytest.skip("Oracle conformance requires VERTEBRA_CONFORMANCE_ORACLE_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")
    if spec.engine == "postgres":
        pytest.skip(f"PostgreSQL conformance requires {' or '.join(spec.env_dsn_vars)} or {_CONFORMANCE_DOCKER_ENV}=1")
    return spec.config_cls.from_url(spec.sample_url)


def _assert_scalar_case_value(type_case: ScalarTypeCase, actual: Any) -> None:
    """Assert that `actual` decoded from the driver matches `type_case`."""
    if type_case.comparison_kind == "null":
        assert actual is None
        return
    if type_case.comparison_kind == "float":
        assert isinstance(actual, float)
        assert actual == pytest.approx(type_case.input_value)
        return
    if type_case.comparison_kind == "bool":
        assert isinstance(actual, bool)
        assert actual is type_case.input_value
        return
    if type_case.comparison_kind == "bytes":
        assert isinstance(actual, bytes)
        assert actual == type_case.input_value
        return
    if type_case.comparison_kind == "datetime":
        assert isinstance(actual, datetime)
        assert actual.tzinfo is not None
        assert actual == type_case.input_value
        return
    if type_case.comparison_kind == "json":
        decoded = decode_json(actual) if isinstance(actual, (str, bytes)) else actual
        assert decoded == type_case.input_value
        return
    assert type(actual) is type(type_case.input_value)
    assert actual == type_case.input_value


def _lowercase_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    """Normalize driver-reported column names to lowercase for assertion."""
    return tuple(col.lower() for col in columns)


def _batch_row_total(batches: list[Any]) -> int:
    """Count total rows across streamed PyArrow RecordBatches or fallback row slices."""
    return sum(int(batch.num_rows) if hasattr(batch, "num_rows") else len(batch) for batch in batches)


async def _collect_async_batches(stream: AsyncIterator[Any]) -> list[Any]:
    """Materialize an asynchronous stream of Arrow record batches into a list."""
    return [batch async for batch in stream]


@pytest.fixture(params=_build_live_engine_params())
def sync_sql_driver(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Iterator[tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]]]:
    """Open, verify, and provision a live synchronous SQL/analytical driver for conformance testing."""
    spec: EngineConformanceSpec = request.param
    config = _resolve_live_config(spec, tmp_path, request)
    registry = DatabaseRegistry()
    driver = provide_driver(config, sync=True, registry=registry)
    assert isinstance(driver, spec.sync_driver_cls)
    driver.connect()
    try:
        assert driver.select_value(spec.probe_sql, value_type=int) == 1
        if isinstance(driver, SyncSpannerDriver):
            driver.update_ddl(
                [
                    "DROP TABLE IF EXISTS vtb_conformance_items",
                    "DROP TABLE IF EXISTS vtb_conformance_types",
                    spec.items_ddl,
                    spec.types_ddl,
                ]
            )
        elif isinstance(driver, SyncOracleDriver):
            driver.execute("DROP TABLE IF EXISTS vtb_conformance_items PURGE")
            driver.execute("DROP TABLE IF EXISTS vtb_conformance_types PURGE")
            driver.execute(spec.items_ddl)
            driver.execute(spec.types_ddl)
        else:
            driver.execute("DROP TABLE IF EXISTS vtb_conformance_items")
            driver.execute("DROP TABLE IF EXISTS vtb_conformance_types")
            driver.execute(spec.items_ddl)
            driver.execute(spec.types_ddl)
        yield spec, driver
    finally:
        driver.close()


@pytest.fixture(params=_build_live_engine_params())
async def async_sql_driver(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AsyncIterator[tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]]]:
    """Open, verify, and provision a live asynchronous SQL/analytical driver for conformance testing."""
    spec: EngineConformanceSpec = request.param
    config = _resolve_live_config(spec, tmp_path, request)
    registry = DatabaseRegistry()
    driver = provide_driver(config, sync=False, registry=registry)
    assert isinstance(driver, spec.async_driver_cls)
    await driver.connect()
    try:
        assert await driver.select_value(spec.probe_sql, value_type=int) == 1
        if isinstance(driver, AsyncSpannerDriver):
            await driver.update_ddl(
                [
                    "DROP TABLE IF EXISTS vtb_conformance_items",
                    "DROP TABLE IF EXISTS vtb_conformance_types",
                    spec.items_ddl,
                    spec.types_ddl,
                ]
            )
        elif isinstance(driver, AsyncOracleDriver):
            await driver.execute("DROP TABLE IF EXISTS vtb_conformance_items PURGE")
            await driver.execute("DROP TABLE IF EXISTS vtb_conformance_types PURGE")
            await driver.execute(spec.items_ddl)
            await driver.execute(spec.types_ddl)
        else:
            await driver.execute("DROP TABLE IF EXISTS vtb_conformance_items")
            await driver.execute("DROP TABLE IF EXISTS vtb_conformance_types")
            await driver.execute(spec.items_ddl)
            await driver.execute(spec.types_ddl)
        yield spec, driver
    finally:
        await driver.close()


@pytest.mark.parametrize("spec", ENGINE_SPECS, ids=[s.engine for s in ENGINE_SPECS])
def test_engine_url_parsing_and_pool_key_contract(spec: EngineConformanceSpec) -> None:
    """Every SQL/analytical config parses its canonical URL and computes a deterministic pool key."""
    parsed = DatabaseRegistry.parse_url(spec.sample_url)
    direct = spec.config_cls.from_url(spec.sample_url)
    assert isinstance(parsed, spec.config_cls)
    assert parsed == direct
    key = parsed.pool_key()
    assert key is not None
    assert isinstance(key, tuple)
    assert DatabaseRegistry.pool_fingerprint(spec.sample_url) == key


@pytest.mark.anyio
@pytest.mark.parametrize("spec", ENGINE_SPECS, ids=[s.engine for s in ENGINE_SPECS])
async def test_engine_driver_resolution_and_provide_driver(
    spec: EngineConformanceSpec,
    tmp_path: Path,
) -> None:
    """Every SQL/analytical config resolves its paired sync/async driver classes and registers cleanly."""
    url = (
        f"sqlite:///{tmp_path / 'reg.sqlite3'}"
        if spec.engine == "sqlite"
        else (f"duckdb:///{tmp_path / 'reg.duckdb'}" if spec.engine == "duckdb" else spec.sample_url)
    )
    config = DatabaseRegistry.parse_url(url)
    assert isinstance(config, BaseDatabaseConfig)
    sync_cls, async_cls = config.resolve_driver_classes()
    assert sync_cls is spec.sync_driver_cls
    assert async_cls is spec.async_driver_cls

    registry = DatabaseRegistry()
    registry.register(spec.engine, config)
    if spec.engine == "duckdb":
        _require_native_module(spec)
    sync_driver = provide_driver(spec.engine, sync=True, registry=registry)
    async_driver = provide_driver(spec.engine, sync=False, registry=registry)
    try:
        assert isinstance(sync_driver, spec.sync_driver_cls)
        assert isinstance(async_driver, spec.async_driver_cls)
    finally:
        sync_driver.close()
        await async_driver.close()


@pytest.mark.parametrize("type_case", SCALAR_TYPE_CASES, ids=[c.name for c in SCALAR_TYPE_CASES])
def test_sync_scalar_and_structured_types(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
    type_case: ScalarTypeCase,
) -> None:
    """Synchronous drivers round-trip all mandatory scalar and structured Python values."""
    spec, driver = sync_sql_driver
    inserted = driver.execute(
        spec.format_sql(f"INSERT INTO vtb_conformance_types (id, {type_case.column}) VALUES (?, ?)"),
        (1, spec.bind_scalar_case(type_case)),
    )
    assert inserted == 1
    actual = driver.select_value(
        spec.format_sql(f"SELECT {type_case.column} FROM vtb_conformance_types WHERE id = ?"),
        (1,),
        value_type=type_case.value_type,
    )
    _assert_scalar_case_value(type_case, actual)


@pytest.mark.anyio
@pytest.mark.parametrize("type_case", SCALAR_TYPE_CASES, ids=[c.name for c in SCALAR_TYPE_CASES])
async def test_async_scalar_and_structured_types(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
    type_case: ScalarTypeCase,
) -> None:
    """Asynchronous drivers round-trip all mandatory scalar and structured Python values."""
    spec, driver = async_sql_driver
    inserted = await driver.execute(
        spec.format_sql(f"INSERT INTO vtb_conformance_types (id, {type_case.column}) VALUES (?, ?)"),
        (1, spec.bind_scalar_case(type_case)),
    )
    assert inserted == 1
    actual = await driver.select_value(
        spec.format_sql(f"SELECT {type_case.column} FROM vtb_conformance_types WHERE id = ?"),
        (1,),
        value_type=type_case.value_type,
    )
    _assert_scalar_case_value(type_case, actual)


def test_sync_execution_contract(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
) -> None:
    """Synchronous drivers satisfy `execute*` and `select*` return shapes and `NotFoundError` semantics."""
    spec, driver = sync_sql_driver
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    single_count = driver.execute(
        insert_sql,
        (1, "alpha", 10),
    )
    assert isinstance(single_count, int)
    assert single_count == 1

    batch_count = driver.execute_many(
        insert_sql,
        [(2, "beta", 20), (3, "gamma", spec.typed_null_int64())],
    )
    assert isinstance(batch_count, int)
    assert batch_count == 2

    tuples = driver.execute_tuples("SELECT id, label FROM vtb_conformance_items ORDER BY id")
    assert tuples == [(1, "alpha"), (2, "beta"), (3, "gamma")]

    by_id_sql = spec.format_sql("SELECT id, label FROM vtb_conformance_items WHERE id = ?")
    described = driver.execute_with_description(by_id_sql, (1,))
    assert isinstance(described, tuple)
    assert len(described) == 2
    rows, cols = described
    assert rows == [(1, "alpha")]
    assert _lowercase_columns(cols) == ("id", "label")

    le_sql = spec.format_sql("SELECT id, label, amount FROM vtb_conformance_items WHERE id <= ? ORDER BY id")
    dict_rows = driver.select(le_sql, (2,))
    assert dict_rows == [
        {"id": 1, "label": "alpha", "amount": 10},
        {"id": 2, "label": "beta", "amount": 20},
    ]

    struct_rows = driver.select(
        le_sql,
        (2,),
        schema_type=ConformanceItemRow,
    )
    assert struct_rows == [
        ConformanceItemRow(id=1, label="alpha", amount=10),
        ConformanceItemRow(id=2, label="beta", amount=20),
    ]

    found = driver.select_one(
        spec.format_sql("SELECT id, label, amount FROM vtb_conformance_items WHERE id = ?"),
        (1,),
        schema_type=ConformanceItemRow,
    )
    assert found == ConformanceItemRow(id=1, label="alpha", amount=10)

    with pytest.raises(NotFoundError) as missing_one_exc:
        driver.select_one(by_id_sql, (999,))
    assert isinstance(missing_one_exc.value, DatabaseError)
    assert isinstance(missing_one_exc.value, ValueError)

    assert driver.select_one_or_none(by_id_sql, (999,)) is None
    assert driver.select_one_or_none(by_id_sql, (2,)) == {
        "id": 2,
        "label": "beta",
    }

    amount_sql = spec.format_sql("SELECT amount FROM vtb_conformance_items WHERE id = ?")
    assert driver.select_value(amount_sql, (2,)) == 20
    assert driver.select_value(amount_sql, (3,)) is None
    with pytest.raises(NotFoundError):
        driver.select_value(amount_sql, (999,))


@pytest.mark.anyio
async def test_async_execution_contract(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
) -> None:
    """Asynchronous drivers satisfy `execute*` and `select*` return shapes and `NotFoundError` semantics."""
    spec, driver = async_sql_driver
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    single_count = await driver.execute(
        insert_sql,
        (1, "alpha", 10),
    )
    assert isinstance(single_count, int)
    assert single_count == 1

    batch_count = await driver.execute_many(
        insert_sql,
        [(2, "beta", 20), (3, "gamma", spec.typed_null_int64())],
    )
    assert isinstance(batch_count, int)
    assert batch_count == 2

    tuples = await driver.execute_tuples("SELECT id, label FROM vtb_conformance_items ORDER BY id")
    assert tuples == [(1, "alpha"), (2, "beta"), (3, "gamma")]

    by_id_sql = spec.format_sql("SELECT id, label FROM vtb_conformance_items WHERE id = ?")
    described = await driver.execute_with_description(by_id_sql, (1,))
    assert isinstance(described, tuple)
    assert len(described) == 2
    rows, cols = described
    assert rows == [(1, "alpha")]
    assert _lowercase_columns(cols) == ("id", "label")

    le_sql = spec.format_sql("SELECT id, label, amount FROM vtb_conformance_items WHERE id <= ? ORDER BY id")
    dict_rows = await driver.select(le_sql, (2,))
    assert dict_rows == [
        {"id": 1, "label": "alpha", "amount": 10},
        {"id": 2, "label": "beta", "amount": 20},
    ]

    struct_rows = await driver.select(
        le_sql,
        (2,),
        schema_type=ConformanceItemRow,
    )
    assert struct_rows == [
        ConformanceItemRow(id=1, label="alpha", amount=10),
        ConformanceItemRow(id=2, label="beta", amount=20),
    ]

    found = await driver.select_one(
        spec.format_sql("SELECT id, label, amount FROM vtb_conformance_items WHERE id = ?"),
        (1,),
        schema_type=ConformanceItemRow,
    )
    assert found == ConformanceItemRow(id=1, label="alpha", amount=10)

    with pytest.raises(NotFoundError) as missing_one_exc:
        await driver.select_one(by_id_sql, (999,))
    assert isinstance(missing_one_exc.value, DatabaseError)
    assert isinstance(missing_one_exc.value, ValueError)

    assert await driver.select_one_or_none(by_id_sql, (999,)) is None
    assert await driver.select_one_or_none(by_id_sql, (2,)) == {
        "id": 2,
        "label": "beta",
    }

    amount_sql = spec.format_sql("SELECT amount FROM vtb_conformance_items WHERE id = ?")
    assert await driver.select_value(amount_sql, (2,)) == 20
    assert await driver.select_value(amount_sql, (3,)) is None
    with pytest.raises(NotFoundError):
        await driver.select_value(amount_sql, (999,))


def test_sync_transaction_commit_and_rollback(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
) -> None:
    """Synchronous `with driver.transaction():` commits clean blocks and rolls back on exception."""
    spec, driver = sync_sql_driver
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    with driver.transaction():
        driver.execute(
            insert_sql,
            (10, "committed", 100),
        )
    assert (
        driver.select_value(spec.format_sql("SELECT label FROM vtb_conformance_items WHERE id = ?"), (10,))
        == "committed"
    )

    with pytest.raises(_RollbackSentinelError), driver.transaction():
        driver.execute(
            insert_sql,
            (11, "rolled_back", 200),
        )
        raise _RollbackSentinelError("abort transaction")

    assert (
        driver.select_one_or_none(spec.format_sql("SELECT id FROM vtb_conformance_items WHERE id = ?"), (11,)) is None
    )


@pytest.mark.anyio
async def test_async_transaction_commit_and_rollback(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
) -> None:
    """Asynchronous `async with driver.transaction():` commits clean blocks and rolls back on exception."""
    spec, driver = async_sql_driver
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    async with driver.transaction():
        await driver.execute(
            insert_sql,
            (10, "committed", 100),
        )
    assert (
        await driver.select_value(spec.format_sql("SELECT label FROM vtb_conformance_items WHERE id = ?"), (10,))
        == "committed"
    )

    with pytest.raises(_RollbackSentinelError):
        async with driver.transaction():
            await driver.execute(
                insert_sql,
                (11, "rolled_back", 200),
            )
            raise _RollbackSentinelError("abort transaction")

    assert (
        await driver.select_one_or_none(spec.format_sql("SELECT id FROM vtb_conformance_items WHERE id = ?"), (11,))
        is None
    )


def test_sync_typed_database_errors(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
) -> None:
    """Synchronous drivers map constraint violations and invalid SQL to `DatabaseError` subclasses."""
    spec, driver = sync_sql_driver
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    driver.execute(
        insert_sql,
        (1, "primary", 10),
    )
    constraint_sql, constraint_params = spec.duplicate_or_constraint_sql or (
        "INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)",
        (1, "duplicate", 20),
    )
    with pytest.raises(spec.constraint_error_types) as dup_exc:
        driver.execute(
            spec.format_sql(constraint_sql),
            constraint_params,
        )
    assert isinstance(dup_exc.value, DatabaseError)

    with pytest.raises(spec.invalid_sql_error_types) as bad_sql_exc:
        driver.select("SELECT * FROM vtb_nonexistent_table_for_error_conformance")
    assert isinstance(bad_sql_exc.value, DatabaseError)


@pytest.mark.anyio
async def test_async_typed_database_errors(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
) -> None:
    """Asynchronous drivers map constraint violations and invalid SQL to `DatabaseError` subclasses."""
    spec, driver = async_sql_driver
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    await driver.execute(
        insert_sql,
        (1, "primary", 10),
    )
    constraint_sql, constraint_params = spec.duplicate_or_constraint_sql or (
        "INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)",
        (1, "duplicate", 20),
    )
    with pytest.raises(spec.constraint_error_types) as dup_exc:
        await driver.execute(
            spec.format_sql(constraint_sql),
            constraint_params,
        )
    assert isinstance(dup_exc.value, DatabaseError)

    with pytest.raises(spec.invalid_sql_error_types) as bad_sql_exc:
        await driver.select("SELECT * FROM vtb_nonexistent_table_for_error_conformance")
    assert isinstance(bad_sql_exc.value, DatabaseError)


def test_sync_arrow_data_plane_roundtrip(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
) -> None:
    """Synchronous drivers support `load_from_arrow`, `select_to_arrow`, and `stream_arrow_batches`."""
    _, driver = sync_sql_driver
    source = pa.Table.from_pylist(
        [
            {"id": 1, "label": "arrow_a", "amount": 11},
            {"id": 2, "label": "arrow_b", "amount": 22},
            {"id": 3, "label": "arrow_c", "amount": 33},
        ]
    )
    loaded = driver.load_from_arrow("vtb_conformance_items", source)
    assert loaded == 3

    with driver.select_to_arrow(
        "SELECT id, label, amount FROM vtb_conformance_items ORDER BY id",
        threshold_rows=1,
    ) as cursor:
        assert isinstance(cursor, ArrowCursor)
        assert cursor.row_count == 3
        assert cursor.to_pylist() == [
            {"id": 1, "label": "arrow_a", "amount": 11},
            {"id": 2, "label": "arrow_b", "amount": 22},
            {"id": 3, "label": "arrow_c", "amount": 33},
        ]
        driver.execute("DELETE FROM vtb_conformance_items WHERE 1=1")
        reloaded = driver.load_from_arrow("vtb_conformance_items", cursor)
        assert reloaded == 3

    batches = list(
        driver.stream_arrow_batches(
            "SELECT id, label, amount FROM vtb_conformance_items ORDER BY id",
            batch_size=2,
        )
    )
    assert len(batches) == 2
    assert _batch_row_total(batches) == 3


@pytest.mark.anyio
async def test_async_arrow_data_plane_roundtrip(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
) -> None:
    """Asynchronous drivers support `load_from_arrow`, `select_to_arrow`, and `stream_arrow_batches`."""
    _, driver = async_sql_driver
    source = pa.Table.from_pylist(
        [
            {"id": 1, "label": "arrow_a", "amount": 11},
            {"id": 2, "label": "arrow_b", "amount": 22},
            {"id": 3, "label": "arrow_c", "amount": 33},
        ]
    )
    loaded = await driver.load_from_arrow("vtb_conformance_items", source)
    assert loaded == 3

    with await driver.select_to_arrow(
        "SELECT id, label, amount FROM vtb_conformance_items ORDER BY id",
        threshold_rows=1,
    ) as cursor:
        assert isinstance(cursor, ArrowCursor)
        assert cursor.row_count == 3
        assert cursor.to_pylist() == [
            {"id": 1, "label": "arrow_a", "amount": 11},
            {"id": 2, "label": "arrow_b", "amount": 22},
            {"id": 3, "label": "arrow_c", "amount": 33},
        ]
        await driver.execute("DELETE FROM vtb_conformance_items WHERE 1=1")
        reloaded = await driver.load_from_arrow("vtb_conformance_items", cursor)
        assert reloaded == 3

    batches = await _collect_async_batches(
        driver.stream_arrow_batches(
            "SELECT id, label, amount FROM vtb_conformance_items ORDER BY id",
            batch_size=2,
        )
    )
    assert len(batches) == 2
    assert _batch_row_total(batches) == 3


def test_sync_cancel_idle_is_idempotent(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
) -> None:
    """Synchronous `cancel()` is an idempotent no-op when no query is in flight."""
    spec, driver = sync_sql_driver
    driver.cancel()
    driver.cancel()
    assert driver.select_value(spec.probe_sql, value_type=int) == 1


@pytest.mark.anyio
async def test_async_cancel_idle_is_idempotent(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
) -> None:
    """Asynchronous `cancel()` is an idempotent no-op when no query is in flight."""
    spec, driver = async_sql_driver
    await driver.cancel()
    await driver.cancel()
    assert await driver.select_value(spec.probe_sql, value_type=int) == 1


def test_sync_pipeline_capability(
    sync_sql_driver: tuple[EngineConformanceSpec, SyncDatabaseDriver[Any]],
) -> None:
    """Drivers advertising `SyncPipelineCapable` execute queued commands in order on `results()`."""
    spec, driver = sync_sql_driver
    if not spec.supports_pipeline:
        assert not isinstance(driver, SyncPipelineCapable)
        return
    assert isinstance(driver, SyncPipelineCapable)
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    with driver.pipeline() as pipe:
        pipe.execute(insert_sql, (1, "piped_a", 5))
        pipe.execute(insert_sql, (2, "piped_b", 6))
        pipe.select("SELECT id, label FROM vtb_conformance_items ORDER BY id")
        results = pipe.results()
    assert len(results) == 3
    assert results[2] == [{"id": 1, "label": "piped_a"}, {"id": 2, "label": "piped_b"}]


@pytest.mark.anyio
async def test_async_pipeline_capability(
    async_sql_driver: tuple[EngineConformanceSpec, AsyncDatabaseDriver[Any]],
) -> None:
    """Drivers advertising `AsyncPipelineCapable` execute queued commands in order on `results()`."""
    spec, driver = async_sql_driver
    if not spec.supports_pipeline:
        assert not isinstance(driver, AsyncPipelineCapable)
        return
    assert isinstance(driver, AsyncPipelineCapable)
    insert_sql = spec.format_sql("INSERT INTO vtb_conformance_items (id, label, amount) VALUES (?, ?, ?)")
    async with driver.pipeline() as pipe:
        pipe.execute(insert_sql, (1, "piped_a", 5))
        pipe.execute(insert_sql, (2, "piped_b", 6))
        pipe.select("SELECT id, label FROM vtb_conformance_items ORDER BY id")
        results = await pipe.results()
    assert len(results) == 3
    assert results[2] == [{"id": 1, "label": "piped_a"}, {"id": 2, "label": "piped_b"}]


@pytest.mark.parametrize(
    ("url", "expected_partitions", "expected_memory_mb"),
    [
        ("datafusion://in_process", 4, 512),
        ("datafusion://in_process?target_partitions=2&memory_limit_mb=1024", 2, 1024),
    ],
    ids=["default", "custom_partitions"],
)
def test_datafusion_config_and_registry_conformance(
    url: str,
    expected_partitions: int,
    expected_memory_mb: int,
) -> None:
    """`DataFusionConfig` parses URLs, exposes `pool_key`, and resolves `DataFusionEngine` via `provide_driver`."""
    parsed = DatabaseRegistry.parse_url(url)
    assert isinstance(parsed, DataFusionConfig)
    assert parsed.target_partitions == expected_partitions
    assert parsed.memory_limit_mb == expected_memory_mb
    assert parsed.pool_key() == (
        "datafusion",
        "in_process",
        expected_partitions,
        expected_memory_mb,
        parsed.pgwire_host,
        parsed.pgwire_port,
        parsed.require_mtls_for_pgwire,
    )
    assert parsed.resolve_driver_classes() == (DataFusionEngine, DataFusionEngine)

    registry = DatabaseRegistry()
    sync_engine = provide_driver(parsed, sync=True, registry=registry)
    async_engine = provide_driver(parsed, sync=False, registry=registry)
    assert isinstance(sync_engine, DataFusionEngine)
    assert isinstance(async_engine, DataFusionEngine)
    assert sync_engine.config == parsed


def test_datafusion_fails_closed_when_optional_dependency_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`DataFusionEngine` raises `MissingDependencyError` when `datafusion` is absent and no context is injected."""
    monkeypatch.setattr(datafusion_driver_mod, "import_optional", lambda _name: None)

    def _raise_missing(module_name: str, *, package: str | None = None, extra: str | None = None) -> Any:
        if module_name == "datafusion":
            raise MissingDependencyError(package or "datafusion", extra=extra)
        return ensure_module(module_name, package=package, extra=extra)

    monkeypatch.setattr(datafusion_driver_mod, "ensure_module", _raise_missing)
    engine = DataFusionEngine(DataFusionConfig())
    reg = engine.register_arrow_table("items", pa.Table.from_pylist([{"id": 1}]))
    assert reg == TableRegistration(name="items", uri="arrow://items", format="arrow")
    assert engine.explain_plan("SELECT id FROM items")["referenced_tables"] == ["items"]

    with pytest.raises(MissingDependencyError):
        engine.execute_arrow("SELECT id FROM items")
    with pytest.raises(MissingDependencyError):
        engine.execute_dicts("SELECT id FROM items")


@pytest.mark.parametrize("input_kind", ["pyarrow_table", "arrow_cursor"])
def test_datafusion_table_registration_and_arrow_execution(input_kind: str) -> None:
    """`DataFusionEngine` normalizes PyArrow tables and `ArrowCursor` inputs for federated execution."""
    rows = [{"id": 1, "label": "north"}, {"id": 2, "label": "south"}]
    ctx = _StubDataFusionSessionContext()
    engine = DataFusionEngine(DataFusionConfig(), ctx=ctx)
    table_input: Any = (
        pa.Table.from_pylist(rows)
        if input_kind == "pyarrow_table"
        else ArrowCursor.from_rows(rows, ["id", "label"], threshold_rows=1)
    )
    try:
        reg = engine.register_arrow_table("federated_items", table_input)
        assert reg.name == "federated_items"
        assert "federated_items" in engine.tables
        assert engine.execute_dicts("SELECT id, label FROM federated_items") == rows
        ipc_bytes = engine.stream_arrow_ipc("SELECT id, label FROM federated_items")
        stream_table = pa_ipc.open_stream(pa.py_buffer(ipc_bytes)).read_all()
        assert stream_table.to_pylist() == rows
    finally:
        if isinstance(table_input, ArrowCursor):
            table_input.close()


@pytest.mark.anyio
@pytest.mark.parametrize("cache_spec", CACHE_ENGINE_SPECS, ids=[s.engine for s in CACHE_ENGINE_SPECS])
async def test_cache_config_url_pool_key_and_provide_driver(cache_spec: CacheConformanceSpec) -> None:
    """`RedisConfig` and `ValkeyConfig` parse URLs, expose `pool_key`, and resolve drivers via `provide_driver`."""
    parsed = DatabaseRegistry.parse_url(cache_spec.tcp_url)
    assert isinstance(parsed, cache_spec.config_cls)
    assert parsed == cache_spec.config_cls.from_url(cache_spec.tcp_url)
    key = parsed.pool_key()
    assert key == (cache_spec.engine, cache_spec.tcp_url)
    assert parsed.resolve_driver_classes() == (cache_spec.sync_driver_cls, cache_spec.async_driver_cls)

    registry = DatabaseRegistry()
    sync_first = provide_driver(parsed, sync=True, registry=registry)
    sync_second = provide_driver(parsed, sync=True, registry=registry)
    async_driver = provide_driver(parsed, sync=False, registry=registry)
    try:
        assert isinstance(sync_first, cache_spec.sync_driver_cls)
        assert isinstance(async_driver, cache_spec.async_driver_cls)
        assert sync_first is sync_second
    finally:
        sync_first.close()
        await async_driver.close()


def _resolve_cache_config(
    cache_spec: CacheConformanceSpec,
    request: pytest.FixtureRequest,
) -> RedisConfig:
    """Resolve a live `RedisConfig` or `ValkeyConfig` from an env DSN or `pytest-databases` container, skipping otherwise."""
    selected_engines = _parse_conformance_env_filter(os.environ, "VERTEBRA_CONFORMANCE_ENGINES")
    if selected_engines and cache_spec.engine not in selected_engines:
        pytest.skip(f"{cache_spec.engine} conformance excluded by VERTEBRA_CONFORMANCE_ENGINES")
    for env_key in cache_spec.env_dsn_vars:
        value = os.environ.get(env_key, "").strip()
        if value:
            return cache_spec.config_cls.from_url(value)
    if cache_spec.container_fixture is not None and os.environ.get("VERTEBRA_CONFORMANCE_DOCKER") == "1":
        fixture_cfg = request.getfixturevalue(cache_spec.container_fixture)
        assert isinstance(fixture_cfg, cache_spec.config_cls)
        return fixture_cfg
    pytest.skip(
        f"{cache_spec.engine} conformance requires {' or '.join(cache_spec.env_dsn_vars)} or VERTEBRA_CONFORMANCE_DOCKER=1"
    )


@pytest.mark.parametrize("cache_spec", CACHE_ENGINE_SPECS, ids=[s.engine for s in CACHE_ENGINE_SPECS])
def test_sync_cache_roundtrip_and_pipeline(
    cache_spec: CacheConformanceSpec,
    request: pytest.FixtureRequest,
) -> None:
    """Synchronous `Redis` and `Valkey` drivers support CRUD, TTL, `scan`, and pipelining."""
    driver = cache_spec.sync_driver_cls(_resolve_cache_config(cache_spec, request))
    try:
        driver.delete("vtb:alpha")
        driver.delete("vtb:beta")
        driver.delete("vtb:gamma")
        driver.set("vtb:alpha", "one", ex=60)
        driver.set("vtb:beta", b"two")
        assert driver.get("vtb:alpha") == b"one"
        assert driver.pttl("vtb:alpha") > 0
        cursor, keys = driver.scan(match="vtb:*")
        assert cursor == 0
        assert sorted(keys) == ["vtb:alpha", "vtb:beta"]

        replies = driver.execute_pipeline(
            [
                ("SET", "vtb:gamma", "three"),
                ("GET", "vtb:gamma"),
                ("DEL", "vtb:beta"),
            ]
        )
        assert replies[1] == b"three"
        assert driver.get("vtb:beta") is None
        assert driver.delete("vtb:alpha") is True
        assert driver.delete("vtb:alpha") is False
        driver.delete("vtb:gamma")
    finally:
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("cache_spec", CACHE_ENGINE_SPECS, ids=[s.engine for s in CACHE_ENGINE_SPECS])
async def test_async_cache_roundtrip_and_pipeline(
    cache_spec: CacheConformanceSpec,
    request: pytest.FixtureRequest,
) -> None:
    """Asynchronous `Redis` and `Valkey` drivers support CRUD, TTL, `scan`, and pipelining."""
    driver = cache_spec.async_driver_cls(_resolve_cache_config(cache_spec, request))
    try:
        await driver.delete("vtb:alpha")
        await driver.delete("vtb:beta")
        await driver.delete("vtb:gamma")
        await driver.set("vtb:alpha", "one", ex=60)
        await driver.set("vtb:beta", b"two")
        assert await driver.get("vtb:alpha") == b"one"
        assert await driver.pttl("vtb:alpha") > 0
        cursor, keys = await driver.scan(match="vtb:*")
        assert cursor == 0
        assert sorted(keys) == ["vtb:alpha", "vtb:beta"]

        replies = await driver.execute_pipeline(
            [
                ("SET", "vtb:gamma", "three"),
                ("GET", "vtb:gamma"),
                ("DEL", "vtb:beta"),
            ]
        )
        assert replies[1] == b"three"
        assert await driver.get("vtb:beta") is None
        assert await driver.delete("vtb:alpha") is True
        assert await driver.delete("vtb:alpha") is False
        await driver.delete("vtb:gamma")
    finally:
        await driver.close()


def test_gcp_conformance_specs_encode_typed_null_json_and_error_contracts() -> None:
    """BigQuery and Spanner conformance specs produce valid typed NULL, JSON scalar, and DatabaseError contracts."""
    by_engine = {spec.engine: spec for spec in ENGINE_SPECS}
    bq_spec = by_engine["bigquery"]
    sp_spec = by_engine["spanner"]
    json_cases = {case.name: case for case in SCALAR_TYPE_CASES if case.comparison_kind == "json"}

    for spec in ENGINE_SPECS:
        assert all(issubclass(err_cls, DatabaseError) for err_cls in spec.constraint_error_types)
        assert all(issubclass(err_cls, DatabaseError) for err_cls in spec.invalid_sql_error_types)

    assert bq_spec.constraint_error_types == (BigQueryError,)
    assert sp_spec.constraint_error_types == (SpannerError,)

    bq_params = decode_json(
        BigQueryCore.encode_parameters_json(
            (
                1,
                bq_spec.bind_scalar_case(json_cases["json_dict"]),
                bq_spec.bind_scalar_case(json_cases["json_list"]),
                bq_spec.typed_null_int64(),
            )
        )
        or "[]"
    )
    assert bq_params[1] == {"type": "JSON", "value": encode_json(json_cases["json_dict"].input_value)}
    assert bq_params[2] == {"type": "JSON", "value": encode_json(json_cases["json_list"].input_value)}
    assert bq_params[3] == {"type": "INT64", "value": None}

    sp_params = decode_json(
        SpannerCore.encode_parameters_json(
            (
                1,
                sp_spec.bind_scalar_case(json_cases["json_dict"]),
                sp_spec.bind_scalar_case(json_cases["json_list"]),
                sp_spec.typed_null_int64(),
            )
        )
        or "{}"
    )
    assert sp_params["p2"] == {"type": "JSON", "value": encode_json(json_cases["json_dict"].input_value)}
    assert sp_params["p3"] == {"type": "JSON", "value": encode_json(json_cases["json_list"].input_value)}
    assert sp_params["p4"] == {"type": "INT64", "value": None}


@pytest.mark.parametrize(
    ("module_name", "pending_flow_id", "docker_env", "expected_outcome"),
    [
        ("vertebra._vtb_conformance_absent", None, None, "skip"),
        ("vertebra._vtb_conformance_absent", None, "1", "fail"),
        ("vertebra._vtb_conformance_absent", None, "0", "skip"),
        ("vertebra._vtb_conformance_absent", "native-probe-driver", None, "skip"),
        ("vertebra._vtb_conformance_absent", "native-probe-driver", "1", "skip"),
        ("json", None, "1", "ok"),
        (None, None, "1", "ok"),
    ],
    ids=[
        "missing_local",
        "missing_docker",
        "missing_docker_zero",
        "missing_pending_local",
        "missing_pending_docker",
        "present",
        "no_module",
    ],
)
def test_require_native_module_outcome_by_docker_mode(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str | None,
    pending_flow_id: str | None,
    docker_env: str | None,
    expected_outcome: str,
) -> None:
    """``_require_native_module`` skips missing modules and fails non-pending specs under Docker conformance."""
    base_spec = next(spec for spec in ENGINE_SPECS if spec.engine == "sqlite")
    spec = structs.replace(
        base_spec,
        native_extension_module=module_name,
        pending_flow_id=pending_flow_id,
    )
    if docker_env is None:
        monkeypatch.delenv(_CONFORMANCE_DOCKER_ENV, raising=False)
    else:
        monkeypatch.setenv(_CONFORMANCE_DOCKER_ENV, docker_env)
    if module_name is not None:
        reset_dependency_cache(module_name)
    expected_reason = "sqlite conformance requires the vertebra._vtb_conformance_absent native module"
    if expected_outcome == "skip":
        with pytest.raises(pytest.skip.Exception) as skip_exc:
            _require_native_module(spec)
        assert str(skip_exc.value) == expected_reason
    elif expected_outcome == "fail":
        with pytest.raises(pytest.fail.Exception) as fail_exc:
            _require_native_module(spec)
        assert str(fail_exc.value) == expected_reason
    else:
        _require_native_module(spec)


def test_live_engine_params_mark_strict_xfail_only_when_native_module_imports() -> None:
    """``_build_live_engine_params`` attaches strict ``xfail`` only when a pending spec's native module imports."""
    base_spec = next(spec for spec in ENGINE_SPECS if spec.engine == "sqlite")
    reset_dependency_cache("vertebra._vtb_conformance_absent")
    reset_dependency_cache("json")
    absent_pending = structs.replace(
        base_spec,
        engine="absent",
        native_extension_module="vertebra._vtb_conformance_absent",
        pending_flow_id="native-probe-driver",
    )
    present_pending = structs.replace(
        base_spec,
        engine="present",
        native_extension_module="json",
        pending_flow_id="native-probe-driver",
    )
    absent_plain = structs.replace(
        base_spec,
        engine="plain",
        native_extension_module="vertebra._vtb_conformance_absent",
        pending_flow_id=None,
    )
    params = _build_live_engine_params((absent_pending, present_pending, absent_plain), env={})
    assert [param.id for param in params] == ["absent", "present", "plain"]
    assert [[mark.name for mark in param.marks] for param in params] == [[], ["xfail"], []]
    assert params[1].marks[0].kwargs == {"strict": True, "reason": "Owned by native-probe-driver"}


def test_resolve_live_config_checks_native_module_before_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    """``_resolve_live_config`` checks the native module before BigQuery credential skips."""
    bq_spec = next(spec for spec in ENGINE_SPECS if spec.engine == "bigquery")
    spec = structs.replace(bq_spec, native_extension_module="vertebra._vtb_conformance_absent")
    monkeypatch.delenv("VERTEBRA_CONFORMANCE_BIGQUERY_DSN", raising=False)
    monkeypatch.delenv(_CONFORMANCE_DOCKER_ENV, raising=False)
    reset_dependency_cache("vertebra._vtb_conformance_absent")
    with pytest.raises(pytest.skip.Exception) as skip_exc:
        _resolve_live_config(spec, tmp_path, request)
    assert str(skip_exc.value) == "bigquery conformance requires the vertebra._vtb_conformance_absent native module"


def test_mssql_driver_raises_missing_dependency_when_wheel_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SyncMSSQLDriver and AsyncMSSQLDriver fail loud with MissingDependencyError when vertebra._mssql is absent."""
    monkeypatch.setitem(sys.modules, "vertebra._mssql", None)
    reset_dependency_cache("vertebra._mssql")
    try:
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            SyncMSSQLDriver()
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            AsyncMSSQLDriver()
    finally:
        reset_dependency_cache("vertebra._mssql")


def test_postgres_conformance_spec_and_version_matrix() -> None:
    """PostgreSQL conformance enables pipelines, native types DDL, clears pending gates, and expands 14..18."""
    pg_spec = next(spec for spec in ENGINE_SPECS if spec.engine == "postgres")
    assert pg_spec.supports_pipeline is True
    assert pg_spec.pending_flow_id is None
    assert pg_spec.native_extension_module == "vertebra._postgres"
    assert "BYTEA" in pg_spec.types_ddl
    assert "NUMERIC(18, 4)" in pg_spec.types_ddl
    assert "TIMESTAMPTZ" in pg_spec.types_ddl
    assert "JSONB" in pg_spec.types_ddl
    assert "BLOB" not in pg_spec.types_ddl

    params = _build_live_engine_params((pg_spec,), env={})
    assert [param.id for param in params] == [
        "postgres-14",
        "postgres-15",
        "postgres-16",
        "postgres-17",
        "postgres-18",
    ]
    assert all(param.marks == () for param in params)
    assert [param.values[0].container_fixture for param in params] == [
        "vtb_postgres_14_config",
        "vtb_postgres_15_config",
        "vtb_postgres_16_config",
        "vtb_postgres_17_config",
        "vtb_postgres_18_config",
    ]


@pytest.mark.parametrize(
    ("env", "expected_ids"),
    [
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "postgres", "VERTEBRA_CONFORMANCE_POSTGRES_VERSION": "17"},
            ["postgres-17"],
        ),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "postgres", "VERTEBRA_CONFORMANCE_POSTGRES_VERSION": "14, 18"},
            ["postgres-14", "postgres-18"],
        ),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "sqlite, postgres", "VERTEBRA_CONFORMANCE_POSTGRES_VERSION": "16"},
            ["sqlite", "postgres-16"],
        ),
    ],
)
def test_live_engine_params_honor_engine_and_pg_version_env_filters(
    env: dict[str, str],
    expected_ids: list[str],
) -> None:
    """Verify conformance collection filters engines and PostgreSQL versions without pytest -k deselections."""
    params = _build_live_engine_params(ENGINE_SPECS, env=env)
    assert [param.id for param in params] == expected_ids


def test_live_engine_params_reject_unknown_postgres_version() -> None:
    """Verify unsupported VERTEBRA_CONFORMANCE_POSTGRES_VERSION tokens fail immediately at collection."""
    with pytest.raises(ValueError, match="Unsupported VERTEBRA_CONFORMANCE_POSTGRES_VERSION"):
        _build_live_engine_params(ENGINE_SPECS, env={"VERTEBRA_CONFORMANCE_POSTGRES_VERSION": "99"})


@pytest.mark.parametrize(
    "type_case",
    [case for case in SCALAR_TYPE_CASES if case.comparison_kind == "json"],
    ids=[case.name for case in SCALAR_TYPE_CASES if case.comparison_kind == "json"],
)
def test_postgres_conformance_wraps_json_cases_with_pg_json(type_case: ScalarTypeCase) -> None:
    """PostgreSQL conformance binds dicts and heterogeneous JSON lists via ``PgJson``."""
    pg_spec = next(spec for spec in ENGINE_SPECS if spec.engine == "postgres")
    bound = pg_spec.bind_scalar_case(type_case)
    assert isinstance(bound, PgJson)
    assert bound.value == type_case.input_value
