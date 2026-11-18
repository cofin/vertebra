---
type: Gotcha
title: Database Driver Limitations
description: Cross-driver runtime caveats covering companion wheel loading, GIL-only DuckDB benchmark packaging, queue executors, and GCS secret configuration.
status: stable
tags:
- data
- database
- postgres
- duckdb
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-executor
  at: "2026-11-17T01:28:00Z"
  commit: 5a833832
- by: agent:flow-executor
  at: "2026-11-14T18:05:00Z"
  commit: 1b191388
- by: agent:flow-learn
  at: "2026-11-14T17:15:00Z"
  commit: 1b191388
sources:
- id: gcs-secrets
  resource: src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider
  title: Shipped DuckDB GCS OAuth2 bearer secret providers in vertebra.storage
- id: pg-driver
  resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
  title: PostgreSQL drivers re-exported over native _sync and _async engines
- id: pg-queue
  resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
  title: PostgreSQL queue backend native queue stores and optional executor
- id: pg-stub
  resource: src/py/vertebra/_postgres.pyi
  title: Native vertebra._postgres client, pool, COPY, Arrow, and listener stubs
- id: duckdb-core
  resource: src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore
  title: DuckDBCore write-statement detection
- id: duckdb-secret
  resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
  title: Cloud-agnostic DuckDB CREATE SECRET declaration
- id: duckdb-driver
  resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
  title: DuckDB driver HTTP 401/403 regex invalidation and startup cleanup
- id: ci-workflow
  resource: .github/workflows/ci.yml
  title: Non-blocking conformance-oracle job and test-db-conformance matrix
- id: datafusion-core
  resource: src/py/vertebra/db/adapters/datafusion/core.py::DataFusionCore
  title: DataFusion session builder, source registration, and planner helpers
- id: entra-auth
  resource: src/py/vertebra/db/driver/_cloud_auth.py::AzureEntraTokenProvider
  title: Azure Entra ID database token provider and PostgreSQL/MySQL default scope wiring
- id: gcp-wif
  resource: src/rs/vertebra-core/src/gcp_auth.rs::parse_external_account_json
  title: Google Cloud external_account workload identity federation parser and restrictions
- id: mssql-entra
  resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig
  title: SQL Server AZURE_MSSQL_SCOPE normalization and set_aad_token reconnect override behavior
- id: driver-stubs
  resource: src/py/vertebra/_bigquery.pyi::BigQueryQueryResult
  title: Native driver stub TypedDict shapes and type_check_only stubtest contract
- id: crdb-alloydb
  resource: src/rs/vertebra-postgres/src/session.rs::PostgresConnection::reset_for_pool_return
  title: CockroachDB RESET ALL / cockroach_restart and AlloyDB v1beta/v1alpha fallback wire contracts
- id: queue-store-protocol
  resource: src/py/vertebra/db/driver/capabilities.py::AsyncQueueStore
  title: Extended AsyncQueueStore and SyncQueueStore runtime-checkable protocol contract
- id: oracle-queue
  resource: src/py/vertebra/db/adapters/oracle/queue.py::AsyncOracleQueueStore
  title: Oracle queue store extended capabilities and named-bind MERGE/JSON_VALUE queries
---

# Database Driver Limitations

<!-- truth: start -->
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`adapters/postgres/driver.py` re-exporting `_sync` / `_async`) execute natively through `vertebra._postgres` (`vertebra-core[postgres]`) with `$1..$N` dialect compilation; runtime pooler, logical replication slot, and Arrow interface boundaries are detailed in [PostgreSQL Native Driver Limitations](postgres-limitations.md) [^pg-driver] [^pg-stub].
- `PostgreSQLQueueBackend` and `OracleQueueBackend` ship native SQL queue stores (`SyncPostgresQueueStore`/`AsyncPostgresQueueStore` and `SyncOracleQueueStore`/`AsyncOracleQueueStore`) alongside optional `executor=` injection; when neither `executor=`, `store=`/`sync_store=`, nor `driver=`/`sync_driver=` is supplied, they probe `vertebra._postgres` / `vertebra._oracle` via `ensure_module` (`MissingDependencyError` when absent) and raise `ImproperConfigurationError` if no non-empty DSN is configured [^pg-queue].
- Optional companion driver wheels (`vertebra-postgres`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-mssql`, `vertebra-duckdb`, `vertebra-sqlite`, `vertebra-redis`, `vertebra-valkey`, `vertebra-bigquery`, `vertebra-spanner`) load dynamically via `ensure_module` and fail loud with `MissingDependencyError(install_package="vertebra-core[<extra>]")` when omitted from the active environment; upstream PyPI `duckdb` publishes no free-threaded wheel and is isolated to `[dependency-groups].bench` (`python_version < '3.14'`) for `tools.benchmarks.store` result persistence while `vertebra._duckdb` builds across all supported CPython ABIs. All adapters live under `src/py/vertebra/db/adapters/<engine>/` (`src/py/vertebra/drivers/` is removed).
- **SQLAlchemy, Alembic & Django Dialect Boundaries**: BigQuery, DuckDB, and SQL Server have no Vertebra Django `DATABASES` backend (`VertebraPlugin` raises `ImproperConfigurationError` if configured; use SQLAlchemy or SQLSpec instead). `duckdb-engine`, `sqlalchemy-bigquery`, `sqlalchemy-spanner`, `django-google-spanner`, and `dogpile.cache` (`vertebra-core[dogpile]`) are optional upstream packages required only when instantiating those specific SQLAlchemy/Django dialect bases or `use_dogpile=True` cache regions, and BigQuery/Spanner execute non-transactional DDL (`autocommit=False` raises PEP 249 `NotSupportedError`). Because `django-google-spanner 5.0.0` raises `ImproperlyConfigured` on import against Django 6.1.x, `vertebra.extensions.django.db.backends.spanner.base.DatabaseWrapper` subclasses `django.db.backends.base.base.BaseDatabaseWrapper` directly and probes `django_spanner.base` via `ensure_module` inside `__init__`.
- **DuckDB In-Memory Catalog Isolation**: DuckDB isolates plain `:memory:` instances per config whereas `:memory:<name>` shares named in-memory catalogs across drivers [^duckdb-driver].
- `DuckDBCore` provides only `is_write_statement`; `vertebra.storage.{SyncGcsBearerSecretProvider,AsyncGcsBearerSecretProvider}` supply GCS OAuth2 bearer-token rotation for `DuckDBSecretConfig(secret_type="gcs", value_provider=...)` (`persistent=True` with `value_provider` or non-positive/boolean `refresh_interval_seconds` fails validation) [^duckdb-core] [^duckdb-secret] [^gcs-secrets].
- DuckDB `httpfs` `(HTTP 401)` / `(HTTP 403)` reactive invalidation relies on error-message regex matching (`\(HTTP (?:401|403)\)`) because `vertebra._duckdb` surfaces HTTP status codes inside the exception message rather than a structured integer attribute, and invalidation marks the provider generation stale for the next statement without implicitly retrying the failed query [^duckdb-driver].
- Live DuckDB `httpfs` extension tests are gated behind `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` when `httpfs` is not pre-cached because `INSTALL httpfs` requires outbound network access, and the CI `conformance-oracle` job runs with `continue-on-error: true` in `.github/workflows/ci.yml` to isolate `gvenzl/oracle-free:23-slim-faststart` container startup variance on shared `ubuntu-latest` runners [^duckdb-driver] [^ci-workflow].
- `DataFusionEngine` / `DataFusionCore` require the optional `datafusion` wheel for live `SessionContext` execution; when `datafusion` is absent and no `ctx` is injected (`self._ctx is None`), `register_table` defers source registration until `_ensure_context()` and `explain_plan` returns metadata-only table references without invoking the planner, while `execute_arrow` and `execute_dicts` fail closed with `MissingDependencyError` [^datafusion-core].
- `AzureEntraTokenProvider` (`db/driver/_cloud_auth.py`) injects `AZURE_POSTGRES_SCOPE` inside `postgres/options.py::build_native_client_options`, `AZURE_MYSQL_SCOPE` inside `SyncMySQLDriver.__init__` / `AsyncMySQLDriver.__init__`, `AZURE_MSSQL_SCOPE` inside `MSSQLConfig.__post_init__`, and `AZURE_REDIS_SCOPE` inside `RedisConfig.__post_init__` / `ValkeyConfig.__post_init__`; calling `PostgreSQLConfig.resolve_iam_password_*`, `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver`, `MySQLConfig.resolve_iam_password_*`, or `MySQLConfig.parse_dsn_params*` directly on an unscoped `AzureEntraTokenProvider` (`scope=None`) without passing `scope` raises `ImproperConfigurationError`. IMDS requests bypass `HTTP_PROXY`/`HTTPS_PROXY` via an empty `ProxyHandler`, whereas client-credentials token POSTs bypass proxies only for loopback test hosts and honor configured proxies for remote Entra endpoints [^entra-auth].
- `MSSQLConfig` (`db/adapters/mssql/config.py`) normalizes `AzureEntraTokenProvider` with `AZURE_MSSQL_SCOPE` in `__post_init__`, and `SyncMSSQLClient.set_aad_token` / `AsyncMSSQLClient.set_aad_token` update the `MssqlClientConfig` runtime override slot for subsequent TDS `LOGIN7` `FEDAUTH` reconnects without rebuilding the Rust client or tearing down an already-healthy active session; live Azure SQL Server `FEDAUTH` handshakes were verified against TDS 7.4 loopback wire servers rather than a live Azure SQL endpoint [^mssql-entra].
- Google Cloud `external_account` Workload Identity Federation (`gcp_auth.rs::parse_external_account_json`) supports file, URL, and `aws1` metadata credential sources; `executable` credential sources are rejected for security reasons [^gcp-wif].
- Stub-only `TypedDict` result shapes in `_bigquery.pyi` and `_spanner.pyi` require `@typing.type_check_only` so `mypy.stubtest` accepts them without runtime PyO3 classes; because `TypedDict` is not assignable to invariant `dict[str, Any]`, adapter structural `Protocol` definitions (`SyncBigQueryClientProtocol`, `AsyncBigQueryClientProtocol`, `SyncSpannerClientProtocol`, `AsyncSpannerClientProtocol`) must use `Mapping[str, Any]` or the `TypedDict` types rather than `dict[str, Any]` [^driver-stubs].
- CockroachDB `--cluster=<id>` routing and DSN `options` forward through `session_gucs["options"]` into `StartupMessage` (and are skipped during SQL `SET` pool-reset restoration), `RESET ALL` replaces `DISCARD ALL` when `crdb_version` is present in `ParameterStatus`, and `cockroach_restart` savepoint methods (`savepoint_cockroach_restart*`, `release_cockroach_restart*`, `rollback_to_cockroach_restart*`) plus `PostgresError::is_retryable_serialization` are exposed on Rust `PostgresConnection` / `SessionHandle` rather than separate Python PyO3 client methods; CockroachDB wire flows, AlloyDB `v1beta` -> `v1alpha` (HTTP 404/501) discovery fallback, and `MetadataExchangeResponse` diagnostics were verified against loopback wire/HTTP mocks rather than live CockroachDB or GCP AlloyDB instances [^crdb-alloydb].
- `AsyncQueueStore` and `SyncQueueStore` (`db/driver/capabilities.py`) are `@runtime_checkable` protocols requiring `cleanup_terminal`, `get_entry_by_key`, `reserve_identity`, `get_reservation`, `reset_identity`, `assign_worker`, `touch_heartbeats`, `null_heartbeats`, `interrupt_task`, `expire_overdue`, and `list_entries(queue: str | None = None)`; until Wave 3 engine implementations (`t15`-`t18`) and test doubles add these methods, `isinstance(store, AsyncQueueStore)` / `isinstance(store, SyncQueueStore)` runtime checks fail on older queue store implementations [^queue-store-protocol].
- `SyncOracleQueueStore` and `AsyncOracleQueueStore` (`db/adapters/oracle/queue.py`) implement the full extended `SyncQueueStore` / `AsyncQueueStore` protocol using `:named` Oracle binds, `JSON_VALUE(j.headers, '$.key')` lookup, and `MERGE INTO vertebra_queue_unique_keys` with nullable `expires_at_ms` for forever reservations; offline unit coverage uses fake drivers (`_FakeSyncOracleDriver` / `_FakeAsyncOracleDriver`) rather than a live Oracle Database instance [^oracle-queue].
- `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` (`db/adapters/spanner/queue.py`) implement the full extended `SyncQueueStore` / `AsyncQueueStore` protocol across GoogleSQL (`@param`) and Spangres (`$1`) dialects using transactional `run_in_transaction` `FOR UPDATE` reservation checks, `LEFT JOIN vertebra_queue_unique_keys` key lookups, and nullable `expires_at` for forever reservations; unit coverage runs against `_InMemorySpannerQueueWire` rather than a live Cloud Spanner instance or emulator [^spanner-queue].
- Database driver and Oracle unit suites were split from monoliths into `test_db_drivers_channels_and_dialers.py`, `test_db_unified_driver_contracts.py`, `test_db_capabilities_and_purity_gates.py`, `oracle_test_doubles.py`, `test_oracle_adapter.py`, and `test_oracle_context_notify_and_queues.py`; until downstream `ch07` tasks (`t30`, `t31`, `t34`, `t35`) land, `test_channel_bus_clean_break_imports`, `test_vertebra_db_ast_upward_import_boundary_gate` (`redis`/`valkey` queue `_resp` imports), `_ConformingSyncQueueStore`/`_ConformingAsyncQueueStore` checks, `Makefile` `oracle-test` wiring, and `adr-002`/`polyglot-test-matrix.md` source links remain out of sync on the branch.
<!-- truth: end -->

## Related Context

- PostgreSQL operational caveats: [PostgreSQL Native Driver Limitations](postgres-limitations.md).
- Driver and cloud dialer behavior: [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md).

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Native PostgreSQL drivers over `vertebra._postgres`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend native queue stores and optional executor.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` client, pool, COPY, Arrow, and listener stubs.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` write-statement detection.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration.
[^gcs-secrets]: `src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider` — Shipped DuckDB GCS OAuth2 bearer secret providers in `vertebra.storage`.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver HTTP 401/403 regex invalidation and startup cleanup.
[^queue-store-protocol]: `src/py/vertebra/db/driver/capabilities.py::AsyncQueueStore` — Extended `AsyncQueueStore` and `SyncQueueStore` protocol methods and `QueueReservation` record.
[^ci-workflow]: `.github/workflows/ci.yml` — Non-blocking `conformance-oracle` job and `test-db-conformance` matrix.
[^datafusion-core]: `src/py/vertebra/db/adapters/datafusion/core.py::DataFusionCore` — DataFusion session builder, source registration, and planner helpers.
[^entra-auth]: `src/py/vertebra/db/driver/_cloud_auth.py::AzureEntraTokenProvider` — Azure Entra ID database token provider and PostgreSQL/MySQL/MSSQL/Redis/Valkey default scope wiring.
[^gcp-wif]: `src/rs/vertebra-core/src/gcp_auth.rs::parse_external_account_json` — Google Cloud `external_account` workload identity federation parser and restrictions.
[^mssql-entra]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig` — SQL Server `AZURE_MSSQL_SCOPE` normalization and `set_aad_token` reconnect override behavior.
[^driver-stubs]: `src/py/vertebra/_bigquery.pyi::BigQueryQueryResult` — Stub-only `@type_check_only` `TypedDict` shapes in `_bigquery.pyi` and `_spanner.pyi`.
[^crdb-alloydb]: `src/rs/vertebra-postgres/src/session.rs::PostgresConnection::reset_for_pool_return` — CockroachDB `RESET ALL` / `cockroach_restart` and AlloyDB `v1beta`/`v1alpha` fallback wire contracts.
[^oracle-queue]: `src/py/vertebra/db/adapters/oracle/queue.py::AsyncOracleQueueStore` — Extended Oracle queue store methods, `JSON_VALUE` key lookups, and forever-safe `vertebra_queue_unique_keys` cleanup.
[^spanner-queue]: `src/py/vertebra/db/adapters/spanner/queue.py::AsyncSpannerQueueStore` — Extended Spanner queue store methods across GoogleSQL and Spangres dialects with transactional `reserve_identity` and `cleanup_terminal`.
