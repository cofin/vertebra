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
  at: "2026-11-19T03:10:00Z"
  commit: 6f76f8a7
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
- id: spanner-queue
  resource: src/py/vertebra/db/adapters/spanner/queue.py::AsyncSpannerQueueStore
  title: Spanner queue store extended capabilities and transactional reservation checks
- id: aws-azure-rs-dialers
  resource: src/rs/vertebra-db/src/dialer.rs::AwsRdsDialer
  title: Native AWS RDS, Aurora DSQL, and Azure Entra endpoint dialer metadata and RDS Root CA bundle
- id: aws-sigv4-auth
  resource: src/py/vertebra/db/driver/_cloud_auth.py::AwsRdsIamTokenProvider
  title: Ambient AWS RDS and Aurora DSQL SigV4 credential chains and temporary credential expiry clamping
- id: py-aws-azure-dialers
  resource: src/py/vertebra/db/dialer/aws.py::SyncAwsRdsDialer
  title: Python sync and async AWS RDS, Aurora DSQL, and Azure Entra TLS dialers
- id: pg-mysql-cloud-tls
  resource: src/py/vertebra/db/adapters/postgres/options.py::build_native_client_options
  title: PostgreSQL and MySQL Azure Entra default scopes, bundled RDS Root CA attachment, and remote TLS enforcement
- id: cargo-release-profile
  resource: src/rs/Cargo.toml
  title: Rust workspace release profile with panic = "unwind" for PyO3 PanicException safety
- id: arrow-cursor-ipc
  resource: src/rs/vertebra-py/src/db.rs::ArrowCursor
  title: Single-pass CompiledIpcLayout validation, overflow guards, cached validation flag, and to_pylist signal checks
- id: bench-arrow-ipc
  resource: tools/benchmarks/micro.py::benchmark_arrow_ipc_stream
  title: Arrow IPC stream open and single-pass iteration microbenchmark
---

# Database Driver Limitations

<!-- truth: start -->
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`adapters/postgres/driver.py` re-exporting `_sync` / `_async`) execute natively through `vertebra._postgres` (`vertebra-core[postgres]`) with `$1..$N` dialect compilation; runtime pooler, logical replication streams, and Arrow interface boundaries are detailed in [PostgreSQL Native Driver Limitations](postgres-limitations.md) [^pg-driver] [^pg-stub].
- `PostgreSQLQueueBackend` and `OracleQueueBackend` ship native SQL queue stores (`SyncPostgresQueueStore`/`AsyncPostgresQueueStore` and `SyncOracleQueueStore`/`AsyncOracleQueueStore`) alongside optional `executor=` injection; when neither `executor=`, `store=`/`sync_store=`, nor `driver=`/`sync_driver=` is supplied, they probe `vertebra._postgres` / `vertebra._oracle` via `ensure_module` (`MissingDependencyError` when absent) and raise `ImproperConfigurationError` if no non-empty DSN is configured [^pg-queue].
- Optional companion driver wheels (`vertebra-postgres`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-mssql`, `vertebra-duckdb`, `vertebra-sqlite`, `vertebra-redis`, `vertebra-valkey`, `vertebra-bigquery`, `vertebra-spanner`) load dynamically via `ensure_module` and fail loud with `MissingDependencyError(install_package="vertebra-core[<extra>]")` when omitted from the active environment; upstream PyPI `duckdb` publishes no free-threaded wheel and is isolated to `[dependency-groups].bench` (`python_version < '3.14'`) for `tools.benchmarks.store` result persistence while `vertebra._duckdb` builds across all supported CPython ABIs. All adapters live under `src/py/vertebra/db/adapters/<engine>/` (`src/py/vertebra/drivers/` is removed).
- **SQLAlchemy, Alembic & Django Dialect Boundaries**: BigQuery, DuckDB, and SQL Server have no Vertebra Django `DATABASES` backend (`VertebraPlugin` raises `ImproperConfigurationError` if configured; use SQLAlchemy or SQLSpec instead). `duckdb-engine`, `sqlalchemy-bigquery`, `sqlalchemy-spanner`, `django-google-spanner`, and `dogpile.cache` (`vertebra-core[dogpile]`) are optional upstream packages required only when instantiating those specific SQLAlchemy/Django dialect bases or `use_dogpile=True` cache regions, and BigQuery/Spanner execute non-transactional DDL (`autocommit=False` raises PEP 249 `NotSupportedError`). Because `django-google-spanner 5.0.0` raises `ImproperlyConfigured` on import against Django 6.1.x, `vertebra.extensions.django.db.backends.spanner.base.DatabaseWrapper` subclasses `django.db.backends.base.base.BaseDatabaseWrapper` directly and probes `django_spanner.base` via `ensure_module` inside `__init__`.
- **DuckDB In-Memory Catalog Isolation**: DuckDB isolates plain `:memory:` instances per config whereas `:memory:<name>` shares named in-memory catalogs across drivers [^duckdb-driver].
- `DuckDBCore` provides only `is_write_statement`; `vertebra.storage.{SyncGcsBearerSecretProvider,AsyncGcsBearerSecretProvider}` supply GCS OAuth2 bearer-token rotation for `DuckDBSecretConfig(secret_type="gcs", value_provider=...)` (`persistent=True` with `value_provider` or non-positive/boolean `refresh_interval_seconds` fails validation) [^duckdb-core] [^duckdb-secret] [^gcs-secrets].
- DuckDB `httpfs` `(HTTP 401)` / `(HTTP 403)` reactive invalidation relies on regex matching (`\(HTTP (?:401|403)\)`) because `vertebra._duckdb` surfaces HTTP status codes inside the exception string. Invalidation marks the provider generation stale for the next statement without implicitly retrying the failed query [^duckdb-driver].
- Live DuckDB `httpfs` extension tests are gated behind `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` when `httpfs` is not pre-cached because `INSTALL httpfs` requires outbound network access, and the CI `conformance-oracle` job runs with `continue-on-error: true` in `.github/workflows/ci.yml` to isolate `gvenzl/oracle-free:23-slim-faststart` container startup variance on shared `ubuntu-latest` runners [^duckdb-driver] [^ci-workflow].
- `DataFusionEngine` / `DataFusionCore` require the optional `datafusion` wheel for live `SessionContext` execution; when `datafusion` is absent and no `ctx` is injected (`self._ctx is None`), `register_table` defers source registration until `_ensure_context()` and `explain_plan` returns metadata-only table references without invoking the planner, while `execute_arrow` and `execute_dicts` fail closed with `MissingDependencyError` [^datafusion-core].
- `AzureEntraTokenProvider` (`db/driver/_cloud_auth.py`) supports explicit client credentials, ambient `AZURE_TENANT_ID`/`AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET`, Workload Identity Federation (`AZURE_FEDERATED_TOKEN_FILE` + `AZURE_AUTHORITY_HOST`), App Service / Container Apps Managed Identity (`IDENTITY_ENDPOINT` + `IDENTITY_HEADER`), and VM IMDS (`169.254.169.254`). Adapter configs and drivers inject `AZURE_{POSTGRES,MYSQL,MSSQL,REDIS}_SCOPE`, while direct calls on an unscoped provider raise `ImproperConfigurationError`. Loopback, link-local, and `IDENTITY_ENDPOINT` requests bypass HTTP proxies via `ProxyHandler({})`, whereas remote HTTPS authority hosts honor configured proxies [^entra-auth].
- `AwsRdsIamTokenProvider` (`ttl <= 900s`) and `AwsAuroraDsqlIamTokenProvider` (`ttl <= 604800s`) resolve explicit keys, `AWS_*` env vars, `~/.aws/credentials` and `~/.aws/config` profiles (`region`, static keys, `web_identity_token_file` + `role_arn`), Web Identity STS, host-allowlisted ECS/Pod Identity endpoints, and EC2 IMDSv2 off-loop via `anyio.to_thread.run_sync`. `credential_process` external executables, AWS SSO (`sso_session`/`sso_start_url`) device flows, and chained `source_profile` `AssumeRole` INI profiles are not executed without an external helper. STS, ECS, IMDSv2, and Entra HTTP flows are verified against local loopback harnesses on port `0` rather than live AWS or Azure metadata services [^aws-sigv4-auth] [^entra-auth].
- `MSSQLConfig` (`db/adapters/mssql/config.py`) normalizes `AzureEntraTokenProvider` with `AZURE_MSSQL_SCOPE` in `__post_init__`, and `SyncMSSQLClient.set_aad_token` / `AsyncMSSQLClient.set_aad_token` update the `MssqlClientConfig` runtime override field for subsequent TDS `LOGIN7` `FEDAUTH` reconnects without rebuilding the Rust client or tearing down an already-healthy active session; live Azure SQL Server `FEDAUTH` handshakes were verified against TDS 7.4 loopback wire servers rather than a live Azure SQL endpoint [^mssql-entra].
- Google Cloud `external_account` Workload Identity Federation (`gcp_auth.rs::parse_external_account_json`) supports file, URL, and `aws1` metadata credential sources; `executable` credential sources are rejected for security reasons [^gcp-wif].
- Stub-only `TypedDict` result shapes in `_bigquery.pyi` and `_spanner.pyi` require `@typing.type_check_only` so `mypy.stubtest` accepts them without runtime PyO3 classes; because `TypedDict` is not assignable to invariant `dict[str, Any]`, adapter structural `Protocol` definitions (`SyncBigQueryClientProtocol`, `AsyncBigQueryClientProtocol`, `SyncSpannerClientProtocol`, `AsyncSpannerClientProtocol`) must use `Mapping[str, Any]` or the `TypedDict` types rather than `dict[str, Any]` [^driver-stubs].
- CockroachDB `--cluster=<id>` routing and DSN `options` forward through `session_gucs["options"]` into `StartupMessage` (and skip SQL `SET` restoration). `RESET ALL` replaces `DISCARD ALL` when `crdb_version` is present, and `cockroach_restart` savepoints plus `PostgresError::is_retryable_serialization` live on Rust `PostgresConnection` / `SessionHandle` rather than Python PyO3 bindings. CockroachDB wire flows, AlloyDB `v1beta`/`v1alpha` discovery fallback, and `MetadataExchangeResponse` diagnostics use loopback wire/HTTP mocks rather than live CockroachDB or Google Cloud AlloyDB instances [^crdb-alloydb].
- `AsyncQueueStore` and `SyncQueueStore` (`db/driver/capabilities.py`) are `@runtime_checkable` protocols requiring `cleanup_terminal`, `get_entry_by_key`, `reserve_identity`, `get_reservation`, `reset_identity`, `assign_worker`, `touch_heartbeats`, `null_heartbeats`, `interrupt_task`, `expire_overdue`, and `list_entries(queue: str | None = None)`. Any engine queue store or test double omitting those methods fails `isinstance(store, AsyncQueueStore)` or `isinstance(store, SyncQueueStore)` runtime checks [^queue-store-protocol].
- `SyncOracleQueueStore` and `AsyncOracleQueueStore` (`db/adapters/oracle/queue.py`) implement the full extended `SyncQueueStore` / `AsyncQueueStore` protocol using `:named` Oracle binds, `JSON_VALUE(j.headers, '$.key')` lookup, and `MERGE INTO vertebra_queue_unique_keys` with nullable `expires_at_ms` for forever reservations; offline unit coverage uses fake drivers (`_FakeSyncOracleDriver` / `_FakeAsyncOracleDriver`) rather than a live Oracle Database instance [^oracle-queue].
- `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` (`db/adapters/spanner/queue.py`) implement the full extended `SyncQueueStore` / `AsyncQueueStore` protocol across GoogleSQL (`@param`) and Spangres (`$1`) dialects using transactional `run_in_transaction` `FOR UPDATE` reservation checks, `LEFT JOIN vertebra_queue_unique_keys` key lookups, and nullable `expires_at` for forever reservations; unit coverage runs against `_InMemorySpannerQueueWire` rather than a live Spanner instance or emulator [^spanner-queue].
- `vertebra_db::dialer` embeds the 114-certificate AWS RDS Global Root CA PEM bundle (`aws_rds_root_ca_pem()`) and exposes `AwsRdsDialer`, `AwsAuroraDsqlDialer`, and `AzureEntraDialer` on `vertebra._vertebra`, paired with `SyncAwsRdsDialer`, `AsyncAwsRdsDialer`, `SyncAwsAuroraDsqlDialer`, `AsyncAwsAuroraDsqlDialer`, `SyncAzureEntraDialer`, and `AsyncAzureEntraDialer` in `vertebra.db.dialer`. `PostgreSQLConfig` (`ca_pem: str | None = None`) and `MySQLConfig` inject `AZURE_{POSTGRES,MYSQL}_SCOPE` for unscoped `AzureEntraTokenProvider` instances and attach `aws_rds_root_ca_pem()` for remote `AwsRdsIamTokenProvider` hosts without custom CAs. With an AWS or Azure token provider on a remote host and no explicit SSL mode, both default to `verify-full`. They reject `disable`, `allow`, and `prefer` there and keep explicit `require` and `verify-ca`. `MSSQLConfig` with an Entra provider rejects unencrypted or login-only encryption and `trust_server_certificate=True` on remote hosts. Loopback hosts, Unix sockets, and Cloud SQL targets keep their defaults so local wire harnesses run without live cloud endpoints [^aws-azure-rs-dialers] [^py-aws-azure-dialers] [^pg-mysql-cloud-tls].
- Cloud token provider limits: refresh is single-flight per event loop, so one sync refresh plus one refresh per loop can run at once. Credential and token requests refuse redirects and cap responses at 1 MiB. `AzureEntraDialer` selects the scope and authority for Azure Government and Azure China hosts, while configs built without the dialer use the public-cloud scope unless `scope=` is set. On MySQL, `verify-ca` also checks the hostname, and an RDS CA attached at construction stays on the config after `msgspec.structs.replace` to another host [^entra-auth] [^aws-sigv4-auth] [^py-aws-azure-dialers].
- Arrow IPC hardening and release unwind: `[profile.release]` in `src/rs/Cargo.toml` uses `panic = "unwind"` so `catch_unwind` and PyO3 `PanicException` protect host Python processes in release wheels [^cargo-release-profile]. `ArrowCursor` compiles stream schemas once into `CompiledIpcLayout`, guards `FixedSizeList`, `FixedSizeBinary`, and length conversions against `usize` overflow, and decodes batches in a single pass. It caches frame validation in `validated: Arc<AtomicBool>` across repeat reads while keeping per-batch layout and panic guards active, enforces `MAX_PYLIST_ROWS` with `checked_add`, and calls `py.check_signals()?` every `4096` rows and `1024` batches in `to_pylist` [^arrow-cursor-ipc] [^bench-arrow-ipc].
<!-- truth: end -->

## Arrow IPC Release Timing Baseline

Measured on a release build of `vertebra-py` using:

```bash
uv run --no-sync maturin develop --release --uv -m src/rs/vertebra-py/Cargo.toml
uv run --no-sync python -m tools.benchmarks.micro --arrow-ipc-only --json
```

| Workload | Payload Size | Phase | Median (ms) | p99 (ms) | Ops/sec |
| --- | --- | --- | --- | --- | --- |
| `2,000 x (8 rows x 20 cols)` (`tiny_2000x8x20`) | 5,249,024 B (5.01 MiB) | Open (`__arrow_c_stream__`) | 0.510 | 0.539 | 1,959.62 |
| `2,000 x (8 rows x 20 cols)` (`tiny_2000x8x20`) | 5,249,024 B (5.01 MiB) | Full iteration (`RecordBatchReader`) | 200.816 | 207.463 | 4.98 |
| `20 x (50,000 rows x 20 cols)` (`large_20x50000x20`) | 171,901,184 B (163.94 MiB) | Open (`__arrow_c_stream__`) | 0.082 | 0.086 | 12,232.27 |
| `20 x (50,000 rows x 20 cols)` (`large_20x50000x20`) | 171,901,184 B (163.94 MiB) | Full iteration (`RecordBatchReader`) | 469.724 | 475.618 | 2.13 |

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
[^aws-azure-rs-dialers]: `src/rs/vertebra-db/src/dialer.rs::AwsRdsDialer` — Native AWS RDS, Aurora DSQL, and Azure Entra dialer metadata, SigV4 token signing, and bundled AWS RDS Root CAs.
[^aws-sigv4-auth]: `src/py/vertebra/db/driver/_cloud_auth.py::AwsRdsIamTokenProvider` — Ambient AWS SigV4 credential chains (`~/.aws`, Web Identity STS, ECS, IMDSv2) and temporary credential expiry clamping.
[^py-aws-azure-dialers]: `src/py/vertebra/db/dialer/aws.py::SyncAwsRdsDialer` — Python sync/async AWS RDS, Aurora DSQL, and Azure Entra `verify-full` TLS dialers and `connection_kwargs()`.
[^pg-mysql-cloud-tls]: `src/py/vertebra/db/adapters/postgres/options.py::build_native_client_options` — PostgreSQL and MySQL Azure Entra default scopes, bundled RDS Root CA attachment, and remote TLS enforcement.
[^cargo-release-profile]: `src/rs/Cargo.toml` — Rust workspace `[profile.release]` `panic = "unwind"` policy.
[^arrow-cursor-ipc]: `src/rs/vertebra-py/src/db.rs::ArrowCursor` — Single-pass `CompiledIpcLayout` validation, overflow guards, cached `validated` flag, and `to_pylist` signal and row-ceiling checks.
[^bench-arrow-ipc]: `tools/benchmarks/micro.py::benchmark_arrow_ipc_stream` — Release Arrow IPC stream open and iteration microbenchmark.
