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
- by: agent:bb-executor
  at: "2026-11-21T21:30:00Z"
  commit: 06e99fbf
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
  title: DuckDBCore parameter normalization for Arrow row ingestion
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
- id: bench-sql-check
  resource: tools/benchmarks/adapters/sql.py::check_report_against_comparators
  title: Adapter benchmark comparator check and allow-list enforcement
---

# Database Driver Limitations

<!-- truth: start -->
- **Companion Wheel Loading & Layout**: All ten optional database wheels load via `ensure_module` and raise `MissingDependencyError(install_package="vertebra-core[<extra>]")` when absent. Upstream PyPI `duckdb` has no free-threaded wheel and stays isolated in `[dependency-groups].bench` (`python_version < '3.14'`), while `vertebra._duckdb` builds for every supported Python ABI.
- **PostgreSQL & Queue Stores**: PostgreSQL drivers run over `vertebra._postgres` (see [PostgreSQL Limitations](postgres-limitations.md)) [^pg-driver] [^pg-stub]. `PostgreSQLQueueBackend` and `OracleQueueBackend` probe their native extensions when no `executor`, `store`, or `driver` is passed, raising `ImproperConfigurationError` if no DSN is configured [^pg-queue]. `AsyncQueueStore` and `SyncQueueStore` (`db/driver/capabilities.py`) are `@runtime_checkable` protocols requiring the full reservation, heartbeat, and cleanup surface [^queue-store-protocol].
- **Django & SQLAlchemy Dialect Gaps**: BigQuery, DuckDB, and SQL Server have no Vertebra Django backend; BigQuery and Spanner reject `autocommit=False` with `NotSupportedError`; `vertebra.extensions.django.db.backends.spanner.base.DatabaseWrapper` subclasses `BaseDatabaseWrapper` directly and probes `django_spanner.base` inside `__init__` because `django-google-spanner 5.0.0` fails on import under Django 6.1.x.
- **DuckDB & DataFusion**: Plain `:memory:` DuckDB databases are isolated per config while `:memory:<name>` shares named catalogs; `httpfs` 401/403 secret rotation relies on exception-message regex matching and re-raises without retrying [^duckdb-core] [^duckdb-secret] [^gcs-secrets] [^duckdb-driver]. `DataFusionEngine` defers registration without the `datafusion` wheel and fails closed on execution [^datafusion-core].
- **Cloud IAM & Dialer Boundaries**: AWS RDS/DSQL and Azure Entra token providers refresh single-flight per event loop, refuse redirects, and cap responses at 1 MiB. Remote AWS/Azure configs default to `verify-full` and reject weaker modes (`disable`, `allow`, `prefer`); `external_account` WIF rejects `executable` sources; `MSSQLConfig` updates tokens via `set_aad_token` for future reconnects [^entra-auth] [^aws-sigv4-auth] [^mssql-entra] [^gcp-wif] [^aws-azure-rs-dialers] [^py-aws-azure-dialers] [^pg-mysql-cloud-tls].
- **Stubs, CI & Arrow Hardening**: `_bigquery.pyi` and `_spanner.pyi` `TypedDict` shapes need `@typing.type_check_only` and `Mapping[str, Any]` protocol signatures [^driver-stubs]. Release wheels compile with `panic = "unwind"` so `catch_unwind` maps Rust panics to `PanicException`; `ArrowCursor` validates `CompiledIpcLayout` once, caches `validated`, guards `MAX_PYLIST_ROWS`, and checks signals every 4,096 rows / 1,024 batches [^cargo-release-profile] [^arrow-cursor-ipc] [^bench-arrow-ipc].
<!-- truth: end -->

## Cloud IAM, Dialers & Test Coverage Caveats

- **Azure Entra ID (`_cloud_auth.py`)**: Supports explicit client credentials, ambient `AZURE_*` env vars, Workload Identity Federation (`AZURE_FEDERATED_TOKEN_FILE`), Managed Identity (`IDENTITY_ENDPOINT` + `IDENTITY_HEADER`), and VM IMDS (`169.254.169.254`). Adapter configs inject `AZURE_{POSTGRES,MYSQL,MSSQL,REDIS}_SCOPE`; direct unscoped calls raise `ImproperConfigurationError`. Loopback, link-local, and `IDENTITY_ENDPOINT` requests bypass HTTP proxies while remote HTTPS authorities honor them [^entra-auth].
- **AWS RDS & Aurora DSQL (`_cloud_auth.py`)**: `AwsRdsIamTokenProvider` (`ttl <= 900s`) and `AwsAuroraDsqlIamTokenProvider` (`ttl <= 604800s`) resolve explicit keys, `AWS_*` env vars, `~/.aws/{credentials,config}`, Web Identity STS, allowlisted container endpoints, and EC2 IMDSv2 off-loop. External `credential_process`, AWS SSO device flows, and chained `source_profile` `AssumeRole` profiles are not executed without an external helper [^aws-sigv4-auth].
- **TLS Defaults & Scope Quirks**: `PostgreSQLConfig` and `MySQLConfig` attach `aws_rds_root_ca_pem()` for remote RDS hosts without custom CAs (note: an attached CA remains if `msgspec.structs.replace` changes the host; MySQL `verify-ca` also checks the hostname). `MSSQLConfig` with Entra rejects unencrypted/login-only modes and `trust_server_certificate=True` on remote hosts. `AzureEntraDialer` picks Azure Government/China authorities automatically, whereas standalone configs default to public cloud unless `scope=` is set. Loopback, Unix socket, and Cloud SQL targets keep local defaults [^aws-azure-rs-dialers] [^py-aws-azure-dialers] [^pg-mysql-cloud-tls].
- **Offline Mock Verification**: STS, ECS, IMDSv2, Entra, MSSQL `FEDAUTH`, CockroachDB (`--cluster=<id>`, `RESET ALL`, `cockroach_restart`), AlloyDB `v1beta`/`v1alpha` fallback, Oracle queue stores (`:named` binds, `JSON_VALUE`, `MERGE`), and Spanner queue stores (`FOR UPDATE`, `LEFT JOIN`) are verified against loopback harnesses or fake drivers rather than live cloud services [^crdb-alloydb] [^oracle-queue] [^spanner-queue].
- **CI Gates**: Live DuckDB `httpfs` tests require `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` (outbound network needed for `INSTALL httpfs`), and `conformance-oracle` in `.github/workflows/ci.yml` uses `continue-on-error: true` to isolate Oracle Free container startup variance [^ci-workflow].

## Arrow IPC Release Timing Baseline

Measured on a release build of `vertebra-py` (`maturin develop --release --uv -m src/rs/vertebra-py/Cargo.toml` + `python -m tools.benchmarks.micro --arrow-ipc-only --json`):

| Workload | Payload Size | Phase | Median (ms) | p99 (ms) | Ops/sec |
| --- | --- | --- | --- | --- | --- |
| `2,000 x (8 rows x 20 cols)` (`tiny_2000x8x20`) | 5.01 MiB | Open (`__arrow_c_stream__`) | 0.510 | 0.539 | 1,959.62 |
| `2,000 x (8 rows x 20 cols)` (`tiny_2000x8x20`) | 5.01 MiB | Full iteration (`RecordBatchReader`) | 200.816 | 207.463 | 4.98 |
| `20 x (50,000 rows x 20 cols)` (`large_20x50000x20`) | 163.94 MiB | Open (`__arrow_c_stream__`) | 0.082 | 0.086 | 12,232.27 |
| `20 x (50,000 rows x 20 cols)` (`large_20x50000x20`) | 163.94 MiB | Full iteration (`RecordBatchReader`) | 469.724 | 475.618 | 2.13 |

## Adapter Results Against Third-Party Drivers

Measured on 2026-11-21 on 64-bit glibc Linux (release builds, Python 3.12, loopback, `tmpfs` containers). Embedded: 2,000 iterations (200 warmup); network: 500 (50 warmup); BigQuery emulator: 100 (10 warmup). All 13 reports pass `python -m tools.benchmarks.adapters.sql check` [^bench-sql-check]:

| Adapter and case | Vertebra | Comparator | Status | Cause of any gap |
| --- | --- | --- | --- | --- |
| SQLite sync `point_select_1` | 0.93 us | `sqlite3` 1.57 us | at or ahead | — |
| SQLite reused cursor `cursor_point_select_1` | 0.75 us | `sqlite3` 1.37 us | at or ahead | — |
| SQLite async `point_select_1` | 38.79 us | `aiosqlite` 84.75 us | at or ahead | — |
| SQLSpec SQLite sync `point_select_1` | 2.19 us | `sqlspec-sqlite-sync` 2.97 us | at or ahead | — |
| SQLAlchemy `sqlite+vtb` `sa_point_select_1` | 26.87 us | `sqlalchemy-vanilla-sync` 26.93 us | at or ahead | — |
| DuckDB sync `point_select_1` | 361.96 us | `duckdb` 256.33 us | allowed | Bundled `libduckdb-sys` lacks jemalloc (no CMake tree shipped). |
| SQLSpec DuckDB sync `point_select_1` | 351.87 us | `sqlspec-duckdb-sync` 254.25 us | allowed | Same bundled `libduckdb-sys` build without jemalloc. |
| PostgreSQL sync `point_select_1` | 67.51 us | `psycopg-sync` 71.34 us | at or ahead | — |
| PostgreSQL async pooled `point_select_1` | 71.55 us | `asyncpg` 71.96 us | at or ahead | — |
| SQLSpec PostgreSQL async `point_select_1` | 71.81 us | `sqlspec-asyncpg` 84.64 us | at or ahead | — |
| MySQL sync `point_select_1` | 117.51 us | `pymysql` 122.27 us | at or ahead | — |
| MySQL async `point_select_1` | 94.48 us | `aiomysql` 144.99 us | at or ahead | — |
| SQL Server sync `point_select_1` | 354.46 us | `pymssql` 287.92 us | allowed | Dominated by engine round trip; within run-to-run variance. |
| Oracle sync `point_select_1` (relay on / off) | 99.94 / 92.64 us | `oracledb-sync` 72.17 / 71.56 us | allowed | TNS reset relay + upstream `oracle/rust-oracledb#101` statement cache clear. |
| Oracle async `point_select_1` (relay on / off) | 164.73 / 152.53 us | `oracledb-async` 100.27 / 104.67 us | allowed | TNS reset relay + `vtb-ora-worker` hand-off + `oracle/rust-oracledb#101`. |
| Spanner emulator sync `point_select_1` | 2.54 ms | `google-cloud-spanner-sync` 3.21 ms | at or ahead | — |
| BigQuery emulator sync `point_select_1` | 54.57 ms | `google-cloud-bigquery-sync` 56.45 ms | at or ahead | — |
| Storage `file://` sync get | 98 us | `obstore` 35 us | behind | `LocalFileSystem` uses Tokio blocking pool; `file://` lacks sync fast path. |
| Storage `memory://` sync put | 2.1 us | `obstore` 1.4 us | behind | Native call is 0.8 us; `PutResult` construction and key normalization add overhead. |

Comparatorless async timings: Spanner `2.52 ms`, BigQuery `54.34 ms`, PostgreSQL `pipeline_select_16` `1.07 ms`. Pooled async PostgreSQL runs clean cached one-shot queries directly on the Python event loop via `PersistentLoopReader` to avoid Tokio wakeups.

## Oracle Session & Pool Caveats

- Sync Oracle calls run on the calling thread under the session mutex, while async calls and `Close` run on `vtb-ora-worker`; mixing sync and async on one session does not guarantee FIFO ordering.
- Out-bind values and session-context tracking return only from `execute` and `execute_dml`. Fetch methods reset `out_binds` to `None`, so statements needing both rows and out-binds must use `cursor()` or `checkout_cursor()`.
- Pooled Oracle release dispatches `WorkerCommand::ResetIfDirty`: skips rollback when `!in_transaction && !may_have_open_txn`, but sends rollback after any query/DML/pipeline because those commands mark `may_have_open_txn`.

## DuckDB Execution, Cursors & Secrets

- **Statement Facts Cache (`vertebra-duckdb/src/lib.rs`)**: `run_statement` classifies row-returning statements lexically via `sql_returns_rows`; misclassified queries go through `execute_sql` and drop results. The per-pool 256-entry facts cache (`std::sync::RwLock`) clears completely on overflow. Arrow entry points still scan SQL text on every call.
- **Sync Cursor (`cursor.rs::SyncDuckDBCursor`)**: `lastrowid` is always `None`. `TIMESTAMP` reads back as UTC-aware `datetime` and `UUID` as `str`. Empty `executemany` returns `rowcount == 0` without calling DuckDB or starting a lazy transaction.
- **Async Cursor (`cursor.rs::AsyncDuckDBCursor`)**: Concurrent `execute`/`executemany` raises `InterfaceError`; fetch and `close` are unblocked. Outcomes apply on `await` inside the completion drainer. Cancellation triggers `CancelOnDropGuard` and clears results immediately while the worker may still be unwinding (untested).
- **Driver & Secrets (`_sync.py`, `_async.py`)**: Drivers bind single-call methods on connect and swap to `_raise_closed` on close; `_client=` test doubles must implement `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`. Dynamic secrets refresh only on open, `refresh_secrets()`, or HTTP 401/403 (re-raising the original error with failed refresh attached as a note). Native cursors are not context managers; close them in `try/finally`.

## SQL Server, Spanner & BigQuery Cursor Paths

- **SQL Server (`adapters/mssql/`)**: Queries run on native `SyncMSSQLCursor` / `AsyncMSSQLCursor`. Broker leases attach once in `connect()` to the Rust lease guard and release on `close()` or GC drop. `MSSQLCore.rewrite_placeholders` remains for config/SQLSpec callers.
- **Spanner (`adapters/spanner/` & SQLSpec)**: Queries and transactions run on native `SyncSpannerCursor` / `AsyncSpannerCursor`; `cursor()` delegates to active transactions when `staleness is None` and rejects `lazy_begin=True`. In SQLSpec, `SpannerAsyncDriver.with_cursor` wraps injected sync drivers in `_ThreadedAsyncCursor` (`asyncio.to_thread`).
- **BigQuery (`adapters/bigquery/`)**: Queries, DML, scripts, and existing-session transactions run on native cursors. Initial session creation in `begin()` (`_session_id is None`) calls `client.query("BEGIN TRANSACTION", None, None, True)` because `CursorQueryOptions` lacks `create_session`. `_run_query_sync` / `_run_query_async` remain for `select_to_arrow` REST fallback.

## Related Context

- [PostgreSQL Native Driver Limitations](postgres-limitations.md)
- [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md)

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Native PostgreSQL drivers over `vertebra._postgres`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend native queue stores and optional executor.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` client, pool, COPY, Arrow, and listener stubs.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` parameter normalization for Arrow row ingestion.
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
[^bench-sql-check]: `tools/benchmarks/adapters/sql.py::check_report_against_comparators` — Adapter benchmark comparator check and allow-list enforcement.
