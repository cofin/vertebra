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
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`adapters/postgres/driver.py` re-exporting `_sync` / `_async`) execute natively through `vertebra._postgres` (`vertebra-core[postgres]`) with `$1..$N` dialect compilation; runtime pooler, logical replication streams, and Arrow interface boundaries are detailed in [PostgreSQL Native Driver Limitations](postgres-limitations.md) [^pg-driver] [^pg-stub].
- `PostgreSQLQueueBackend` and `OracleQueueBackend` ship native SQL queue stores (`SyncPostgresQueueStore`/`AsyncPostgresQueueStore` and `SyncOracleQueueStore`/`AsyncOracleQueueStore`) alongside optional `executor=` injection; when neither `executor=`, `store=`/`sync_store=`, nor `driver=`/`sync_driver=` is supplied, they probe `vertebra._postgres` / `vertebra._oracle` via `ensure_module` (`MissingDependencyError` when absent) and raise `ImproperConfigurationError` if no non-empty DSN is configured [^pg-queue].
- Optional companion driver wheels (`vertebra-postgres`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-mssql`, `vertebra-duckdb`, `vertebra-sqlite`, `vertebra-redis`, `vertebra-valkey`, `vertebra-bigquery`, `vertebra-spanner`) load dynamically via `ensure_module` and fail loud with `MissingDependencyError(install_package="vertebra-core[<extra>]")` when omitted from the active environment; upstream PyPI `duckdb` publishes no free-threaded wheel and is isolated to `[dependency-groups].bench` (`python_version < '3.14'`) for `tools.benchmarks.store` result persistence while `vertebra._duckdb` builds across all supported CPython ABIs. All adapters live under `src/py/vertebra/db/adapters/<engine>/` (`src/py/vertebra/drivers/` is removed).
- **SQLAlchemy, Alembic & Django Dialect Boundaries**: BigQuery, DuckDB, and SQL Server have no Vertebra Django `DATABASES` backend (`VertebraPlugin` raises `ImproperConfigurationError` if configured; use SQLAlchemy or SQLSpec instead). `duckdb-engine`, `sqlalchemy-bigquery`, `sqlalchemy-spanner`, `django-google-spanner`, and `dogpile.cache` (`vertebra-core[dogpile]`) are optional upstream packages required only when instantiating those specific SQLAlchemy/Django dialect bases or `use_dogpile=True` cache regions, and BigQuery/Spanner execute non-transactional DDL (`autocommit=False` raises PEP 249 `NotSupportedError`). Because `django-google-spanner 5.0.0` raises `ImproperlyConfigured` on import against Django 6.1.x, `vertebra.extensions.django.db.backends.spanner.base.DatabaseWrapper` subclasses `django.db.backends.base.base.BaseDatabaseWrapper` directly and probes `django_spanner.base` via `ensure_module` inside `__init__`.
- **DuckDB In-Memory Catalog Isolation**: DuckDB isolates plain `:memory:` instances per config whereas `:memory:<name>` shares named in-memory catalogs across drivers [^duckdb-driver].
- `DuckDBCore` provides `normalize_param` for `load_from_arrow` row-sequence ingestion. `vertebra.storage.{SyncGcsBearerSecretProvider,AsyncGcsBearerSecretProvider}` supply GCS OAuth2 bearer-token rotation for `DuckDBSecretConfig(secret_type="gcs", value_provider=...)`, and `persistent=True` with `value_provider` or non-positive/boolean `refresh_interval_seconds` fails validation [^duckdb-core] [^duckdb-secret] [^gcs-secrets].
- DuckDB `httpfs` `(HTTP 401)` / `(HTTP 403)` reactive invalidation relies on regex matching (`\(HTTP (?:401|403)\)`) because `vertebra._duckdb` surfaces HTTP status codes inside the exception string. On a match, `_on_native_error` invalidates tracked generations, runs a forced refresh, and re-raises the original error without retrying the failed query [^duckdb-driver].
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

## Adapter Results Against Third-Party Drivers

Measured on 2026-11-21 on 64-bit glibc Linux with release builds, Python 3.12, loopback connections, and `tmpfs` containers from `FIXTURE_SPECS`. Embedded captures used `--iterations 2000 --warmup 200`, network captures used `--iterations 500 --warmup 50`, and the BigQuery emulator used `--iterations 100 --warmup 10` because its job history slows linearly per call. All 13 captured reports pass `python -m tools.benchmarks.adapters.sql check` with exit code `0` [^bench-sql-check].

| Adapter and case | Vertebra | Comparator | Status | Cause of any gap |
| --- | --- | --- | --- | --- |
| SQLite sync `point_select_1` | 0.93 us | `sqlite3` 1.57 us | at or ahead | — |
| SQLite reused cursor `cursor_point_select_1` | 0.75 us | `sqlite3` 1.37 us | at or ahead | — |
| SQLite async `point_select_1` | 38.79 us | `aiosqlite` 84.75 us | at or ahead | — |
| SQLSpec SQLite sync `point_select_1` | 2.19 us | `sqlspec-sqlite-sync` 2.97 us | at or ahead | — |
| SQLAlchemy `sqlite+vtb` `sa_point_select_1` | 26.87 us | `sqlalchemy-vanilla-sync` 26.93 us | at or ahead | — |
| DuckDB sync `point_select_1` | 361.96 us | `duckdb` 256.33 us | allowed | `vertebra-duckdb` keeps the `bundled` build, which has no jemalloc, because the published `libduckdb-sys` package ships no CMake tree. |
| SQLSpec DuckDB sync `point_select_1` | 351.87 us | `sqlspec-duckdb-sync` 254.25 us | allowed | Same bundled `libduckdb-sys` build without jemalloc. |
| PostgreSQL sync `point_select_1` | 67.51 us | `psycopg-sync` 71.34 us | at or ahead | — |
| PostgreSQL async pooled `point_select_1` | 71.55 us | `asyncpg` 71.96 us | at or ahead | — |
| SQLSpec PostgreSQL async `point_select_1` | 71.81 us | `sqlspec-asyncpg` 84.64 us | at or ahead | — |
| MySQL sync `point_select_1` | 117.51 us | `pymysql` 122.27 us | at or ahead | — |
| MySQL async `point_select_1` | 94.48 us | `aiomysql` 144.99 us | at or ahead | — |
| SQL Server sync `point_select_1` | 354.46 us | `pymssql` 287.92 us | allowed | Nearly all time is inside the engine round trip, and results sit within run-to-run variation. |
| Oracle sync `point_select_1` (relay on) | 99.94 us | `oracledb-sync` 72.17 us | allowed | In-process TNS reset relay plus upstream `oracle/rust-oracledb#101` clearing the statement cache when `AUTH_MAX_OPEN_CURSORS` is absent. |
| Oracle sync `point_select_1` (relay off) | 92.64 us | `oracledb-sync` 71.56 us | allowed | Upstream `oracle/rust-oracledb#101` clears the statement cache when `AUTH_MAX_OPEN_CURSORS` is absent. |
| Oracle async `point_select_1` (relay on) | 164.73 us | `oracledb-async` 100.27 us | allowed | TNS reset relay, `vtb-ora-worker` hand-off, and upstream `oracle/rust-oracledb#101`. |
| Oracle async `point_select_1` (relay off) | 152.53 us | `oracledb-async` 104.67 us | allowed | `vtb-ora-worker` hand-off plus upstream `oracle/rust-oracledb#101`. |
| Spanner emulator sync `point_select_1` | 2.54 ms | `google-cloud-spanner-sync` 3.21 ms | at or ahead | — |
| BigQuery emulator sync `point_select_1` | 54.57 ms | `google-cloud-bigquery-sync` 56.45 ms | at or ahead | — |
| Storage `file://` sync get | 98 us | `obstore` 35 us | behind | `LocalFileSystem` reads go through the Tokio blocking pool; `memory://` has a synchronous fast path, `file://` does not. |
| Storage `memory://` sync put | 2.1 us | `obstore` 1.4 us | behind | The native call takes 0.8 us; `PutResult` construction and key normalization add the rest. |

Spanner async measures `2.52 ms`, BigQuery async measures `54.34 ms`, and PostgreSQL async `pipeline_select_16` measures `1.07 ms` (`[SKIP]` in `COMPARATORLESS_CASES`). No asynchronous official comparator exists for Spanner, BigQuery, or `pymssql`, so those async cases have no comparator. The BigQuery emulator slows as its job history grows, so the harness alternates drivers one query at a time.

Native SQLite, DuckDB, PostgreSQL, and Oracle drivers route direct query and DML calls through native single-call methods (`fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`). SQLAlchemy `+vtb` binds the native cursor factory directly on the DB-API connection, and SQLSpec adapters register native cache-hit executors. Pooled async PostgreSQL runs clean cached one-shot queries directly on the Python event loop via a persistent per-connection loop reader (`PersistentLoopReader`) to avoid Tokio cross-thread wakeups.

Synchronous Oracle calls run on the calling thread under the session mutex, while asynchronous calls and `Close` still go through the `vtb-ora-worker` thread. A client that mixes both styles on one session does not get strict first-in-first-out ordering between a queued asynchronous command and a later synchronous one.

On the Oracle driver, out-bind values and session-context tracking come back only from `execute` and `execute_dml`, not from the fetch methods (`execute_tuples`, `execute_with_description`, `select`, `select_value` reset `out_binds` to `None`). A statement that needs both rows and out-binds runs on the cursor (`cursor()` or `checkout_cursor()`).

A pooled Oracle release dispatches `WorkerCommand::ResetIfDirty`, which sends no rollback round trip when the connection has no open transaction (`!in_transaction && !may_have_open_txn`). It sends a rollback whenever a transaction may be open, which includes every release after a query because `Query`, `Execute`, `ExecuteMany`, `QueryArrow`, and `PipelineBatch` mark `may_have_open_txn`.

## DuckDB Statement Outcome and Facts Cache

`DuckDBInner::run_statement` and `run_statement_many` in `src/rs/vertebra-duckdb/src/lib.rs` report columns, rows, and the changed-row count in one call.

- `rowcount` is `-1` for a result set from a non-mutating statement, and the returned row count for a mutating one (`INSERT ... RETURNING`). `run_statement_many` reports the summed count only, with no columns or rows.
- Whether a statement returns rows is decided from the SQL text (`sql_returns_rows`), not from the prepared statement. A statement the scan classifies as not returning rows goes through `execute_sql` and its result set, if any, is dropped.
- The per-pool statement facts cache holds 256 SQL texts and is emptied in full when a new text arrives at capacity. Statements that inline literal values produce one entry each and trigger that clear repeatedly.
- The cache uses `std::sync::RwLock`. `parking_lot` is not a dependency of `vertebra-duckdb`.
- `query_sql` still scans the SQL text once more when a statement yields zero columns (the Arrow-schema fallback in `query_rows_on_conn`). The Arrow entry points (`query_arrow_payload`, `stream_arrow_shm_batches`) still scan on every call.

## DuckDB Native Sync Cursor

`SyncDuckDBCursor` in `src/rs/vertebra-duckdb/src/cursor.rs` is the PEP 249 cursor returned by `SyncDuckDBClient.cursor()`.

- `lastrowid` is always `None`.
- The cursor uses `std::sync::Mutex` for its state and execute lock. The execute lock is held inside `py.detach` until the result is installed, and the cached `description` tuple is moved out of the state there and dropped after the interpreter is attached again.
- Row values keep the conversions of `duck_value_to_py`: a `TIMESTAMP` column reads back as a UTC-aware `datetime`, and a `UUID` column reads back as `str`. A naive `datetime` or a `uuid.UUID` parameter does not compare equal to the value fetched back.
- `executemany` with an empty sequence reports `rowcount == 0` without calling the engine and does not open a lazy-begin transaction.
- The two-thread test checks that both `execute` calls return and that the cursor holds one whole result. It does not observe the lock wait itself.

## DuckDB Native Async Cursor

`AsyncDuckDBCursor` in `src/rs/vertebra-duckdb/src/cursor.rs` is the awaitable cursor returned by `AsyncDuckDBClient.cursor()`.

- A second `execute` or `executemany` before the first awaitable settles raises `InterfaceError` (`cursor is already executing`). The fetch methods and `close` are not blocked by an in-flight statement.
- State errors and parameter conversion errors are raised by the method call. Engine errors are raised when the awaitable is awaited. A parameter conversion error also clears the buffered result.
- The outcome is installed when the completion drainer runs the success mapper, so `description` and `rowcount` change at the `await`, not when the engine finishes.
- Cancelling an in-flight awaitable interrupts the statement through `CancelOnDropGuard`, clears the result, and releases the in-flight flag while the worker may still be unwinding the interrupted call. No test covers cancellation.
- When that cleanup runs on a Tokio thread, a cached `description` tuple is released through PyO3's deferred reference pool instead of under an attached interpreter.
- `executemany` with an empty sequence resolves with `rowcount == 0` without calling the engine and does not open a lazy-begin transaction.

## DuckDB Sync and Async Drivers

`SyncDuckDBDriver` (`_sync.py`) and `AsyncDuckDBDriver` (`_async.py`) bind `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml` directly on the client via `_bind_client` (rebound to `_raise_closed` when `_client` is `None`). `cursor()` returns the native `SyncDuckDBCursor` or `AsyncDuckDBCursor`.

- Query methods take no broker lease and register no statement plan. The `broker` argument is accepted and unused by queries.
- Dynamic secrets are refreshed at open (`__init__` for sync; `connect()` or first use for async), by `refresh_secrets()`, and after an HTTP 401 or 403 rejection. A query does not pick up an elapsed `refresh_interval_seconds` or a changed provider generation; call `refresh_secrets(force=False)` for that.
- After a 401 or 403 rejection, the driver refreshes every dynamic secret, including ones whose provider has no `invalidate` method or `generation` attribute, and re-raises the original error. A failed refresh is attached as a note.
- `execute` returns `0` for a statement that returns rows without changing any. `execute_many` returns the cursor `rowcount` as reported.
- Native cursors have no context-manager protocol. Close them with `try/finally`.
- `DuckDBCore.normalize_param` remains in `core.py` for `load_from_arrow` row-sequence ingestion.
- Custom `_client=` test doubles passed to `SyncDuckDBDriver` or `AsyncDuckDBDriver` must expose `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`.

## SQL Server Drivers on the Native Cursor

`SyncMSSQLDriver` (`_sync.py`) and `AsyncMSSQLDriver` (`_async.py`) run `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, and the `select` family on native `SyncMSSQLCursor` / `AsyncMSSQLCursor`. `cursor(*, lazy_begin: bool = False)` returns the native cursor class.

- Broker leases are acquired once in `connect()` and attached to the native client's Rust lease guard, released on `close()` or client GC drop. Query methods take no broker lease and register no statement plans.
- `MSSQLCore.rewrite_placeholders` and `rewrite_mssql_placeholders` remain in `src/py/vertebra/db/adapters/mssql/core.py` for config and SQLSpec callers.

## Spanner Drivers on the Native Cursor

`SyncSpannerDriver` (`_sync.py`), `AsyncSpannerDriver` (`_async.py`), and `SyncSpannerTransaction` / `AsyncSpannerTransaction` (`transactions.py`) run queries and DML on native `SyncSpannerCursor` / `AsyncSpannerCursor`.

- `cursor(*, staleness=None, request_tag=None, transaction_tag=None, lazy_begin=False)` delegates to the active transaction when `staleness is None` and rejects `lazy_begin=True` with `NotSupportedError`.
- `SpannerCore.is_ddl_statement` remains for `execute_script`.

### BigQuery Native Cursor Driver Path

`SyncBigQueryDriver` (`_sync.py`) and `AsyncBigQueryDriver` (`_async.py`) run queries, DML, `execute_script`, `commit`, `rollback`, and existing-session `begin` on native `SyncBigQueryCursor` / `AsyncBigQueryCursor`.

- Initial session creation in `begin()` when `self._session_id is None` calls `client.query("BEGIN TRANSACTION", None, None, True)` followed by `_after_query(res)` because `CursorQueryOptions` does not accept `create_session`. Subsequent `begin()` calls on an existing session, in-transaction queries, `commit()`, and `rollback()` run through `_execute_cursor`.
- `_run_query_sync` and `_run_query_async` are retained for the REST fallback in `select_to_arrow` and delegate session, metadata, and FinOps updates to `_after_query`.

## Spanner SQLSpec Native Cursor

`SpannerSyncDriver` and `SpannerAsyncDriver` (`src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py`) compile statements once via `compile_spanner_statement` and execute queries, DML, and `execute_many` through native cursors and cache-hit executors.

- `_SyncSpannerAsyncProxy` in `adapters/spanner/config.py` does not expose `cursor()`, so `SpannerAsyncDriver.with_cursor` wraps `connection._sync.cursor(...)` in `_ThreadedAsyncCursor` (`asyncio.to_thread`) when a sync driver is injected into `SpannerAsyncConfig`.
- Offline adapter tests verify cursor execution, `TypedParameter` encoding, `ALREADY_EXISTS` mapping, and `SyncSpannerCursor` DB-API instantiation without a live Spanner emulator.

## Related Context

- PostgreSQL operational caveats: [PostgreSQL Native Driver Limitations](postgres-limitations.md).
- Driver and cloud dialer behavior: [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md).

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
