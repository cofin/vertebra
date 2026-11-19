---
type: Research
flow_id: open-issues-backlog
title: Open issue backlog audit and target architecture at 6f76f8a7
description: Primary-source audit of open issues against HEAD 6f76f8a7 covering resolved storage and SQLSpec issues (#89, #99, #131) and target designs for remaining runtime, Arrow C Stream, SQLSpec pipeline, cloud IAM database auth, storage credential validation, and OKF Dataplex Knowledge Catalog deliverables (#67, #68, #100, #116, #128, #129, #130).
status: stable
state: promoted
promoted_to: open-issues-backlog
created_at: '2026-11-18T20:44:00Z'
updated_at: '2026-11-18T20:44:00Z'
generated:
  by: agent:research
  at: '2026-11-18T20:44:00Z'
verified:
  - by: agent:research
    at: '2026-11-18T20:44:00Z'
    commit: 6f76f8a7
sources:
  - id: server-loops
    resource: src/py/vertebra/server/_loops.py
    title: Event loop resolution, eager task factory, and EventLoopPool lifecycle
  - id: sync-tools
    resource: src/py/vertebra/utils/sync_tools.py
    title: Blocking async runner wrapper and thread-offload helpers
  - id: server-lanes
    resource: src/py/vertebra/server/lanes.py
    title: WorkerRunner background lane execution and task scheduling
  - id: telemetry-otel
    resource: src/py/vertebra/telemetry/otel.py
    title: W3C Trace Context, SpanExporter, and metric recorders
  - id: server-platform
    resource: src/py/vertebra/server/platform.py
    title: Cgroup sizing, lame-duck drain, and ZPagesRegistry diagnostics
  - id: rs-py-db
    resource: src/rs/vertebra-py/src/db.rs
    title: PyO3 ArrowCursor FFI bindings and cloud dialer wrappers
  - id: py-arrow
    resource: src/py/vertebra/db/arrow.py
    title: Python ArrowCursor facade and shared-memory IPC helpers
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: SQLSpec ObjectStoreBackend direct streaming and Arrow codecs
  - id: storage-store
    resource: src/rs/vertebra-storage/src/store.rs
    title: Native multicloud object store construction, digests, and verified reads
  - id: cloud-auth
    resource: src/py/vertebra/db/driver/_cloud_auth.py
    title: AWS SigV4 RDS/DSQL and Azure Entra ID database token providers
  - id: rs-db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs
    title: Native cloud database URI parsers and dialers
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs
    title: Native Google Cloud token cache and HTTP/2 client transport
---

# Open issue backlog audit and target architecture at `6f76f8a7`

<!-- truth: start -->
- Issues `#89` (fail-closed `ObjectStore` construction), `#99` (streaming SHA-256, CRC32C, and MD5 write receipts and verified reads), and `#131` (per-engine SQLSpec `connection_type` and `DatabaseConfigProtocol` parity) are implemented and tested on `main` at `6f76f8a7` and closed on GitHub [^storage-store] [^sqlspec-storage].
- `create_event_loop()` in `src/py/vertebra/server/_loops.py` attaches `_eager_task_factory` only on the stdlib `asyncio` fallback path, `_RunWrapper.__call__` in `sync_tools.py` hardcodes `uvloop` instead of `create_event_loop()`, and `EventLoopPool.stop()` skips draining and closing a running inline loop (`#129`) [^server-loops] [^sync-tools].
- `LoopBackend.ZUVLOOP` is selectable on Python 3.14+, while `vertebra.telemetry` and `ZPagesRegistry` lack an optional `opentelemetry-api` loop metric bridge, standardized `EventLoopPool` and `WorkerRunner` task names, and Python 3.14 `asyncio.capture_call_graph` task diagnostics (`#130`) [^telemetry-otel] [^server-lanes] [^server-platform].
- `vertebra._vertebra.ArrowCursor` exports `__arrow_c_stream__` via `arrow-array/ffi`, while `__arrow_c_schema__` is absent, `ArrowCursor.to_polars()` still imports `pyarrow`, and raw-bytes `ArrowCursor` instances require `pyarrow` when `_shm_cursor` is `None` (`#128`) [^rs-py-db] [^py-arrow].
- Upstream `sqlspec==0.72.2` dispatches `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` through `supports_direct_arrow_write` and `open_write_stream_*`, while `ObjectStoreBackend.write_arrow_sync(..., file_format="csv")` raises `ValueError`, `overwrite=False` is dropped on `write_arrow_*`, and the pipeline streaming benchmark and tests for `#100` are missing [^sqlspec-storage].
- AWS/Azure IAM credential chains, RDS Root CAs, cloud dialers, PG/MySQL Entra scopes, and SQLSpec/SQLAlchemy forwarding (`#67`, `#68`) remain open [^cloud-auth] [^rs-db-dialer].
- Cloud storage credential option validation and `vertebra.agent` OKF v0.2 plus Dataplex v1 Knowledge Catalog clients (`#116`) also remain open [^core-gcp-auth].
<!-- truth: end -->

## 1. Issue Triage Summary at `6f76f8a7`

| Issue | Status on `main` | Resolution or Target Scope |
| --- | --- | --- |
| `#89` | Closed (`#101`, `#102`) | Explicit target required, `_memory` fallback removed, `MissingDependencyError` and `ImproperConfigurationError` enforced; residual Azure/OCI/GCS credential option validation tracked in Chapter 2. |
| `#99` | Closed (`#126`) | Streaming Rust SHA-256, CRC32C, and MD5 `PutResult` receipts, abort-before-publish digest checks, and verified reads (`IntegrityError`) across sync/async stores and SQLSpec, Alchemy, and Django integrations. |
| `#131` | Closed (`#143`, `#144`, `#145`) | Concrete `connection_type` and `driver_type` across all 16 sync/async SQLSpec engine configs and 2 proxies satisfying `sqlspec.config.DatabaseConfigProtocol` without a base `Any` fallback. |
| `#129` | Open | Attach `_eager_task_factory` across every resolved `LoopBackend`, route `sync_tools.run_()` through `create_event_loop()`, and drain/close inline `EventLoopPool` loops consistently. |
| `#130` | Open | Optional `opentelemetry-api` loop telemetry bridge and `EventLoopRecorder`, standardized task naming across `EventLoopPool` and `WorkerRunner`, and Python 3.14+ `asyncio` call-graph diagnostics in `ZPagesRegistry`. |
| `#128` | Partial | Add `__arrow_c_schema__` (`arrow_schema` PyCapsule), lazy `StreamReader` FFI streaming, raw IPC byte native `ArrowCursor` construction, and `pyarrow`-free `to_polars()`, `to_pylist()`, and DuckDB/DataFusion consumption. |
| `#100` | Partial | Align `ObjectStoreBackend.write_arrow_*` CSV and `overwrite=False` support with `supports_direct_arrow_write`, add `SyncStoragePipeline` / `AsyncStoragePipeline` streaming and abort tests, add throughput/peak-memory microbenchmark, and update `upstream-sqlspec-direct-arrow-write.md`. |
| `#67` | Partial | Full ambient AWS credential chain (`~/.aws` profiles, ECS container URI, WebIdentity STS, EC2 IMDSv2), RDS/DSQL TTL validation, bundled AWS RDS Root CAs, `SyncAwsRdsDialer` / `AsyncAwsRdsDialer` and DSQL dialers, and SQLSpec/SQLAlchemy wiring. |
| `#68` | Partial | Azure Workload Identity (`AZURE_FEDERATED_TOKEN_FILE`) and `IDENTITY_ENDPOINT` Managed Identity, `AZURE_POSTGRES_SCOPE` / `AZURE_MYSQL_SCOPE` auto-injection on `PostgreSQLConfig` and `MySQLConfig`, `SyncAzureEntraDialer` / `AsyncAzureEntraDialer`, and SQLSpec/SQLAlchemy wiring. |
| `#116` | Open | Application-agnostic OKF v0.2 bundle builder, parser, validator, SHA-256 digester, and directory/ZIP exporter plus `SyncKnowledgeCatalogClient` and `AsyncKnowledgeCatalogClient` over native Rust HTTP/2 (`dataplex.googleapis.com/v1`) and `GcpTokenCache`. |

## 2. Server Event Loops, Telemetry, and Python 3.14 Task Diagnostics (`#129`, `#130`)

### 2.1 Event-Loop Fast-Path and Inline Drain Defects (`#129`)

1. **Fast-loop eager task execution**: `create_event_loop()` (`src/py/vertebra/server/_loops.py:110-130`) constructs backend loops via `mod.new_event_loop()` and only calls `loop.set_task_factory(_eager_task_factory)` inside `if loop is None:`. Fast event loops (`uvloop>=0.23.0`, `winloop>=0.8.1`, `rloop>=0.5.0`, `zuvloop>=0.0.17`) accept custom task factories on Python 3.12+; moving `set_task_factory(_eager_task_factory)` outside `if loop is None:` under `with suppress(AttributeError, NotImplementedError, TypeError):` activates eager coroutine start across all backends [^server-loops].
2. **Unified `sync_tools.run_()` loop selection**: `_RunWrapper.__call__` (`src/py/vertebra/utils/sync_tools.py:351-365`) checks `UVLOOP_INSTALLED` on non-Windows non-free-threaded builds and otherwise calls `asyncio.run(...)`. Replacing that branch with `asyncio.run(self._function(*args, **kwargs), loop_factory=create_event_loop)` honors `VERTEBRA_LOOP`, `GilAuditReport` quarantine, `winloop` on Windows, `zuvloop` / `rloop`, and free-threaded `uvloop>=0.23.0` [^sync-tools].
3. **Inline `EventLoopPool` task tracking and shutdown**: When `_inline_single` is `True`, `_dispatch_shard_coros` (`_loops.py:431-433`) calls `loop.run_until_complete(self._gather_coros(items))` without tracking tasks in `self._task_sets[0]`, and `stop()` (`_loops.py:386-396`) on a running inline loop only schedules `loop.call_soon_threadsafe(loop.stop)` without scheduling `self._drain_loop()` (`shutdown_asyncgens()`) or closing the loop once stopped. Scheduling a drain-then-stop callback when called from outside/inside the running inline loop and tracking tasks in `self._task_sets[0]` brings single-shard inline pools to parity with multi-shard pools [^server-loops].

### 2.2 Event-Loop OpenTelemetry Metrics and Task Introspection (`#130`)

- **Optional `opentelemetry-api` loop bridge and `EventLoopRecorder`**: Add `[project.optional-dependencies].opentelemetry = ["opentelemetry-api>=1.28.0"]` (and register `("opentelemetry", "opentelemetry")` in `tools/scripts/check_extras.py`) so `vertebra.telemetry` (`EventLoopMetricSample`, `EventLoopRecorder`, and optional `opentelemetry.metrics` meter emission) records loop scheduling lag histograms, active/pending task gauges, and `zuvloop` loop telemetry configuration with zero mandatory runtime dependencies [^telemetry-otel].
- **Standardized task naming and Python 3.14 call graphs**: Assign structured task names (`vertebra.loop.shard.{idx}.task.{seq}`, `vertebra.worker.lane.{lane}.{seq}`, `vertebra.worker.runner`) in `EventLoopPool` (`_loops.py`) and `WorkerRunner` (`lanes.py`). Expose `capture_loop_task_snapshots()` / `format_task_call_graphs()` using Python 3.14 `asyncio.capture_call_graph` and `asyncio.format_call_graph` when `hasattr(asyncio, "capture_call_graph")` is `True` (degrading cleanly to `Task.get_stack()` frames on Python 3.12–3.13) and surface task diagnostics via `ZPagesRegistry` in `src/py/vertebra/server/platform.py` [^server-lanes] [^server-platform].

## 3. Zero-PyArrow `ArrowCursor`, SQLSpec Pipeline Streaming, and Storage Fail-Closed Validation (`#128`, `#100`, `#89` Residual)

### 3.1 Native `__arrow_c_schema__` and `pyarrow`-Free IPC Consumption (`#128`)

- **Rust `vertebra-py` FFI exports (`src/rs/vertebra-py/src/db.rs`)**:
  - `ARROW_SCHEMA_CAPSULE_NAME` (`c"arrow_schema"`) is already declared in `db.rs`; add `ArrowCursor.__arrow_c_schema__(self) -> PyCapsule` exporting `FFI_ArrowSchema::try_from(reader.schema().as_ref())` with capsule destructor without decoding record batches [^rs-py-db].
  - Stream batches lazily via `StreamReader` inside `FFI_ArrowArrayStream::new(...)` instead of eagerly collecting `Vec<RecordBatch>` in `decode_arrow_ipc_reader_from_slice` [^rs-py-db].
  - Add `ArrowCursor.from_ipc_bytes(payload: &[u8])` (`Arc<[u8]>` in `ArrowBacking`), `columns`, and a pure-Rust `to_pylist()` row materializer over `arrow-array` columns [^rs-py-db].
  - Both `VTBARROW` SHM cursors and raw IPC byte cursors then expose `__arrow_c_stream__`, `__arrow_c_schema__`, `to_polars()` (`polars.from_arrow(slf)`), and `to_pylist()` without `pyarrow` [^py-arrow].
  - Keep `[arrow]` (`pyarrow`) strictly optional for `to_arrow_table()` / `to_pyarrow()` and Parquet codecs; `arrow-ipc` + `arrow-array/ffi` already linked in `vertebra-py` eliminate any need for an extra `nanoarrow` runtime dependency.

### 3.2 SQLSpec Pipeline Streaming Alignment and Benchmark (`#100`)

- Extend `normalize_arrow_format` and `_BatchWriter` in `src/py/vertebra/storage/_arrow.py` to support streaming `"csv"` (`pyarrow.csv.CSVWriter`) alongside `"parquet"` and `"arrow-ipc"`, and forward `overwrite: bool = True` (`if_not_exists=not overwrite`) through `ObjectStoreBackend.write_arrow_sync`, `write_arrow_async`, `write_arrow_cursor_sync`, and `write_arrow_cursor_async` (`src/py/vertebra/extensions/sqlspec/storage.py`) [^sqlspec-storage].
- Add end-to-end `SyncStoragePipeline` and `AsyncStoragePipeline` tests (`parquet`, `arrow-ipc`, `csv`, `overwrite=False`, mid-stream writer failure abort leaving existing object intact, and `tracemalloc` peak memory verification), add a streaming vs buffered Arrow storage write benchmark in `tools/benchmarks/micro.py`, and update `.agents/bundles/research/upstream-sqlspec-direct-arrow-write.md` to reflect `sqlspec>=0.72.2` [^sqlspec-storage].

### 3.3 Cloud Storage Credential Fail-Closed Hardening (`#89` Residual)

- In `src/rs/vertebra-storage/src/store.rs` (`BackendConfig::Azure` and `ObjectStoreEngine::from_url_with_cache`), wire `sas_token`, `tenant_id`, `client_id`, and `client_secret` onto `MicrosoftAzureBuilder` instead of dropping them, and return `StorageError::InvalidInput` when partial OCI Cavage fields (`1..=3` of `tenancy_ocid`, `user_ocid`, `fingerprint`, `private_key_pem`) are provided [^storage-store].
- In `src/py/vertebra/storage/_store.py` (`SyncObjectStore.__init__` and `AsyncObjectStore.__init__`), raise `ImproperConfigurationError` instead of suppressing `ValueError` / `GoogleCloudAuthError` / `OSError` whenever explicit GCS credential options (`service_account_path`, `service_account_key`, `application_credentials_path`) were supplied [^storage-store].

## 4. AWS SigV4 and Azure Entra ID Database Authentication and Dialers (`#67`, `#68`)

### 4.1 AWS RDS, Aurora, RDS Proxy, and Aurora DSQL (`#67`)

- **SigV4 token rules**: RDS, Aurora, and RDS Proxy sign `rds-db` with `Action=connect&DBUser=<user>` (`ttl <= 900s`); Aurora DSQL signs `dsql` with `Action=DbConnect` or `Action=DbConnectAdmin` (`DBUser` omitted, default `ttl = 900s`, max `604800s`), stripping any URL scheme before computing `Host` [^cloud-auth].
- **Ambient AWS credential chain (`_cloud_auth.py` + `vertebra-db`)**: Resolve credentials without `boto3`/`botocore` across explicit fields, `AWS_*` env vars, and shared `~/.aws/credentials` and `~/.aws/config` profiles. Fall back through Web Identity (`AWS_WEB_IDENTITY_TOKEN_FILE` STS `AssumeRoleWithWebIdentity`), ECS/EKS container endpoints (`AWS_CONTAINER_CREDENTIALS_*`), and EC2 IMDSv2 session tokens [^cloud-auth].
- **Bundled AWS RDS Root CAs and dialers**: Bundle AWS RDS Global Root CA PEMs (`rds-ca-2019`, `rds-ca-rsa2048-g1`, `rds-ca-ecc384-g1`; Aurora DSQL uses public Amazon Trust Services roots already in `webpki-root-certs`) and expose `AwsRdsDialer` / `AwsAuroraDsqlDialer` in `src/rs/vertebra-db/src/dialer.rs` (`vertebra._vertebra`) plus `SyncAwsRdsDialer`, `AsyncAwsRdsDialer`, `SyncAwsAuroraDsqlDialer`, and `AsyncAwsAuroraDsqlDialer` in `src/py/vertebra/db/dialer/aws.py` enforcing `verify-full` TLS 1.2+/1.3 and pre-expiry password token injection [^rs-db-dialer].

### 4.2 Azure Database for PostgreSQL/MySQL and Azure SQL Entra ID (`#68`)

- **Complete Entra ID credential chain**: Extend `AzureEntraTokenProvider` (`_cloud_auth.py`) with Workload Identity Federation (`AZURE_FEDERATED_TOKEN_FILE`, reading the projected JWT fresh on every refresh). Also support App Service / Container Apps Managed Identity (`IDENTITY_ENDPOINT` + `IDENTITY_HEADER`), VM IMDS, and ambient `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` fallback [^cloud-auth].
- **Default scope injection and dialers**: Auto-inject `AZURE_POSTGRES_SCOPE` (`https://ossrdbms-aad.database.windows.net/.default`) in `PostgreSQLConfig.__post_init__` and `AZURE_MYSQL_SCOPE` in `MySQLConfig.__post_init__` (matching `MSSQLConfig` and `RedisConfig`), expose `AzureEntraDialer` (`src/rs/vertebra-db/src/dialer.rs` + `SyncAzureEntraDialer` / `AsyncAzureEntraDialer` in `src/py/vertebra/db/dialer/azure.py`), and forward `token_provider` and cloud auth options across SQLSpec (`Postgres*Config`, `MySQL*Config`, `MSSQL*Config`) and SQLAlchemy (`postgresql+vtb`, `mysql+vtb`, `mssql+vtb` `create_connect_args`) [^cloud-auth] [^rs-db-dialer].

## 5. OKF v0.2 Bundle Serializer and Dataplex v1 Knowledge Catalog Client (`#116`)

### 5.1 Application-Agnostic OKF v0.2 Serializer (`src/py/vertebra/agent/okf.py`)

- Pure-Python canonical YAML frontmatter parser/serializer + Markdown body processor (`OkfDocument`, `OkfSource`, `OkfBundle`, `OkfValidationError`) with zero external `PyYAML` dependency:
  - Parses and emits canonical YAML frontmatter, extracts bounded `<!-- truth: start --> ... <!-- truth: end -->` summary blocks, and validates `sources[]` <-> `[^id]:` footnote parity.
  - Enforces Dataplex's `120,832`-byte per-entry payload ceiling, computes canonical SHA-256 digests (`64` lowercase hex chars), and exports/imports directory trees and reproducible ZIP archives.

### 5.2 Native HTTP/2 Google Cloud Knowledge Catalog Client (`src/py/vertebra/agent/knowledge_catalog.py`)

- Generalize `vertebra-core::gcp_auth::gcp_http2_post` into `gcp_http2_request` (`gcp_http2_request_sync` and `gcp_http2_request_async` in `vertebra._vertebra`, supporting `GET`, `POST`, `PATCH`, `DELETE` while preserving `gcp_http2_post_sync` / `gcp_http2_post_async`) [^core-gcp-auth].
- Implement `SyncKnowledgeCatalogClient` and `AsyncKnowledgeCatalogClient` targeting `https://dataplex.googleapis.com/v1` authenticated via `SyncGcpTokenCache` / `AsyncGcpTokenCache` (automatic 401 token-generation invalidation + retry and `x-goog-user-project` quota header):
  - `entryGroups` (`get_entry_group`, `list_entry_groups`, `create_entry_group`) with RFC-1035 `entry_group_id` validation (`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`).
  - Context-set resolution, progressive OKF `entries` sync (`create_entry`, `update_entry` with `updateMask` and `aspects`, `delete_entry`, and chunked batch sync `<= 250` entries skipping unchanged SHA-256 digests), and context-set-aware `search_entries` (`POST /v1/{name}:searchEntries`).

[^server-loops]: `src/py/vertebra/server/_loops.py` — `create_event_loop`, `EventLoopPool`, and `_eager_task_factory`.
[^sync-tools]: `src/py/vertebra/utils/sync_tools.py` — `run_` and `_RunWrapper.__call__`.
[^server-lanes]: `src/py/vertebra/server/lanes.py` — `WorkerRunner` background lane execution.
[^telemetry-otel]: `src/py/vertebra/telemetry/otel.py` — `SpanExporter`, `HistogramSnapshot`, and metric recorders.
[^server-platform]: `src/py/vertebra/server/platform.py` — `ZPagesRegistry` diagnostic endpoints.
[^rs-py-db]: `src/rs/vertebra-py/src/db.rs` — `ArrowCursor` PyO3 FFI bindings and dialers.
[^py-arrow]: `src/py/vertebra/db/arrow.py` — Python `ArrowCursor` and `VTBARROW` SHM helpers.
[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend` direct streaming and Arrow write hooks.
[^storage-store]: `src/rs/vertebra-storage/src/store.rs` and `src/py/vertebra/storage/_store.py` — multicloud store config, digests, and verified reads.
[^cloud-auth]: `src/py/vertebra/db/driver/_cloud_auth.py` — `AwsRdsIamTokenProvider`, `AwsAuroraDsqlIamTokenProvider`, and `AzureEntraTokenProvider`.
[^rs-db-dialer]: `src/rs/vertebra-db/src/dialer.rs` — native cloud database URI parsers and dialer primitives.
[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs` and `src/rs/vertebra-py/src/gcp_auth.rs` — `GcpTokenCache` and HTTP/2 client transport.
