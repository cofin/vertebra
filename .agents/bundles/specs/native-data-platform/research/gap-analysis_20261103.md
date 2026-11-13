---
type: Research
flow_id: native-data-platform
title: "Broker, native driver, and storage gap analysis"
description: "Verified gaps between baseline code and shared broker pools, standalone Rust database drivers, and complete object storage, mapped to owning child flows."
status: stable
state: promoted
promoted_to: native-data-platform
created_at: "2026-11-03T04:10:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: Shared-memory broker lease table implementation
  - id: http-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs
    title: Worker supervisor SHM broker wiring
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: Shared vertebra-db codecs and connection primitives
  - id: storage-lib
    resource: src/rs/vertebra-storage/src/lib.rs
    title: PyO3 bindings for vertebra-storage
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py
    title: Python SHM broker transport classes
  - id: sqlspec-plugin
    resource: src/py/vertebra/extensions/sqlspec/plugin.py
    title: SQLSpec plugin and proxy configuration
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py
    title: SQLSpec proxy driver wrappers
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py
    title: SQLSpec provider Arrow result surface
  - id: alchemy-plugin
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: Advanced Alchemy plugin configuration
  - id: storage-store
    resource: src/py/vertebra/storage/_store.py
    title: Python object storage facade
tags:
  - research
  - broker
  - drivers
  - storage
  - gap-analysis
---

# Gap Analysis: Shared Broker Pools, Native Drivers, Storage

<!-- truth: start -->
- **Broker Admission vs. Pool Ownership**: `ShmLeaseTable` (`vertebra-core`) and `vertebra-http` worker wiring provide slot admission and crash reclamation hooks, while cross-worker RPC pooling (`dbrpc`), `ProxySyncDriver`/`ProxyAsyncDriver`, and `BrokerPool` belong to `broker-pool-sharing` [^core-broker] [^http-factory] [^broker-transport] [^sqlspec-plugin] [^sqlspec-proxy] [^sqlspec-provider] [^alchemy-plugin].
- **Native Driver Cutovers**: Relational, OLAP, and KV engines require dedicated per-engine Rust crates (`vertebra-<engine>`) sharing `vertebra-db` primitives to eliminate Python client, `sqlite3`, and `memory://` stand-ins [^db-lib].
- **Storage Completeness Scope**: `vertebra-storage` and `vertebra.storage` require streaming reads/uploads, unified `vertebra._vertebra` GCP token caches, fail-closed constructors, and complete SQLSpec, Advanced Alchemy, Django, and Litestar contracts [^storage-lib] [^storage-store].
<!-- truth: end -->

Baseline: `main @ 142cc49`. Confidence is high unless marked otherwise. Each item names its owning child flow in brackets.

## 1. Shared broker pools

### What exists
- **Lease table.** `vertebra-core/src/broker.rs::ShmLeaseTable` is an SHM admission counter, not a pool.
  - Operations: `try_acquire`, `acquire`, `release`, `reap_expired`, `reclaim_worker`.
  - Slots carry only a `bind_key_hash` tag.
  - A small channel ring carries payloads of at most 256 B.
  - `ShmStatementCache` is a seqlock map from SQL hash to `compiled_sql` + `param_plan`, at most 224 B.
- **Supervisor wiring is real.**
  - `vertebra-http/src/workers/factory.rs` creates one table (`init_in_region_with_statements(pool_size=20, max_overflow=10, 512)`) and passes `VERTEBRA_BROKER_SHM_FD`/`SIZE` to the workers.
  - `workers/supervisor.rs` calls `reclaim_worker(pid)` when a worker dies.
  - Python attaches through `vertebra-py/src/broker.rs::LeaseBroker::from_env`.
- **Dead pool wrappers.** `vertebra-db/src/{postgres,mysql,oracle,mssql}.rs::*Pool`, `sqlite.rs::SqlitePool`, and `duckdb.rs::DuckdbBrokerPool` each wrap a private anonymous lease table. Nothing references them, and they hold no connections.

### The answer today
- Neither an upstream `sqlspec.adapters.*` config nor an AA config can share one pool across workers. Each worker owns its real connections:
  1. `ProxySyncConfig` creates a per-worker `SQLSpecProvider`.
  2. The provider calls `upstream.provide_session()`.
  3. That uses the upstream pool inside the same worker.
- `BrokerRpcRequest`/`BrokerRpcResponse` and `handle_raw_frame[_async]` are only used by tests. No ring, socket, or broker process serves them.
- AA sets `poolclass=NullPool` and `connect_args["broker"]`, which only the Vertebra DB-API accepts. With third-party URLs this raises `TypeError` (medium-high confidence).
- `alchemy/broker.py::SHMSessionBroker.attach_to_engine` is never called.

### Missing or fake
1. No broker process owns connections, and no SHM RPC ring carries DB requests. Reusable pieces: `vertebra-core/src/transport/ring_pair.rs`, `channel.rs`, `tiered_ring.rs`, `platform::WaitWord`. [broker-pool-sharing]
2. There is one global capacity (20+10) for every database and bind key; nothing partitions it per `pool_key`. [broker-pool-sharing]
3. Python `hash()` is seeded per process. `SHMBrokerTransport.acquire`/`acquire_async` use `hash(bind_key)` and `get_or_compile_statement` uses `hash((dialect_tag, raw_sql))`, so spawned workers disagree. [broker-pool-sharing]
4. The 30 s lease TTL is reaped under contention while the connection is still held, so long transactions overcommit. [broker-pool-sharing]
5. Leases are per statement. `db/postgres/driver.py::SyncPostgreSQLDriver._acquire_pg_broker_lease` (MySQL does the same) calls `broker.acquire(bind_key=...)`. That raises `TypeError` on the Rust `LeaseBroker`, so it falls back to `acquire(os.getpid())`. [broker-pool-sharing]
6. The statement cache is a no-op: every caller passes `lambda s: (s, ())`, and no server-side statement names are stored. [native-postgres-driver, broker-pool-sharing]
7. `proxy.py::ProxySyncDriver` is not a `SyncDriverAdapterBase`: no `SQLResult`, `StatementConfig`, filters, or `execute_stack`. Upstream stores, ADK, and the Litestar DI `driver_type` break. [broker-pool-sharing]
8. The provider is duck-typed:
   - `_call_broker_acquire` uses `inspect.signature`;
   - `_exec_begin_sync` falls back to `session.execute("BEGIN")`;
   - release wraps rollback in `suppress(Exception)`.

   [broker-pool-sharing]
9. There is no cursor streaming; results are fully materialized. [broker-pool-sharing]
10. AA has no broker `Pool` or `creator`. `extensions/sqlalchemy/dialects.py::DBAPIConnection.__init__` silently falls back to `SQLiteConfig(":memory:")` on any `parse_url` exception. [broker-pool-sharing, sqlalchemy-and-django-integration]
11. `db/_registry.py::DatabaseRegistry.default_broker()` and the BigQuery/Spanner drivers create a process-local `LeaseBroker` when the env FD is absent. Under uvicorn or granian, every worker gets its own table. [broker-pool-sharing]
12. Docs overclaim:
    - `docs/usage/extensions.md` (proxy configs "lease pooled connections ... from the parent process");
    - `docs/architecture/pyo3-and-workers.md` ("Rust connection pools").

    [broker-pool-sharing]

## 2. Native drivers

| Engine | Rust today | Python I/O today | Owner |
| --- | --- | --- | --- |
| Postgres | `postgres.rs`: startup, Parse/Bind/Execute/Sync/Query encoders, binary COPY rows, JSONB, pgvector, `PoolerMode`. No auth, backend decoder, SSLRequest, or socket. | `psycopg_pool` sync. Async `_ensure_async_pool` returns the asyncpg module. `memory://` fake rows. | native-postgres-driver |
| MySQL | `mysql.rs`: packet framing, `compute_caching_sha2_scramble`, COM_QUERY/STMT_PREPARE/CLOSE. No handshake, native_password, auth_switch, RSA, resultset decoding, or TLS. | `pymysql` sync, `asyncmy` async, `memory://` fakes. | native-mysql-driver |
| Oracle | `oracle.rs`: TNS connect/data, TTC piggyback, OSON, VECTOR. `tns.rs`: descriptors, tnsnames, wallets. No O5LOGON, TTC calls, or fetch. | `sqlite3.connect(":memory:")`. | native-oracle-driver |
| MSSQL | `mssql.rs`: TDS packets, `encode_tds_sql_batch`, `rewrite_mssql_placeholders`, a PreLogin enum. No PRELOGIN/LOGIN7 encoders and no token decoder. | `sqlite3.connect(":memory:")`. | native-mssql-driver |
| SQLite | `sqlite.rs`: pragmas, lease wrapper, msgpack rows. No libsqlite binding. | stdlib `sqlite3`. | native-sqlite-driver |
| DuckDB | `duckdb.rs`: bootstrap SQL, lease wrapper. The `duckdb = []` feature is empty. | `duckdb` module with a silent `sqlite3` fallback. | native-duckdb-driver |
| BigQuery / Spanner | Real I/O through `grpc.rs` (hyper HTTP/2, rustls) with known field-number bugs. | Rust. | gcp-wire-protocol-correctness |
| Redis/Valkey | `resp3.rs`: encoder, `parse_resp3_frame`, HELLO3, invalidation, CRC16 slots, redirects. Only the encoder and the slot/redirect helpers are bound. | Pure-Python sockets (`db/redis/driver.py::_create_raw_socket`, `asyncio.open_connection`) and a Python RESP parser (`db/redis/core.py::RedisCore._parse_view`). | native-redis-driver |

**Reusable pieces.**
- `grpc.rs`: rustls connector and Tokio runtime.
- `completion.rs`: once it is panic- and cancel-safe.
- `gcp_auth.rs`: IAM DB passwords.
- `dialer.rs`: Cloud SQL/AlloyDB connectors that work over any stream.
- The existing frame encoders and codecs.
- `arrow_shm.rs` and `broker.rs`.
- Crate dependencies `sha2`, `hmac`, `rsa`.

**Shared connection core.** Before any engine client: a Connection trait, stream + TLS, cancellation, a panic-safe completion bridge, and a typed error-to-Python-exception map. [db-layout-and-contract]

**Required capabilities per engine.**
- **Postgres:**
  - SSLRequest + rustls.
  - Auth: cleartext, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS.
  - Full backend decoder: RowDescription, DataRow, CommandComplete, Error/Notice fields, ParameterStatus, BackendKeyData, ReadyForQuery transaction status.
  - Named prepared statements.
  - Binary codecs: numeric, temporal, uuid, json(b), arrays, ranges, composites, enums, pgvector.
  - Multi-statement pipelining with one Sync, COPY in/out, a LISTEN/NOTIFY stream, CancelRequest, portal row streaming, logical replication for CDC, multi-host and `target_session_attrs`, IAM auth.
- **MySQL:**
  - Handshake v10, capability flags, SSL upgrade.
  - Auth: native_password, caching_sha2 fast and full (RSA or TLS), auth_switch.
  - Text and binary resultsets, STMT_PREPARE/EXECUTE, multi-results, OK/ERR with DEPRECATE_EOF, KILL QUERY cancel, pipelined execute.
- **Oracle Thin:**
  - TNS CONNECT/ACCEPT/REDIRECT/RESEND, ANO negotiation, TLS with wallet (`tns.rs::OracleWalletMaterial`).
  - TTC protocol/datatype negotiation, O5LOGON (AUTH_SESSKEY, AES, PBKDF2 for 12c verifiers).
  - OALL8 execute/fetch/describe, LOB ops, commit/rollback.
  - Types: NUMBER, DATE/TIMESTAMP(TZ), RAW, CLOB/BLOB, OSON, VECTOR.
  - Array DML, statement cache, DRCP, 23ai pipelining, OOB break/reset cancel.
  - This is the largest item. Split it into connect+O5LOGON, execute/fetch/types, LOB/OSON/VECTOR, and pipelining/DRCP.
- **MSSQL TDS 7.4:**
  - PRELOGIN encryption negotiation and TLS-in-TDS during login.
  - LOGIN7 with SQL auth (FEDAUTH/Entra is in the backlog as #68), ENVCHANGE.
  - Token stream: COLMETADATA, ROW/NBCROW, DONE*, ERROR/INFO, RETURNVALUE.
  - RPC `sp_executesql`/`sp_prepexec`, ATTENTION cancel, transaction descriptors.
  - Types: decimal, datetime2, datetimeoffset, uniqueidentifier, PLP.
- **SQLite and DuckDB:**
  - Embed `rusqlite` (bundled) and the `duckdb` crate (bundled).
  - The duckdb crate adds a heavy C++ build: several minutes and roughly 30–50 MB (medium confidence).
  - DuckDB Arrow export must pass through the Arrow C Data Interface to keep the no-arrow-rs stance.
- **Redis/Valkey:**
  - Tokio + rustls I/O, HELLO3/AUTH/IAM.
  - A multiplexed pipelined connection.
  - RESP3 push for CLIENT TRACKING.
  - Pub/sub including sharded; cluster slot map with MOVED/ASK; Sentinel.
  - Streams (XREADGROUP).

## 3. Storage

### Complete
- `vertebra-storage/src/store.rs::ObjectStoreEngine` (`object_store` 0.13: S3, GCS, Azure, HTTP, local, memory), `oci.rs::OciObjectStore`, R2 chunking.
- `signing.rs`: S3 v4, GCS v4 via signBlob, Azure SAS, OCI Cavage, HMAC.
- Real sync and async bindings.
- The sqlspec 0.69.0 `ObjectStoreProtocol` and the AA 1.12.0 `StorageBackend` abstract methods are all implemented (checked by introspection).

### Gaps [storage-completeness]
1. `storage/_store.py::ObjectStore.__init__` catches `RuntimeError`/`OSError`/`ValueError` from the native constructor and falls back to a per-process `self._memory` dict, even for `s3://`/`gs://`.
2. `memory://` defaults in:
   - `ObjectStore(url="memory://")`;
   - `extensions/sqlspec/storage.py::ObjectStoreBackend(uri="memory://")`;
   - `extensions/alchemy/storage.py::ObjectStoreBackend` when the target is None, and `register_storage_backend` for a bare key;
   - `extensions/django/storage.py::DjangoStorage` (`MemoryConfig()`);
   - `extensions/sqlspec/spanner.py` artifact store.
3. No streaming in Rust:
   - `store.rs::get` returns full `Bytes`;
   - `put_multipart(parts: Vec<Bytes>)` holds every part, and R2 re-concatenates them;
   - `list` returns a full `Vec`, with no pagination or `list_with_delimiter`;
   - no rename, conditional put, or content-type/attributes.
4. Python reimplements Rust auth. `GCSConfig._fetch_adc_token_with_expiry`/`_open_http_json` and the Python `GcpTokenCache` duplicate Rust `gcp_auth.rs`, which itself exists twice (`_vertebra`, `_storage`).
5. `extensions/sqlspec/storage.py::_iter_arrow_async` materializes every batch, and writes buffer the full payload.
6. `extensions/alchemy/storage.py::register_storage_backend` monkeypatches `storages.get`.
7. Django storage:
   - `DjangoStorage` does not subclass `django.core.files.storage.Storage`.
   - Missing: `listdir`, `path`, `get_available_name`, `generate_filename`, `get_valid_name`, `get_*_time`, `get_alternative_name`.
   - `open()` returns bytes instead of a `File`, the class is not `@deconstructible`, and `url()` ignores signing.
8. Litestar has no `litestar.stores.base.Store` over ObjectStore and no file-system adapter for static files. `storage/_streaming.py::to_fastpath_tuple` copies with `bytes(view)` and never returns 416.
9. Arrow data plane:
   - `db/_arrow.py::ArrowCursor.from_rows` writes JSON when pyarrow is absent.
   - `arrow_shm.rs::allocate_shm_region` falls back to anonymous memory.
   - `provider.py::SharedArrowBuffer` duplicates the Rust module.

## 4. Other production fakes
- **DataFusion.** `db/datafusion.py::execute_dicts` swallows DataFusion errors and re-runs the SQL on `sqlite3.connect(":memory:")`. [db-layout-and-contract]
- **AA cache.** `extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url("shm://...")` stores only `_shm_name`, and L1 is per-process. Back it with a Rust SHM slab or remove the scheme. [sqlalchemy-and-django-integration]
- **DB-API.** `DBAPIConnection` falls back to SQLite on URL parse errors. [sqlalchemy-and-django-integration]
- **Channels.** `db/channels.py` gates on a `memory://` driver and has a bare `pass`. [db-engine-primitives, deleted with `DatabaseChannelBackend`]
- **Queues, embeddings, stores.** In-memory executors, hash-vector backends, and the unbounded `KeyValueStore`. [queues-tasks-and-channels, critical-correctness-fixes]
- **Telemetry.** No stubs found (medium confidence).

## 5. Suggested ordering
1. Shared connection core (`db-layout-and-contract`).
2. `native-postgres-driver`.
3. `broker-pool-sharing`. The broker needs owned connections; Postgres proves the upstream-SQLSpec and AA paths end to end.
4. `native-redis-driver`, `native-sqlite-driver`, and `native-duckdb-driver`.
5. `native-mysql-driver` and `native-mssql-driver`.
6. `native-oracle-driver`.

The storage and remaining-fakes items run in parallel.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs` — Shared-memory broker lease table implementation.
[^http-factory]: `src/rs/vertebra-http/src/workers/factory.rs` — Worker supervisor SHM broker wiring.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — Shared vertebra-db codecs and connection primitives.
[^storage-lib]: `src/rs/vertebra-storage/src/lib.rs` — PyO3 bindings for vertebra-storage.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py` — Python SHM broker transport classes.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py` — SQLSpec plugin and proxy configuration.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py` — SQLSpec proxy driver wrappers.
[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py` — SQLSpec provider Arrow result surface.
[^alchemy-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — Advanced Alchemy plugin configuration.
[^storage-store]: `src/py/vertebra/storage/_store.py` — Python object storage facade.
