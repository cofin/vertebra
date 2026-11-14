---
type: Concept
title: "PostgreSQL Native Extension (vertebra-postgres)"
description: "Standalone free-threaded PyO3 vertebra._postgres wheel covering Protocol 3.0 framing, SCRAM-SHA-256-PLUS, Cloud SQL and AlloyDB mTLS/IAM discovery, lossless binary codecs, physical pools, single-Sync pipelines, binary COPY, VTBARROW cursors, notifications, and pgoutput v1 replication sessions."
status: stable
tags:
  - domains
  - database
  - postgres
  - rust
  - arrow
  - replication
generated:
  by: flow-setup
  at: "2026-11-12T23:35:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-14T01:15:00Z"
    commit: 8c615b73
  - by: agent:flow-setup
    at: "2026-11-12T23:35:00Z"
    commit: 18baadcd
sources:
  - id: pg-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: "Standalone gil_used=false vertebra._postgres PyO3 module registration"
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: "Typed PyO3 surface for vertebra._postgres"
  - id: pg-protocol-rs
    resource: src/rs/vertebra-postgres/src/protocol.rs
    title: "Bounded PostgreSQL Wire Protocol 3.0 frame codec and startup negotiation"
  - id: pg-auth-rs
    resource: src/rs/vertebra-postgres/src/auth.rs
    title: "Cleartext, MD5, SCRAM-SHA-256, and SCRAM-SHA-256-PLUS tls-server-end-point channel binding"
  - id: pg-cloud-mod-rs
    resource: src/rs/vertebra-postgres/src/cloud/mod.rs
    title: "Cached Cloud SQL Admin v1beta4 and AlloyDB Admin v1beta ephemeral mTLS material and IAM tokens"
  - id: pg-codecs-mod-rs
    resource: src/rs/vertebra-postgres/src/codecs/mod.rs
    title: "Lossless scalar, numeric, temporal, interval, JSON/JSONB, array, composite, and pgvector codecs"
  - id: pg-pool-rs
    resource: src/rs/vertebra-postgres/src/pool.rs
    title: "Physical LIFO connection pool, reset/evict hygiene, and PgBouncer statement-cache gating"
  - id: pg-pipeline-rs
    resource: src/rs/vertebra-postgres/src/pipeline.rs
    title: "Single-Sync extended-query pipeline execution, transaction scopes, and out-of-band CancelRequest"
  - id: pg-copy-rs
    resource: src/rs/vertebra-postgres/src/copy.rs
    title: "Binary COPY IN / COPY OUT streaming codecs and session state machines"
  - id: pg-arrow-rs
    resource: src/rs/vertebra-postgres/src/arrow.rs
    title: "Native Arrow 60 RecordBatch builders, VTBARROW ShmCapsule cursors, and Arrow COPY import"
  - id: pg-notifications-rs
    resource: src/rs/vertebra-postgres/src/notifications.rs
    title: "Dedicated AsyncPostgresListener LISTEN/UNLISTEN notification pump"
  - id: pg-pgoutput-rs
    resource: src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder
    title: "Incremental pgoutput v1 logical decoding into bounded transaction envelopes"
  - id: pg-feedback-rs
    resource: src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback
    title: "XLogData/keepalive codecs and commit-fenced StandbyStatusUpdate tracking"
  - id: pg-stream-rs
    resource: "src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession"
    title: "Bounded single-generation CopyBoth replication pump, delivery leases, and deadline shutdown"
  - id: pg-supervisor-rs
    resource: "src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor"
    title: "Generation-fenced ReplicationSupervisor, ReplicationEventDelivery leases, and identity-verified reconnect recovery"
  - id: pg-types-py
    resource: src/py/vertebra/db/adapters/postgres/types.py
    title: "Python Pg* value wrappers, PgUnchangedToast sentinel, and replication structs"
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Public SyncPostgreSQLDriver and AsyncPostgreSQLDriver exports backed by _sync.py and _async.py"
  - id: pg-binds-py
    resource: src/py/vertebra/db/adapters/postgres/binds.py
    title: "Single-pass lexical bind compiler and immutable parameter normalizer"
  - id: pg-options-py
    resource: src/py/vertebra/db/adapters/postgres/options.py
    title: "Libpq conninfo, percent-decoded Unix socket, multi-host, and PG* env resolution"
  - id: pg-change-stream-py
    resource: src/py/vertebra/db/adapters/postgres/change_stream.py
    title: "Durable PostgresChangeSource with single-flight shielded CAS checkpoints"
  - id: pg-events-py
    resource: src/py/vertebra/db/adapters/postgres/events.py
    title: "EventSessionRegistry tracking active listeners and replication handles"
---

# PostgreSQL Native Extension (`vertebra-postgres`)

<!-- truth: start -->
- **Standalone Free-Threaded Wheel (`vertebra._postgres`)**: `src/rs/vertebra-postgres` (`#[pymodule(gil_used = false)]`, `vertebra-core[postgres]`) exports 14 classes (`SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `SyncPostgresPortal`, `AsyncPostgresPortal`, `SyncPostgresArrowStream`, `AsyncPostgresArrowStream`, `SyncPostgresCopyOut`, `SyncPostgresCopyIn`, `AsyncPostgresCopyOut`, `AsyncPostgresCopyIn`, `AsyncPostgresListener`, `AsyncPostgresReplication`), releasing the GIL on blocking calls and routing cancel-safe futures through `spawn_rust_db_task_with_attr_and_cancel` (query tasks) and `spawn_rust_db_task_with_attr_and_future` (`AsyncPostgresReplication`) over `_vertebra_postgres_completion_drainer` [^pg-lib-rs] [^pg-pyi].
- **Wire Protocol 3.0, SCRAM-PLUS & Cloud Dialers**: `protocol.rs` and `auth.rs` implement bounded Protocol `3.0` framing (`14`–`18`), `SSLRequest` + `rustls` (`ring`), Unix domain sockets, cleartext/MD5, and `SCRAM-SHA-256` / `SCRAM-SHA-256-PLUS` (`tls-server-end-point` SHA-256 peer certificate digest); `cloud/` caches ephemeral RSA keys, Cloud SQL `:3307` (`project:instance` SAN/CN) and AlloyDB `:5433` (`MetadataExchangeRequest`) mTLS certificates, and scoped IAM login tokens [^pg-protocol-rs] [^pg-auth-rs] [^pg-cloud-mod-rs].
- **Lossless Binary & Text Codecs**: `codecs/` decodes and encodes booleans, integers, floats, `Decimal` (`NUMERIC` `NaN`/`±Infinity`), `UUID`, `BYTEA`, `INET`/`CIDR`, UTC/naive/ambiguous-zone timestamps, `DATE`/`TIME`/`TIMETZ`, `INTERVAL`, `JSON`/`JSONB` (`0x01` header), multidimensional binary/text arrays, session-introspected `pg_type` composites/domains/ranges, and `pgvector` (`vector` `f32`, `halfvec` `f16`, `bit`) [^pg-codecs-mod-rs].
- **Pools, Portals, Single-`Sync` Pipelines & Cancellation**: `pool.rs`, `pipeline.rs`, and `portal.rs` provide LIFO physical pooling with reset-or-evict health checks, PgBouncer `transaction`/`statement` unnamed-statement gating, bounded portal batch cursors, per-command deadlines, out-of-band `CancelRequest` socket retirement, and single-`Sync` extended-query pipelines isolated in explicit or implicit savepoint scopes [^pg-pool-rs] [^pg-pipeline-rs].
- **Binary `COPY`, `VTBARROW`, Notifications & `pgoutput` v1 CDC**: `copy.rs` and `arrow.rs` drive `PGCOPY` streams and `VTBARROW` `ShmCapsule` cursors; `notifications.rs` pumps `AsyncPostgresListener`; `PgOutputDecoder` (`pgoutput.rs`), `ReplicationFeedback` (`feedback.rs`), `ReplicationSession` (`stream.rs`), and `ReplicationSupervisor` (`supervisor.rs`) supervise identity-verified reconnects (`100ms..=5s`, `MAX_RAW_RECONNECT_ATTEMPTS = 8`) for `AsyncPostgresReplication` [^pg-copy-rs] [^pg-arrow-rs] [^pg-notifications-rs] [^pg-pgoutput-rs] [^pg-feedback-rs] [^pg-stream-rs] [^pg-supervisor-rs] [^pg-types-py].
- **Python Drivers, Lexical Compiler & Durable Change Stream**: `adapters/postgres/driver.py` re-exports [`SyncPostgreSQLDriver`](../../../../../src/py/vertebra/db/adapters/postgres/_sync.py) and [`AsyncPostgreSQLDriver`](../../../../../src/py/vertebra/db/adapters/postgres/_async.py) (`anyio.CancelScope(shield=True)` rollback/eviction), paired with `binds.py` (single-pass `%s`/`%(name)s`/`$N` compiler), `options.py`, `EventSessionRegistry`, and `PostgresChangeSource` (single-flight CAS checkpoint before wire ack) [^pg-driver-py] [^pg-binds-py] [^pg-options-py] [^pg-change-stream-py] [^pg-events-py].
<!-- truth: end -->

[`src/rs/vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^pg-lib-rs], [`_postgres.pyi`](../../../../../src/py/vertebra/_postgres.pyi)[^pg-pyi], and [`vertebra.db.adapters.postgres`](../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) implement Vertebra's pure-Rust PostgreSQL `14`–`18` driver (`postgres-protocol =0.6.12`, wire Protocol `3.0` without `libpq`). Operational boundaries and queue executor requirements live in [PostgreSQL Native Driver Limitations](postgres-limitations.md).

## 1. Transport, Authentication & Cloud Discovery

- **Protocol 3.0 & TLS Channel Binding**: [`protocol.rs`](../../../../../src/rs/vertebra-postgres/src/protocol.rs)[^pg-protocol-rs] enforces frame length bounds before allocation, negotiates `SSLRequest` (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`) or Unix sockets, and tracks backend `ParameterStatus`, `BackendKeyData` (`process_id`, `secret_key`), and transaction state (`I`/`T`/`E`). [`auth.rs`](../../../../../src/rs/vertebra-postgres/src/auth.rs)[^pg-auth-rs] binds `SCRAM-SHA-256-PLUS` to the X.509 leaf certificate digest per RFC 5929.
- **Zero-Sidecar Cloud SQL & AlloyDB**: [`cloud/mod.rs`](../../../../../src/rs/vertebra-postgres/src/cloud/mod.rs)[^pg-cloud-mod-rs] caches ephemeral RSA keypairs, refreshes SQL Admin (`generateEphemeralCert`, `:3307`) and AlloyDB Admin (`generateClientCertificate`, `:5433` `MetadataExchangeRequest`) certificates ahead of expiry, and injects scoped OAuth2 IAM database tokens on direct TCP and mTLS paths.

## 2. Codecs, Extended Query, Pools & Pipelines

| Subsystem | Key Modules & Invariants |
| --- | --- |
| **Type Catalog & Codecs** | [`codecs/mod.rs`](../../../../../src/rs/vertebra-postgres/src/codecs/mod.rs)[^pg-codecs-mod-rs] and [`types.py`](../../../../../src/py/vertebra/db/adapters/postgres/types.py)[^pg-types-py] map static built-in OIDs and session-cached dynamic OIDs (`pg_type` / `pg_attribute`) across binary and text formats, preserving exact base-10000 `NUMERIC` scale, `JSONB` `0x01` prefix stripping, `1`-based/custom lower-bound arrays, nested composites, and `pgvector` binary frames. |
| **Pools, Portals & Deadlines** | [`pool.rs`](../../../../../src/rs/vertebra-postgres/src/pool.rs)[^pg-pool-rs] manages bounded physical sessions, resets tainted state (`DISCARD ALL` / `ROLLBACK`) or evicts broken sockets on release, disables named prepared statements under PgBouncer `transaction`/`statement` modes unless `protocol_level_pooler_cache` is set, and pairs per-command deadlines with out-of-band `CancelRequest` sockets. |
| **Single-`Sync` Pipelines** | [`pipeline.rs`](../../../../../src/rs/vertebra-postgres/src/pipeline.rs)[^pg-pipeline-rs] flushes queued `Parse`/`Bind`/`Describe`/`Execute` batches in a single trailing `Sync` message, isolating failures inside ambient transactions or automatic savepoints and retiring desynchronized connections on protocol abort. |

## 3. Binary `COPY`, `VTBARROW`, Notifications & Logical Replication

- **Binary `COPY` & Zero-Copy Arrow**: [`copy.rs`](../../../../../src/rs/vertebra-postgres/src/copy.rs)[^pg-copy-rs] drives `CopyIn` (`SyncPostgresCopyIn` / `AsyncPostgresCopyIn`) and `CopyOut` (`SyncPostgresCopyOut` / `AsyncPostgresCopyOut`) with explicit `CopyDone`/`CopyFail` fencing. [`arrow.rs`](../../../../../src/rs/vertebra-postgres/src/arrow.rs)[^pg-arrow-rs] builds `arrow 60` `RecordBatch`es directly from wire rows into `ShmArrowStreamWriter` (`VTBARROW` `ShmCapsule` -> `ArrowCursor`) and streams Arrow tables/batches into PostgreSQL via binary `COPY FROM STDIN`.
- **Notifications, `pgoutput` v1 Replication & Durable `PostgresChangeSource`**: [`notifications.rs`](../../../../../src/rs/vertebra-postgres/src/notifications.rs)[^pg-notifications-rs] runs a dedicated non-pooled `AsyncPostgresListener` socket pump. [`pgoutput.rs`](../../../../../src/rs/vertebra-postgres/src/replication/pgoutput.rs)[^pg-pgoutput-rs], [`feedback.rs`](../../../../../src/rs/vertebra-postgres/src/replication/feedback.rs)[^pg-feedback-rs], [`stream.rs`](../../../../../src/rs/vertebra-postgres/src/replication/stream.rs)[^pg-stream-rs], and [`supervisor.rs`](../../../../../src/rs/vertebra-postgres/src/replication/supervisor.rs)[^pg-supervisor-rs] decode streaming v1 WAL changes incrementally (`PgUnchangedToast`, raw non-UTF-8 `bytea`), gate `StandbyStatusUpdate` on published `Commit` end LSNs, supervise verified reconnects, and feed [`PostgresChangeSource`](../../../../../src/py/vertebra/db/adapters/postgres/change_stream.py)[^pg-change-stream-py] (`PostgresCheckpointStore` / `MemoryPostgresCheckpointStore` CAS persistence before `acknowledge(lsn)`, with `EventSessionRegistry`[^pg-events-py] closing active handles on driver shutdown).

## 4. Python Driver Layer (`_sync.py`, `_async.py`, `binds.py`, `options.py`)

- **Sync & Async Drivers ([`_sync.py`](../../../../../src/py/vertebra/db/adapters/postgres/_sync.py), [`_async.py`](../../../../../src/py/vertebra/db/adapters/postgres/_async.py))**: [`driver.py`](../../../../../src/py/vertebra/db/adapters/postgres/driver.py)[^pg-driver-py] re-exports `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`, executing directly over `vertebra._postgres` (`SyncPostgresPool` / `AsyncPostgresPool`), `_project_public_row` / `hydrate_rows` projection, shielded rollback/eviction (`anyio.CancelScope(shield=True)`), `SyncPostgresPipeline` / `AsyncPostgresPipeline`, `VTBARROW` `ArrowCursor` + `load_from_arrow`, and `ChangeFeed` hooks.
- **Single-Pass Lexer & Libpq Options ([`binds.py`](../../../../../src/py/vertebra/db/adapters/postgres/binds.py)[^pg-binds-py], [`options.py`](../../../../../src/py/vertebra/db/adapters/postgres/options.py)[^pg-options-py])**: `compile_postgres_binds` translates `%s`, `%(name)s`, and `$N` placeholders while preserving single/double quotes, `E'...'`, `$tag$...$tag$`, comments, `::` casts, and `?`/`?|`/`?&` JSONB operators; `build_native_client_options` resolves libpq `conninfo`, percent-decoded Unix sockets preserving `+`, multi-host failover lists, and `PGUSER`/`PGDATABASE`/`PGPASSWORD`/`PGHOST`/`PGPORT` precedence.

[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `gil_used=false` `vertebra._postgres` PyO3 module registration.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Typed PyO3 surface for `vertebra._postgres`.
[^pg-protocol-rs]: `src/rs/vertebra-postgres/src/protocol.rs` — Bounded PostgreSQL Wire Protocol 3.0 frame codec and startup negotiation.
[^pg-auth-rs]: `src/rs/vertebra-postgres/src/auth.rs` — Cleartext, MD5, SCRAM-SHA-256, and SCRAM-SHA-256-PLUS channel binding.
[^pg-cloud-mod-rs]: `src/rs/vertebra-postgres/src/cloud/mod.rs` — Cached Cloud SQL and AlloyDB ephemeral mTLS material and IAM tokens.
[^pg-codecs-mod-rs]: `src/rs/vertebra-postgres/src/codecs/mod.rs` — Lossless scalar, numeric, temporal, JSON/JSONB, array, composite, and pgvector codecs.
[^pg-pool-rs]: `src/rs/vertebra-postgres/src/pool.rs` — Physical LIFO connection pool, reset/evict hygiene, and PgBouncer statement-cache gating.
[^pg-pipeline-rs]: `src/rs/vertebra-postgres/src/pipeline.rs` — Single-`Sync` extended-query pipeline execution, transaction scopes, and cancellation.
[^pg-copy-rs]: `src/rs/vertebra-postgres/src/copy.rs` — Binary `COPY IN` / `COPY OUT` streaming codecs and session state machines.
[^pg-arrow-rs]: `src/rs/vertebra-postgres/src/arrow.rs` — Native Arrow 60 `RecordBatch` builders, `VTBARROW` `ShmCapsule` cursors, and Arrow `COPY` import.
[^pg-notifications-rs]: `src/rs/vertebra-postgres/src/notifications.rs` — Dedicated `AsyncPostgresListener` `LISTEN`/`UNLISTEN` notification pump.
[^pg-pgoutput-rs]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental `pgoutput` v1 logical decoding into bounded transaction envelopes.
[^pg-feedback-rs]: `src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback` — `XLogData`/keepalive codecs and commit-fenced `StandbyStatusUpdate` tracking.
[^pg-stream-rs]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded single-generation `CopyBoth` replication pump, delivery leases, and deadline shutdown.
[^pg-supervisor-rs]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Generation-fenced `ReplicationSupervisor`, `ReplicationEventDelivery` leases, and identity-verified reconnect recovery.
[^pg-types-py]: `src/py/vertebra/db/adapters/postgres/types.py` — Python `Pg*` value wrappers, `PgUnchangedToast` sentinel, and replication structs.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py` — Public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` exports.
[^pg-binds-py]: `src/py/vertebra/db/adapters/postgres/binds.py` — Single-pass lexical bind compiler and immutable parameter normalizer.
[^pg-options-py]: `src/py/vertebra/db/adapters/postgres/options.py` — Libpq conninfo, Unix socket, multi-host, and `PG*` resolution.
[^pg-change-stream-py]: `src/py/vertebra/db/adapters/postgres/change_stream.py` — Durable `PostgresChangeSource` with single-flight shielded CAS checkpoints.
[^pg-events-py]: `src/py/vertebra/db/adapters/postgres/events.py` — `EventSessionRegistry` tracking active listeners and replication handles.
