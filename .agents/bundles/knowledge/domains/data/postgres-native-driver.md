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
    at: "2026-11-13T01:52:00Z"
    commit: ecf184e4
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
    title: "Python Pg* value wrappers,PgUnchangedToast sentinel, and replication structs"
---

# PostgreSQL Native Extension (`vertebra-postgres`)

<!-- truth: start -->
- **Standalone Free-Threaded Wheel (`vertebra._postgres`)**: `src/rs/vertebra-postgres` (`#[pymodule(gil_used = false)]`, `vertebra-core[postgres]`) exports `SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `SyncPostgresPortal`, `AsyncPostgresPortal`, `SyncPostgresCopyWriter`, `AsyncPostgresCopyWriter`, `SyncPostgresCopyReader`, `AsyncPostgresCopyReader`, `SyncPostgresPipeline`, `AsyncPostgresPipeline`, and `AsyncPostgresListener` (`_postgres.pyi`), releasing the GIL on blocking calls and routing async completions through `vertebra_db::py_async` [^pg-lib-rs] [^pg-pyi].
- **Wire Protocol 3.0, SCRAM-PLUS & Cloud Dialers**: `protocol.rs` and `auth.rs` implement bounded Protocol `3.0` framing (`14`–`18`), `SSLRequest` + `rustls` (`ring`), Unix domain sockets, cleartext/MD5, and `SCRAM-SHA-256` / `SCRAM-SHA-256-PLUS` (`tls-server-end-point` SHA-256 peer certificate digest); `cloud/` caches ephemeral RSA keys, Cloud SQL `:3307` (`project:instance` SAN/CN) and AlloyDB `:5433` (`MetadataExchangeRequest`) mTLS certificates, and scoped IAM login tokens [^pg-protocol-rs] [^pg-auth-rs] [^pg-cloud-mod-rs].
- **Lossless Binary & Text Codecs**: `codecs/` decodes and encodes booleans, integers, floats, `Decimal` (`NUMERIC` `NaN`/`±Infinity`), `UUID`, `BYTEA`, `INET`/`CIDR`, UTC/naive/ambiguous-zone timestamps, `DATE`/`TIME`/`TIMETZ`, `INTERVAL`, `JSON`/`JSONB` (`0x01` header), multidimensional binary/text arrays, session-introspected `pg_type` composites/domains/ranges, and `pgvector` (`vector` `f32`, `halfvec` `f16`, `bit`) [^pg-codecs-mod-rs].
- **Pools, Portals, Single-`Sync` Pipelines & Cancellation**: `pool.rs`, `pipeline.rs`, and `portal.rs` provide LIFO physical pooling with reset-or-evict health checks, PgBouncer `transaction`/`statement` unnamed-statement gating, bounded portal batch cursors, per-command deadlines, out-of-band `CancelRequest` socket retirement, and single-`Sync` extended-query pipelines isolated in explicit or implicit savepoint scopes [^pg-pool-rs] [^pg-pipeline-rs].
- **Binary `COPY`, `VTBARROW` & Notifications**: `copy.rs` and `arrow.rs` stream `PGCOPY\n\xff\r\n\0` binary frames, materialize `arrow =60.1.0` batches into inline IPC or `VTBARROW` `ShmCapsule` `ArrowCursor` regions, and bulk-load Arrow batches via binary `COPY FROM STDIN`; `notifications.rs` runs a dedicated `LISTEN`/`UNLISTEN` pump (`AsyncPostgresListener`) [^pg-copy-rs] [^pg-arrow-rs] [^pg-notifications-rs].
- **Logical Replication (`CopyBoth` & `pgoutput` v1)**: `PgOutputDecoder` emits ordered `Begin`/`Relation`/`Insert`/`Update`/`Delete`/`Truncate`/`Message`/`Commit` records (`PgUnchangedToast`) within bounded `1..=1024` / `8 MiB` queues; `ReplicationFeedback` separates received, published, and acknowledged `Commit` end LSNs (`<= 1024` in flight); `ReplicationSession` runs concurrent reader/heartbeat tasks with cancel-safe delivery leases and single-deadline `CopyDone` shutdown; and `ReplicationSupervisor` (`supervisor.rs`) manages replacement generations over `connect_logical_verified` with generation-fenced `ReplicationEventDelivery` leases and bounded `100ms..=5s` jittered reconnect (`MAX_RAW_RECONNECT_ATTEMPTS = 8`) [^pg-pgoutput-rs] [^pg-feedback-rs] [^pg-stream-rs] [^pg-supervisor-rs] [^pg-types-py].
<!-- truth: end -->

[`src/rs/vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^pg-lib-rs] and [`_postgres.pyi`](../../../../../src/py/vertebra/_postgres.pyi)[^pg-pyi] implement Vertebra's pure-Rust PostgreSQL `14`–`18` transport crate (`postgres-protocol =0.6.12`, zero `libpq` linkage). Public Python adapter cutover (`SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`) and remaining split-state boundaries are tracked in [PostgreSQL Native Driver Limitations](postgres-limitations.md).

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

- **Binary `COPY` & Zero-Copy Arrow**: [`copy.rs`](../../../../../src/rs/vertebra-postgres/src/copy.rs)[^pg-copy-rs] drives `CopyIn` (`SyncPostgresCopyWriter` / `AsyncPostgresCopyWriter`) and `CopyOut` (`SyncPostgresCopyReader` / `AsyncPostgresCopyReader`) with explicit `CopyDone`/`CopyFail` fencing. [`arrow.rs`](../../../../../src/rs/vertebra-postgres/src/arrow.rs)[^pg-arrow-rs] builds `arrow 60` `RecordBatch`es directly from wire rows into `ShmArrowStreamWriter` (`VTBARROW` `ShmCapsule` -> `ArrowCursor`) and streams Arrow tables/batches into PostgreSQL via binary `COPY FROM STDIN`.
- **Notifications & `pgoutput` v1 Replication**: [`notifications.rs`](../../../../../src/rs/vertebra-postgres/src/notifications.rs)[^pg-notifications-rs] runs a dedicated non-pooled `AsyncPostgresListener` socket pump. [`pgoutput.rs`](../../../../../src/rs/vertebra-postgres/src/replication/pgoutput.rs)[^pg-pgoutput-rs], [`feedback.rs`](../../../../../src/rs/vertebra-postgres/src/replication/feedback.rs)[^pg-feedback-rs], [`stream.rs`](../../../../../src/rs/vertebra-postgres/src/replication/stream.rs)[^pg-stream-rs], and [`supervisor.rs`](../../../../../src/rs/vertebra-postgres/src/replication/supervisor.rs)[^pg-supervisor-rs] decode streaming v1 WAL changes incrementally without whole-transaction buffering, preserve unchanged TOAST columns (`PG_UNCHANGED_TOAST`) and raw non-UTF-8 bytes, gate `StandbyStatusUpdate` flush/apply positions strictly on published `Commit` end LSNs, restore dropped receiver leases ahead of the FIFO queue, supervise identity-verified reconnect recovery from `acknowledged_lsn`, and drain graceful `CopyDone` shutdown under one absolute deadline.

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
