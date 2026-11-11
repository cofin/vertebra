---
type: Spec
flow_id: native-postgres-driver
title: Complete Standalone Native PostgreSQL Driver
description: One complete PR implementing native PostgreSQL transport/authentication, typed queries and pools, COPY/Arrow, pipelines/cancellation, notifications/replication, production adapter and queue cutover, and complete 33-file documentation/OKF sync.
status: stable
state: active
has_chapters: true
chapters:
  - ch01-wheel-scaffold-and-config
  - ch02-wire-codec-and-session-startup
  - ch03-scram-and-channel-binding
  - ch04-cloud-material-and-admin-discovery
  - ch05-cloud-tls-and-scoped-iam
  - ch06-alloydb-keepalive-unix-and-direct-iam
  - ch07-async-bridge-and-client-sessions
  - ch08-constructor-parity-and-wheel-gil
  - ch09-owned-results-and-numeric-codecs
  - ch10-temporal-interval-and-json-codecs
  - ch11-type-catalog-arrays-and-composites
  - ch12-pgvector-and-extended-query
  - ch13-transactions-portals-and-statement-cache
  - ch14-physical-pools-and-query-facades
  - ch15-pool-facades-and-single-sync-pipeline
  - ch16-pipeline-scope-and-cancellation
  - ch17-command-deadlines-portals-and-tz-fallback
  - ch18-binary-copy-streaming
  - ch19-native-arrow-batches-and-shm
  - ch20-arrow-copy-import-and-portals
  - ch21-live-notifications
  - ch22-copyboth-and-pgoutput-decoding
  - ch23-replication-feedback-resume-and-bridge
  - ch24-change-source-and-checkpoints
  - ch25-dsn-bridge-lexer-and-permanent-drivers
  - ch26-compiler-pipeline-arrow-and-event-hooks
  - ch27-atomic-cutover-and-legacy-removal
  - ch28-live-conformance-and-service-acceptance
  - ch29-wheel-ci-and-benchmarks
  - ch30-user-and-workflow-docs
  - ch31-data-and-testing-okf-sync
  - ch32-ffi-okf-and-review-routing-sync
  - ch33-final-review-and-release-gate
current_chapter: ch21-live-notifications
current_task: null
parent_prd: native-data-platform
depends_on_flows:
  - driver-fail-loud
research:
  - research/protocol-and-execution_20261105.md
  - research/cloud-material_20261105.md
plan_revision: 6
plan_commit: ef86844f
state_revision: 5
last_verified_checkpoint: 2a575ce3
last_reviewed_chapter: ch20-arrow-copy-import-and-portals
last_reviewed_commit: e5366213
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-10T23:44:00Z"
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: ef86844f
sources:
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TCP, Unix, TLS, and cloud mTLS stream primitives
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame
    title: Shared stateless PostgreSQL wire frame and codec helpers in vertebra-db
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Upstream flow that deletes PostgreSQL memory:// paths and vertebra_db::postgres::PostgresPool
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue and independent event-loop drainer
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Shared-memory Arrow IPC stream writer for VTBARROW capsules
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python SyncPostgreSQLDriver and AsyncPostgreSQLDriver target cutover module
  - id: sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver
    title: SQLSpec stack execution and PostgreSQL adapter integration seam
  - id: db-arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor wrapper over inline IPC and VTBARROW ShmCapsule
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix and pipeline suite
  - id: dialer-cloudsql-py
    resource: src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig
    title: Existing Python Cloud SQL dialer configuration and ephemeral certificate helper
  - id: dialer-alloydb-py
    resource: src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig
    title: Existing Python AlloyDB dialer configuration and MetadataExchange helper
tags:
  - rust
  - postgres
  - driver
  - wheel
  - pipeline
  - one-pr
---

# Specification: Complete Standalone Native PostgreSQL Driver

<!-- truth: start -->
- **Standalone Optional Wheel**: `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) owns live PostgreSQL Protocol 3.0 sockets over `postgres-protocol =0.6.12` and `rustls` (`ring`) without linking database clients into `vertebra-core` [^pg-conn-rs] [^pg-core-rs].
- **Complete Single-PR Delivery**: Authentication (`SCRAM-SHA-256`, `SCRAM-SHA-256-PLUS`, Cloud SQL / AlloyDB Admin discovery + mTLS + IAM), lossless codecs, statement caching, single-`Sync` `pipeline()`, binary `COPY`, native `VTBARROW`, `CancelRequest`, `LISTEN`/`NOTIFY`, `pgoutput` v1 logical replication, and `PostgreSQLQueueBackend` dependency-guard cutover ship together with full `psycopg`/`asyncpg` runtime removal [^py-async-rs] [^arrow-shm-rs] [^pg-driver-py] [^sqlspec-py].
- **Fail-Loud Baseline**: `driver-fail-loud` lands first (`88c30004`) and deletes `_in_memory_rows`, every PostgreSQL `memory://` branch, and `vertebra_db::postgres::PostgresPool`; this flow starts from that baseline and adds no memory or SHM-admission pool paths [^fail-loud-baseline].
- **Nested Chapter Roadmap (`plan_revision: 6`)**: Decomposed into `33` dependency-ordered chapters (`ch01`–`ch33`, `118` vertical-slice tasks, each owning `<= 3` files) anchored at `plan_commit: ef86844f`, pairing implementation chapters (`ch01`–`ch29`) with `33`-file documentation, OKF, and review-routing synchronization (`ch30`–`ch32`) and full release gates (`ch33`); live execution and documentation progress are tracked in frontmatter and Sections 3, 5, and 6.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Upstream Execution Baseline

The governing scope rule is: **“Keep the complete driver in one PR.”** All `33` chapters (`118` worksheets) execute on `feat/native-postgres-driver`, stacked above `feat/driver-fail-loud` (`88c30004`), `feat/storage-updates` (`d2b168dc`), and `main @ 077a3fe9`. The database foundation ([PR #76](https://github.com/cofin/vertebra/pull/76), `21c99f95`), engine primitives ([PR #77](https://github.com/cofin/vertebra/pull/77), `80ff8a65`), post-merge review remediation ([PR #80](https://github.com/cofin/vertebra/pull/80), `2b737240`), runtime structure ([PR #92](https://github.com/cofin/vertebra/pull/92), `d2fa5072`), and MCP/WebMCP core ([PR #94](https://github.com/cofin/vertebra/pull/94), `077a3fe9`) are squash-merged on `main`; those prior PRs do not validate this driver.

The complete charter decomposes into `33` nested chapters ([chapters/index.md](chapters/index.md)) so every worksheet remains bounded (`<= 3` owned files, `<= 250` estimated LOC) and produces one signed local conventional commit without splitting delivery across partial PRs. No chapter constitutes a partial release; atomic Python adapter and queue cutover (`ch27`), strict zero-skip PG14–18 and live-service conformance (`ch28`), outside-checkout wheel and benchmark gates (`ch29`), `33`-file public documentation, OKF knowledge, and review-routing synchronization (`ch30`–`ch32`), and routed correctness plus fresh quality reviews (`ch33`) are required before `/flow:finish`. Hosted writes require separate explicit user permission.

This flow depends on [`driver-fail-loud`](../../knowledge/domains/data/database-drivers-and-dialers.md) (`88c30004`), which deletes `_in_memory_rows` and every `memory://` branch from `src/py/vertebra/db/adapters/postgres/driver.py` (a `memory://` DSN raises `ImproperConfigurationError`) and deletes `vertebra_db::postgres::PostgresPool` with its re-exports [^fail-loud-baseline]. Chapters `ch14`, `ch22`, `ch27`, and `ch30`–`ch32` start from that baseline; the `postgres` extra stays owned by `ch08:t02`.

### 1.2 Non-Goals

- Splitting transport, query, `COPY`/Arrow, replication, or adapter cutover across multiple pull requests or leaving `psycopg`/`asyncpg` fallbacks behind feature flags.
- Negotiating PostgreSQL wire protocol `3.2` (Protocol `3.0` governs the entire PostgreSQL `14`–`18` support matrix).
- Implicitly creating or dropping PostgreSQL replication slots/publications, performing automatic initial table snapshot backfills, or advancing WAL `confirmed_flush_lsn` on stream yield before durable checkpoint persistence.
- Adding engine-specific PostgreSQL imports, SQL strings, or class branches to generic `src/py/vertebra/db/*.py` or `src/py/vertebra/db/driver/*.py` modules.
- Requiring live external Google Cloud SQL or AlloyDB network credentials during local or default CI runs (cloud Admin, mTLS, and IAM paths are proven via deterministic wire/TLS harnesses).

### 1.3 Code Analysis Summary

| Existing symbol | Verified consequence |
| --- | --- |
| `src/rs/vertebra-db/src/connection.rs::TransportStream` [^pg-conn-rs] | Reuse transport/TLS. Direct PostgreSQL sends `SSLRequest` before upgrade; cloud proxy endpoints negotiate immediate mTLS. |
| `src/rs/vertebra-db/src/connection.rs::{Connection,CancelHandle}` [^pg-conn-rs] | Lifecycle traits require real ping/cancel/close implementations. No placeholder success. |
| `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` [^pg-core-rs] | Stateless frame, `JSONB`, `PGCOPY`, and vector helpers only; `vertebra-db` holds no PostgreSQL pool type after `driver-fail-loud` [^fail-loud-baseline], so live connections and server prepared statements belong to `vertebra-postgres`. |
| `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` [^py-async-rs] | Reuse completion queues and independent drainer with fallible-setup abort ownership and `PendingDeliveryGuard` stream cancellation safety. |
| `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` [^arrow-shm-rs] | Accepts serialized IPC; does not build columns. Its padding API cannot receive arbitrary encoder fragments. |
| `src/py/vertebra/db/adapters/postgres/driver.py::{SyncPostgreSQLDriver,AsyncPostgreSQLDriver}` [^pg-driver-py] | Replace Python-client and duck-typed execution with real native pool leases (`vertebra-core[postgres]`; memory execution is absent from `driver-fail-loud` [^fail-loud-baseline]). Preserve count/tuple/description/hydration contracts. |
| `src/py/vertebra/extensions/sqlspec/adapters.py::{BaseSyncDriver.execute_stack,BaseAsyncDriver.execute_stack}` [^sqlspec-py] | Caller conversion and strict result validation occur inside pipeline context; results cannot commit early, and `select_to_arrow` remains Liskov-compliant. |
| `src/py/vertebra/db/arrow.py::ArrowCursor` [^db-arrow-py] | Public cursor wraps native SHM output; inline IPC tracks explicit native row-count metadata without Python rows. |
| `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` [^conformance-py] | PostgreSQL uses native type DDL, enabled pipeline acceptance, and strict zero-skip Docker conformance (`VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`). |
| `src/py/vertebra/db/dialer/cloudsql.py` [^dialer-cloudsql-py], `src/py/vertebra/db/dialer/alloydb.py` [^dialer-alloydb-py] | Existing helpers do not complete real discovery/issued-material lifecycle. Native cloud support includes actual Admin APIs and login scopes. |

### 1.4 Relevant OKF Knowledge, Research & Glossary

Read [database-drivers-and-dialers.md](../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [adr-004-optional-driver-wheels-and-shm-ownership.md](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [ffi-and-gil-discipline.md](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [pyo3-and-python-runtime.md](../../knowledge/gotchas/pyo3-and-python-runtime.md), [protocol-and-execution_20261105.md](research/protocol-and-execution_20261105.md), [cloud-material_20261105.md](research/cloud-material_20261105.md), [contracts.md](contracts.md), [review.md](review.md), and the project-local [pyo3-bridge](../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../skills/runtime-ipc/SKILL.md), [python-framework](../../../skills/python-framework/SKILL.md), and routed review skills.

`postgres-protocol = "=0.6.12"` owns low-level message framing and SCRAM helpers; `src/rs/vertebra-postgres` owns live connection state. `tokio-postgres = "=0.7.18"` is a dev-only test oracle. Protocol `3.0` supports PostgreSQL `14`–`18`. Arrow crates are pinned to `60.1.0`; cross-extension transfer uses producer-owned `ShmCapsule` callbacks, never Rust `Arc` or trait-object layouts across `cdylib` boundaries. A **physical session** is one socket; a **lease** exclusively owns that session; a **portal** pins its lease and transaction; a **checkpoint** records durably acknowledged logical replication progress.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements (`R1`–`R27`)

The [public contracts](contracts.md) freeze constructor options, pool ownership, result/bind shapes, pipeline completion, lossless value policy, Arrow selection/import, event ownership, and checkpointing. Engine values, SQL, tool contracts, and checkpoint protocols stay inside `vertebra-postgres` and `vertebra.db.adapters.postgres`; shared `vertebra-db` and `vertebra.db` code remains engine-agnostic.

- **R1.** Optional standalone wheel, package/build/release registration, and typed exports.
- **R2.** Pure validated connection options, one URL/libpq parser, and lexical bind lowering.
- **R3.** Bounded protocol framing, real TCP/Unix startup, metadata, and total connection deadline.
- **R4.** Direct `SSLRequest`/TLS, verified certificate binding, and service-specific cloud identity.
- **R5.** Trust/password/MD5/SCRAM/PLUS and scope-correct direct/cloud IAM authentication.
- **R6.** Native synchronous client/query/pool surfaces with detached blocking I/O.
- **R7.** Native asynchronous completion, setup-abort ownership, and cancellation-safe publication.
- **R8.** Complete-driver gates and correctness review followed by fresh quality review.
- **R9.** Automatic Cloud SQL/AlloyDB discovery, issued material, expiry, and refresh.
- **R10.** Owned descriptions/results, affected command counts, and exact core SQLSTATE errors.
- **R11.** Lossless scalar/numeric/temporal/JSON/container/vector values and unknown-type formats.
- **R12.** Simple multi-results and extended single-statement query execution.
- **R13.** Context-owned transactions/savepoints and exclusive physical-session affinity.
- **R14.** Real physical pools with bounded acquisition, reset, shutdown, and configuration isolation.
- **R15.** Per-connection prepared LRU and verified/configured pooler capabilities.
- **R16.** Bounded portals and sync/async stream ownership through exhaustion or abandonment.
- **R17.** Cold/warm one-`Sync`/one-flush pipeline with ordered outcomes and delayed context commit.
- **R18.** Backend-key cancellation, server cleanup, and protection against delayed cross-borrower cancel.
- **R19.** Incremental binary `COPY` input/output and exclusive-mode error recovery.
- **R20.** Native typed Arrow builders/import, exact IPC, threshold/count, and producer-owned capsule lifetime.
- **R21.** Dedicated live notifications, subscription changes, bounded delivery, and cleanup.
- **R22.** Dedicated `pgoutput`-v1 logical replication with relation/type state and independent feedback.
- **R23.** Adapter-owned checkpoint store, stable `int64` replay IDs, and explicit durable-before-feedback ack.
- **R24.** Native Python adapter, queue backend, and SQLSpec/SQLAlchemy/helper cutover without old execution fallbacks.
- **R25.** Strict PG14–18 native conformance and real `COPY`/Arrow/events/vector/pooler acceptance.
- **R26.** Installed-wheel isolation, namespace/exception identity, and actual free-threaded GIL checks.
- **R27.** Driver-specific performance evidence, public/OKF/skill documentation, and canonical verification.

### 2.2 Non-Functional, Protocol & Safety Constraints

- Every native module declares `gil_used = false`; synchronous network waits detach the GIL (`py.detach(...)`), async workers carry owned Rust data and never attach Python, standard-GIL runtimes verify `FREE_THREADED is _is_free_threaded_runtime()`, and isolated child subprocesses verify actual disabled-GIL import and concurrent query behavior on Python `3.14t` and `3.15t`.
- One bounded coalesced `read_buf` framing implementation handles standard messages while standalone `replication=database` sessions enable `CopyBoth`. Lengths, counts, and arity are validated before allocation; resource limits fail with typed errors rather than silent truncation.
- One socket owner services duplex backpressure. Abandoned portals, `COPY` / `copy_from_arrow`, pipeline failures, and cancellations drain to a synchronized protocol boundary or discard the physical socket, while async notification and replication `next()` futures restore popped records on drop via `PendingDeliveryGuard`. Any connection targeted by a possibly sent `CancelRequest` retires permanently before another borrower can acquire it.
- Pool configuration identity includes credentials, TLS, session GUCs, cloud resource/network, IAM options, and preparation policy (`PoolerMode`, `protocol_level_pooler_cache`, `statement_cache_size`). Ordinary driver calls acquire transient leases; transaction, portal, and pipeline scopes pin them with thread/task identity checks, and `DISCARD ALL` resets invalidate prepared statements while retaining `'Y'` `pg_type` catalog entries.
- Cold pipelines use text formats with received descriptors while warm known OIDs use binary and unknown OIDs fall back to text, emitting exactly one `Sync` and one logical flush per batch (without inserting hidden discovery round trips or claiming one TCP packet).
- `Decimal` scale/special values, array lower bounds and `NULL` dimensions, composite attribute order, independent `PgInterval` `(months, days, microseconds)`, and `PgTemporal` ranges remain lossless; ambiguous display `wire_text` is preserved on output and rejected with `DataError` before any typed rebind or binary `COPY`.
- Native Arrow builders, raw client `copy_from_arrow(table_name, first_batch_ipc, *, columns=None, schema_name=None)` writers (`write_arrow_ipc`/`finish`/`close`/`aclose`), and Python driver `load_from_arrow(table_name, data, *, columns=None, schema_name=None)` streaming import remain bounded, honor inline vs `VTBARROW` SHM row thresholds, preserve nested types and OID metadata, and claim zero-copy exclusively for `ShmCapsule` adoption.
- Notifications and logical replication run on dedicated direct sessions (`direct_dsn` when queries traverse a pooler) tracked via `EventSessionRegistry` (`await client.close()`). `pgoutput` v1 tails/resumes caller-owned slots and publications in `vertebra.db.adapters.postgres.change_stream` with explicit CAS checkpoint persistence before WAL feedback. Zero third-party Python database clients, SQLite stand-ins, memory executors, or sync offload fallbacks remain in PostgreSQL production paths (`[dependency-groups].postgres-bench` isolates `python_version == '3.12'` benchmark comparators only).

### 2.3 Cross-Chapter Architecture & Seam Topology

```mermaid
flowchart LR
    Core["vertebra-core (GcpTokenCache, ShmCapsule)"] --> Db["vertebra-db (TransportStream, py_async, ShmArrowStreamWriter)"]
    Db --> PgRs["vertebra-postgres (vertebra._postgres wheel: ch01-ch23)"]
    PgRs --> PyAdapters["vertebra.db.adapters.postgres (config, binds, types, events, change_stream, notify, drivers: ch24-ch27)"]
    PyAdapters --> Consumers["SQLSpec + SQLAlchemy +vtb + Queues + Embeddings (ch26-ch27)"]
    Consumers --> Gates["Live PG14-18 / pgvector / PgBouncer, Wheel Isolation, Benchmarks, Docs/OKF & Final Review (ch28-ch33)"]
```

### 2.4 Acceptance Scenarios

| Given | When | Then |
| --- | --- | --- |
| Direct/TLS/Unix or cloud resource config | Both native clients connect | Auth/UTF8/Ready is real; wrong identity/proof/order/expiry fails and closes. |
| Scoped ADC credentials and expiring issued certificate | Concurrent cloud opens | Correct Admin/login scopes and single-flight renewal; no localhost or invented expiry. |
| Full scalar/container values, typed `NULL`, and unknown type | Query/bind/`COPY` | Lossless values and correct type formats; no binary-as-UTF8 or float `Decimal` conversion. |
| Shared driver and saturated native pool | Concurrent calls, nested transaction, or cancelled acquire | Exclusive real leases, bounded capacity, isolated scope, and no dirty reuse. |
| Cold/warm mixed-return pipeline | Middle `Parse`/`Bind`/`Execute` fails or caller conversion raises | One `Sync`/flush batch; ordered failure/skipped outcomes; transaction rolls back. |
| Active query or `COPY`/portal/event stream | Cancel/abandon/close | Original failure preserved; server work cancelled or undelivered event restored; lease recovered or retired. |
| Empty, small, large, or nested Arrow result/input | `select_to_arrow`, batch portal, raw `copy_from_arrow`, or driver `load_from_arrow` | Metadata/counts survive; threshold/schema applies; native IPC/capsule ownership and `CopyFail` abort stay valid. |
| Caller-owned slot/publication and saved checkpoint | Tail, reconnect, and acknowledge | Ordered at-least-once events, stable replay IDs, and durable-store-before-feedback. |
| Native PostgreSQL 14–18 and SCRAM `PgBouncer` modes | Driver/SQLSpec/SQLAlchemy acceptance (`make postgres-test`) | Actual native transactions, `$n`/`numeric_dollar` binds, pools, and pipelines pass with zero skips or xfails. |
| Installed optional wheel outside checkout | Import/query/uninstall across supported and free-threaded runtimes | Core identity and namespace survive; six stubs and `FREE_THREADED` / disabled-GIL states match. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen seam and alternative |
| --- | --- |
| Delivery | One complete PR follows the user instruction. Partial connection PRs are excluded; atomic commits remain review boundaries. |
| Protocol | Owned state machine over `postgres-protocol` permits single-`Sync` batches and standalone `CopyBoth`. A production `tokio-postgres` `Client` would own `Sync` behavior and prevent that contract. |
| Integration ownership | `vertebra-postgres` and `vertebra.db.adapters.postgres` own native SQL, values, cloud material, and checkpoint protocols while `PostgreSQLQueueBackend` guards missing `executor=` on `vertebra._postgres` (`vertebra-core[postgres]`) and remains strictly caller-`PostgreSQLQueueExecutor`-injected (built-in native queue executor shipping belongs to `queues-tasks-and-channels`). Feature-owned vendor packages would split one engine across consumers. |
| Configuration | Explicit typed native kwargs and one adapter DSN/bind compiler keep validation local. Generic mapping forwarding or duplicate parsers leave runtime decisions unresolved. |
| Results | Owned engine result/column/value records preserve metadata (`PostgresReplicationRecord` omits `gc=False` safely for container fields); Python base-driver projections retain established shapes. |
| Pooling | Real Rust pool leases separate session ownership from SHM admission. Reusing admission counters as a pool would not manage sockets. |
| Error policy | Raise the first canonical pipeline error with ordered outcomes. Exception rows or fabricated errors for skipped operations misstate server execution. |
| CDC | Explicit adapter-owned checkpoint store/ack in `vertebra.db.adapters.postgres.change_stream` preserves the generic stream contract. Ack-on-yield would falsely treat delivery as durable application. |
| Arrow | Native typed builders, raw client `copy_from_arrow` / Python driver `load_from_arrow`, and IPC-to-SHM preserve bounded row processing. Python row materialization and arbitrary padded IPC fragments violate that path. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/33 files documented]` across [ch30-user-and-workflow-docs](chapters/ch30-user-and-workflow-docs/spec.md) (`12` files, `ch30:t01`–`t04`), [ch31-data-and-testing-okf-sync](chapters/ch31-data-and-testing-okf-sync/spec.md) (`12` files, `ch31:t01`–`t04`), and [ch32-ffi-okf-and-review-routing-sync](chapters/ch32-ffi-okf-and-review-routing-sync/spec.md) (`9` files, `ch32:t01`–`t03`).

- `docs/usage/databases.md` (`ch30:t01`) — Linked `uv`/`pip`/`pdm`/`poetry` `[postgres]` tabs, `$n` vs `E'...'` pyformat lowering, SQLSpec `NUMERIC` and SQLAlchemy `numeric_dollar`, `query_multi`, single-`Sync` `pipeline()`, lossless `Pg*` records, raw native `copy_to`/`copy_from`/`copy_from_arrow` vs Python driver `load_from_arrow`/`select_to_arrow`/`stream_arrow_batches`/`VTBARROW`, `PgBouncer` modes, `direct_dsn`, and DSN precedence (zero external driver names).
- `docs/usage/analytics-and-cdc.md` (`ch30:t01`) — Unified `$n` PostgreSQL tenant binds, `PostgresNotificationSource`, and `vertebra.db.adapters.postgres.change_stream.PostgresChangeSource` `pgoutput` v1 CAS checkpointing with conceptual `4`–`6` node Mermaid topology where depicted.
- `docs/reference/database-capabilities.md` (`ch30:t01`) — `heading_level: 4` mkdocstrings blocks across `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresChangeSource`, `PostgresCheckpointConflictError`, `PostgresReplicationGapError`, `PostgresEmbeddingPushdown`, `PostgresNotifyConfig`, and `PostgresNotificationSource` (`TOC <= 14`, HTML `<= 120,000` bytes).
- `docs/reference/native-stubs.md` (`ch30:t02`) — Six compiled `gil_used = false` modules including `vertebra._postgres` (`src/rs/vertebra-postgres`) rendered with `heading_level: 4` and concise members (`TOC <= 14`, HTML `<= 120,000` bytes).
- `docs/reference/index.md` (`ch30:t02`) — Reference index updated to six native `.pyi` stub modules and PostgreSQL CDC/embedding capabilities.
- `docs/getting-started/installation.md` (`ch30:t02`) — `[postgres]` linked package-manager tabs, `Optional Extras` row, and `vertebra._postgres` in Free-Threaded Python (`3.14t`/`3.15t`).
- `docs/usage/cloud-dialers.md` (`ch30:t03`) — Native `PostgreSQLConfig` Cloud SQL / AlloyDB Admin discovery, mTLS 1.3 renewal, and scoped IAM (`cloud_auth_options`, `iam_auth_options`).
- `docs/usage/packaging-and-deployment.md` (`ch30:t03`) — Six PyO3 extensions including `_postgres` (`vertebra-core[postgres]`) and core-5 PGO vs release-LTO `vertebra-postgres` packaging.
- `docs/usage/queues-and-realtime.md` (`ch30:t03`) — `PostgreSQLQueueBackend` `vertebra-core[postgres]` install tabs and caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract replacing stale third-party package names.
- `README.md` (`ch30:t04`) — Nine Rust crates (`vertebra-postgres`), six PyO3 extensions (`_postgres`), `[postgres]` extra, and `postgres-*` Make targets.
- `.agents/bundles/knowledge/workflow.md` (`ch30:t04`) — Trimmed and synchronized `<= 200`-line workflow reference covering six extensions, `[dependency-groups].postgres-bench`, `postgres-*` gates, and `make review-route BASE=88c30004`.
- `AGENTS.md` (`ch30:t04`) — Synchronized `<= 100`-line agent instructions covering `vertebra-postgres` (`_postgres`) and `postgres-*` quick verification commands.
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`ch31:t01`) — Refactored `<= 200`-line cross-engine overview linking to `postgres-native-driver.md` and `postgres-limitations.md`.
- `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` (`ch31:t01`) — New `<= 200`-line OKF `Concept` for `vertebra-postgres` sessions, pools, statement caches, SCRAM-SHA-256-PLUS, Cloud SQL/AlloyDB mTLS+IAM, single-`Sync` `pipeline()`, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1.
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` (`ch31:t01`) — New `<= 200`-line OKF `Gotcha` recording single-host DSN/`sslmode=allow` limits, tail-only `pgoutput` v1, non-durable `MemoryPostgresCheckpointStore`, `%s` backslash refusal, `execute` vs `query_multi`, cold pipeline text fallback, `PgBouncer` `statement` & `direct_dsn` boundaries, Ed25519 channel binding, wire-mock cloud CI, core-5 PGO vs release-LTO `_postgres`, and `ch20`–`ch29` runtime caveats.
- `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`ch31:t02`) — Present-tense `<= 200`-line `$n` `ParameterStyle.NUMERIC`, delayed-commit `pipeline()`, `EventSessionRegistry`, and `vertebra.db.adapters.postgres.change_stream` CAS checkpoint contracts.
- `.agents/bundles/knowledge/index.md` (`ch31:t02`) — Frontmatter-free root knowledge catalog registering `postgres-native-driver.md` and `postgres-limitations.md`.
- `.agents/bundles/knowledge/domains/index.md` (`ch31:t02`) — Frontmatter-free domain knowledge catalog registering both new PostgreSQL chapters.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`ch31:t03`) — `<= 200`-line `ParameterStyle.NUMERIC` (`$n`) and `default_paramstyle = paramstyle = "numeric_dollar"` `+vtb` dialect contracts.
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`ch31:t03`) — `<= 200`-line six `gil_used = false` extensions, `[dependency-groups].postgres-bench` `python_version == '3.12'`, `postgres-bench*`, and `FREE_THREADED is _is_free_threaded_runtime()`.
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`ch31:t03`) — Trimmed `<= 200`-line nine-crate, six-extension, zero-skip `make postgres-test` (`VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`) and `make postgres-wheel-check` matrix.
- `.agents/bundles/product/tech-stack.md` (`ch31:t04`) — `<= 200`-line nine-crate workspace and five optional companion PyO3 wheels including `vertebra-postgres`.
- `.agents/bundles/product/product.md` (`ch31:t04`) — `<= 200`-line native PostgreSQL driver, CDC, `PostgreSQLQueueBackend` `vertebra-core[postgres]` caller-injected `PostgreSQLQueueExecutor` baseline, and six-extension free-threaded product capabilities.
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` (`ch31:t04`) — `<= 200`-line `PostgreSQLQueueBackend` `vertebra-core[postgres]` dependency guard and caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract.
- `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` (`ch32:t01`) — `<= 200`-line six-extension FFI pattern (`_postgres`), cross-`cdylib` primitive-kwargs & `VTBARROW`, and `PendingDeliveryGuard` async drop restoration.
- `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` (`ch32:t01`) — `<= 200`-line six-extension runtime gotchas, `-m` `maturin`, `[dependency-groups].postgres-bench`, and async drop-loss prevention.
- `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`ch32:t01`) — Trimmed `<= 200`-line six-extension surface (`_postgres.pyi`) and `_bigquery`/`_spanner`/`_postgres` database boundary.
- `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` (`ch32:t02`) — `<= 200`-line five optional companion wheels including `vertebra-postgres` and core-5 PGO vs release-LTO `_postgres` boundary.
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` (`ch32:t02`) — `<= 200`-line ADR-004 optional driver wheel and `VTBARROW` ownership alignment covering `vertebra-postgres`.
- `.agents/skills/pyo3-bridge/SKILL.md` (`ch32:t02`) — `<= 200`-line six-extension `pyo3-bridge` engineering skill including `vertebra-postgres` / `_postgres.pyi`.
- `.agents/skills/review-core/references/areas.toml` (`ch32:t03`) — Verified `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` globs/shards plus PostgreSQL OKF `knowledge` and `focused_commands`.
- `.agents/skills/review-core/references/checklist.md` (`ch32:t03`) — Six-extension `FFI-09` `{_vertebra,_bundler,_storage,_bigquery,_postgres,_spanner}.pyi` parity and `[dependency-groups].postgres-bench` `PERF-06` rule.
- `.agents/skills/review-pyo3-bridge/SKILL.md` (`ch32:t03`) — `<= 200`-line read-only six-extension `review-pyo3-bridge` skill covering `vertebra-postgres` and `_postgres.pyi`.

## 4. Requirement-to-Chapter Traceability Matrix

| Requirement | Owning Chapters & Tasks | Verification Strategy | Canonical Public Seam / Gate |
| --- | --- | --- | --- |
| R1 | [ch01](chapters/ch01-wheel-scaffold-and-config/spec.md) (`ch01:t01`–`t03`), [ch08](chapters/ch08-constructor-parity-and-wheel-gil/spec.md) (`ch08:t02`–`t04`), [ch30](chapters/ch30-user-and-workflow-docs/spec.md) (`ch30:t02`, `ch30:t04`), [ch31](chapters/ch31-data-and-testing-okf-sync/spec.md) (`ch31:t04`), [ch32](chapters/ch32-ffi-okf-and-review-routing-sync/spec.md) (`ch32:t01`–`t03`) | `regression_tdd`, `documentation_validation`, `static_validation` | `src/py/tests/unit/test_freethreaded_gil.py`, `src/py/vertebra/_postgres.pyi`, `make review-check` |
| R2 | [ch01](chapters/ch01-wheel-scaffold-and-config/spec.md) (`ch01:t04`), [ch06](chapters/ch06-alloydb-keepalive-unix-and-direct-iam/spec.md) (`ch06:t02`–`t03`), [ch08](chapters/ch08-constructor-parity-and-wheel-gil/spec.md) (`ch08:t01`), [ch25](chapters/ch25-dsn-bridge-lexer-and-permanent-drivers/spec.md) (`ch25:t01`–`t02`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t01`) | `behavior_tdd`, `characterization` | `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs`, `src/py/tests/unit/db/test_postgres_options.py`, `src/py/tests/unit/db/test_postgres_binds.py`, `src/py/tests/unit/extensions/test_postgres_compiler_transport.py` |
| R3 | [ch01](chapters/ch01-wheel-scaffold-and-config/spec.md) (`ch01:t04`), [ch02](chapters/ch02-wire-codec-and-session-startup/spec.md) (`ch02:t01`–`t02`), [ch06](chapters/ch06-alloydb-keepalive-unix-and-direct-iam/spec.md) (`ch06:t02`–`t03`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs`, `src/rs/vertebra-db/src/connection.rs` |
| R4 | [ch02](chapters/ch02-wire-codec-and-session-startup/spec.md) (`ch02:t03`), [ch03](chapters/ch03-scram-and-channel-binding/spec.md) (`ch03:t02`), [ch04](chapters/ch04-cloud-material-and-admin-discovery/spec.md) (`ch04:t02`), [ch05](chapters/ch05-cloud-tls-and-scoped-iam/spec.md) (`ch05:t01`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |
| R5 | [ch02](chapters/ch02-wire-codec-and-session-startup/spec.md) (`ch02:t04`), [ch03](chapters/ch03-scram-and-channel-binding/spec.md) (`ch03:t01`–`t03`), [ch04](chapters/ch04-cloud-material-and-admin-discovery/spec.md) (`ch04:t01`), [ch05](chapters/ch05-cloud-tls-and-scoped-iam/spec.md) (`ch05:t02`–`t04`), [ch06](chapters/ch06-alloydb-keepalive-unix-and-direct-iam/spec.md) (`ch06:t01`, `ch06:t04`) | `behavior_tdd`, `regression_tdd` | `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs`, `src/rs/vertebra-core/tests/it/gcp_auth.rs` |
| R6 | [ch07](chapters/ch07-async-bridge-and-client-sessions/spec.md) (`ch07:t02`), [ch08](chapters/ch08-constructor-parity-and-wheel-gil/spec.md) (`ch08:t01`, `ch08:t04`), [ch14](chapters/ch14-physical-pools-and-query-facades/spec.md) (`ch14:t03`–`t04`), [ch15](chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md) (`ch15:t01`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t01`–`t03`) | `behavior_tdd`, `characterization`, `regression_tdd` | `src/py/tests/unit/db/test_postgres_native_queries.py`, `src/py/tests/unit/db/test_postgres_native_pools.py`, `src/py/tests/unit/db/test_postgres_native_portals.py`, `src/rs/vertebra-postgres/tests/command_deadlines.rs` |
| R7 | [ch02](chapters/ch02-wire-codec-and-session-startup/spec.md) (`ch02:t02`), [ch07](chapters/ch07-async-bridge-and-client-sessions/spec.md) (`ch07:t01`, `ch07:t03`), [ch08](chapters/ch08-constructor-parity-and-wheel-gil/spec.md) (`ch08:t01`, `ch08:t04`), [ch14](chapters/ch14-physical-pools-and-query-facades/spec.md) (`ch14:t03`–`t04`), [ch15](chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md) (`ch15:t01`), [ch16](chapters/ch16-pipeline-scope-and-cancellation/spec.md) (`ch16:t04`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t01`–`t03`), [ch21](chapters/ch21-live-notifications/spec.md) (`ch21:t02`), [ch23](chapters/ch23-replication-feedback-resume-and-bridge/spec.md) (`ch23:t03`), [ch25](chapters/ch25-dsn-bridge-lexer-and-permanent-drivers/spec.md) (`ch25:t04`) | `behavior_tdd`, `characterization`, `regression_tdd` | `src/rs/vertebra-db/src/py_async.rs`, `src/rs/vertebra-db/tests/py_async_cancel_tests.rs`, `src/py/tests/unit/db/test_postgres_native_cancel.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py` |
| R8 | [ch32](chapters/ch32-ffi-okf-and-review-routing-sync/spec.md) (`ch32:t03`), [ch33](chapters/ch33-final-review-and-release-gate/spec.md) (`ch33:t01`) | `static_validation` | `make lint-all`, `make review-check`, `make review-route BASE=88c30004`, `review.md` |
| R9 | [ch04](chapters/ch04-cloud-material-and-admin-discovery/spec.md) (`ch04:t01`–`t04`), [ch05](chapters/ch05-cloud-tls-and-scoped-iam/spec.md) (`ch05:t01`–`t04`), [ch06](chapters/ch06-alloydb-keepalive-unix-and-direct-iam/spec.md) (`ch06:t01`) | `behavior_tdd`, `regression_tdd` | `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs`, `src/rs/vertebra-core/tests/it/gcp_auth.rs` |
| R10 | [ch09](chapters/ch09-owned-results-and-numeric-codecs/spec.md) (`ch09:t01`–`t02`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t01`–`t02`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/result_contract.rs`, `src/rs/vertebra-postgres/tests/query_protocol.rs`, `src/py/tests/integration/db/test_postgres_command_deadlines.py` |
| R11 | [ch09](chapters/ch09-owned-results-and-numeric-codecs/spec.md) (`ch09:t01`, `ch09:t03`–`t04`), [ch10](chapters/ch10-temporal-interval-and-json-codecs/spec.md) (`ch10:t01`–`t04`), [ch11](chapters/ch11-type-catalog-arrays-and-composites/spec.md) (`ch11:t01`–`t04`), [ch12](chapters/ch12-pgvector-and-extended-query/spec.md) (`ch12:t01`), [ch14](chapters/ch14-physical-pools-and-query-facades/spec.md) (`ch14:t03`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t04`) | `behavior_tdd`, `regression_tdd` | `src/py/tests/unit/db/test_postgres_types.py`, `src/rs/vertebra-postgres/tests/scalar_codecs.rs`, `src/rs/vertebra-postgres/tests/numeric_codecs.rs`, `src/rs/vertebra-postgres/tests/temporal_codecs.rs`, `src/rs/vertebra-postgres/tests/vector_codecs.rs`, `src/rs/vertebra-postgres/tests/timezone_text.rs` |
| R12 | [ch09](chapters/ch09-owned-results-and-numeric-codecs/spec.md) (`ch09:t02`), [ch12](chapters/ch12-pgvector-and-extended-query/spec.md) (`ch12:t02`–`t03`), [ch14](chapters/ch14-physical-pools-and-query-facades/spec.md) (`ch14:t04`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t01`–`t02`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t03`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/query_protocol.rs`, `src/rs/vertebra-postgres/tests/extended_protocol.rs`, `src/rs/vertebra-postgres/tests/metadata_normalization.rs`, `src/py/tests/integration/db/test_postgres_services.py` |
| R13 | [ch12](chapters/ch12-pgvector-and-extended-query/spec.md) (`ch12:t02`), [ch13](chapters/ch13-transactions-portals-and-statement-cache/spec.md) (`ch13:t01`), [ch14](chapters/ch14-physical-pools-and-query-facades/spec.md) (`ch14:t04`), [ch15](chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md) (`ch15:t02`–`t03`), [ch16](chapters/ch16-pipeline-scope-and-cancellation/spec.md) (`ch16:t01`–`t02`), [ch25](chapters/ch25-dsn-bridge-lexer-and-permanent-drivers/spec.md) (`ch25:t03`–`t04`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t02`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/transaction_state.rs`, `src/rs/vertebra-postgres/tests/pipeline_scope.rs`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py` |
| R14 | [ch14](chapters/ch14-physical-pools-and-query-facades/spec.md) (`ch14:t01`–`t02`), [ch15](chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md) (`ch15:t01`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/physical_pool.rs`, `src/rs/vertebra-postgres/tests/pool_reset.rs`, `src/py/tests/unit/db/test_postgres_native_pools.py` |
| R15 | [ch11](chapters/ch11-type-catalog-arrays-and-composites/spec.md) (`ch11:t01`), [ch12](chapters/ch12-pgvector-and-extended-query/spec.md) (`ch12:t02`), [ch13](chapters/ch13-transactions-portals-and-statement-cache/spec.md) (`ch13:t03`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t03`–`t04`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/statement_cache.rs`, `src/py/tests/integration/db/test_postgres_services.py`, `src/py/tests/integration/db/test_postgres_features.py` |
| R16 | [ch13](chapters/ch13-transactions-portals-and-statement-cache/spec.md) (`ch13:t02`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t03`), [ch20](chapters/ch20-arrow-copy-import-and-portals/spec.md) (`ch20:t03`–`t04`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/portal_protocol.rs`, `src/py/tests/unit/db/test_postgres_native_portals.py`, `src/py/tests/unit/db/test_postgres_native_data.py` |
| R17 | [ch15](chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md) (`ch15:t02`–`t04`), [ch16](chapters/ch16-pipeline-scope-and-cancellation/spec.md) (`ch16:t01`–`t02`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t02`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/pipeline_wire.rs`, `src/rs/vertebra-postgres/tests/pipeline_outcomes.rs`, `src/rs/vertebra-postgres/tests/pipeline_scope.rs`, `src/py/tests/unit/db/test_postgres_native_pipeline.py` |
| R18 | [ch16](chapters/ch16-pipeline-scope-and-cancellation/spec.md) (`ch16:t03`–`t04`), [ch17](chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md) (`ch17:t01`–`t02`), [ch25](chapters/ch25-dsn-bridge-lexer-and-permanent-drivers/spec.md) (`ch25:t03`–`t04`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t01`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/cancellation_protocol.rs`, `src/py/tests/unit/db/test_postgres_native_cancel.py`, `src/py/tests/integration/db/test_postgres_native.py` |
| R19 | [ch18](chapters/ch18-binary-copy-streaming/spec.md) (`ch18:t01`–`t03`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t04`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs`, `src/py/tests/integration/db/test_postgres_features.py` |
| R20 | [ch19](chapters/ch19-native-arrow-batches-and-shm/spec.md) (`ch19:t01`–`t04`), [ch20](chapters/ch20-arrow-copy-import-and-portals/spec.md) (`ch20:t01`–`t04`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t03`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t04`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` |
| R21 | [ch21](chapters/ch21-live-notifications/spec.md) (`ch21:t01`–`t03`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t04`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t04`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs`, `src/py/tests/unit/db/test_postgres_native_notifications.py` |
| R22 | [ch22](chapters/ch22-copyboth-and-pgoutput-decoding/spec.md) (`ch22:t01`–`t03`), [ch23](chapters/ch23-replication-feedback-resume-and-bridge/spec.md) (`ch23:t01`–`t03`), [ch24](chapters/ch24-change-source-and-checkpoints/spec.md) (`ch24:t01`–`t03`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t04`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t02`) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs`, `src/py/tests/integration/db/test_postgres_replication.py` |
| R23 | [ch23](chapters/ch23-replication-feedback-resume-and-bridge/spec.md) (`ch23:t03`), [ch24](chapters/ch24-change-source-and-checkpoints/spec.md) (`ch24:t01`–`t03`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t04`), [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t02`) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_change_source.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py`, `src/py/tests/integration/db/test_postgres_replication.py` |
| R24 | [ch25](chapters/ch25-dsn-bridge-lexer-and-permanent-drivers/spec.md) (`ch25:t02`–`t04`), [ch26](chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md) (`ch26:t01`–`t04`), [ch27](chapters/ch27-atomic-cutover-and-legacy-removal/spec.md) (`ch27:t01`–`t04`) | `behavior_tdd`, `characterization` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/vertebra/framework/queues/postgres.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |
| R25 | [ch28](chapters/ch28-live-conformance-and-service-acceptance/spec.md) (`ch28:t01`–`t04`) | `behavior_tdd` | `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test`, `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/db/test_postgres_native.py`, `src/py/tests/integration/db/test_postgres_replication.py`, `src/py/tests/integration/db/test_postgres_services.py`, `src/py/tests/integration/db/test_postgres_features.py` |
| R26 | [ch08](chapters/ch08-constructor-parity-and-wheel-gil/spec.md) (`ch08:t02`–`t04`), [ch29](chapters/ch29-wheel-ci-and-benchmarks/spec.md) (`ch29:t01`), [ch30](chapters/ch30-user-and-workflow-docs/spec.md) (`ch30:t02`), [ch32](chapters/ch32-ffi-okf-and-review-routing-sync/spec.md) (`ch32:t02`) | `regression_tdd`, `documentation_validation`, `static_validation` | `tools/scripts/verify_postgres_wheel.py`, `src/py/tests/unit/test_freethreaded_gil.py`, `.github/workflows/ci.yml`, `.github/workflows/release.yaml` |
| R27 | [ch29](chapters/ch29-wheel-ci-and-benchmarks/spec.md) (`ch29:t02`–`t03`), [ch30](chapters/ch30-user-and-workflow-docs/spec.md) (`ch30:t01`–`t04`), [ch31](chapters/ch31-data-and-testing-okf-sync/spec.md) (`ch31:t01`–`t04`), [ch32](chapters/ch32-ffi-okf-and-review-routing-sync/spec.md) (`ch32:t01`–`t03`), [ch33](chapters/ch33-final-review-and-release-gate/spec.md) (`ch33:t01`) | `behavior_tdd`, `documentation_validation`, `static_validation` | `src/py/tests/unit/tools/test_postgres_benchmarks.py`, `src/rs/vertebra-postgres/benches/postgres_native.rs`, `src/py/tests/integration/db/test_postgres_benchmarks.py`, `make docs-check`, `make review-check`, `make review-route BASE=88c30004` |

## 5. Implementation Plan

Every worksheet (`118` total across `33` chapters) executes in one bounded invocation and produces one signed local conventional commit. Each chapter owns `1`–`4` vertical-slice tasks (`<= 3` files per task, zero overlapping `files:` across ready tasks) in a strict serial chapter dependency chain (`ch01` -> `ch33`). See [review.md](review.md) for adjudicated planning decisions.

- [x] [b973f800] ch01-wheel-scaffold-and-config — Standalone Optional Wheel Scaffold, Build Matrix, and Validated Connection Options (chapters/ch01-wheel-scaffold-and-config/spec.md)
- [x] [42e3586d] ch02-wire-codec-and-session-startup — Bounded Protocol Framing, Trust Lifecycle, SSLRequest TLS, and Cleartext/MD5 Startup (chapters/ch02-wire-codec-and-session-startup/spec.md)
- [x] [02af81ee] ch03-scram-and-channel-binding — Ordered SCRAM-SHA-256, Peer Certificate Binding, and SCRAM-SHA-256-PLUS Negotiation (chapters/ch03-scram-and-channel-binding/spec.md)
- [x] [2aef3d64] ch04-cloud-material-and-admin-discovery — Single-Flight Cloud Material Cache, Ephemeral Keys, and Cloud SQL / AlloyDB Admin Discovery (chapters/ch04-cloud-material-and-admin-discovery/spec.md)
- [x] [eb48155b] ch05-cloud-tls-and-scoped-iam — Cloud TLS 1.3 Peer Identities, Static Login Token Contract, Scoped Auth Caches, and Cloud SQL mTLS/IAM (chapters/ch05-cloud-tls-and-scoped-iam/spec.md)
- [x] [eb48155b] ch06-alloydb-keepalive-unix-and-direct-iam — AlloyDB Metadata Exchange, TCP Keepalives, Unix Socket Projection, and Direct-Host IAM Refresh (chapters/ch06-alloydb-keepalive-unix-and-direct-iam/spec.md)
- [x] [4b1870c7] ch07-async-bridge-and-client-sessions — Setup-Safe Async Worker Bridge and Raw Sync/Async Physical PostgreSQL Client Sessions (chapters/ch07-async-bridge-and-client-sessions/spec.md)
- [x] [4b1870c7] ch08-constructor-parity-and-wheel-gil — Sync/Async Constructor Parity, Packaging & Stub Exclusions, Outside-Checkout Wheel Bootstrap, and Isolated Free-Threaded GIL Probe (chapters/ch08-constructor-parity-and-wheel-gil/spec.md)
- [x] [d3bb0d14] ch09-owned-results-and-numeric-codecs — Owned Query Result/Value Records, Simple Query SQLSTATE Mapping, and Scalar/Numeric Codecs (chapters/ch09-owned-results-and-numeric-codecs/spec.md)
- [x] [d3bb0d14] ch10-temporal-interval-and-json-codecs — Lossless Temporal/Interval Binary and Multi-Style Text Codecs plus JSON/JSONB Codecs (chapters/ch10-temporal-interval-and-json-codecs/spec.md)
- [x] [2ebd79f5] ch11-type-catalog-arrays-and-composites — Session Type Catalog Discovery, Dimensioned Binary/Text Arrays, and Ordered Composite Codecs (chapters/ch11-type-catalog-arrays-and-composites/spec.md)
- [x] [2ebd79f5] ch12-pgvector-and-extended-query — Validated pgvector/halfvec/bit Codecs, Cold Parameter/Result Metadata Normalization, and Mixed-Format Extended Query (chapters/ch12-pgvector-and-extended-query/spec.md)
- [x] [fa38544c] ch13-transactions-portals-and-statement-cache — Pinned Transaction/Savepoint State, Bounded Portals, and Pooler-Safe Prepared Statement Cache (chapters/ch13-transactions-portals-and-statement-cache/spec.md)
- [x] [fa38544c] ch14-physical-pools-and-query-facades — Concrete Rust Physical Connection Pool, Rollback/Drain Lifecycle, and Python Value/Query Facades (chapters/ch14-physical-pools-and-query-facades/spec.md)
- [x] [7b67a4ed] ch15-pool-facades-and-single-sync-pipeline — Python Pool Facades, SQL-Head Pipeline Guard, Single-Sync Outbound Batching, and Ordered Pipeline Outcomes (chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md)
- [x] [94caf47a] ch16-pipeline-scope-and-cancellation — Delayed-Commit Pipeline Transaction Scope, Python Pipeline Contexts, and Backend-Key Cancellation Retirement (chapters/ch16-pipeline-scope-and-cancellation/spec.md)
- [x] [7db43548] ch17-command-deadlines-portals-and-tz-fallback — Native Per-Command I/O Deadlines, Python Portal Facades, and Ambiguous Timezone Text Fallback (chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md)
- [x] [03e9b772] ch18-binary-copy-streaming — Incremental Binary COPY Framing, COPY TO Streaming, and Duplex Backpressured COPY FROM Recovery (chapters/ch18-binary-copy-streaming/spec.md)
- [x] [3c7a9355] ch19-native-arrow-batches-and-shm — Lossless PostgreSQL Arrow Schemas, Bounded Scalar/Nested Batch Builders, and IPC-to-SHM Capsules (chapters/ch19-native-arrow-batches-and-shm/spec.md)
- [x] [e5366213] ch20-arrow-copy-import-and-portals — Arrow-to-COPY Field Import, Schema-Qualified Streaming Writers, and Sync/Async Arrow Portal Cancellation (chapters/ch20-arrow-copy-import-and-portals/spec.md)
- [~] ch21-live-notifications — Dedicated Notification Session Pump, PendingDeliveryGuard Async Bridge, and Python PostgresNotificationSource (chapters/ch21-live-notifications/spec.md)
- [ ] ch22-copyboth-and-pgoutput-decoding — Dedicated Standalone CopyBoth Logical Replication Sessions and Lossless pgoutput v1 Decoding (chapters/ch22-copyboth-and-pgoutput-decoding/spec.md)
- [ ] ch23-replication-feedback-resume-and-bridge — Durable Commit-Boundary Feedback, Checkpoint-Verified Reconnect Resume, and Cancellation-Safe Replication Bridge (chapters/ch23-replication-feedback-resume-and-bridge/spec.md)
- [ ] ch24-change-source-and-checkpoints — PostgresChangeSource, PostgresCheckpointStore, Serialized CAS Ack Reconciliation, and Replay/Shutdown Semantics (chapters/ch24-change-source-and-checkpoints/spec.md)
- [ ] ch25-dsn-bridge-lexer-and-permanent-drivers — URL/libpq Config Bridge, Lexer-Aware Bind Compiler, and Pre-Activation Permanent Sync/Async Drivers (chapters/ch25-dsn-bridge-lexer-and-permanent-drivers/spec.md)
- [ ] ch26-compiler-pipeline-arrow-and-event-hooks — SQLSpec/SQLAlchemy Compiler Boundaries, Transaction-Bound Pipelines, Liskov Arrow Forwarding, and EventSessionRegistry Hooks (chapters/ch26-compiler-pipeline-arrow-and-event-hooks/spec.md)
- [ ] ch27-atomic-cutover-and-legacy-removal — Atomic Public Driver/Queue/Embeddings/SQLSpec/SQLAlchemy Activation and Legacy Async/Sync Driver Removal (chapters/ch27-atomic-cutover-and-legacy-removal/spec.md)
- [ ] ch28-live-conformance-and-service-acceptance — Zero-Skip Live PG14–18 Conformance, Logical Replication, Multi-Arch pgvector 0.8.1, and SCRAM PgBouncer 1.26.0 Acceptance (chapters/ch28-live-conformance-and-service-acceptance/spec.md)
- [ ] ch29-wheel-ci-and-benchmarks — Outside-Checkout Wheel/GIL Parity, Strict CI/Release Matrix, and Paired PG18 postgres-bench Comparators (chapters/ch29-wheel-ci-and-benchmarks/spec.md)
- [ ] ch30-user-and-workflow-docs — User Guides, Capability & Native Stub References, Cloud/Packaging/Queue Docs, and Workflow/AGENTS Sync (chapters/ch30-user-and-workflow-docs/spec.md)
- [ ] ch31-data-and-testing-okf-sync — PostgreSQL Driver & Limitations OKF Split, Knowledge Indexes, Extensions/Testing Matrix, and Product/Queue OKF Sync (chapters/ch31-data-and-testing-okf-sync/spec.md)
- [ ] ch32-ffi-okf-and-review-routing-sync — Six-Extension PyO3 FFI OKF, Packaging/ADR-004, pyo3-bridge Skill, and Review Routing Synchronization (chapters/ch32-ffi-okf-and-review-routing-sync/spec.md)
- [ ] ch33-final-review-and-release-gate — Complete-Driver Verification Gates, Routed Multi-Area Correctness Review, and Fresh Zero-Context Quality Review (chapters/ch33-final-review-and-release-gate/spec.md)

**Flow Recovery Checkpoint:** `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check && make docs-check && make review-check && make review-route BASE=88c30004` exits `0` with zero PostgreSQL skips or xfails, followed by clean correctness and fresh independent quality reviews over `88c30004..HEAD` before `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_commit: ef86844f` above `feat/driver-fail-loud` (`88c30004`) and `main @ 077a3fe9`; `state: active`, `has_chapters: true`, `plan_revision: 6`, `last_verified_checkpoint: 2a575ce3`, `last_reviewed_chapter: ch20-arrow-copy-import-and-portals` (`e5366213`).
- **Upstream Flow Dependency**: [`driver-fail-loud`](../../knowledge/domains/data/database-drivers-and-dialers.md) is completed at `88c30004`, deleting PostgreSQL `memory://` execution and `vertebra_db::postgres::PostgresPool` [^fail-loud-baseline].
- **Active Chapter & Next Task**: [ch21-live-notifications](chapters/ch21-live-notifications/spec.md) (`3/3` tasks verified through `2a575ce3`, awaiting chapter review); `20/33` chapters (`ch01`–`ch20`) are review-completed, `78/118` tasks are completed, and `12/33` chapters (`ch22`–`ch33`, `40/118` tasks) remain planned.
- **Documentation & OKF Status**: `[0/33 files documented]`, scheduled across [ch30-user-and-workflow-docs](chapters/ch30-user-and-workflow-docs/spec.md) (`12` files), [ch31-data-and-testing-okf-sync](chapters/ch31-data-and-testing-okf-sync/spec.md) (`12` files), and [ch32-ffi-okf-and-review-routing-sync](chapters/ch32-ffi-okf-and-review-routing-sync/spec.md) (`9` files), followed by final verification and `88c30004..HEAD` review in [ch33-final-review-and-release-gate](chapters/ch33-final-review-and-release-gate/spec.md) (`ch33:t01`, keeping `state: active` and `current_task: null` for `DOC-07` before finish/archive).
- **Delivery & Next Action**: Complete single-PR delivery on `feat/native-postgres-driver`; intermediate chapters never trigger early `/flow:finish` or `/flow:archive`. Next canonical command: `/flow:implement native-postgres-driver ch21`.

[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP, Unix, TLS, and cloud mTLS stream primitives.
[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` — Shared stateless PostgreSQL wire frame and codec helpers in `vertebra-db`.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed upstream flow that deletes PostgreSQL `memory://` paths and `vertebra_db::postgres::PostgresPool`.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue` and independent event-loop drainer.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Shared-memory Arrow IPC stream writer for `VTBARROW` capsules.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` target cutover module.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec stack execution and PostgreSQL adapter integration seam.
[^db-arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public `ArrowCursor` wrapper over inline IPC and `VTBARROW` `ShmCapsule`.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix and pipeline suite.
[^dialer-cloudsql-py]: `src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig` — Existing Python Cloud SQL dialer configuration and ephemeral certificate helper.
[^dialer-alloydb-py]: `src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig` — Existing Python AlloyDB dialer configuration and `MetadataExchange` helper.
