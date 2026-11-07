---
type: Spec
flow_id: native-postgres-driver
title: Complete standalone native PostgreSQL driver
description: One complete PR implementing native PostgreSQL transport/authentication, typed queries and pools, COPY/Arrow, pipelines/cancellation, notifications/replication, and production adapter cutover.
status: stable
state: planned
has_chapters: false
parent_prd: native-data-platform
research:
  - research/protocol-and-execution_20261105.md
  - research/cloud-material_20261105.md
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
current_task: null
last_verified_checkpoint: 077a3fe9
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TCP, Unix, TLS, and cloud mTLS stream primitives
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: Existing PostgreSQL frame codecs and admission metadata in vertebra-db
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue and independent event-loop drainer
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python SyncPostgreSQLDriver and AsyncPostgreSQLDriver target cutover module
tags:
  - rust
  - postgres
  - driver
  - wheel
  - pipeline
  - one-pr
---

# Standalone native PostgreSQL driver

<!-- truth: start -->
- **Standalone Optional Wheel**: `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) owns live PostgreSQL Protocol 3.0 sockets on `postgres-protocol =0.6.12` + `rustls` (`ring`) without linking database clients into `vertebra-core` [^pg-conn-rs] [^pg-core-rs].
- **Complete Single-PR Delivery**: Authentication (`SCRAM-SHA-256`, `SCRAM-SHA-256-PLUS`, Cloud SQL / AlloyDB mTLS + IAM), binary/lossless codecs, statement caching, single-`Sync` `pipeline()`, `COPY`, `VTBARROW`, `CancelRequest`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 logical replication ship together with full `psycopg`/`asyncpg` removal [^py-async-rs] [^pg-driver-py].
- **Flat Worksheet Layout Status**: All 102 worksheets (`t01`–`t102`) are anchored at `plan_revision: 2` and `plan_commit: cdb978d3` (`has_chapters: false`; nested chapter decomposition deferred to `/flow:revise native-postgres-driver`).
<!-- truth: end -->

## 1. Specification & Context

### 1.1 Upstream Execution Baseline

The user decision is: “Keep the complete driver in one PR.” All worksheets run
on `feat/native-postgres-driver`, stacked on `feat/storage-updates` (`cdb978d3`) above `main @ 077a3fe9`. The database foundation ([PR #76](https://github.com/cofin/vertebra/pull/76), `21c99f95`), engine primitives ([PR #77](https://github.com/cofin/vertebra/pull/77), `80ff8a65`), post-merge review remediation ([PR #80](https://github.com/cofin/vertebra/pull/80), `2b737240`), runtime structure ([PR #92](https://github.com/cofin/vertebra/pull/92), `d2fa5072`), and MCP/WebMCP core ([PR #94](https://github.com/cofin/vertebra/pull/94), `077a3fe9`) are squash-merged on `main`. Those results do not validate this driver.

The complete charter requires more than the default eight worksheets. The user's
one-PR scope governs delivery; small worksheets and signed local commits govern
implementation. No phase is a partial release or a separate PR. The full native
adapter cutover, strict conformance and final reviews remain acceptance conditions.
No production implementation begins while `plan_commit` is null. Hosted writes
require the user's separate exact permission.

### 1.2 Code Analysis Summary

| Existing symbol | Verified consequence |
| --- | --- |
| `src/rs/vertebra-db/src/connection.rs::TransportStream` | Reuse transport/TLS. Direct PostgreSQL sends SSLRequest before upgrade; cloud proxy endpoints negotiate immediate mTLS. |
| `src/rs/vertebra-db/src/connection.rs::{Connection,CancelHandle}` | Lifecycle traits require real ping/cancel/close implementations. No placeholder success. |
| `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` | SHM admission and compiled-plan metadata do not own live connections or server prepared statements. |
| `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` | Reuse completion queues and independent drainer. Add abort ownership across fallible setup before using it for sessions. |
| `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` | Accepts serialized IPC; does not build columns. Its padding API cannot receive arbitrary encoder fragments. |
| `src/py/vertebra/db/adapters/postgres/driver.py::{SyncPostgreSQLDriver,AsyncPostgreSQLDriver}` | Replace Python-client, duck-typed and memory execution with real native pool leases. Preserve count/tuple/description/hydration contracts. |
| `src/py/vertebra/extensions/sqlspec/adapters.py::{BaseSyncDriver.execute_stack,BaseAsyncDriver.execute_stack}` | Caller conversion and strict result validation occur inside pipeline context; results cannot commit early. |
| `src/py/vertebra/db/arrow.py::ArrowCursor` | Public cursor wraps native SHM output; inline IPC needs explicit native row-count metadata without Python rows. |
| `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` | PostgreSQL needs native type DDL, enabled pipeline acceptance and removal of pending-flow xfail after cutover. |
| `src/py/vertebra/db/dialer/{cloudsql,alloydb}.py` | Existing helpers do not complete real discovery/issued-material lifecycle. Native cloud support includes actual APIs and scopes. |

### 1.3 Relevant Patterns and Glossary

Read [driver ownership](../../knowledge/domains/data/database-drivers-and-dialers.md),
[pipelines and CDC](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md),
[optional wheels and ownership](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md),
[FFI discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md),
[runtime gotchas](../../knowledge/gotchas/pyo3-and-python-runtime.md), and the
project-local `pyo3-bridge`, `runtime-ipc`, `python-framework` and routed review skills.

`postgres-protocol = "=0.6.12"` owns codecs/SCRAM; the new engine crate owns live
connection state. `tokio-postgres = "=0.7.18"` is a dev oracle. Protocol3.0 supports
the PG14–18 target; protocol3.2 is outside this charter. Arrow crates are60.1.0;
cross-extension transfer uses producer-owned ShmCapsule callbacks, never Rust Arc
or trait-object layouts. A physical session is one socket; a lease exclusively
owns that session; a portal pins its lease and transaction; a checkpoint records
durably acknowledged replication progress.

## 2. Requirements

### 2.1 Functional Requirements and Architecture

The [public contracts](contracts.md) freeze constructor options, pool ownership,
result/bind shapes, pipeline completion, lossless value policy and checkpointing.
The owning worksheets specify exact methods, new symbols, test cases and commands.
Engine values, SQL, tool contracts and checkpoint protocols stay in the PostgreSQL
adapter/crate. Shared database and transport code remain engine agnostic.

- **R1.** Optional standalone wheel, package/build/release registration and typed exports.
- **R2.** Pure validated connection options, one URL/libpq parser and lexical bind lowering.
- **R3.** Bounded protocol framing, real TCP/Unix startup, metadata and total connection deadline.
- **R4.** Direct SSLRequest/TLS, verified certificate binding and service-specific cloud identity.
- **R5.** Trust/password/MD5/SCRAM/PLUS and scope-correct direct/cloud IAM authentication.
- **R6.** Native synchronous client/query/pool surfaces with detached blocking I/O.
- **R7.** Native asynchronous completion, setup-abort ownership and cancellation-safe publication.
- **R8.** Complete-driver gates and correctness review followed by fresh quality review.
- **R9.** Automatic Cloud SQL/AlloyDB discovery, issued material, expiry and refresh.
- **R10.** Owned descriptions/results, affected command counts and exact core SQLSTATE errors.
- **R11.** Lossless scalar/numeric/temporal/JSON/container/vector values and unknown-type formats.
- **R12.** Simple multi-results and extended single-statement query execution.
- **R13.** Context-owned transactions/savepoints and exclusive physical-session affinity.
- **R14.** Real physical pools with bounded acquisition, reset, shutdown and configuration isolation.
- **R15.** Per-connection prepared LRU and verified/configured pooler capabilities.
- **R16.** Bounded portals and sync/async stream ownership through exhaustion or abandonment.
- **R17.** Cold/warm one-Sync/one-flush pipeline with ordered outcomes and delayed context commit.
- **R18.** Backend-key cancellation, server cleanup and protection against delayed cross-borrower cancel.
- **R19.** Incremental binary COPY input/output and exclusive-mode error recovery.
- **R20.** Native typed Arrow builders/import, exact IPC, threshold/count and producer-owned capsule lifetime.
- **R21.** Dedicated live notifications, subscription changes, bounded delivery and cleanup.
- **R22.** Dedicated pgoutput-v1 logical replication with relation/type state and independent feedback.
- **R23.** Adapter-owned checkpoint store, stable int64 replay IDs and explicit durable-before-feedback ack.
- **R24.** Native Python adapter and SQLSpec/SQLAlchemy/helper cutover without old execution fallbacks.
- **R25.** Strict PG14–18 native conformance and real COPY/Arrow/events/vector/pooler acceptance.
- **R26.** Installed-wheel isolation, namespace/exception identity and actual free-threaded GIL checks.
- **R27.** Driver-specific performance evidence, public documentation and canonical verification.

### 2.2 Non-Functional and Safety Constraints

- Every native module declares `gil_used=false`; sync network waits detach the GIL.
  Async workers carry owned Rust data and never attach Python. An isolated child
  checks actual disabled-GIL import behavior on3.14t and3.15t.
- One bounded framing implementation handles standard messages; replication adds
  only the missing CopyBoth frame. Validate lengths/counts/arity before allocation.
  Document resource limits and typed refusal rather than silently truncating data.
- One socket owner services duplex backpressure. Abandoned portals, COPY, pipeline
  failures and cancellation reach synchronized state or discard the physical socket.
  A CancelRequest-sent connection retires before another operation can borrow it.
- Pool identity includes credentials, TLS, GUCs, cloud resource/network and IAM.
  Ordinary driver calls borrow transient leases; transaction/stream/pipeline scopes
  pin them with thread/task identity. Session reset invalidates prepared metadata
  when DISCARD ALL removes server statements. No SQL-classification reset shortcut.
- Cold pipelines may use text formats with received descriptors; warm known OIDs
  use binary and unknown OIDs use text. No hidden discovery round trip is inserted
  into the batch. One Sync and one logical flush do not mean one TCP packet.
- Preserve Decimal scale/special values, array lower bounds/null dimensions,
  composite order, interval months and temporal range. Ambiguous display text is
  retained as a tagged exact value; binary COPY refuses unresolved display values.
- Arrow builders are native and bounded. Inline/SHM threshold behavior and row
  counts remain observable. Typed nested storage and explicit numeric/temporal
  fallbacks carry OID metadata. Only SHM capsule adoption is claimed zero-copy.
- Notifications and replication use independently owned direct/session endpoints.
  Logical replication implements pgoutput v1 tail/resume of pre-existing caller-owned
  slots/publications. Initial snapshot alignment and provisioning belong to callers.
  No implicit slot creation/drop or automatic checkpoint-on-yield occurs.
- No third-party Python database client, SQLite stand-in, memory execution or sync
  fallback remains in native PostgreSQL production paths. Existing CPU helpers and
  unrelated engines keep their contracts.

### 2.3 Acceptance Scenarios

| Given | When | Then |
| --- | --- | --- |
| Direct/TLS/Unix or cloud resource config | Both native clients connect | Auth/UTF8/Ready is real; wrong identity/proof/order/expiry fails and closes. |
| Scoped ADC credentials and expiring issued certificate | Concurrent cloud opens | Correct Admin/login scopes and single-flight renewal; no localhost or invented expiry. |
| Full scalar/container values, typed NULL and unknown type | Query/bind/COPY | Lossless values and correct type formats; no binary-as-UTF8 or float Decimal conversion. |
| Shared driver and saturated native pool | Concurrent calls, nested transaction or cancelled acquire | Exclusive real leases, bounded capacity, isolated scope and no dirty reuse. |
| Cold/warm mixed-return pipeline | Middle Parse/Bind/Execute fails or caller conversion raises | One Sync/flush batch; ordered failure/skipped outcomes; transaction rolls back. |
| Active query or COPY/portal stream | Cancel/abandon/close | Original failure preserved; server work cancelled; lease recovered or retired. |
| Empty, small, large or nested Arrow result | Select, stream or import | Metadata/counts survive; threshold applies; native IPC/capsule ownership stays valid. |
| Caller-owned slot/publication and saved checkpoint | Tail, reconnect and acknowledge | Ordered at-least-once events, stable replay IDs and durable-store-before-feedback. |
| Native PostgreSQL14–18 and modern PgBouncer modes | Driver/SQLSpec/SQLAlchemy acceptance | Actual native transactions, binds, pools and pipeline behavior pass without PostgreSQL xfails. |
| Installed optional wheel outside checkout | Import/query/uninstall on supported runtimes | Core identity and namespace survive; stubs/real GIL state match. |

### 2.4 Alternatives Considered

| Decision | Chosen seam and alternative |
| --- | --- |
| Delivery | One complete PR follows the user instruction. Partial connection PRs are excluded; atomic commits remain review boundaries. |
| Protocol | Owned state machine over postgres-protocol permits single-Sync batches. A production tokio-postgres Client would own Sync behavior and require changing that contract. |
| Integration ownership | The PostgreSQL adapter/crate owns native SQL, values, cloud material and checkpoint protocols. Feature-owned vendor packages would split one engine across consumers. |
| Configuration | Explicit typed native kwargs and one adapter DSN/bind compiler keep validation local. Generic mapping forwarding or duplicate parsers leave runtime decisions unresolved. |
| Results | Owned engine result/column/value records preserve metadata; Python base-driver projections retain established shapes. Generic untyped rows lose counts and empty descriptions. |
| Pooling | Real Rust pool leases separate session ownership from SHM admission. Reusing admission counters as a pool would not manage sockets. |
| Error policy | Raise the first canonical pipeline error with ordered outcomes. Exception rows or fabricated errors for skipped operations misstate server execution. |
| CDC | Explicit adapter-owned checkpoint store/ack preserves the generic stream contract. Ack-on-yield would falsely treat delivery as durable application. |
| Arrow | Native typed builders and IPC-to-SHM preserve bounded row processing. Python row materialization and arbitrary padded IPC fragments violate that path. |

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning tasks | Verification strategy | Canonical public seam / gate |
| --- | --- | --- | --- |
| R1 | [t01](tasks/t01-package-scaffold.md), [t26](tasks/t26-ffi-wheel-gil.md) | regression_tdd, static_validation | `src/py/tests/unit/test_freethreaded_gil.py` |
| R2 | [t02](tasks/t02-configuration-errors.md), [t19](tasks/t19-tcp-keepalive.md), [t20](tasks/t20-unix-options-projection.md), [t25](tasks/t25-constructor-parity.md), [t83](tasks/t83-dsn-config-bridge.md), [t84](tasks/t84-bind-compiler.md), [t87](tasks/t87-compiler-boundaries.md) | behavior_tdd, characterization | `src/rs/vertebra-postgres/src/config.rs`, `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs`, `src/rs/vertebra-db/src/connection.rs`, `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_options.py`, `src/py/tests/unit/db/test_postgres_binds.py`, `src/py/tests/unit/extensions/test_postgres_compiler_transport.py` |
| R3 | [t02](tasks/t02-configuration-errors.md), [t03](tasks/t03-bounded-codec.md), [t04](tasks/t04-trust-lifecycle.md), [t19](tasks/t19-tcp-keepalive.md), [t20](tasks/t20-unix-options-projection.md) | behavior_tdd | `src/rs/vertebra-postgres/src/config.rs`, `src/rs/vertebra-postgres/src/protocol.rs`, `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs`, `src/rs/vertebra-db/src/connection.rs` |
| R4 | [t05](tasks/t05-tls.md), [t08](tasks/t08-certificate-binding.md), [t11](tasks/t11-cloud-key-material.md), [t14](tasks/t14-cloud-tls-identities.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs`, `src/rs/vertebra-db/src/connection.rs` |
| R5 | [t06](tasks/t06-password-md5.md), [t07](tasks/t07-scram.md), [t08](tasks/t08-certificate-binding.md), [t09](tasks/t09-scram-plus.md), [t10](tasks/t10-cloud-material-cache.md), [t15](tasks/t15-static-login-contract.md), [t16](tasks/t16-cloud-scoped-auth.md), [t17](tasks/t17-cloudsql.md), [t18](tasks/t18-alloydb.md), [t21](tasks/t21-direct-host-iam.md) | behavior_tdd, regression_tdd | `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs`, `src/rs/vertebra-postgres/src/credentials.rs`, `src/rs/vertebra-core/tests/it/gcp_auth.rs`, `src/rs/vertebra-db/src/connection.rs` |
| R6 | [t23](tasks/t23-sync-client.md), [t25](tasks/t25-constructor-parity.md), [t26](tasks/t26-ffi-wheel-gil.md), [t47](tasks/t47-python-value-bridge.md), [t48](tasks/t48-python-query-facades.md), [t49](tasks/t49-python-pool-facades.md), [t57](tasks/t57-command-deadlines.md), [t58](tasks/t58-python-portals.md) | behavior_tdd, characterization, regression_tdd | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/test_freethreaded_gil.py`, `src/rs/vertebra-postgres/tests/python_value_contract.rs`, `src/py/tests/unit/db/test_postgres_native_queries.py`, `src/py/tests/unit/db/test_postgres_native_pools.py`, `src/rs/vertebra-postgres/tests/command_deadlines.rs`, `src/py/tests/integration/db/test_postgres_command_deadlines.py`, `src/py/tests/unit/db/test_postgres_native_portals.py` |
| R7 | [t04](tasks/t04-trust-lifecycle.md), [t22](tasks/t22-async-bridge-setup.md), [t24](tasks/t24-async-client.md), [t25](tasks/t25-constructor-parity.md), [t26](tasks/t26-ffi-wheel-gil.md), [t47](tasks/t47-python-value-bridge.md), [t48](tasks/t48-python-query-facades.md), [t49](tasks/t49-python-pool-facades.md), [t56](tasks/t56-python-cancellation.md), [t57](tasks/t57-command-deadlines.md), [t58](tasks/t58-python-portals.md), [t86](tasks/t86-async-adapter.md) | behavior_tdd, characterization, regression_tdd | `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs`, `src/rs/vertebra-db/src/py_async.rs`, `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/test_freethreaded_gil.py`, `src/rs/vertebra-postgres/tests/python_value_contract.rs`, `src/py/tests/unit/db/test_postgres_native_queries.py`, `src/py/tests/unit/db/test_postgres_native_pools.py`, `src/rs/vertebra-db/tests/py_async_cancel_tests.rs`, `src/py/tests/unit/db/test_postgres_native_cancel.py`, `src/rs/vertebra-postgres/tests/command_deadlines.rs`, `src/py/tests/integration/db/test_postgres_command_deadlines.py`, `src/py/tests/unit/db/test_postgres_native_portals.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py` |
| R8 | [t102](tasks/t102-full-review-gates.md) | static_validation |  |
| R9 | [t10](tasks/t10-cloud-material-cache.md), [t11](tasks/t11-cloud-key-material.md), [t12](tasks/t12-cloudsql-admin.md), [t13](tasks/t13-alloydb-admin.md), [t14](tasks/t14-cloud-tls-identities.md), [t15](tasks/t15-static-login-contract.md), [t16](tasks/t16-cloud-scoped-auth.md), [t17](tasks/t17-cloudsql.md), [t18](tasks/t18-alloydb.md) | behavior_tdd, regression_tdd | `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs`, `src/rs/vertebra-db/src/connection.rs`, `src/rs/vertebra-postgres/src/credentials.rs`, `src/rs/vertebra-core/tests/it/gcp_auth.rs` |
| R10 | [t27](tasks/t27-owned-results.md), [t28](tasks/t28-simple-query.md), [t57](tasks/t57-command-deadlines.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/result_contract.rs`, `src/py/tests/unit/db/test_postgres_types.py`, `src/rs/vertebra-postgres/tests/query_protocol.rs`, `src/rs/vertebra-postgres/tests/command_deadlines.rs`, `src/py/tests/integration/db/test_postgres_command_deadlines.py` |
| R11 | [t27](tasks/t27-owned-results.md), [t29](tasks/t29-scalar-codecs.md), [t30](tasks/t30-numeric-codecs.md), [t31](tasks/t31-temporal-binary.md), [t32](tasks/t32-datetime-text.md), [t33](tasks/t33-interval-text.md), [t34](tasks/t34-json-codecs.md), [t35](tasks/t35-type-catalog.md), [t36](tasks/t36-array-binary.md), [t37](tasks/t37-array-text.md), [t38](tasks/t38-composite-codecs.md), [t39](tasks/t39-pgvector-codecs.md), [t47](tasks/t47-python-value-bridge.md), [t59](tasks/t59-timezone-text-fallback.md) | behavior_tdd, regression_tdd | `src/rs/vertebra-postgres/tests/result_contract.rs`, `src/py/tests/unit/db/test_postgres_types.py`, `src/rs/vertebra-postgres/tests/scalar_codecs.rs`, `src/rs/vertebra-postgres/tests/numeric_codecs.rs`, `src/rs/vertebra-postgres/tests/temporal_codecs.rs`, `src/rs/vertebra-postgres/tests/datetime_text.rs`, `src/rs/vertebra-postgres/tests/interval_text.rs`, `src/rs/vertebra-postgres/tests/json_codecs.rs`, `src/rs/vertebra-postgres/tests/type_catalog.rs`, `src/rs/vertebra-postgres/tests/array_codecs.rs`, `src/rs/vertebra-postgres/tests/array_text.rs`, `src/rs/vertebra-postgres/tests/composite_codecs.rs`, `src/rs/vertebra-db/tests/postgres_vector_tests.rs`, `src/rs/vertebra-postgres/tests/vector_codecs.rs`, `src/rs/vertebra-postgres/tests/python_value_contract.rs`, `src/rs/vertebra-postgres/tests/timezone_text.rs`, `src/py/tests/unit/db/test_postgres_native_queries.py` |
| R12 | [t28](tasks/t28-simple-query.md), [t40](tasks/t40-cold-query-metadata.md), [t41](tasks/t41-extended-query.md), [t48](tasks/t48-python-query-facades.md), [t57](tasks/t57-command-deadlines.md), [t96](tasks/t96-advanced-service-fixtures.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/query_protocol.rs`, `src/rs/vertebra-postgres/tests/metadata_normalization.rs`, `src/rs/vertebra-postgres/tests/extended_protocol.rs`, `src/py/tests/unit/db/test_postgres_native_queries.py`, `src/rs/vertebra-postgres/tests/command_deadlines.rs`, `src/py/tests/integration/db/test_postgres_command_deadlines.py`, `src/py/tests/integration/db/test_postgres_services.py` |
| R13 | [t40](tasks/t40-cold-query-metadata.md), [t42](tasks/t42-transaction-state.md), [t48](tasks/t48-python-query-facades.md), [t50](tasks/t50-pipeline-command-guard.md), [t51](tasks/t51-pipeline-outbound.md), [t53](tasks/t53-pipeline-scope.md), [t54](tasks/t54-python-pipeline.md), [t85](tasks/t85-sync-adapter.md), [t86](tasks/t86-async-adapter.md), [t88](tasks/t88-pipeline-context.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/metadata_normalization.rs`, `src/rs/vertebra-postgres/tests/transaction_state.rs`, `src/py/tests/unit/db/test_postgres_native_queries.py`, `src/rs/vertebra-postgres/src/pipeline_guard.rs`, `src/rs/vertebra-postgres/tests/pipeline_wire.rs`, `src/rs/vertebra-postgres/tests/pipeline_scope.rs`, `src/py/tests/unit/db/test_postgres_native_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py` |
| R14 | [t45](tasks/t45-physical-pool.md), [t46](tasks/t46-pool-reset-drain.md), [t49](tasks/t49-python-pool-facades.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/physical_pool.rs`, `src/rs/vertebra-postgres/tests/pool_reset.rs`, `src/py/tests/unit/db/test_postgres_native_pools.py` |
| R15 | [t35](tasks/t35-type-catalog.md), [t40](tasks/t40-cold-query-metadata.md), [t44](tasks/t44-prepared-cache.md), [t96](tasks/t96-advanced-service-fixtures.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/type_catalog.rs`, `src/rs/vertebra-postgres/tests/metadata_normalization.rs`, `src/rs/vertebra-postgres/tests/statement_cache.rs`, `src/py/tests/unit/db/test_postgres_types.py`, `src/py/tests/integration/db/test_postgres_services.py` |
| R16 | [t43](tasks/t43-portal-streaming.md), [t58](tasks/t58-python-portals.md), [t69](tasks/t69-sync-arrow-portal.md), [t70](tasks/t70-async-arrow-portal.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/portal_protocol.rs`, `src/py/tests/unit/db/test_postgres_native_portals.py`, `src/py/tests/unit/db/test_postgres_native_data.py` |
| R17 | [t50](tasks/t50-pipeline-command-guard.md), [t51](tasks/t51-pipeline-outbound.md), [t52](tasks/t52-pipeline-outcomes.md), [t53](tasks/t53-pipeline-scope.md), [t54](tasks/t54-python-pipeline.md), [t88](tasks/t88-pipeline-context.md) | behavior_tdd | `src/rs/vertebra-postgres/src/pipeline_guard.rs`, `src/rs/vertebra-postgres/tests/pipeline_wire.rs`, `src/rs/vertebra-postgres/tests/pipeline_outcomes.rs`, `src/rs/vertebra-postgres/tests/pipeline_scope.rs`, `src/py/tests/unit/db/test_postgres_native_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py` |
| R18 | [t55](tasks/t55-cancel-engine.md), [t56](tasks/t56-python-cancellation.md), [t57](tasks/t57-command-deadlines.md), [t85](tasks/t85-sync-adapter.md), [t86](tasks/t86-async-adapter.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/cancellation_protocol.rs`, `src/rs/vertebra-db/tests/py_async_cancel_tests.rs`, `src/py/tests/unit/db/test_postgres_native_cancel.py`, `src/rs/vertebra-postgres/tests/command_deadlines.rs`, `src/py/tests/integration/db/test_postgres_command_deadlines.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py` |
| R19 | [t60](tasks/t60-incremental-copy-codec.md), [t61](tasks/t61-copy-out.md), [t62](tasks/t62-copy-in.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs` |
| R20 | [t63](tasks/t63-arrow-schema.md), [t64](tasks/t64-arrow-scalars.md), [t65](tasks/t65-arrow-nested.md), [t66](tasks/t66-arrow-ipc-shm.md), [t67](tasks/t67-arrow-import.md), [t68](tasks/t68-arrow-input-bridge.md), [t69](tasks/t69-sync-arrow-portal.md), [t70](tasks/t70-async-arrow-portal.md), [t89](tasks/t89-arrow-and-owned-binds.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs`, `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` |
| R21 | [t71](tasks/t71-notification-owner.md), [t72](tasks/t72-notification-bridge.md), [t73](tasks/t73-notification-source.md), [t90](tasks/t90-driver-event-hooks.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs`, `src/py/tests/unit/db/test_postgres_native_notifications.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py` |
| R22 | [t74](tasks/t74-replication-copyboth.md), [t75](tasks/t75-pgoutput-types.md), [t76](tasks/t76-pgoutput-transactions.md), [t77](tasks/t77-replication-feedback.md), [t78](tasks/t78-replication-resume.md), [t79](tasks/t79-replication-bridge.md), [t80](tasks/t80-change-source.md), [t81](tasks/t81-checkpoint-ack-reconciliation.md), [t82](tasks/t82-change-source-recovery.md), [t90](tasks/t90-driver-event-hooks.md), [t95](tasks/t95-live-logical-replication.md) | behavior_tdd | `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs`, `src/py/tests/unit/db/test_postgres_change_source.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py`, `src/py/tests/integration/db/test_postgres_replication.py` |
| R23 | [t79](tasks/t79-replication-bridge.md), [t80](tasks/t80-change-source.md), [t81](tasks/t81-checkpoint-ack-reconciliation.md), [t82](tasks/t82-change-source-recovery.md), [t90](tasks/t90-driver-event-hooks.md), [t95](tasks/t95-live-logical-replication.md) | behavior_tdd | `src/py/tests/unit/db/test_postgres_change_source.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py`, `src/py/tests/integration/db/test_postgres_replication.py` |
| R24 | [t84](tasks/t84-bind-compiler.md), [t85](tasks/t85-sync-adapter.md), [t86](tasks/t86-async-adapter.md), [t87](tasks/t87-compiler-boundaries.md), [t88](tasks/t88-pipeline-context.md), [t89](tasks/t89-arrow-and-owned-binds.md), [t90](tasks/t90-driver-event-hooks.md), [t91](tasks/t91-port-migration.md), [t92](tasks/t92-remove-legacy-async.md), [t93](tasks/t93-remove-legacy-sync.md) | behavior_tdd, characterization | `src/py/tests/unit/db/test_postgres_binds.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_postgres_compiler_transport.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py`, `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| R25 | [t94](tasks/t94-pg14-18-conformance.md), [t95](tasks/t95-live-logical-replication.md), [t96](tasks/t96-advanced-service-fixtures.md), [t97](tasks/t97-live-feature-acceptance.md) | behavior_tdd | `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/db/test_postgres_native.py`, `src/py/tests/integration/db/test_postgres_replication.py`, `src/py/tests/integration/db/test_postgres_services.py`, `src/py/tests/integration/db/test_postgres_features.py` |
| R26 | [t26](tasks/t26-ffi-wheel-gil.md), [t98](tasks/t98-release-ci.md) | regression_tdd, static_validation | `src/py/tests/unit/test_freethreaded_gil.py`, `tools/scripts/verify_postgres_wheel.py` |
| R27 | [t99](tasks/t99-postgres-benchmark-harness.md), [t100](tasks/t100-native-benchmarks.md), [t101](tasks/t101-docs-and-contracts.md), [t102](tasks/t102-full-review-gates.md) | behavior_tdd, static_validation | `src/py/tests/unit/tools/test_postgres_benchmarks.py`, `src/rs/vertebra-postgres/benches/postgres_native.rs`, `src/py/tests/integration/db/test_postgres_benchmarks.py` |

## 4. Implementation Plan

Every worksheet is one invocation and one signed local conventional commit. The
number of worksheets preserves the complete one-PR scope without oversized executor
turns. Estimates near400 lines include tests; deletion-only cleanup may be larger
when required for one coherent removal. Generated locks are separately reviewed.
No two ready tasks mutate overlapping files. The final serialized dependency chain
is authoritative; review/research may run in parallel, and execution parallelism
requires a later worksheet revision demonstrating disjoint ownership.


### Phase 1: Native connections, authentication, cloud material and FFI

- [ ] t01 — Register the standalone optional PostgreSQL wheel (tasks/t01-package-scaffold.md)
- [ ] t02 — Validate native connection options and error categories (tasks/t02-configuration-errors.md)
- [ ] t03 — Bound startup framing and backend message decoding (tasks/t03-bounded-codec.md)
- [ ] t04 — Establish authenticated trust sessions with publication ownership (tasks/t04-trust-lifecycle.md)
- [ ] t05 — Negotiate PostgreSQL SSLRequest and verify TLS identities (tasks/t05-tls.md)
- [ ] t06 — Authenticate cleartext and MD5 startup exchanges (tasks/t06-password-md5.md)
- [ ] t07 — Authenticate ordered SCRAM-SHA-256 exchanges (tasks/t07-scram.md)
- [ ] t08 — Derive tls-server-end-point binding from the verified peer certificate (tasks/t08-certificate-binding.md)
- [ ] t09 — Select and verify SCRAM-SHA-256-PLUS channel binding (tasks/t09-scram-plus.md)
- [ ] t10 — Cache cloud connection material under one total deadline (tasks/t10-cloud-material-cache.md)
- [ ] t11 — Generate real cloud public keys and validate issued certificate material (tasks/t11-cloud-key-material.md)
- [ ] t12 — Discover Cloud SQL addresses and issue scoped ephemeral certificates (tasks/t12-cloudsql-admin.md)
- [ ] t13 — Discover AlloyDB network targets and issue metadata-enabled certificates (tasks/t13-alloydb-admin.md)
- [ ] t14 — Enforce cloud TLS1.3 and service-specific peer identities (tasks/t14-cloud-tls-identities.md)
- [ ] t15 — Validate designated static login scope and actual lifetime (tasks/t15-static-login-contract.md)
- [ ] t16 — Create scope-correct cloud Admin and login credential caches (tasks/t16-cloud-scoped-auth.md)
- [ ] t17 — Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens (tasks/t17-cloudsql.md)
- [ ] t18 — Authenticate automatic AlloyDB metadata and IAM sessions (tasks/t18-alloydb.md)
- [ ] t19 — Apply validated TCP keepalive controls to physical sessions (tasks/t19-tcp-keepalive.md)
- [ ] t20 — Project complete connection options and establish Unix startup (tasks/t20-unix-options-projection.md)
- [ ] t21 — Refresh direct-host IAM passwords without changing transport policy (tasks/t21-direct-host-iam.md)
- [ ] t22 — Abort shared database workers when Python completion setup fails (tasks/t22-async-bridge-setup.md)
- [ ] t23 — Expose physical synchronous native PostgreSQL sessions (tasks/t23-sync-client.md)
- [ ] t24 — Expose async physical sessions with cancellation-safe publication (tasks/t24-async-client.md)
- [ ] t25 — Characterize the complete sync and async constructor contract (tasks/t25-constructor-parity.md)
- [ ] t26 — Verify raw-session wheel parity and isolated free-threaded imports (tasks/t26-ffi-wheel-gil.md)

**Phase 1 Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`, `make dev`, `uv run --no-sync python -m mypy.stubtest vertebra._postgres`, and focused isolated-GIL tests from the owning worksheet exit0. Startup, cloud mock APIs and TLS transcript evidence remain separate from live cloud execution.


### Phase 2: Descriptions and lossless value codecs

- [ ] t27 — Owned query results and PostgreSQL value records (tasks/t27-owned-results.md)
- [ ] t28 — Simple query completion and canonical SQLSTATE errors (tasks/t28-simple-query.md)
- [ ] t29 — Scalar binary and text value codecs (tasks/t29-scalar-codecs.md)
- [ ] t30 — Arbitrary precision numeric binary and text codecs (tasks/t30-numeric-codecs.md)
- [ ] t31 — Lossless temporal and interval binary codecs (tasks/t31-temporal-binary.md)
- [ ] t32 — Temporal text decoding across session output styles (tasks/t32-datetime-text.md)
- [ ] t33 — Lossless interval text for all PostgreSQL styles (tasks/t33-interval-text.md)
- [ ] t34 — JSON and versioned JSONB codecs (tasks/t34-json-codecs.md)
- [ ] t35 — Session type discovery and format negotiation (tasks/t35-type-catalog.md)
- [ ] t36 — Dimensioned binary arrays with exact bounds (tasks/t36-array-binary.md)
- [ ] t37 — Escaped multidimensional text arrays (tasks/t37-array-text.md)
- [ ] t38 — Typed ordered composite binary and text codecs (tasks/t38-composite-codecs.md)
- [ ] t39 — Validated vector halfvec and bit send formats (tasks/t39-pgvector-codecs.md)
- [ ] t40 — Cold query parameter discovery and owned result normalization (tasks/t40-cold-query-metadata.md)
- [ ] t41 — Extended query parameter description and mixed formats (tasks/t41-extended-query.md)

**Phase 2 Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres` and the codec/owned-value Python test modules in these worksheets exit0, including malformed/null/range/style/type-identity cases.


### Phase 3: Queries, transactions, pools, portals, pipelines and cancellation

- [ ] t42 — Pinned transaction and savepoint command state (tasks/t42-transaction-state.md)
- [ ] t43 — Bounded portals that retain session ownership (tasks/t43-portal-streaming.md)
- [ ] t44 — Session statement cache with modern pooler capability (tasks/t44-prepared-cache.md)
- [ ] t45 — Concrete Rust physical connection pool and leases (tasks/t45-physical-pool.md)
- [ ] t46 — Rollback reset and draining pool shutdown (tasks/t46-pool-reset-drain.md)
- [ ] t47 — Owned Python value extraction and typed result projection (tasks/t47-python-value-bridge.md)
- [ ] t48 — Typed sync and async query transaction facades (tasks/t48-python-query-facades.md)
- [ ] t49 — Native pool facades carrying real session leases (tasks/t49-python-pool-facades.md)
- [ ] t50 — Native pipeline SQL-head command guard (tasks/t50-pipeline-command-guard.md)
- [ ] t51 — Single-Sync cold and warm pipeline submission (tasks/t51-pipeline-outbound.md)
- [ ] t52 — Ordered pipeline results errors and skipped outcomes (tasks/t52-pipeline-outcomes.md)
- [ ] t53 — Pipeline transaction scope through caller result conversion (tasks/t53-pipeline-scope.md)
- [ ] t54 — Python native pipeline capability contexts (tasks/t54-python-pipeline.md)
- [ ] t55 — Protocol cancellation and delayed-request retirement (tasks/t55-cancel-engine.md)
- [ ] t56 — Explicit and asyncio cancellation with native cleanup (tasks/t56-python-cancellation.md)
- [ ] t57 — Optional native command I/O deadlines (tasks/t57-command-deadlines.md)
- [ ] t58 — Typed sync and async portal facades (tasks/t58-python-portals.md)
- [ ] t59 — Captured timezone text and exact ambiguous temporal fallback (tasks/t59-timezone-text-fallback.md)

**Phase 3 Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`, native Python pool/pipeline/cancel/portal modules, and native stubtest exit0. Wire evidence counts one Sync/flush independently; cancellation verifies server effects and physical retirement.


### Phase 4: Binary COPY and native Arrow data plane

- [ ] t60 — Frame binary COPY rows incrementally (tasks/t60-incremental-copy-codec.md)
- [ ] t61 — Own COPY TO streaming and recovery (tasks/t61-copy-out.md)
- [ ] t62 — Send COPY FROM with duplex backpressure and abort recovery (tasks/t62-copy-in.md)
- [ ] t63 — Define lossless PostgreSQL Arrow schemas (tasks/t63-arrow-schema.md)
- [ ] t64 — Build bounded native Arrow scalar batches (tasks/t64-arrow-scalars.md)
- [ ] t65 — Preserve typed arrays composites and vectors in Arrow (tasks/t65-arrow-nested.md)
- [ ] t66 — Serialize complete Arrow IPC messages into SHM (tasks/t66-arrow-ipc-shm.md)
- [ ] t67 — Convert Arrow batches into exact COPY field values (tasks/t67-arrow-import.md)
- [ ] t68 — Stream Arrow input buffers into native COPY (tasks/t68-arrow-input-bridge.md)
- [ ] t69 — Expose synchronous Arrow selection and pinned batch streams (tasks/t69-sync-arrow-portal.md)
- [ ] t70 — Own asynchronous Arrow portal cancellation (tasks/t70-async-arrow-portal.md)

**Phase 4 Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`, focused Python PostgreSQL Arrow/COPY modules and `make dev`/stubtest exit0. Check inline/SHM counts, bounded continuation, IPC framing, retained views and producer cleanup.


### Phase 5: Live notifications, logical replication and checkpoints

- [ ] t71 — Pump notifications on a dedicated bounded session (tasks/t71-notification-owner.md)
- [ ] t72 — Expose async notification handles and reconnect gaps (tasks/t72-notification-bridge.md)
- [ ] t73 — Integrate live notification sources with active subscriptions (tasks/t73-notification-source.md)
- [ ] t74 — Start dedicated CopyBoth logical replication sessions (tasks/t74-replication-copyboth.md)
- [ ] t75 — Decode pgoutput relation metadata and lossless tuples (tasks/t75-pgoutput-types.md)
- [ ] t76 — Order logical transactions and change envelopes (tasks/t76-pgoutput-transactions.md)
- [ ] t77 — Acknowledge only completed durable commit boundaries (tasks/t77-replication-feedback.md)
- [ ] t78 — Reconnect logical streams from verified checkpoints (tasks/t78-replication-resume.md)
- [ ] t79 — Expose cancellation-safe native replication handles (tasks/t79-replication-bridge.md)
- [ ] t80 — Implement PostgreSQL change sources and checkpoint stores (tasks/t80-change-source.md)
- [ ] t81 — Serialize checkpoint acknowledgements and uncertain outcomes (tasks/t81-checkpoint-ack-reconciliation.md)
- [ ] t82 — Preserve change-source replay identity and shutdown semantics (tasks/t82-change-source-recovery.md)

**Phase 5 Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres` plus notification/replication/checkpoint Python modules exit0, including replay/ack failure/backpressure/cleanup. Live WAL acceptance remains required in Phase7.


### Phase 6: Native adapter and consumer cutover

- [ ] t83 — Bridge PostgreSQL URL and libpq configuration into native options (tasks/t83-dsn-config-bridge.md)
- [ ] t84 — Lower PostgreSQL legacy binds with a lexer-aware adapter compiler (tasks/t84-bind-compiler.md)
- [ ] t85 — Implement the permanent synchronous native PostgreSQL driver before activation (tasks/t85-sync-adapter.md)
- [ ] t86 — Implement the permanent asynchronous native PostgreSQL driver before activation (tasks/t86-async-adapter.md)
- [ ] t87 — Prepare native PostgreSQL compiler boundaries without changing public defaults (tasks/t87-compiler-boundaries.md)
- [ ] t88 — Bind permanent native driver pipeline factories to transaction ownership (tasks/t88-pipeline-context.md)
- [ ] t89 — Complete native Arrow forwarding before public PostgreSQL activation (tasks/t89-arrow-and-owned-binds.md)
- [ ] t90 — Open and close dedicated native PostgreSQL event sessions from drivers (tasks/t90-driver-event-hooks.md)
- [ ] t91 — Activate both permanent native PostgreSQL drivers and consumers atomically (tasks/t91-port-migration.md)
- [ ] t92 — Remove unreachable legacy async PostgreSQL implementation (tasks/t92-remove-legacy-async.md)
- [ ] t93 — Remove unreachable legacy sync PostgreSQL implementation (tasks/t93-remove-legacy-sync.md)

**Phase 6 Recovery Checkpoint:** `make dev`, PostgreSQL adapter/SQLSpec/SQLAlchemy regression modules named in these worksheets, native stubtest and `uv lock --check` exit0. Forbidden old production execution paths are absent; shared engine-purity checks remain green.


### Phase 7: Real service, wheel, performance and complete-driver acceptance

- [ ] t94 — Activate strict native PostgreSQL 14 through 18 driver conformance (tasks/t94-pg14-18-conformance.md)
- [ ] t95 — Accept live PostgreSQL logical replication and durable source recovery (tasks/t95-live-logical-replication.md)
- [ ] t96 — Provide local PostgreSQL vector and transaction-pooler acceptance services (tasks/t96-advanced-service-fixtures.md)
- [ ] t97 — Accept native bulk notifications vectors and transaction poolers live (tasks/t97-live-feature-acceptance.md)
- [ ] t98 — Verify complete native PostgreSQL wheels and strict CI service lanes (tasks/t98-release-ci.md)
- [ ] t99 — Implement reproducible PostgreSQL benchmark capture and comparison harness (tasks/t99-postgres-benchmark-harness.md)
- [ ] t100 — Measure native PostgreSQL connection query and pipeline performance (tasks/t100-native-benchmarks.md)
- [ ] t101 — Document complete native PostgreSQL installation behavior and operations (tasks/t101-docs-and-contracts.md)
- [ ] t102 — Run complete native PostgreSQL correctness and fresh quality acceptance (tasks/t102-full-review-gates.md)

**Phase 7 Recovery Checkpoint:** All exact PG14–18, logical/vector/PgBouncer, installed-wheel, free-threaded and driver-benchmark commands in the worksheets pass. Run `make lint-all`, `make rs-test`, `make py-test`, `make ts-test`, `make docs`, driver benchmarks and the before/after comparison. Complete correctness review, then a fresh independent quality review. Unrelated pre-existing skips/xfails are reported; the PostgreSQL acceptance lanes have none. Serialize SHM-cleaning suites. Complete/archive this flow on the feature branch only after its full acceptance.


The [planning review and adjudication](review.md) records adopted findings, independent review results and structural verification limits.

## 5. Continuity Snapshot

- State is `planned`; all 102 worksheets are `pending`, `plan_revision: 2`, anchored at
  `plan_commit: cdb978d3`. No native PostgreSQL
  production implementation or conformance acceptance exists yet.
- Delivery is the complete driver on one branch/PR (`feat/native-postgres-driver`).
  Intermediate tasks do not complete/archive this flow or enable a partial native adapter.
- Merged foundation PRs [#76](https://github.com/cofin/vertebra/pull/76) (`21c99f95`) and
  [#77](https://github.com/cofin/vertebra/pull/77) (`80ff8a65`) cover the preceding source only;
  this plan requires its own execution evidence.
- Primary-source research and read-only PG18.3 prototypes establish protocol/API
  facts. They are not PG14–18, cloud-service, installed-wheel or performance acceptance.
- Next action on its stacked branch: `/flow:implement native-postgres-driver` from `t01`.

[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP, Unix, TLS, and cloud mTLS stream primitives.
[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — Existing PostgreSQL frame codecs and admission metadata in vertebra-db.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared AsyncCompletionQueue and independent event-loop drainer.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python SyncPostgreSQLDriver and AsyncPostgreSQLDriver target cutover module.
