---
type: Review
flow_id: native-postgres-driver
title: Native PostgreSQL planning review
description: Adjudication and validation of the 36-chapter complete-driver plan and revision 8 replication seams.
status: draft
state: active
plan_revision: 8
plan_commit: 5eb75b2a
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame
    title: Shared PostgreSQL wire frame helpers in vertebra-db
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Upstream flow that deletes PostgreSQL memory:// paths and vertebra_db::postgres::PostgresPool
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python PostgreSQL driver target cutover seam
tags:
  - postgres
  - planning
  - driver
---

# Planning review

<!-- truth: start -->
- Revision 8 is a draft blueprint at baseline `5eb75b2a`; approval remains pending. The roadmap has `36` chapters and `129` tasks, with `22` reviewed chapters and `82` completed tasks [^pg-core-rs].
- Replication execution order is `ch23` feedback, `ch34` bounded session, `ch35` verified supervision, `ch36` Python bridge, then `ch24` checkpoint sources. The public Python drivers retain their legacy implementation until `ch27` [^pg-driver-py].
- Planning validation proves blueprint consistency; live services, wheels, benchmarks, and final release acceptance remain downstream gates [^fail-loud-baseline].
<!-- truth: end -->

This review covers the root roadmap, frozen contracts, and replication chapter
worksheets at revision 8. Completed task and chapter attestations remain unchanged.

## Adjudication

The contract decisions below are ADOPT. Revision 8 awaits fresh blueprint review and user approval.

| Finding | Correction and verification contract |
| --- | --- |
| Cold statement and backend lifetime | Resolve required parameter metadata before `Parse`; execute cold text results before optional enrichment; retain completed results on later failure and never replay SQL. Named preflight pins or re-`Parse`s (`ch12:t02`). |
| SCRAM downgrade detection | `prefer` uses GS2 `y` when binding is usable and `PLUS` is absent (`n` on Ed25519 or disabled binding); wire assertions verify downgrade rejection (`ch03:t03`). |
| Pipeline boundary before adapter cutover | Native pipeline (`ch15`–`ch16`) owns scope and single-`Sync` framing; Python adapters (`ch25`–`ch26`) bind transaction/savepoint compiler input cleanly. |
| Lossless public value records | Contracts freeze units, discriminants, array bounds, vector/bit carriers, JSON null, nested conversion, and unresolved `PgTemporal(special="wire_text")` typed-rebind refusal (`contracts.md`, `ch09`–`ch12`, `ch14:t03`, `ch17:t04`). |
| Static login token expiry & AlloyDB staged TLS | Require real `expires_at_unix_ms` and matching login scopes (`ch05:t02`); verify scoped login usability between TLS 1.3 peer verification and `MetadataExchange` while preserving TCP keepalives via `DialedCloudTransport` (`ch06:t01`–`t02`). |
| Feature-gated cancellation & pool identity | Enable required PyO3 test features (`ch16:t04`) and enumerate cache, `PoolerMode`, sizing, timeout, TLS, GUC, and cloud/IAM fields in canonical configuration validation (`ch14`–`ch15:t01`). |
| Delayed pipeline commit | Validate every SQL head before frame emission (`ch15:t02`), reject transaction control and `COPY`, retain scope through caller conversion, and roll back on caught server or conversion failures (`ch16:t01`–`t02`). |
| Upstream `driver-fail-loud` separation | `driver-fail-loud` deletes `_in_memory_rows`, PostgreSQL `memory://` branches, and `vertebra_db::postgres::PostgresPool` before `ch01`; `ch14`, `ch22`, `ch27`, and `ch30`–`ch32` assume them absent [^fail-loud-baseline]. |
| Cargo lock, error/result types, Arrow sink & deadlines | Pin `vertebra-postgres` and `vertebra-db` dependencies upfront (`ch01`), stabilize `PostgresError::Data`/`ServerError`/`TransactionStatus`, stream Arrow via `query_with_row_sink` (`ch19`), and anchor reservation-started command deadlines in `session.rs` (`ch17:t01`–`t02`). |
| `ch20` Arrow import, `select_to_arrow` & portal fencing | Align `copy_from_arrow(table_name, first_batch_ipc, *, columns=None, schema_name=None)` (`write_arrow_ipc`, `finish`, `close`/`aclose` with `CopyFail` recovery), `select_to_arrow(sql, parameters=None, *, parameter_oids=None, timeout=None, threshold_rows=256, max_result_bytes=268_435_456)`, and generation-fenced sync/async Arrow portals (`ch20:t01`–`t04`, `contracts.md`). |
| `ch21`–`ch23` standalone replication, `PoolerMode`, drop guards & catalog retention | Open dedicated `replication=database` startup sessions, enforce `PoolerMode` rejects on dedicated features, guard cancelled async `next()` reads with `PendingDeliveryGuard`, keep `_postgres.pyi` `Awaitable[...]` stubs exact, retain `'Y'` `pg_type` entries across `DISCARD ALL`, coalesce `read_buf` framing, and declare `PostgresReplicationRecord(msgspec.Struct, frozen=True)` without `gc=False` (`ch21`–`ch23`, `contracts.md`). |
| `ch24` durable checkpoint & `change_stream.py` ownership | Own `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` in `vertebra.db.adapters.postgres.change_stream` with shielded CAS save-before-feedback (`ch24:t01`–`t03`). |
| `ch25`–`ch27` unified `$n`/`numeric_dollar`, `EventSessionRegistry`, queue & cutover seams | Freeze `PG*`/OS DSN defaults and `E'...'` pyformat lexing (`ch25:t01`–`t02`), set SQLSpec `ParameterStyle.NUMERIC` and SQLAlchemy `default_paramstyle = paramstyle = "numeric_dollar"` with Liskov-preserving `select_to_arrow` (`ch26:t01`, `ch26:t03`), close dedicated sessions via `await client.close()` in `EventSessionRegistry` (`ch26:t04`), resolve optional imports via `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, cut `PostgreSQLQueueBackend`'s `executor is None` dependency guard over to `vertebra._postgres` while keeping `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected, update cross-suite unit/docstring checks, replace the SQLite-backed asyncpg and psycopg test doubles, delete the legacy async/sync driver bodies and the dialer `psycopg`/`asyncpg` connect factories, and trim the split-state caveats from `postgres-limitations.md` (`ch27:t01`–`t04`); the native queue store belongs to `queues-tasks-and-channels` and DB-API extraction to `broker-pool-sharing`. |
| `ch28`–`ch29` strict Docker zero-skip gate, `PgBouncer` SCRAM, `pgvector`, GIL & `postgres-bench` | Gate live PG14–18/replication/service suites with `VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, extract `PgBouncer` `1.26.0` SCRAM verifiers from `pg_authid.rolpassword`, pin multi-arch `pgvector` `0.8.1` digests (`ch28:t01`–`t04`), assert standard-GIL `FREE_THREADED is _is_free_threaded_runtime()` alongside isolated `3.14t`/`3.15t` disabled-GIL wheel probes (`ch29:t01`), and isolate `psycopg[binary]==3.2.13` / `asyncpg==0.31.0` in `[dependency-groups].postgres-bench` (`python_version == '3.12'`) with `[tool.mypy]` overrides (`ch29:t02`–`t03`). |
| `ch30`–`ch33` `33`-file docs/OKF/limitations/routing split & `DOC-07` gate | Partition `33` invalidated user docs (`verify_docs_artifacts.py` `H2+H3 <= 14`, reference HTML `<= 120,000` bytes, tabbed installs, zero external driver names in `docs/`), workflow/`AGENTS.md`, split `postgres-native-driver.md` + `postgres-limitations.md` + indexes, testing/product/queue OKF, and `_postgres` FFI/`ADR-004`/`pyo3-bridge`/`areas.toml`/`checklist.md` updates across `ch30` (`12`), `ch31` (`12`), and `ch32` (`9`), then run `make review-route BASE=c8722c76`, dual reviews, and full gates in `ch33:t01` while keeping resident `spec.md` `state: active` (`current_task: null`) for `DOC-07` compliance ahead of `/flow:finish` -> `/flow:archive`. |
| Split state after the PR #104 squash | Re-anchor the plan to `c8722c76`, map pre-squash task SHAs to it, record the split state in `postgres-limitations.md` (`ch22:t04`), wire the test-only `pipeline.py`/`arrow.py` into the permanent drivers (`ch26:t02`–`t03`), and replace `src/py/vertebra/build/packaging/pgo.py` references with `tools/pgo/rust_pgo.py::discover_extension_manifests`, which already includes `vertebra-postgres` in `make build-pgo`. |
| Decoder baseline | Chapter 22 is reviewed at `9c1865b7`; `PgOutputDecoder` and `BoundedReplicationQueue` supply ordered transaction records to the planned session pump. |

## Revision 8 review and validation

| Finding | Adjudication | Concrete plan contract |
| --- | --- | --- |
| Missing public stream seam | ADOPT | `ReplicationSession` owns split transport and bounded delivery; `ReplicationDelivery` retains the waiter until publication or restoration. |
| Drop-safe ownership transfer | ADOPT | Consuming `into_stream` transfers the real transport and buffered startup bytes without unsafe extraction or dummy connections. |
| Oversized feedback/session/supervisor/bridge worksheet | ADOPT | Four chapters contain 2, 4, 3, and 4 bounded worksheets respectively; the full flow has 129 tasks. |
| Generation and cancellation safety | ADOPT | A supervisor delivery lease fences publication; a future-aware mapper rechecks cancellation after Python constructors and before publication. A synchronous handle reservation preserves invocation order. |
| Session lifetime and task ordering | ADOPT | Normal publication is implemented with the read pump; external leases retain borrowed records until Drop, while shutdown fences them. Session cancellation supports supervisor abort without a runtime. |
| Resume classification | ADOPT | Frame-boundary transport loss is replayable; truncated frames, invalid clean EOF, and server `08P01` remain terminal. Initial feedback derives from the connection-owned successful START LSN. |

Independent chapter blueprint reviews pass after adopted test-matrix, publication-order, cancellation, EOF-classification, and ownership fixes. Deterministic worksheet bounds/count checks, review-check, whitespace checks, and the Flow stop gate pass. The final independent cross-chapter reread passes with zero remaining Critical or Important findings. No runtime
implementation or acceptance is claimed by this planning revision. Approval stamps
`status: stable` and re-anchors `plan_commit` before `/flow:implement native-postgres-driver`.

[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` — Shared PostgreSQL wire frame helpers in `vertebra-db`.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Upstream flow that deletes PostgreSQL `memory://` paths and `vertebra_db::postgres::PostgresPool`.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver target cutover seam.
