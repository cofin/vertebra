---
type: Review
flow_id: native-postgres-driver
title: Native PostgreSQL planning review
description: Adjudication and validation of the nested 33-chapter complete-driver implementation plan.
status: stable
state: active
plan_revision: 6
plan_commit: 8e9b324e
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-10T14:45:00Z"
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 8e9b324e
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
- **Adjudicated Active Plan Baseline (`plan_revision: 6`)**: All planning and execution-reconciliation findings are adopted across `spec.md`, `contracts.md`, and `33` nested chapters (`ch01`–`ch33`, `118` vertical-slice worksheets) at `state: active`, `has_chapters: true`, and `plan_commit: 8e9b324e`; `19/33` chapters (`71/118` tasks) are completed through `ch19-native-arrow-batches-and-shm` (`59a6dc09`), `ch20`–`ch33` (`14` chapters, `47` tasks) and `[0/33 files documented]` remain planned [^pg-core-rs] [^pg-driver-py].
- **Scope of Evidence**: Chapters `ch01`–`ch19` carry verified local unit and wire-harness commits through `59a6dc09`; Arrow import/portals, notifications, `pgoutput` v1 replication, Python adapter/queue cutover, strict zero-skip PG14–18/service conformance, outside-checkout wheels, PG18 benchmarks, `33`-file docs/OKF/skill sync, and final `213967be..HEAD` correctness and quality reviews execute across `ch20`–`ch33`.
<!-- truth: end -->

This review governs the root specification, chapter catalog, chapter specifications,
and task worksheets at `plan_revision: 6`. The complete driver ships in one PR on
`feat/native-postgres-driver` stacked above `feat/driver-fail-loud` (`213967be`);
individual worksheets define atomic local signed commits.

## Adjudication

All findings below are ADOPT. No product decision remains unresolved.

| Finding | Correction and verification contract |
| --- | --- |
| Cold statement and backend lifetime | Resolve required parameter metadata before `Parse`; execute cold text results before optional enrichment; retain completed results on later failure and never replay SQL. Named preflight pins or re-`Parse`s (`ch12:t02`). |
| SCRAM downgrade detection | `prefer` uses GS2 `y` when binding is usable and `PLUS` is absent (`n` on Ed25519 or disabled binding); wire assertions verify downgrade rejection (`ch03:t03`). |
| Pipeline boundary before adapter cutover | Native pipeline (`ch15`–`ch16`) owns scope and single-`Sync` framing; Python adapters (`ch25`–`ch26`) bind transaction/savepoint compiler input cleanly. |
| Lossless public value records | Contracts freeze units, discriminants, array bounds, vector/bit carriers, JSON null, nested conversion, and unresolved `PgTemporal(special="wire_text")` typed-rebind refusal (`contracts.md`, `ch09`–`ch12`, `ch14:t03`, `ch17:t04`). |
| Static login token expiry & AlloyDB staged TLS | Require real `expires_at_unix_ms` and matching login scopes (`ch05:t02`); verify scoped login usability between TLS 1.3 peer verification and `MetadataExchange` while preserving TCP keepalives via `DialedCloudTransport` (`ch06:t01`–`t02`). |
| Feature-gated cancellation & pool identity | Enable required PyO3 test features (`ch16:t04`) and enumerate cache, `PoolerMode`, sizing, timeout, TLS, GUC, and cloud/IAM fields in canonical configuration validation (`ch14`–`ch15:t01`). |
| Delayed pipeline commit | Validate every SQL head before frame emission (`ch15:t02`), reject transaction control and `COPY`, retain scope through caller conversion, and roll back on caught server or conversion failures (`ch16:t01`–`t02`). |
| Upstream `driver-fail-loud` separation | `driver-fail-loud` (`213967be`) deletes `_in_memory_rows`, PostgreSQL `memory://` branches, and `vertebra_db::postgres::PostgresPool` before `ch01`; `ch14`, `ch22`, `ch27`, and `ch30`–`ch32` assume them absent [^fail-loud-baseline]. |
| Cargo lock, error/result types, Arrow sink & deadlines | Pin `vertebra-postgres` and `vertebra-db` dependencies upfront (`ch01`), stabilize `PostgresError::Data`/`ServerError`/`TransactionStatus`, stream Arrow via `query_with_row_sink` (`ch19`), and anchor reservation-started command deadlines in `session.rs` (`ch17:t01`–`t02`). |
| `ch20` Arrow import, `select_to_arrow` & portal fencing | Align `copy_from_arrow(table_name, first_batch_ipc, *, columns=None, schema_name=None)` (`write_arrow_ipc`, `finish`, `close`/`aclose` with `CopyFail` recovery), `select_to_arrow(sql, parameters=None, *, parameter_oids=None, timeout=None, threshold_rows=256, max_result_bytes=268_435_456)`, and generation-fenced sync/async Arrow portals (`ch20:t01`–`t04`, `contracts.md`). |
| `ch21`–`ch23` standalone replication, `PoolerMode`, drop guards & catalog retention | Open dedicated `replication=database` startup sessions, enforce `PoolerMode` rejects on dedicated features, guard cancelled async `next()` reads with `PendingDeliveryGuard`, keep `_postgres.pyi` `Awaitable[...]` stubs exact, retain `'Y'` `pg_type` entries across `DISCARD ALL`, coalesce `read_buf` framing, and declare `PostgresReplicationRecord(msgspec.Struct, frozen=True)` without `gc=False` (`ch21`–`ch23`, `contracts.md`). |
| `ch24` durable checkpoint & `change_stream.py` ownership | Own `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` in `vertebra.db.adapters.postgres.change_stream` with shielded CAS save-before-feedback (`ch24:t01`–`t03`). |
| `ch25`–`ch27` unified `$n`/`numeric_dollar`, `EventSessionRegistry`, queue & cutover seams | Freeze `PG*`/OS DSN defaults and `E'...'` pyformat lexing (`ch25:t01`–`t02`), set SQLSpec `ParameterStyle.NUMERIC` and SQLAlchemy `default_paramstyle = paramstyle = "numeric_dollar"` with Liskov-preserving `select_to_arrow` (`ch26:t01`, `ch26:t03`), close dedicated sessions via `await client.close()` in `EventSessionRegistry` (`ch26:t04`), resolve optional imports via `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, cut `PostgreSQLQueueBackend`'s `executor is None` dependency guard over to `vertebra._postgres` while keeping `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected, update cross-suite unit/docstring checks, and delete all legacy async/sync driver bodies (`ch27:t01`–`t04`). |
| `ch28`–`ch29` strict Docker zero-skip gate, `PgBouncer` SCRAM, `pgvector`, GIL & `postgres-bench` | Gate live PG14–18/replication/service suites with `VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, extract `PgBouncer` `1.26.0` SCRAM verifiers from `pg_authid.rolpassword`, pin multi-arch `pgvector` `0.8.1` digests (`ch28:t01`–`t04`), assert standard-GIL `FREE_THREADED is _is_free_threaded_runtime()` alongside isolated `3.14t`/`3.15t` disabled-GIL wheel probes (`ch29:t01`), and isolate `psycopg[binary]==3.2.13` / `asyncpg==0.31.0` in `[dependency-groups].postgres-bench` (`python_version == '3.12'`) with `[tool.mypy]` overrides (`ch29:t02`–`t03`). |
| `ch30`–`ch33` `33`-file docs/OKF/limitations/routing split & `DOC-07` gate | Partition `33` invalidated user docs (`verify_docs_artifacts.py` `H2+H3 <= 14`, reference HTML `<= 120,000` bytes, tabbed installs, zero external driver names in `docs/`), workflow/`AGENTS.md`, split `postgres-native-driver.md` + `postgres-limitations.md` + indexes, testing/product/queue OKF, and six-extension FFI/`ADR-004`/`pyo3-bridge`/`areas.toml`/`checklist.md` updates across `ch30` (`12`), `ch31` (`12`), and `ch32` (`9`), then run `make review-route BASE=213967be`, dual reviews, and full gates in `ch33:t01` while keeping resident `spec.md` `state: active` (`current_task: null`) for `DOC-07` compliance ahead of `/flow:finish` -> `/flow:archive`. |

## Review and validation evidence

- Chapters `ch01`–`ch19` (`71` tasks) are implemented, verified, and reviewed through `ch19-native-arrow-batches-and-shm` (`59a6dc09`).
- `plan_revision: 6` (`plan_commit: 8e9b324e`) reconciles remaining chapters `ch20`–`ch33` (`14` chapters, `47` pending worksheets, `<= 3` files and `read_set <= 6` per task, continuous cross-chapter `depends_on` DAG, `27` traced requirements `R1`–`R27`, and `[0/33 files documented]` across `ch30`–`ch32`).
- Repository `make review-check` validates multi-line YAML frontmatter, `<= 40`-line truth blocks, on-disk `sources:` paths, 1:1 provenance footnotes, resolving relative links, frontmatter-free directory `index.md` files, and privacy rules.

Current flow state is `active` at `plan_revision: 6` and `plan_commit: 8e9b324e` (`current_chapter: ch20-arrow-copy-import-and-portals`, `current_task: "native-postgres-driver:ch20:t01"`). The next canonical command is `/flow:implement native-postgres-driver ch20`. Live PG14–18/service conformance (`ch28`), outside-checkout release/benchmark acceptance (`ch29`), `33`-file documentation/OKF synchronization (`ch30`–`ch32`), and complete-driver `213967be..HEAD` correctness plus fresh quality reviews (`ch33`) remain pending until executed.

[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` — Shared PostgreSQL wire frame helpers in `vertebra-db`.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Upstream flow that deletes PostgreSQL `memory://` paths and `vertebra_db::postgres::PostgresPool`.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver target cutover seam.
