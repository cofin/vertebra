---
type: Review
flow_id: native-postgres-driver
title: Native PostgreSQL planning review
description: Adjudication and validation of the nested 31-chapter complete-driver implementation plan.
status: stable
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:40:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:40:00Z"
    commit: dd432f18
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: Shared PostgreSQL wire frame helpers in vertebra-db
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
- **Adjudicated Plan Baseline**: All planning and zero-context specification review findings are adopted across `spec.md`, `contracts.md`, and `31` nested chapters (`ch01`–`ch31`, `111` vertical-slice worksheets) at `state: planned`, `has_chapters: true`, `plan_revision: 3`, and `plan_commit: dd432f18` [^pg-core-rs] [^pg-driver-py].
- **Scope of Evidence**: Planning review validates contracts, chapter/task dependency order, `<= 3`-file worksheet ownership bounds, cross-chapter `depends_on` edges, and OKF v0.2 schema compliance; runtime, live-service, wheel, and benchmark evidence are produced during `/flow:implement native-postgres-driver`.
<!-- truth: end -->

This review covers the root specification, chapter catalog, chapter specifications,
and task worksheets. Runtime, wheel, live-service, and performance acceptance remain
pending until implementation. The complete driver ships in one PR; individual
worksheets define atomic local signed commits.

## Adjudication

All findings below are ADOPT. No product decision remains escalated.

| Finding | Correction and verification contract |
| --- | --- |
| Cold statement and backend lifetime | Resolve required parameter metadata before Parse; execute cold text results before optional enrichment; retain completed results on later failure and never replay SQL. Named preflight pins or re-Parses (`ch12:t02`). |
| SCRAM downgrade detection | Prefer uses GS2 `y` when binding is usable but PLUS is absent; actual wire assertions verify downgrade rejection (`ch03:t03`). |
| Pipeline depended on future driver helpers | Early native pipeline (`ch15`–`ch16`) takes explicit preparation/release callbacks; later adapter (`ch25`–`ch26`) supplies its compiler. |
| Incomplete public value records | Contracts freeze units, discriminants, array bounds, vector/bit carriers, JSON null, and nested conversion (`contracts.md`, `ch09`–`ch12`, `ch14:t03`). |
| Static login token expiry | Require supplied real expiry and declared scopes; reject unusable credentials before transmission (`ch05:t02`). |
| Feature-gated cancellation test could run empty | Register the target with required PyO3 features and verify named behavioral tests execute (`ch16:t04`). |
| Pool constructor inventory | Enumerate cache, pooler, limits, and timeout options with canonical configuration identity tests (`ch14`–`ch15:t01`). |
| Pipeline could commit before context exit | Validate every operation before frames (`ch15:t02`); reject transaction controls and raw same-scope API escapes; caught errors retain rollback disposition (`ch16:t01`–`t02`). |
| AlloyDB token expiry check & cloud keepalive seam | Split verified TLS from metadata exchange via `DialedCloudTransport` preserving socket TCP keepalive configuration across direct and cloud paths; check scoped login material between stages and test held-TLS expiry (`ch06:t01`–`t02`). |
| Missing production event hook owner & `notify.py` parity | Specify dedicated-client hooks and structural protocols (`vertebra.db.adapters.postgres.events`) on both driver facades (`ch26:t04`), complete `PostgresNotificationSource` migration in `ch21:t03`, and verify public-import parity in `ch27:t02` without duplicate `notify.py` edits. |
| Durable checkpoint & `change_stream.py` module alignment | Standardize CDC contracts, `_EXPORT_MAP` barrel entries, and documentation references on `vertebra.db.adapters.postgres.change_stream` (`PostgresChangeSource`, `PostgresCheckpointStore`), `vertebra.db.adapters.postgres.types` (`PostgresArrowResult`, `PostgresReplicationIdentity`), and `vertebra.db.adapters.postgres.events` (`ch24`, `ch27:t01`, `ch30`). |
| `pgvector` version was not pinned | Integration fixtures use verified multi-arch OCI image digests (`0.8.1-pg14`, `0.8.1-pg18`) rather than floating tags (`ch28:t03`). |
| Local services contradicted TLS default | Explicitly disable TLS (`sslmode="disable"`) for test-owned local fixtures while retaining separate certificate and identity acceptance (`ch28`–`ch29`). |
| Session-dependent bind lexing & numeric passthrough | Reject ambiguous ordinary backslash-quote forms during emulation; require `E'...'` strings, respect numeric-token identifier boundaries, and pass numeric-only SQL unchanged (`ch25:t02`). |
| Benchmark harness & `[tool.mypy]` strict overrides | Freeze reference versions (`psycopg[binary]==3.2.13`, `asyncpg==0.31.0`), `schema_version=1` capture schema, `[tool.mypy].files` registration for `tools/benchmarks/postgres.py`, and `ignore_missing_imports = true` overrides for `asyncpg`/`psycopg` so `make lint-all` passes without `--group bench` (`ch29:t02`–`t03`). |
| DSN defaults were unresolved | Freeze environment/OS precedence, explicit-empty behavior, strict percent decoding, and adapter TLS defaults (`ch25:t01`). |
| Python cutover & Griffe docstring readiness | Implement permanent private `_sync.py` and `_async.py` modules (`ch25`–`ch26`), switch public exports atomically with `tools.docs.scan_public_docstrings` verification (`ch27:t01`–`t02`), and delete unreachable legacy async (`ch27:t03`) and sync (`ch27:t04`) bodies before conformance (`ch28`). |
| Cargo lockfile, `PostgresError::Data`/`ServerError`/`TransactionStatus`, Arrow sink, & `session.rs` deadlines | Lock `vertebra-postgres` and `vertebra-db` Cargo pins upfront (`ch01:t01`–`t02`), keep `PostgresConfig`, `PostgresError::Data`, `ServerError`, canonical SQLSTATE subclass mapping (`ch05:t02`), and `TransactionStatus` shapes stable from `ch01:t04`–`ch05`, stream Arrow batches directly via `query_with_row_sink` (`ch19`–`ch20`), and anchor command deadlines in `session.rs` (`ch17:t01`–`t02`). |
| Packaging split, wheel script ownership, & cross-chapter DAG edges | Split root `[postgres]` extra, `areas.toml`, and all four sibling wheel `_postgres.pyi` exclusions into `ch08` (`4` tasks), create `tools/scripts/verify_postgres_wheel.py` cleanly in `ch29:t01`, and populate explicit cross-chapter entry `depends_on` edges across `ch02`–`ch31`. |
| Unresolved temporal display text was unsafe to rebind | Preserve output text/context (`PgTemporal(special="wire_text")`); reject typed unresolved binds before submission (`ch17:t04`). |
| `11`-file `DOC-04`/`DOC-08` documentation & OKF synchronization | Expand documentation and OKF synchronization to all `11` invalidated user, workflow, agent, domain, extension, benchmark, and tech-stack files in `ch30-user-and-domain-docs` (`ch30:t01`–`t04`) within `DOC-08` line budgets (`AGENTS.md <= 100`, `knowledge/** <= 200`), followed by final complete-driver reviews in `ch31-final-review-and-release-gate` (`ch31:t01`). |

## Review and validation evidence

- Core correctness review: three bounded rounds; all original and follow-up Important findings and signature/reference issues are resolved across the normalized contracts and worksheets.
- Data and acceptance correctness review: two rounds; all seven original Important findings and lexer/activation-reference corrections are resolved.
- Zero-context chaptered specification review (`plan_revision: 3`, `plan_commit: dd432f18`): all `ADOPT` findings across Cargo/lockfile ownership, `DialedCloudTransport` keepalives, `PostgresConfig`/`ServerError`/`TransactionStatus` stability, `query_with_row_sink` Arrow streaming, `session.rs` command deadlines, `vertebra.db.adapters.postgres.change_stream` barrel/docs alignment, `PostgresNotificationSource` `ch21:t03` parity, cross-chapter `depends_on` edges, `ch08` 4-task packaging split, `[tool.mypy]` benchmark overrides, `ch27:t01` `tools.docs` docstring scan, and `11`-file `DOC-04`/`DOC-08` coverage are integrated across `31` planned chapters (`ch01`–`ch31`, `111` pending vertical-slice worksheets), `27` traced requirements (`R1`–`R27`), and `[0/11 files documented]` in `ch30`.
- Repository `make review-check` validates review capabilities, multi-line YAML frontmatter, source-to-footnote parity, clickable relative Bounded Read Manifest links, and privacy rules; runtime implementation acceptance remains pending (`ch31:t01`).

Plan state is `planned` at `plan_revision: 3` and `plan_commit: dd432f18` (`has_chapters: true`). The next canonical command is `/flow:implement native-postgres-driver`, starting with [ch01-wheel-scaffold-and-config](chapters/ch01-wheel-scaffold-and-config/spec.md) (`native-postgres-driver:ch01:t01`). This document claims no production build, native/live service, installed-wheel, free-threaded, or performance acceptance prior to implementation.

[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — Shared PostgreSQL wire frame helpers in `vertebra-db`.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver target cutover seam.
