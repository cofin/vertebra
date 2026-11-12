---
type: Task
id: "native-postgres-driver:ch30:t03"
chapter_id: ch30-user-and-workflow-docs
title: Synchronize cloud dialers, packaging/deployment, and PostgreSQL queue documentation
description: Document native PostgreSQLConfig Cloud SQL and AlloyDB mTLS+IAM discovery in cloud-dialers.md, the vertebra-postgres companion wheel and discovery-based make build-pgo in packaging-and-deployment.md, and the vertebra-core[postgres] caller-injected PostgreSQLQueueExecutor contract in queues-and-realtime.md.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t02"
read_set:
  - docs/usage/cloud-dialers.md
  - docs/usage/packaging-and-deployment.md
  - docs/usage/queues-and-realtime.md
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/framework/queues/postgres.py
  - tools/pgo/rust_pgo.py
files:
  - docs/usage/cloud-dialers.md
  - docs/usage/packaging-and-deployment.md
  - docs/usage/queues-and-realtime.md
tests: []
estimated_loc: 165
plan_revision: 8
plan_commit: d90d4625
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-10T14:45:00Z"
updated_at: "2026-11-12T04:25:15Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: doc-dialers
    resource: docs/usage/cloud-dialers.md
    title: User-facing Cloud SQL and AlloyDB dialers guide
  - id: doc-packaging
    resource: docs/usage/packaging-and-deployment.md
    title: User-facing standalone, onefile, and compiler-optimization packaging guide
  - id: doc-queues
    resource: docs/usage/queues-and-realtime.md
    title: User-facing Distributed Queues and Realtime PubSub guide
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQLConfig Cloud SQL, AlloyDB, and IAM fields
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQLQueueBackend missing-executor dependency guard and caller-injected executor contract
  - id: pgo-discovery
    resource: tools/pgo/rust_pgo.py::discover_extension_manifests
    title: Rust PGO extension discovery from crate-local maturin pyproject.toml files
tags:
  - postgres
  - documentation
  - cloudsql
  - alloydb
  - queues
---

# Task t03: Synchronize Cloud Dialers, Packaging/Deployment, and PostgreSQL Queue Documentation

## Objective & Scope Boundary

Update `docs/usage/cloud-dialers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/usage/queues-and-realtime.md` in present tense to align cloud discovery, wheel packaging, and distributed queue documentation with `vertebra-postgres` (`vertebra-core[postgres]`) [^doc-dialers] [^doc-packaging] [^doc-queues] [^pg-config] [^pg-queue] [^pgo-discovery]:
1. `docs/usage/cloud-dialers.md`:
   - Document native `PostgreSQLConfig` (`SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`) Cloud SQL (`cloud_sql_instance="project:region:instance"`, `cloud_sql_ip_type="PRIVATE" | "PUBLIC" | "PSC"`) and AlloyDB (`alloydb_instance_uri="projects/.../instances/..."`, `alloydb_ip_type="PRIVATE" | "PUBLIC" | "PSC"`) automatic Admin API endpoint and ephemeral X.509 certificate discovery, single-flight mTLS 1.3 renewal with service-specific peer identity verification, and scoped IAM authentication (`enable_iam_auth=True`, `cloud_auth_options` for Admin credentials, `iam_auth_options` for `sqlservice.login` / `alloydb.login` credentials or validated static tokens carrying `access_token`, `expires_at_unix_ms`, and `scopes`) alongside the standalone `CloudSQLDialer` and `AlloyDBDialer` primitives for non-PostgreSQL Cloud SQL engines [^doc-dialers] [^pg-config].
2. `docs/usage/packaging-and-deployment.md`:
   - Add `_postgres` / `vertebra-postgres` (installed through `vertebra-core[postgres]`, linked `=== "uv"`/`"pip"`/`"pdm"`/`"poetry"` tabs if adding an install snippet) to the existing native extension and companion wheel descriptions without stating a fixed extension count [^doc-packaging].
   - Rewrite `## Production Performance & Compiler Optimization` so it matches the repository: published wheels compile with the workspace `[profile.release]` settings (`lto = true`, `codegen-units = 1`, `opt-level = 3`, `panic = "abort"`) and `.github/workflows/release.yaml` runs no PGO step, while contributor `make build-pgo` (`python -m tools.pgo.rust_pgo`) profiles `vertebra-http` plus every extension returned by `tools/pgo/rust_pgo.py::discover_extension_manifests`, which includes `vertebra-postgres` through its crate-local `src/rs/vertebra-postgres/pyproject.toml` [^doc-packaging] [^pgo-discovery].
3. `docs/usage/queues-and-realtime.md`:
   - Replace every `psycopg[binary,pool]` reference (the `Queue Backends` table row, the install prose sentence, and all four linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"` install tabs) with `vertebra-core[postgres]`.
   - Document the delivered `PostgreSQLQueueBackend` contract from `ch27`: when `executor is None` the constructor calls `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` (raising `MissingDependencyError` naming `vertebra-core[postgres]` when the wheel is absent) and then raises `ImproperConfigurationError` instructing callers to pass `executor=` implementing `PostgreSQLQueueExecutor`, for example an adapter wrapping `AsyncPostgreSQLDriver` and `PostgresNotificationSource` over `CLAIM_POSTGRES_JOBS_SQL` `FOR UPDATE SKIP LOCKED`, `POSTGRES_QUEUE_DDL`, and dedicated `LISTEN`/`NOTIFY` wakeups. Never claim a built-in default executor, a settings-driven backend registry, or a `litestar-queues` backend (owned by the `queues-tasks-and-channels` flow), and never pass raw `AsyncPostgreSQLDriver` as `executor=`; pass configuration arguments inline without single-use intermediate variables or inline `#` comments [^doc-queues] [^pg-queue].

Out of scope: Modifying Python/Rust source files, `databases.md` / `analytics-and-cdc.md` (`ch30:t01`), `native-stubs.md` / `installation.md` (`ch30:t02`), or `README.md` / `workflow.md` / `AGENTS.md` (`ch30:t04`).

## Bounded Read Manifest

- [docs/usage/cloud-dialers.md](../../../../../../../docs/usage/cloud-dialers.md) — Current low-level `CloudSQLDialer` and `AlloyDBDialer` usage page [^doc-dialers].
- [docs/usage/packaging-and-deployment.md](../../../../../../../docs/usage/packaging-and-deployment.md) — Current standalone, onefile, and compiler-optimization deployment guide [^doc-packaging].
- [docs/usage/queues-and-realtime.md](../../../../../../../docs/usage/queues-and-realtime.md) — Current queue backend comparison, PostgreSQL install tabs, and executor example [^doc-queues].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` Cloud SQL, AlloyDB, and IAM parameters [^pg-config].
- [src/py/vertebra/framework/queues/postgres.py](../../../../../../../src/py/vertebra/framework/queues/postgres.py) — `PostgreSQLQueueBackend.__init__` missing-executor guard as updated by `ch27` [^pg-queue].
- [tools/pgo/rust_pgo.py](../../../../../../../tools/pgo/rust_pgo.py) — `discover_extension_manifests` crate-local `pyproject.toml` discovery [^pgo-discovery].

## Symbol & Signature Contracts

- `docs/usage/queues-and-realtime.md` documents the exact delivered `PostgreSQLQueueBackend(executor=...)` contract read from `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` (caller-supplied `PostgreSQLQueueExecutor`; the example adapter wraps `AsyncPostgreSQLDriver` + `PostgresNotificationSource` with inline `PostgreSQLConfig`) with zero external library imports or inline `#` comments.
- `docs/usage/packaging-and-deployment.md` names `tools/pgo/rust_pgo.py::discover_extension_manifests` behavior (crate-local `pyproject.toml` discovery) and never states that release CI applies PGO.
- All installation snippets use linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, and `=== "poetry"` blocks; any `msgspec.Struct` payload example omits `frozen=True, gc=False` boilerplate.

## Edge Cases & Error Semantics

- Clarify in `docs/usage/cloud-dialers.md` that cloud resource options (`cloud_sql_instance`, `alloydb_instance_uri`) and `unix_socket` are mutually exclusive, cloud TLS 1.3 peer verification is mandatory regardless of direct `sslmode`, and opaque Admin tokens in `cloud_auth_options` are never reused as database login passwords without matching login scopes in `iam_auth_options` [^pg-config].
- If `ch27` has not yet switched the queue guard when this task runs (the guard still names `psycopg`), stop and route through `/flow:revise` instead of documenting a contract that is not on disk.
- Keep `queues-and-realtime.md`, `cloud-dialers.md`, and `packaging-and-deployment.md` each within `H2+H3 <= 14` secondary TOC items and `<= 180,000` HTML bytes, with zero mentions of external Python PostgreSQL driver packages.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs`, `make docs-check`, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Zensical build & live HTTP smoke | `make docs && make docs-check` | Exits `0`; `cloud-dialers`, `packaging-and-deployment`, and `queues-and-realtime` render within `<= 14` TOC items and `<= 180,000` bytes and return HTTP `200`. |
| Stale queue dependency & external driver audit | `git grep -n -i -E "psycopg|asyncpg" -- docs/usage/cloud-dialers.md docs/usage/packaging-and-deployment.md docs/usage/queues-and-realtime.md` | No output; `vertebra-core[postgres]` appears in all four linked package-manager tabs and the backend table row of `queues-and-realtime.md`, and zero inline `#` Python comments appear in examples. |
| PGO accuracy audit | `git grep -n -i "pgo" -- docs/usage/packaging-and-deployment.md` | Every match describes `make build-pgo` contributor tooling with crate-local `pyproject.toml` discovery; no line claims release CI applies PGO. |
| Bundle & privacy verification | `make review-check` | Exits `0` with zero privacy, link, or forbidden-pattern errors. |

## Verification

- Build & live-serve docs check: `make docs && make docs-check`
- External driver audit: `git grep -n -i -E "psycopg|asyncpg" -- docs/usage/cloud-dialers.md docs/usage/packaging-and-deployment.md docs/usage/queues-and-realtime.md` (expects no output)
- PGO wording audit: `git grep -n -i "pgo" -- docs/usage/packaging-and-deployment.md`
- Review & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `docs/usage/cloud-dialers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/usage/queues-and-realtime.md` document native `PostgreSQLConfig` Cloud SQL / AlloyDB mTLS+IAM discovery, the `_postgres` companion wheel, discovery-based `make build-pgo` with release wheels built from `[profile.release]`, and `PostgreSQLQueueBackend`'s `vertebra-core[postgres]` dependency guard plus caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract with zero external driver names (R9, R24, R27).
- [ ] `make docs`, `make docs-check`, and `make review-check` exit `0` (R27).

## Notes & Discoveries

- Notification source naming: this worksheet executes before `queues-tasks-and-channels` t01 splits `NotificationSource` into `SyncNotificationSource` / `AsyncNotificationSource` and renames `PostgresNotificationSource` to `AsyncPostgresNotificationSource` alongside a new `SyncPostgresNotificationSource` (see [native-data-platform](../../../../native-data-platform/spec.md) Seam 2). It keeps the HEAD names, adds no new unprefixed notification protocol, class, or alias, and leaves the rename of every symbol, doc, and OKF reference it touches to that task.
- Preserve standalone `CloudSQLDialer` and `AlloyDBDialer` coverage in `docs/usage/cloud-dialers.md` for MySQL and raw tunnel consumers while positioning `PostgreSQLConfig` as the integrated path for PostgreSQL Cloud SQL and AlloyDB instances.

[^doc-dialers]: `docs/usage/cloud-dialers.md` — User-facing Cloud SQL and AlloyDB dialers guide.
[^doc-packaging]: `docs/usage/packaging-and-deployment.md` — User-facing standalone, onefile, and compiler-optimization packaging guide.
[^doc-queues]: `docs/usage/queues-and-realtime.md` — User-facing Distributed Queues and Realtime PubSub guide.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — `PostgreSQLConfig` Cloud SQL, AlloyDB, and IAM fields.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — `PostgreSQLQueueBackend` missing-executor dependency guard and caller-injected executor contract.
[^pgo-discovery]: `tools/pgo/rust_pgo.py::discover_extension_manifests` — Rust PGO extension discovery from crate-local maturin `pyproject.toml` files.
