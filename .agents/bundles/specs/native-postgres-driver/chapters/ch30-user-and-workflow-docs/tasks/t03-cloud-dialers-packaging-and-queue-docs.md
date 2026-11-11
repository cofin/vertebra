---
type: Task
id: "native-postgres-driver:ch30:t03"
chapter_id: ch30-user-and-workflow-docs
title: Synchronize cloud dialers, packaging/deployment, and PostgreSQL queue documentation
description: Document native PostgreSQLConfig Cloud SQL and AlloyDB mTLS+IAM discovery in cloud-dialers.md, 6 PyO3 extensions and PGO/release-LTO packaging in packaging-and-deployment.md, and vertebra-core[postgres] caller-injected PostgreSQLQueueExecutor contracts in queues-and-realtime.md.
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
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - docs/usage/cloud-dialers.md
  - docs/usage/packaging-and-deployment.md
  - docs/usage/queues-and-realtime.md
tests: []
estimated_loc: 165
plan_revision: 6
plan_commit: 8e9b324e
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
sources:
  - id: doc-dialers
    resource: docs/usage/cloud-dialers.md
    title: User-facing Cloud SQL and AlloyDB dialers guide
  - id: doc-packaging
    resource: docs/usage/packaging-and-deployment.md
    title: User-facing standalone, onefile, and PGO packaging guide
  - id: doc-queues
    resource: docs/usage/queues-and-realtime.md
    title: User-facing Distributed Queues and Realtime PubSub guide
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQLConfig Cloud SQL, AlloyDB, and IAM fields
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQLQueueBackend SKIP LOCKED and LISTEN/NOTIFY implementation
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL cloud mTLS/IAM and configuration contracts
tags:
  - postgres
  - documentation
  - cloudsql
  - alloydb
  - queues
---

# Task t03: Synchronize Cloud Dialers, Packaging/Deployment, and PostgreSQL Queue Documentation

## Objective & Scope Boundary

Update `docs/usage/cloud-dialers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/usage/queues-and-realtime.md` in present tense to align cloud discovery, standalone/wheel packaging, and distributed queue documentation with `vertebra-postgres` (`vertebra-core[postgres]`) [^doc-dialers] [^doc-packaging] [^doc-queues] [^pg-config] [^pg-queue] [^pg-contracts]:
1. `docs/usage/cloud-dialers.md`:
   - Document native `PostgreSQLConfig` (`SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`) Cloud SQL (`cloud_sql_instance="project:region:instance"`, `cloud_sql_ip_type="PRIVATE" | "PUBLIC" | "PSC"`) and AlloyDB (`alloydb_instance_uri="projects/.../instances/..."`, `alloydb_ip_type="PRIVATE" | "PUBLIC" | "PSC"`) automatic Admin API endpoint and ephemeral X.509 certificate discovery, single-flight mTLS 1.3 renewal with service-specific peer identity verification, and scoped IAM authentication (`enable_iam_auth=True`, `cloud_auth_options` for Admin credentials, `iam_auth_options` for `sqlservice.login` / `alloydb.login` credentials or validated static tokens carrying `access_token`, `expires_at_unix_ms`, and `scopes`) alongside the standalone `CloudSQLDialer` and `AlloyDBDialer` primitives for non-PostgreSQL Cloud SQL engines [^doc-dialers] [^pg-config] [^pg-contracts].
2. `docs/usage/packaging-and-deployment.md`:
   - Update native extension architecture and distribution notes for six compiled `gil_used = false` PyO3 extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres`) and optional `vertebra-core[postgres]` (`vertebra-postgres`) companion wheel installation (linked `=== "uv"`/`"pip"`/`"pdm"`/`"poetry"` tabs if adding an install snippet), accurately noting that `make build-pgo` profiles `vertebra-http` plus the five core/GCP extensions in `EXTENSION_MANIFESTS` while `vertebra-postgres` ships with workspace release LTO (`lto = true`, `codegen-units = 1`, `opt-level = 3`) via `maturin` [^doc-packaging].
3. `docs/usage/queues-and-realtime.md`:
   - Replace every stale `psycopg[binary,pool]` dependency reference (in the `Queue Backends` table, prose, and all four linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"` install tabs) with `vertebra-core[postgres]`.
   - Preserve the truthful caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract on `PostgreSQLQueueBackend` (`executor is None` checks `vertebra._postgres` raising `MissingDependencyError` for `vertebra-core[postgres]` when uninstalled and raises `ImproperConfigurationError` instructing callers to pass `executor=` implementing `PostgreSQLQueueExecutor`, for example wrapping `AsyncPostgreSQLDriver` and `PostgresNotificationSource` over `CLAIM_POSTGRES_JOBS_SQL` `FOR UPDATE SKIP LOCKED`, `POSTGRES_QUEUE_DDL`, and dedicated `LISTEN`/`NOTIFY` wakeups; never claim `PostgreSQLQueueBackend` ships a built-in default executor or accepts raw `AsyncPostgreSQLDriver` directly without a `PostgreSQLQueueExecutor` adapter), passing configuration arguments inline without single-use intermediate variables or inline `#` comments [^doc-queues] [^pg-queue].

Out of scope: Modifying Python/Rust source files, `databases.md` / `analytics-and-cdc.md` (`ch30:t01`), `native-stubs.md` / `installation.md` (`ch30:t02`), or `README.md` / `workflow.md` / `AGENTS.md` (`ch30:t04`).

## Bounded Read Manifest

- [docs/usage/cloud-dialers.md](../../../../../../../docs/usage/cloud-dialers.md) — Current low-level `CloudSQLDialer` and `AlloyDBDialer` usage page [^doc-dialers].
- [docs/usage/packaging-and-deployment.md](../../../../../../../docs/usage/packaging-and-deployment.md) — Current standalone, onefile, and Rust/Python PGO deployment guide [^doc-packaging].
- [docs/usage/queues-and-realtime.md](../../../../../../../docs/usage/queues-and-realtime.md) — Current queue backend comparison, PostgreSQL install tabs, and executor example [^doc-queues].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` Cloud SQL, AlloyDB, and IAM parameters [^pg-config].
- [src/py/vertebra/framework/queues/postgres.py](../../../../../../../src/py/vertebra/framework/queues/postgres.py) — Final `PostgreSQLQueueBackend` constructor (`vertebra._postgres` dependency check + caller-injected `PostgreSQLQueueExecutor` requirement) from `ch27` [^pg-queue].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Cloud mTLS 1.3, Admin vs login scope separation, and static token validation rules [^pg-contracts].

## Symbol & Signature Contracts

- Inspect `src/py/vertebra/framework/queues/postgres.py` as updated by `ch27` so `docs/usage/queues-and-realtime.md` documents the exact delivered `PostgreSQLQueueBackend(executor=...)` contract (caller-supplied `PostgreSQLQueueExecutor`, illustrating how an executor adapter wraps `AsyncPostgreSQLDriver` + `PostgresNotificationSource` with inline `PostgreSQLConfig` where shown, never passing raw `AsyncPostgreSQLDriver` directly as `executor=` or claiming a built-in default executor) with zero external library imports or inline `#` comments.
- All installation snippets use linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, and `=== "poetry"` blocks; any `msgspec.Struct` payload example omits `frozen=True, gc=False` boilerplate.

## Edge Cases & Error Semantics

- Clarify in `docs/usage/cloud-dialers.md` that cloud resource options (`cloud_sql_instance`, `alloydb_instance_uri`) and `unix_socket` are mutually exclusive, cloud TLS 1.3 peer verification is mandatory regardless of direct `sslmode`, and opaque Admin tokens in `cloud_auth_options` are never reused as database login passwords without matching login scopes in `iam_auth_options` [^pg-contracts].
- Keep `queues-and-realtime.md`, `cloud-dialers.md`, and `packaging-and-deployment.md` each within `H2+H3 <= 14` secondary TOC items and `<= 180,000` HTML bytes, with zero mentions of external Python PostgreSQL driver packages.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs`, `make docs-check`, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Zensical build & live HTTP smoke | `make docs && make docs-check` | Exits `0`; `cloud-dialers`, `packaging-and-deployment`, and `queues-and-realtime` render cleanly within `<= 14` TOC items and `<= 180,000` bytes and return HTTP `200`. |
| Stale queue dependency & external driver audit | Inspect `docs/usage/{cloud-dialers,packaging-and-deployment,queues-and-realtime}.md` | Zero occurrences of `psycopg` or `asyncpg`, `vertebra-core[postgres]` present in all four linked package-manager tabs in `queues-and-realtime.md`, and zero inline `#` Python comments. |
| Bundle & privacy verification | `make review-check` | Exits `0` with zero privacy, link, or forbidden-pattern errors. |

## Verification

- Build & live-serve docs check: `make docs && make docs-check`
- Review & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `docs/usage/cloud-dialers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/usage/queues-and-realtime.md` accurately document native `PostgreSQLConfig` Cloud SQL / AlloyDB mTLS+IAM discovery, `_postgres` packaging/PGO boundaries, and `PostgreSQLQueueBackend`'s `vertebra-core[postgres]` dependency guard plus caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract with zero external driver names (R9, R24, R27).
- [ ] `make docs`, `make docs-check`, and `make review-check` exit `0` cleanly (R27).

## Notes & Discoveries

- Preserve standalone `CloudSQLDialer` and `AlloyDBDialer` coverage in `docs/usage/cloud-dialers.md` for MySQL and raw tunnel consumers while positioning `PostgreSQLConfig` as the integrated path for PostgreSQL Cloud SQL and AlloyDB instances.

[^doc-dialers]: `docs/usage/cloud-dialers.md` — User-facing Cloud SQL and AlloyDB dialers guide.
[^doc-packaging]: `docs/usage/packaging-and-deployment.md` — User-facing standalone, onefile, and PGO packaging guide.
[^doc-queues]: `docs/usage/queues-and-realtime.md` — User-facing Distributed Queues and Realtime PubSub guide.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — `PostgreSQLConfig` Cloud SQL, AlloyDB, and IAM fields.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — `PostgreSQLQueueBackend` `SKIP LOCKED` and `LISTEN`/`NOTIFY` implementation.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL cloud mTLS/IAM and configuration contracts.
