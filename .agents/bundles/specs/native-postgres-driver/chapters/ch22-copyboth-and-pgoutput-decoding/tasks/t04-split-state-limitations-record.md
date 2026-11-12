---
type: Task
id: "native-postgres-driver:ch22:t04"
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: Record the split native-wheel and legacy-driver state in the PostgreSQL limitations OKF entry
description: Create postgres-limitations.md as an OKF Gotcha recording that main carries the vertebra-postgres wheel while the Python drivers, queue guard, dialer helpers, and driver tests still use psycopg/asyncpg, and register it in the root and domain knowledge indexes.
status: draft
state: in_progress
priority: P1
verification_strategy: documentation_validation
depends_on: []
read_set:
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/index.md
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/tests/unit/db/test_postgres_driver.py
files:
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
estimated_loc: 110
plan_revision: 7
plan_commit: c8722c76
state_revision: 0
claimed_by: agent:bulk-pg-docs
claimed_at: "2026-11-12T02:22:54Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T02:56:52Z"
last_verified_commit: null
verification_evidence: "Documentation baseline and final review-check exit 0; bundles/privacy validation exit 0; 8 unique sources and matching footnotes, both index links, 71-line entry, bounded truth block and source paths verified; git diff --check exit 0."
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T02:22:54Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: storage-limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: House format for a domain limitations OKF Gotcha entry
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: SyncPostgreSQLDriver over psycopg_pool and AsyncPostgreSQLDriver over asyncio.to_thread
  - id: pg-queue-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: Missing-executor guard probing psycopg
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py::test_asyncpg_pool_transaction_and_affected_rows
    title: AsyncPostgreSQLDriver tests over a SQLite-backed asyncpg pool double
tags:
  - postgres
  - limitations
  - okf
  - documentation
---

# Task t04: Record the Split Native-Wheel and Legacy-Driver State in the PostgreSQL Limitations OKF Entry

## Objective & Scope Boundary

Create `.agents/bundles/knowledge/domains/data/postgres-limitations.md`, an OKF v0.2 `Gotcha` modeled on `object-storage-limitations.md` [^storage-limitations], that records the verified split state on `main` at `c8722c76`, and register it in `.agents/bundles/knowledge/index.md` and `.agents/bundles/knowledge/domains/index.md`. The entry states what exists (the `vertebra._postgres` wheel built by `make dev` / `make build` and checked by `make postgres-test` / `make postgres-wheel-check`) and what does not yet (native Python drivers, native queue guard, `psycopg`/`asyncpg` removal), each with a symbol anchor and a pointer to the chapter that closes it [^pg-driver-py] [^pg-queue-py] [^pg-driver-tests].

Out of scope: any production, test, `docs/**`, or other OKF file; runtime caveats for features not yet built (`ch31:t01` adds them); removing the split-state caveats (`ch27:t04`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — frontmatter, truth block, `## Caller Guidance`, and footnote format [^storage-limitations].
- `.agents/bundles/knowledge/index.md` — `Storage, Data, Telemetry & Extensions` list where `Object Storage Implementation Limitations` is registered.
- `.agents/bundles/knowledge/domains/index.md` — data domain list with one-line descriptions.
- `src/py/vertebra/db/adapters/postgres/driver.py::{SyncPostgreSQLDriver, AsyncPostgreSQLDriver}` — `psycopg_pool.ConnectionPool` construction and `asyncio.to_thread` offloads [^pg-driver-py].
- `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — `ensure_module("psycopg", ...)` when `executor is None` [^pg-queue-py].
- `src/py/tests/unit/db/test_postgres_driver.py::{_SQLiteAsyncPGPool, MockConnectionPool, test_postgres_sync_and_async_guards_name_psycopg_pool}` — legacy test doubles [^pg-driver-tests].

## Symbol & Signature Contracts

- `postgres-limitations.md` frontmatter: `type: Gotcha`, `title: PostgreSQL Native Driver Limitations`, `description:` one sentence, `status: stable`, `tags: [postgres, limitations, driver]`, `generated: {by: flow-implement, at: <UTC>}`, `verified: [{by: agent:flow-implement, at: <UTC>, commit: <HEAD at execution>}]`, and `sources:` entries `pg-driver-py`, `pg-queue-py`, `pg-dialer-cloudsql` (`src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig`), `pg-dialer-alloydb` (`src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig`), `pg-driver-tests`, `pg-pipeline-py` (`src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline`), `pg-arrow-py` (`src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync`), and `pg-wheel` (`src/rs/vertebra-postgres/src/lib.rs`), each with a matching `[^id]:` footnote.
- Truth block bullets (present tense, each ending with its footnote):
  - `vertebra._postgres` (`vertebra-core[postgres]`) is built by `make dev` / `make build` and verified by `make postgres-test` and `make postgres-wheel-check`, but no public PostgreSQL driver uses it yet.
  - `SyncPostgreSQLDriver` executes through `psycopg_pool.ConnectionPool` and raises `MissingDependencyError` naming `psycopg[binary,pool]` when it is absent; `AsyncPostgreSQLDriver` runs the sync driver through `asyncio.to_thread`.
  - `PostgreSQLQueueBackend` probes `psycopg` before raising `ImproperConfigurationError` for a missing `executor=`.
  - `create_psycopg_connect_func` and `create_asyncpg_connect_func` remain on the Cloud SQL and AlloyDB dialers.
  - `pipeline.py` and `arrow.py` are imported only by unit tests; drivers do not expose native pipelines or Arrow COPY import through them.
  - `test_postgres_driver.py` exercises `AsyncPostgreSQLDriver` through a SQLite-backed asyncpg pool double and psycopg pool mocks.
  - The [PR #104](https://github.com/cofin/vertebra/pull/104) title and body describe a driver cutover that `main` does not contain; `native-postgres-driver` chapters `ch25`–`ch27` perform it.
- `## Caller Guidance`: install `vertebra-core[postgres]` for raw `SyncPostgresClient` / `AsyncPostgresClient` / pool use; public drivers still require `psycopg[binary,pool]` until the cutover; queue users pass `executor=`.
- Index entries: `- [PostgreSQL Native Driver Limitations](data/postgres-limitations.md) — <one-line description>` in `domains/index.md` under the data list, and `  - [PostgreSQL Native Driver Limitations](domains/data/postgres-limitations.md)` in `knowledge/index.md` directly after `Database Drivers & Cloud Dialers`. Both index files stay frontmatter-free.

## Edge Cases & Error Semantics

- Every cited symbol is re-verified with `git grep -n` at execution time; if a symbol no longer matches (for example a sibling flow already removed a dialer helper), the bullet is dropped or corrected rather than recorded unverified.
- No internal application names, local paths, customer names, or reviewer finding IDs appear in the entry (privacy checks in `make review-check`).
- No `---` body rules, no revision history, and the file stays `<= 200` lines.

## Public-Seam Test Blueprint

- **Canonical gate**: `documentation_validation` over the three owned files.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Frontmatter and footnotes | `uv run --no-sync python -c` regex parse of the frontmatter in `postgres-limitations.md` and comparison of `sources` ids with `[^id]:` footnotes | `type == "Gotcha"`, `8` sources, every source id has exactly one footnote and every footnote id is a source. |
| Symbol anchors | `git grep -n "psycopg_pool" -- src/py/vertebra/db/adapters/postgres/driver.py`, `git grep -n 'ensure_module("psycopg"' -- src/py/vertebra/framework/queues/postgres.py`, `git grep -n "_SQLiteAsyncPGPool" -- src/py/tests/unit/db/test_postgres_driver.py` | Each command prints at least one line, matching the bullets recorded. |
| Index registration | `git grep -n "postgres-limitations.md" -- .agents/bundles/knowledge/index.md .agents/bundles/knowledge/domains/index.md` | Exactly one line per index; neither index gains frontmatter. |
| Review manifests and privacy | `make review-check` | Exits `0`. |
| Line budget | `wc -l .agents/bundles/knowledge/domains/data/postgres-limitations.md` | `<= 200`. |

## Verification

- Initial baseline: `test ! -e .agents/bundles/knowledge/domains/data/postgres-limitations.md && git grep -n "postgres-limitations" -- .agents/bundles/knowledge/index.md .agents/bundles/knowledge/domains/index.md; test $? -eq 1`
- Final gate:
  ```bash
  uv run --no-sync python -c 'import pathlib, re; t = pathlib.Path(".agents/bundles/knowledge/domains/data/postgres-limitations.md").read_text(); fm = t.split("\n---\n", 1)[0]; ids = set(re.findall(r"^\s*-?\s*id:\s*([a-z0-9-]+)\s*$", fm, re.M)); notes = set(re.findall(r"^\[\^([a-z0-9-]+)\]:", t, re.M)); assert re.search(r"^type: Gotcha$", fm, re.M) and len(ids) == 8 and ids == notes, (ids, notes)'
  git grep -n "postgres-limitations.md" -- .agents/bundles/knowledge/index.md .agents/bundles/knowledge/domains/index.md
  make review-check
  ```

## Acceptance Criteria

- [x] `postgres-limitations.md` exists as an OKF `Gotcha` with matched sources/footnotes and a truth block recording the split state on `main` with symbol anchors (R27).
- [x] Both knowledge indexes register the entry once each and remain frontmatter-free (R27).
- [x] `make review-check` exits `0`.
- [ ] Commit message follows `docs(postgres): record split native driver state in limitations OKF` once verified.

## Notes & Discoveries

- Explicit source-anchor verification also covers the dialer, pipeline, Arrow, wheel, and Makefile symbols named by the contract but omitted from the bounded read list. Native verification commands are documented as lanes; this documentation task does not claim to execute wheel acceptance. The PR #104 title and body are verified read-only against GitHub.

- `ch27:t04` deletes the split-state bullets once the cutover lands, and `ch31:t01` adds the runtime caveats listed in the root documentation plan.

[^storage-limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — House format for a domain limitations OKF `Gotcha` entry.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — `SyncPostgreSQLDriver` over `psycopg_pool` and `AsyncPostgreSQLDriver` over `asyncio.to_thread`.
[^pg-queue-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Missing-executor guard probing `psycopg`.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py::test_asyncpg_pool_transaction_and_affected_rows` — `AsyncPostgreSQLDriver` tests over a SQLite-backed asyncpg pool double.
