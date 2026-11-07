---
type: Task
id: "native-postgres-driver:ch28:t03"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Provision digest-pinned pgvector 0.8.1 and PgBouncer 1.26.0 acceptance service fixtures
description: Add digest-pinned pgvector 0.8.1 (PG14 and PG18) and SCRAM-authenticated PgBouncer 1.26.0 transaction/session pooler Docker fixtures in integration/conftest.py and verify capability, cold-metadata, and negative smoke controls in test_postgres_services.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch28:t02"
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/integration/db/test_postgres_services.py
estimated_loc: 240
plan_revision: 4
plan_commit: 45ff9563
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration pytest-databases Docker harness fixtures
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL pooler_mode, protocol_level_pooler_cache, and direct_dsn configuration
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Pooler statement-cache gating and PostgreSQL adapter contracts
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Cold query metadata normalization, pooler modes, and vector contracts
tags:
  - postgres
  - pgvector
  - pgbouncer
  - fixtures
  - integration
---

# Task t03: Provision Digest-Pinned `pgvector` 0.8.1 and `PgBouncer` 1.26.0 Acceptance Service Fixtures

## Objective & Scope Boundary

Add reusable local Docker acceptance fixtures for digest-pinned `pgvector` 0.8.1 (`PG14` and `PG18`) and `edoburu/pgbouncer:v1.26.0-p0` (`transaction` mode `max_prepared_statements=0` and `200`, plus `session` control) in `src/py/tests/integration/conftest.py`, and verify service readiness, capability truth, cold-metadata rotation safety, `VACUUM` outside implicit `BEGIN`, and negative credential/cache controls in `src/py/tests/integration/db/test_postgres_services.py` [^int-conftest] [^pg-config] [^okf-drivers] [^pg-contracts].

Out of scope: Exhaustive `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, `PostgresEmbeddingPushdown`, and full `PgBouncer` workload matrices (owned by `ch28:t04`).

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `DockerService`, `provide_postgres_service`, and existing PostgreSQL fixtures [^int-conftest].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig`, `PoolerMode`, `protocol_level_pooler_cache`, and `direct_dsn` [^pg-config].
- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`.
- [.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md](../../../../../knowledge/domains/data/database-drivers-and-dialers.md) — Pooler cache gating invariants [^okf-drivers].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Cold query metadata normalization and pooler configuration contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - Digest-pinned `pgvector` 0.8.1 service fixtures via `provide_postgres_service` with explicit `sslmode="disable"`:
    - PG14 image: `"pgvector/pgvector:0.8.1-pg14@sha256:2b8cdfaad8e380ef81202cb2df1db35a83482ca7c6f4d933d01d84c294ba64ff"`.
    - PG18 image: `"pgvector/pgvector:0.8.1-pg18@sha256:508c5290cda481d4f5f846446a26e9c1b804766828a394a5861de1b348a18b4c"`.
    - Provision isolated owned schema/database, run `CREATE EXTENSION IF NOT EXISTS vector`, and assert `pg_extension.extversion == "0.8.1"` plus `server_version_num` major `14` / `18` before yielding.
  - Ephemeral `PgBouncer` fixture using `edoburu/pgbouncer:v1.26.0-p0` (`container_port=5432`, `ENTRYPOINT /entrypoint.sh`, `CMD /usr/bin/pgbouncer /etc/pgbouncer/pgbouncer.ini`) via `DockerService.run(..., transient=True)`:
    - Inspect owned backing PostgreSQL container `NetworkSettings` bridge IP (`assert bridge_ip`) and pass `DB_HOST=bridge_ip`, `DB_PORT="5432"`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `AUTH_TYPE="scram-sha-256"`, `POOL_MODE="transaction"` (parameterized `MAX_PREPARED_STATEMENTS="0"` and `"200"`, plus `"session"` mode control), and `SERVER_RESET_QUERY` / `SERVER_RESET_QUERY_ALWAYS` (using `tmp_path_factory` read-only config/userlist mounts only when reset overrides require them; zero committed credential files).
    - Yield paired `(proxy_config: PostgreSQLConfig, direct_config: PostgreSQLConfig)` with `sslmode="disable"`, dialing `ServiceContainer.host` + dynamic mapped host port from tests and closing native pools before container teardown.

## Edge Cases & Error Semantics

- Never dial `localhost` or a host-mapped loopback port from inside the `PgBouncer` container; always use the backing PostgreSQL container's Docker default-bridge IP on `:5432`.
- When `PgBouncer` advertises/runs `max_prepared_statements=0` in `transaction` mode, an optimistic `protocol_level_pooler_cache=True` override must not silently succeed with stale unnamed/named `Bind` collisions; cold queries with custom result OIDs (e.g., `vector` / composite) and custom parameter OIDs execute once without inserting a catalog `Query` between unnamed `Parse` and `Bind`, and `VACUUM` executes outside any implicit `BEGIN`.
- Wrong backend credentials fail readiness deterministically, and failed setup still cleans up owned ephemeral containers without killing unrelated processes.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_services.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Digest-pinned `pgvector` 0.8.1 PG14 & PG18 | Query `extversion` from `pg_extension WHERE extname = 'vector'` and `SHOW server_version_num` + typed `PgVector((1.0, 2.0, 3.0), "vector")` round-trip | `extversion == "0.8.1"`, major versions `14` and `18` verified, and `PgVector` round-trips losslessly. |
| `PgBouncer` `transaction` (`0` vs `200`) & `session` readiness | Connect via SCRAM-SHA-256 through `proxy_config`, verify `SELECT 1`, transaction rollback, and admin `SHOW CONFIG` | Both `max_prepared_statements=0` and `200` plus `session` mode connect, roll back cleanly, and expose distinct `direct_config`. |
| Cold custom metadata + backend rotation + `VACUUM` on `max_prepared_statements=0` | Execute first-use custom enum/composite/vector parameter & result query across interleaved connections + `VACUUM` | Each statement executes once (`affected_rows` / value exact), `VACUUM` succeeds outside implicit `BEGIN`, and zero `26000` unnamed-statement clobber occurs. |
| Negative credential & optimistic cache controls | Dial `PgBouncer` with wrong password or misconfigured cache against `max_prepared_statements=0` rotation | Wrong credentials fail with SQLSTATE `28P01` / readiness failure; teardown leaves zero leaked containers or pools. |

## Verification

- Initial Red baseline: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_services.py -q -rs`
- Final Green + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_services.py -q -rs && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_services.py`

## Acceptance Criteria

- [ ] Digest-pinned `pgvector` 0.8.1 (`PG14`, `PG18`) and `edoburu/pgbouncer:v1.26.0-p0` (`transaction` `0`/`200` and `session`) fixtures pass strict readiness, capability, cold-metadata, `VACUUM`, and negative controls without skips (R12, R15, R25).
- [ ] Backing PostgreSQL and `PgBouncer` communicate via Docker bridge IP with ordered pool-before-container teardown and zero committed secrets (R25).

## Notes & Discoveries

- [2026-11-07 16:45] Keep `proxy_config` and `direct_config` paired on the `PgBouncer` fixture so `ch28:t04` exercises `direct_dsn` bypass for `LISTEN`/`NOTIFY` and `PostgresChangeSource`.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration `pytest-databases` Docker harness fixtures.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL `pooler_mode`, `protocol_level_pooler_cache`, and `direct_dsn` configuration.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Pooler statement-cache gating and PostgreSQL adapter contracts.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cold query metadata normalization, pooler modes, and vector contracts.
