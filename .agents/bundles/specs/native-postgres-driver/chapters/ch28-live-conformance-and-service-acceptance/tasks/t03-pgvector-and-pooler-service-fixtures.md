---
type: Task
id: "native-postgres-driver:ch28:t03"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Provision digest-pinned pgvector 0.8.1 and PgBouncer 1.26.0 acceptance service fixtures
description: Add VERTEBRA_CONFORMANCE_DOCKER-gated multi-arch OCI-index digest-pinned pgvector 0.8.1 (PG14 and PG18) and pg_authid SCRAM-authenticated PgBouncer 1.26.0 Docker fixtures in integration/conftest.py and verify capability, cold-metadata, and negative controls in test_postgres_services.py.
status: draft
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
estimated_loc: 245
plan_revision: 7
plan_commit: c8722c76
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
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration pytest-databases Docker harness fixtures and require_live_postgres_docker gate
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

Add reusable `VERTEBRA_CONFORMANCE_DOCKER == "1"`-gated local Docker acceptance fixtures for multi-arch digest-pinned `pgvector` 0.8.1 (`PG14` and `PG18`) and SCRAM-authenticated `edoburu/pgbouncer:v1.26.0-p0` (`transaction` mode `max_prepared_statements=0` and `200`, plus `session` control) in `src/py/tests/integration/conftest.py`, and verify parameterized service readiness, `extversion == "0.8.1"`, cold-metadata rotation safety, `VACUUM` outside implicit `BEGIN`, and negative credential/cache controls in `src/py/tests/integration/db/test_postgres_services.py` [^int-conftest] [^pg-config] [^okf-drivers] [^pg-contracts].

Out of scope: Exhaustive `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, `PostgresEmbeddingPushdown`, and full `PgBouncer` workload matrices (owned by `ch28:t04`).

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `require_live_postgres_docker`, `DockerService`, and PostgreSQL fixtures [^int-conftest].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig`, `PoolerMode`, `protocol_level_pooler_cache`, and `direct_dsn` [^pg-config].
- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`.
- [.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md](../../../../../knowledge/domains/data/database-drivers-and-dialers.md) — Pooler cache gating invariants [^okf-drivers].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Cold query metadata normalization and pooler configuration contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py` (every live fixture calls `require_live_postgres_docker()` before touching `docker_service` so offline runs skip cleanly):
  - Verified multi-arch OCI manifest-index (`application/vnd.oci.image.index.v1+json`, `linux/amd64` + `linux/arm64`, inspected via `docker buildx imagetools inspect`) `pgvector` 0.8.1 fixture `@pytest.fixture(params=(14, 18), scope="session") def vtb_pgvector_config(request: pytest.FixtureRequest) -> Iterator[tuple[int, PostgreSQLConfig]]`:
    - PG14 multi-arch index image: `"pgvector/pgvector:0.8.1-pg14@sha256:2b8cdfaad8e380ef81202cb2df1db35a83482ca7c6f4d933d01d84c294ba64ff"`.
    - PG18 multi-arch index image: `"pgvector/pgvector:0.8.1-pg18@sha256:508c5290cda481d4f5f846446a26e9c1b804766828a394a5861de1b348a18b4c"`.
    - Provisions an isolated owned schema/database with explicit `sslmode="disable"`, runs `CREATE EXTENSION IF NOT EXISTS vector`, and asserts mandatory live SQL checks `SELECT extversion FROM pg_extension WHERE extname = 'vector'` (`== "0.8.1"`) and `SHOW server_version_num` major `== 14` / `18` before yielding.
  - Ephemeral SCRAM `PgBouncer` fixture using multi-arch `"edoburu/pgbouncer:v1.26.0-p0@sha256:b17551c776ef7e5769ef80b956d20f85e2fd25dd8912d31d58f782aad495b711"` parameterized via `@pytest.fixture(params=...)` across `("transaction", 0)`, `("transaction", 200)`, and `("session", 200)`:
    - Inspects the owned backing PostgreSQL container's Docker default-bridge IP (`assert bridge_ip`), then queries `SELECT rolpassword FROM pg_authid WHERE rolname = $1` on the backing PostgreSQL container over a superuser native connection and asserts `isinstance(rolpassword, str) and rolpassword.startswith("SCRAM-SHA-256$")`.
    - Launches `PgBouncer` via `docker_service.run(..., container_port=5432, entrypoint=["sh", "-c"], command=[bootstrap_script], env=..., check=..., transient=True)` where `bootstrap_script` (`set -eu` without `-x`) writes ephemeral in-container `/tmp/pgbouncer.users` (`"<user>" "<rolpassword>"` loaded from container env vars so `$` inside `SCRAM-SHA-256$...` is never corrupted by shell interpolation, `chmod 600`) and `/tmp/pgbouncer.ini` (`[databases]` routing to `bridge_ip:5432`, `[pgbouncer]` setting `listen_addr = 0.0.0.0`, `listen_port = 5432`, `auth_type = scram-sha-256`, `auth_file = /tmp/pgbouncer.users`, `admin_users`, `pool_mode`, `max_prepared_statements`, `server_reset_query`, `server_reset_query_always`, `max_client_conn`, `default_pool_size`) before `exec /usr/bin/pgbouncer /tmp/pgbouncer.ini` — completely avoiding `/entrypoint.sh`'s broken `DB_PASSWORD` plaintext userlist generation and host `tmp_path` UID bind-mount permission failures.
    - Yields `(pool_mode, max_prepared_statements, proxy_config, direct_config)` with `sslmode="disable"`, `direct_dsn=direct_config.dsn`, and ordered pool-before-container teardown.
- `src/py/tests/integration/db/test_postgres_services.py`:
  - Uses `@pytest.mark.parametrize` and parameterized fixtures exclusively (zero test-body `for` loops iterating over versions, pooler configs, or sync/async drivers).

## Edge Cases & Error Semantics

- Never dial `localhost` or host loopback from inside `PgBouncer`; always dial the backing PostgreSQL container's Docker bridge IP on `:5432`, and never pass unquoted `$` from `SCRAM-SHA-256$<iter>:<salt>$<stored>:<server>` through shell double-quote expansion (print env vars via `printf '"%s" "%s"\n' "$PGB_USER" "$PGB_SCRAM_VERIFIER" > /tmp/pgbouncer.users`).
- Under `transaction` mode with `max_prepared_statements=0`, cold queries with custom parameter/result OIDs (`vector`, composite, enum) execute cleanly across backend rotation without catalog `Query` interleaving between unnamed `Parse` and `Bind`, and `VACUUM` executes outside any implicit `BEGIN`.
- Wrong SCRAM passwords fail closed with SQLSTATE `28P01`, and teardown drains native pools before stopping ephemeral containers.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_services.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Parameterized multi-arch `pgvector` 0.8.1 (`PG14` & `PG18`) | Query `SELECT extversion FROM pg_extension WHERE extname = 'vector'`, `SHOW server_version_num`, and `PgVector((1.0, 2.0, 3.0), "vector")` round-trip on sync & async drivers | `extversion == "0.8.1"`, major versions `14` and `18` verified, and `PgVector` round-trips losslessly. |
| SCRAM `PgBouncer` 1.26.0 (`transaction` `0` & `200`, `session` `200`) | Connect via SCRAM-SHA-256 through `proxy_config`, verify `SELECT 1`, transaction rollback, and `pgbouncer` admin `SHOW CONFIG` (`auth_type`, `pool_mode`, `max_prepared_statements`) | All three modes authenticate via `SCRAM-SHA-256$...` verifier, report exact `SHOW CONFIG` values, roll back cleanly, and expose live `direct_config`. |
| Cold custom metadata + backend rotation + `VACUUM` (`max_prepared_statements=0`) | Parameterized sync/async first-use custom enum/composite/`vector` queries across concurrent pooled clients (`default_pool_size=2`) + standalone `VACUUM` | Exact row/count results with zero `26000`/`0A000` errors; `VACUUM` succeeds outside implicit `BEGIN`. |
| Negative SCRAM password & optimistic cache controls | Dial `proxy_config` with wrong password (`28P01`) and test `protocol_level_pooler_cache` gating on `max_prepared_statements=0` | Wrong password raises `DatabaseError`/`OperationalError` (`sqlstate == "28P01"`); `0` leaked containers or pools after teardown. |

## Verification

- Offline safety check: `env -u VERTEBRA_CONFORMANCE_DOCKER -u VERTEBRA_CONFORMANCE_POSTGRES_DSN -u VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS uv run --no-sync pytest src/py/tests/integration/db/test_postgres_services.py -q`
- Final strict live Green + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_services.py -q -rs && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_services.py`

## Acceptance Criteria

- [ ] Multi-arch digest-pinned `pgvector` 0.8.1 (`PG14`, `PG18` with live `extversion == "0.8.1"` and major `14`/`18` SQL assertions) and in-container `/tmp/pgbouncer.{users,ini}` `pg_authid.rolpassword` SCRAM `PgBouncer` 1.26.0 fixtures pass strict `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` readiness, cold-metadata, `VACUUM`, and negative controls without test-body `for` loops (R12, R15, R25).
- [ ] Offline runs skip cleanly without touching Docker, while live runs communicate over Docker bridge IPs with zero host `tmp_path` permission dependencies or leaked containers (R25).

## Notes & Discoveries

- Pairing `proxy_config` (`direct_dsn=direct_config.dsn`) with `direct_config` directly unblocks `ch28:t04`'s transaction-pooled `LISTEN`/`NOTIFY` and `PostgresChangeSource` bypass acceptance tests.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration `pytest-databases` Docker harness fixtures and `require_live_postgres_docker` gate.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL `pooler_mode`, `protocol_level_pooler_cache`, and `direct_dsn` configuration.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Pooler statement-cache gating and PostgreSQL adapter contracts.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cold query metadata normalization, pooler modes, and vector contracts.
