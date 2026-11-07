---
type: Task
id: "native-postgres-driver:t96"
title: "Provide local PostgreSQL vector and transaction-pooler acceptance services"
description: "Provide local PostgreSQL vector and transaction-pooler acceptance services for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t95"
files:
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_postgres_services.py
tests:
  - src/py/tests/integration/db/test_postgres_services.py
plan_revision: 2
plan_commit: cdb978d3
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
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - pooler
  - fixtures
---

# Provide local PostgreSQL vector and transaction-pooler acceptance services

## Objective

Supply truthful reusable local PostgreSQL, pgvector and PgBouncer services before full native feature acceptance.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, completed native pooler capability/cache worksheets, integration/conftest.py and installed pytest-databases source/skill. Core PostgreSQL version fixtures exist; PgBouncer fixture does not. This setup task separates service lifecycle/configuration from t97 feature assertions. One invocation/one signed local conventional commit, no implementation with null plan_commit, no live external database or hosted artifact mutation.

## Steps

Every test-owned vanilla PostgreSQL, pgvector, PgBouncer, administrative, cancellation observer and installed-wheel native config in this worksheet explicitly sets `sslmode="disable"`; these local images do not provide TLS. This is scoped to PostgreSQL fixtures and never changes other engine defaults or dedicated t05 TLS authentication fixtures.

1. Use exact installed postgres_14..18_service for vanilla servers. Override the vector fixtures with `provide_postgres_service(...,image="pgvector/pgvector:0.8.1-pg14@sha256:2b8cdfaad8e380ef81202cb2df1db35a83482ca7c6f4d933d01d84c294ba64ff",...)` and corresponding PG18 image `pgvector/pgvector:0.8.1-pg18@sha256:508c5290cda481d4f5f846446a26e9c1b804766828a394a5861de1b348a18b4c`. Both official OCI index digests were verified active for amd64/arm64; do not inherit package floating pgvector:pg14/pg18 tags. The v0.8.1 official Dockerfile sources git#v0.8.1 against requested PG_MAJOR. Compose native PostgreSQLConfig with sslmode="disable" and quoted credentials. In unique owned database/schema CREATE EXTENSION vector and assert pg_extension.extversion exactly0.8.1 plus server_version_num major14/18 before publishing fixture. Negative version mismatch fails setup; teardown removes only test-owned state.
2. Implement PgBouncer fixture with exact verified maintainer image edoburu/pgbouncer:v1.26.0-p0 (PgBouncer1.26.0, Alpine3.24), container_port5432, ENTRYPOINT /entrypoint.sh and CMD /usr/bin/pgbouncer /etc/pgbouncer/pgbouncer.ini. Use DockerService.run env DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME, AUTH_TYPE=scram-sha-256, POOL_MODE=transaction, MAX_PREPARED_STATEMENTS=0 or200 and test-only username/password constants. The verified entrypoint creates plaintext userlist credentials for SCRAM. Generate explicit read-only config/userlist only in fixture temporary storage for reset controls, using supported volumes/command/entrypoint APIs; assert existing mounted config is retained. Source contains no test credentials or generated config file. SERVER_RESET_QUERY and SERVER_RESET_QUERY_ALWAYS are verified env options. No unsupported network argument or guessed env keys.
3. Backing PostgreSQL and PgBouncer are both dedicated ephemeral transient owned containers on the same default Docker bridge. Read the owned backend container NetworkSettings bridge IP and point DB_HOST there with DB_PORT5432; assert bridge IP is nonempty and reachable. The host native client uses PgBouncer ServiceContainer.host plus its dynamic mapped port. Never dial proxy-container localhost or a backend host-mapped loopback port from inside PgBouncer. Parameterize transaction mode max_prepared_statements0 and200 and a session-mode control; readiness is an actual native SELECT1 through PgBouncer plus SHOW CONFIG/POOLS through controlled admin connection where required. Assert proxy version/capability values and backing PostgreSQL version/identity. Direct endpoint and proxy endpoint remain separate fixture values so t97 tests notifications/CDC bypass proxy. Fixture teardown closes dependent native pools before PgBouncer and backing services; no unrelated process/container termination.
4. Add RED smoke tests showing direct and pooled SQL/transaction rollback, pgvector extension and known typed vector query, cache0 versus protocol-cache-enabled config, and ephemeral-container cleanup ownership. Negative controls prove wrong backend credentials fail readiness, advertised0 defeats an optimistic cache flag, incorrect direct/proxy identity is rejected and teardown still removes only owned resources after failed setup. These validate setup and capability truth; t97 owns statement cache/stream/event feature acceptance. PgCat/pgdog unverified capability does not imply PgBouncer named-prepared support: test pure policy via predecessor unit matrix and configure conservative cache unless a specific supported server capability has live evidence.
5. Run all fixture smoke cases with strict no-skip service mode and record actual image/server versions. Commit test(postgres): provide native advanced acceptance services.

## Verification

Estimated changed lines including fixture files/tests250–350. Readiness failures are test failures in strict mode, not acceptance skips. Commands are planned:

```bash
make dev
VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_services.py -q -rs
uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_services.py
```

Add live cache-disabled transaction-pooler cases using first-use custom result and custom parameter metadata, deliberate backend rotation and VACUUM outside implicit BEGIN. Assert each original Execute occurs once; no Bind targets a statement destroyed by catalog Query and no automatic replay follows metadata failure.

## Acceptance Criteria

- [ ] R25: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Native-ready vector/direct/PgBouncer services expose explicit version/cache capability and isolated endpoints with package-owned cleanup.
- [ ] No fabricated fixture options, unverified proxy assumptions or external service actions enter acceptance.
- [ ] One local signed conventional commit and synchronized task-first evidence precede t97.

## Notes & Discoveries

None.
