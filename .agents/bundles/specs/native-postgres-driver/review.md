---
type: Review
flow_id: native-postgres-driver
title: "Native PostgreSQL planning review"
description: "Adjudicated contract invariants for the merged vertebra-postgres baseline and consolidated ch01-ch09 completion plan."
status: stable
state: active
plan_revision: 9
plan_commit: 18baadcd
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
sources:
  - id: pg-native-okf
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: "Merged standalone vertebra-postgres wheel and bounded ReplicationSession baseline"
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: "Public interface and ownership contracts for native PostgreSQL"
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: "Python PostgreSQL driver target cutover seam"
tags:
  - postgres
  - planning
  - driver
---

# Native PostgreSQL Planning Review

<!-- truth: start -->
- **Merged Baseline (`18baadcd`)**: Standalone `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) and bounded single-generation `ReplicationSession` transport are merged on `main` and recorded in `postgres-native-driver.md` [^pg-native-okf].
- **Consolidated Remaining Delivery (`ch01`–`ch09`, `32` Tasks)**: Open work progresses through `ch01` verified replication supervision, `ch02` PyO3 replication bridge + FFI OKF, `ch03` CDC `PostgresChangeSource` + DSN bridge + lexical bind compiler, `ch04` permanent `_sync.py`/`_async.py` drivers, `ch05` atomic cutover + legacy/dialer cleanup, `ch06` live PG14–18 conformance, `ch07` wheel/CI/benchmarks, `ch08` user/reference docs, and `ch09` OKF/routing sync [^pg-contracts] [^pg-driver-py].
<!-- truth: end -->

## Adjudicated Contract Decisions

| Area | Enforced Contract |
| --- | --- |
| Replication supervision & PyO3 bridge (`ch01`–`ch02`) | `ReplicationSupervisor` fences stale leases across reconnect generations, verifies server/database/slot/publication identity and WAL bounds before `START_REPLICATION`, and uses `spawn_rust_db_task_with_attr_and_future` (`-> PyResult<RustDbTaskAwaitable>`) so cancelled event-loop completions never publish commits [^pg-contracts]. |
| Durable CDC & adapter options/binds (`ch03`) | `PostgresChangeSource` opens native streams with `reconnect=False`, persists CAS checkpoints (`MemoryPostgresCheckpointStore` non-durable default) inside a shielded single-flight task before wire WAL feedback, and pairs `options.py` (`PG*`/OS defaults, `+`-preserving percent decode) with `binds.py` single-pass `E'...'` pyformat lexing [^pg-contracts]. |
| Permanent drivers & atomic cutover (`ch04`–`ch05`) | `_sync.py` and `_async.py` wire `pipeline.py`, `arrow.py`, and `EventSessionRegistry` (`await handle.aclose()`, `await client.close()`); `ch05` cuts SQLSpec (`ParameterStyle.NUMERIC`), SQLAlchemy (`numeric_dollar`), the 12-line `driver.py` barrel, `embeddings.py` (`$1`/`$2`), and `PostgreSQLQueueBackend` (`vertebra._postgres` guard) over in one batch while deleting legacy `psycopg`/`asyncpg`/`asyncmy` dialer factories and hardening `cloudsql.py` and `alloydb.py` [^pg-driver-py]. |
| Offline chapter gates vs live `/flow:finish` (`ch06`–`ch07`) | Chapter gates execute `< 60s` offline unit, stubtest, and benchmark comparator checks; live Docker PG14–18/`pgvector`/`PgBouncer` suites (`VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`), `make postgres-wheel-check`, and `make postgres-bench` execute during `/flow:finish` and CI (`BASE=18baadcd`). |
| Documentation & OKF budgets (`ch08`–`ch09`) | User docs obey `H2+H3 <= 14`, reference HTML `<= 120,000B`, tabbed installs, and zero external library names; OKF chapters land `<= 200` lines (`AGENTS.md <= 100` lines), updating `database-driver-limitations.md`, `postgres-native-driver.md`, and `postgres-limitations.md` in place [^pg-native-okf]. |

[^pg-native-okf]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` — Merged standalone `vertebra-postgres` wheel and bounded `ReplicationSession` baseline.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public interface and ownership contracts for native PostgreSQL.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver target cutover seam.
