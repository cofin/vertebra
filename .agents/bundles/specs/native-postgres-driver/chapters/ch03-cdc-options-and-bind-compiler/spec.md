---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch03-cdc-options-and-bind-compiler
title: "Chapter 03: PostgresChangeSource, Options Bridge, and Lexical Bind Compiler"
description: "Implement EventSessionRegistry, CAS checkpoint-before-ack PostgresChangeSource, DSN/cloud options.py bridge, and single-pass lexer binds.py."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
last_reviewed_commit: 65577e7a
updated_at: "2026-11-13T05:15:30Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T05:15:30Z"
    commit: 65577e7a
tags:
  - python
  - postgres
  - cdc
  - parser
sources:
  - id: pg-events
    resource: src/py/vertebra/db/adapters/postgres/events.py
    title: "PostgreSQL event driver protocols"
  - id: pg-config
    resource: "src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig"
    title: "PostgreSQL DSN, pooler, TLS, and cloud configuration"
  - id: pg-core
    resource: "src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore"
    title: "PostgreSQL SQL and parameter helpers"
  - id: db-cdc
    resource: "src/py/vertebra/db/cdc.py::ChangeFeed"
    title: "Engine-neutral ChangeFeed and spill hydration"
---

# Chapter 03: PostgresChangeSource, Options Bridge, and Lexical Bind Compiler

<!-- truth: start -->
- `EventSessionRegistry` tracks dedicated notification and replication handles for deterministic driver `close()`, while `PostgresChangeSource` persists CAS checkpoints before calling wire `acknowledge(lsn)` [^pg-events] [^db-cdc].
- `options.py` normalizes DSN, percent-decoded Unix sockets (preserving `+`), multi-host targets, libpq `PG*` defaults, `PoolerMode`, and Cloud SQL / AlloyDB material options [^pg-config].
- `binds.py` rewrites `%s` and `%(name)s` to `$N` in one lexical pass while preserving string literals (`E'...'`), dollar-quoted `$tag$...$tag$`, comments, `::` casts, and `jsonb` `?`/`?|`/`?&` operators [^pg-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Builds the pure-Python CDC coordinator, option normalizer, and SQL bind lexer consumed by the permanent native drivers in `ch04`.

## 2. Requirements, Concrete Contracts & Architecture

- `events.py` & `change_stream.py`: `PostgresReplicationDriver` protocol, `EventSessionRegistry`, `PostgresCheckpoint(FrozenBaseStruct)`, `PostgresCheckpointStore` protocol, `MemoryPostgresCheckpointStore`, and `PostgresChangeSource` (`AsyncChangeSource` with single-flight shielded CAS save-before-wire-ack, `ChangeFeed` integration, `replay_since`, `aclose`).
- `options.py`: `PostgresClientOptions` builder from `PostgreSQLConfig` (`sslmode=prefer` adapter default when unset, `disable` on Unix sockets, `verify-full` raw native default).
- `binds.py`: `compile_postgres_binds(sql, params)` + owned parameter normalization (`bytes` buffer copies, `PG_JSONB_TEXT`, `VectorBatchView`).

## 3. Documentation & OKF Knowledge Plan

User and OKF documentation for CDC, options, and `$N` binds land in `ch08` and `ch09`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R2` `EventSessionRegistry` & checkpoint store | `t01` | `events.py`, `change_stream.py` | `pytest test_postgres_change_stream.py` |
| `R2` `PostgresChangeSource` coordinator | `t02` | `change_stream.py` | `pytest test_postgres_change_stream.py` |
| `R2` DSN/libpq/cloud `options.py` bridge | `t03` | `options.py`, `config.py` | `pytest test_postgres_options_and_binds.py` |
| `R2` Single-pass lexer `binds.py` | `t04` | `binds.py`, `core.py` | `pytest test_postgres_options_and_binds.py` |

## 5. Implementation Plan

- [x] [a09818b9] [t01-event-session-registry-and-checkpoint-stores.md](tasks/t01-event-session-registry-and-checkpoint-stores.md) — Extend events.py with PostgresReplicationDriver and EventSessionRegistry and create checkpoint structs/stores in change_stream.py.
- [x] [a09818b9] [t02-postgres-change-source-coordinator.md](tasks/t02-postgres-change-source-coordinator.md) — Build PostgresChangeSource streaming transaction records into ChangeFeed and persisting CAS checkpoints before wire WAL acks.
- [x] [a09818b9] [t03-dsn-and-cloud-options-bridge.md](tasks/t03-dsn-and-cloud-options-bridge.md) — Translate PostgreSQLConfig, libpq PG* env defaults, percent-decoded sockets, and Cloud SQL/AlloyDB settings into native client kwargs.
- [x] [a09818b9] [t04-lexer-bind-compiler-and-owned-params.md](tasks/t04-lexer-bind-compiler-and-owned-params.md) — Compile %s, %(name)s, and $N binds in one lexical pass while normalizing mutable buffers, JSONB text, and pgvector inputs.

## 6. Current State & Remaining Plan Snapshot

Completed (`4/4` tasks at `a09818b9`: `ruff`, `mypy` across `15` source files, and `111` unit tests pass). Fast `< 20s` pure-Python unit, Ruff, and Mypy gate.

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_change_stream.py src/py/tests/unit/db/test_postgres_options_and_binds.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/tests/unit/db/test_postgres_change_stream.py src/py/tests/unit/db/test_postgres_options_and_binds.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

[^pg-events]: `src/py/vertebra/db/adapters/postgres/events.py` — PostgreSQL event driver protocols.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL DSN, pooler, TLS, and cloud configuration.
[^pg-core]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — PostgreSQL SQL and parameter helpers.
[^db-cdc]: `src/py/vertebra/db/cdc.py::ChangeFeed` — Engine-neutral ChangeFeed and spill hydration.
