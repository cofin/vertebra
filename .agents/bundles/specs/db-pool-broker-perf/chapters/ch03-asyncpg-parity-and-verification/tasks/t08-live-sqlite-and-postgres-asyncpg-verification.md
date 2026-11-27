---
type: Task
id: t08
chapter_id: ch03-asyncpg-parity-and-verification
title: Run live SQLite and Docker PostgreSQL 17 asyncpg/psycopg verification suites
description: Sync --group postgres-bench into .venv, rebuild release extensions with make build, and execute full Suite B (SQLite) and Suite C (Docker PostgreSQL 17 HTTP and standalone comparators) benchmarks comparing Vertebra against asyncpg and psycopg.
status: stable
state: pending
priority: high
verification_strategy: integration_acceptance
worksheet_format: compact
depends_on:
  - t07
read_set:
  - pyproject.toml
  - tools/benchmarks/targets.py
  - tools/benchmarks/adapters/sql.py
files: []
tests: []
estimated_loc: 0
plan_revision: 2
plan_commit: "16ccb56e"
commit: null
sources:
  - id: src-pyproject
    resource: pyproject.toml
    title: Dependency group definitions including postgres-bench
  - id: src-bench-sql
    resource: tools/benchmarks/adapters/sql.py
    title: Standalone SQL, SQLSpec, and Advanced Alchemy comparator CLI
tags:
  - benchmarks
  - postgres
  - sqlite
  - asyncpg
---

# t08: Run live SQLite and Docker PostgreSQL 17 asyncpg/psycopg verification suites

## Objective & Scope Boundary

Empirically verify all three improvements (`ch01` SQLite SQLAlchemy fastpath, `ch02` lock-free async SHM broker, and `ch03` 1:1 `asyncpg`/`psycopg` local baseline) in optimized release builds (`make build`) on this 12-core Linux host.

In scope:
- Install `--group postgres-bench` (`asyncpg`, `psycopg[binary]`) into `.venv` preserving all installed extras/groups (`uv sync --all-extras --dev --group doc --group bench --group bench-comparators --group postgres-bench`).
- Run `make build` so `_vertebra` and `_sqlite` release wheels reflect `ch01` and `ch02` changes.
- Run **Suite B (SQLite 4w HTTP + standalone)**:
  - `vtb-data-shm-native-4w`, `vtb-data-sqlspec-native-4w`, `vtb-data-aa-native-4w`, `granian-asgi-data-stdlib`, `granian-asgi-data-sqlspec`, `granian-asgi-data-aa` on `-s db,queries_20,updates_5`.
  - Standalone `tools.benchmarks.adapters.sql --engine advanced_alchemy --aa-engine sqlite`.
- Run **Suite C (Docker `postgres:17-alpine` 4w HTTP + standalone)**:
  - Launch `FixtureLauncher` for `postgres` (`max_connections=300`).
  - HTTP targets on `-s db,queries_20,updates_5`: `vtb-data-shm-native-4w`, `vtb-data-shm-native-4w-broker`, `vtb-data-sqlspec-native-4w`, `vtb-data-sqlspec-shm-4w-broker`, `vtb-data-aa-native-4w`, `vtb-data-aa-shm-4w-broker`, `granian-rsgi-data-stdlib`, `granian-asgi-data-stdlib`, `granian-asgi-data-sqlspec`, `granian-asgi-data-aa`, `uvicorn-httptools-data-stdlib`, `uvicorn-httptools-data-sqlspec`, `uvicorn-httptools-data-aa`.
  - Standalone comparators (`--include-external`): `--engine postgres`, `--engine sqlspec`, and `--engine advanced_alchemy --aa-engine postgres`.

Out of scope:
- Committing `tools/benchmarks/results/benchmarks.{duckdb,jsonl}` or temporary `/tmp/*.json` reports.

## Bounded Read Manifest

- `pyproject.toml` (lines 295-338) [^src-pyproject]
- `tools/benchmarks/adapters/sql.py` (CLI entrypoint) [^src-bench-sql]

## Contracts & Edge Cases

- Use `BypassSandbox: true` only for `uv sync` (network), `docker` fixture commands, and live loopback benchmark runs; keep build steps sandboxed where possible.
- Always invoke benchmark CLI as `PYTHONPATH=. uv run --no-sync python -m tools.benchmarks run ...`.
- Stop Docker container cleanly in `finally` after Suite C finishes.

## Test Matrix

- Suite B SQLite 4w HTTP (`0` errors on all Vertebra targets; `vtb-data-aa-native-4w` matches or beats `granian-asgi-data-aa`).
- Suite C Postgres 17 4w HTTP (`0` errors on all Vertebra targets; `shm_broker` closes gap with `per_worker`; complete 1:1 `asyncpg` numbers captured).
- Standalone Postgres `--include-external` (`vertebra-sync` vs `psycopg-sync`, `vertebra-async` vs `asyncpg`, `vertebra-sqlspec-postgres-async` vs `sqlspec-asyncpg`, `vertebra-aa-postgres-async` vs `vanilla-aa-postgres-async`).

## Verification

```bash
uv run --no-sync python -c "import asyncpg, psycopg; print(asyncpg.__version__, psycopg.__version__)"
```

## Acceptance Criteria

- Live Suite B and Suite C tables recorded with run IDs, RPS, P99 latency, and zero Vertebra errors.
- `vtb-data-aa-native-4w` SQLite `/db` reaches `>= 4,500 RPS` and `queries_20` / `updates_5` improve by `>= 2×`.
- `vtb-data-shm-native-4w-broker` closes the `shm_broker` contention penalty under `c=128`.

[^src-pyproject]: `pyproject.toml` — Dependency group definitions including postgres-bench.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py` — Standalone SQL, SQLSpec, and Advanced Alchemy comparator CLI.
