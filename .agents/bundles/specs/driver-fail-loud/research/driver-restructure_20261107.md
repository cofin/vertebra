---
type: Research
research_id: "driver-restructure-20261107"
title: "Codebase Audit: Native Driver State and Wheel Packaging"
description: "Only BigQuery and Spanner execute natively; Oracle and MSSQL run on sqlite3 stand-ins, vertebra-db holds codecs without drivers, and per-engine wheels stay justified because applications load one or two engines."
scope: architecture
status: stable
state: promoted
promoted_to: driver-fail-loud
tags: [research, database, packaging, drivers]
generated:
  by: flow/0.28.0
  at: 2026-11-07T00:00:00Z
verified:
  - by: agent:researcher
    at: 2026-11-07T00:00:00Z
sources:
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db re-exports and crate docs
  - id: db-connection
    resource: src/rs/vertebra-db/src/connection.rs
    title: Transport stream, TLS, Connection and CancelHandle traits
  - id: db-arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ACTIVE_ARROW_SHM_REGIONS
    title: Per-binary VTBARROW region registry
  - id: py-db-bindings
    resource: src/rs/vertebra-py/src/db.rs::register_db_bindings
    title: Codec and dialer exposure on vertebra._vertebra
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver backed by sqlite3 in-memory connection
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: MSSQL driver backed by sqlite3 in-memory connection
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver with silent sqlite3 fallback
  - id: sqla-dialect
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: DB-API shim with SQLite fallback on URL parse failure
  - id: registry
    resource: src/py/vertebra/db/registry.py::REGISTERED_CONFIG_TYPES
    title: Closed config registry
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance suite targeting vertebra._<engine> modules
  - id: pyproject
    resource: pyproject.toml
    title: Optional dependency extras
  - id: prd
    resource: .agents/bundles/specs/native-data-platform/spec.md
    title: Native data platform PRD and resolved Q5 crate table
---

# Codebase Audit: Native Driver State and Wheel Packaging

<!-- truth: start -->
- Native execution exists only in `vertebra._bigquery` and `vertebra._spanner`; every other engine executes through Python.[^registry]
- Oracle and MSSQL drivers execute against `sqlite3.connect(":memory:")` and ignore host, user, and password.[^oracle-driver][^mssql-driver]
- DuckDB silently falls back to `sqlite3` when the `duckdb` module is absent, and the SQLAlchemy shim falls back to an in-memory SQLite config on any URL parse error.[^duckdb-driver][^sqla-dialect]
- `vertebra-db` contains encoders, a complete TLS transport, the async completion bridge, and the `VTBARROW` writer; no engine module opens a socket or decodes a result set.[^db-lib][^db-connection]
- The six `*Pool` types in `vertebra-db` are lease tables with no connections and have no callers.[^db-lib]
- `connection.rs::Connection` and `CancelHandle` have only test implementers.[^db-connection]
- `pyproject.toml` declares no `postgres`, `mysql`, `oracle`, `mssql`, `sqlite`, `duckdb`, or `redis` extras, while driver code calls `ensure_module(..., extra="postgres")`; `psycopg`, `pymysql`, and `asyncmy` are undeclared.[^pyproject]
- The conformance suite resolves non-existent `vertebra._<engine>` modules, so those parametrized cases error rather than skip.[^conformance]
- `DatabaseRegistry` imports all eleven config classes eagerly through a hard-coded tuple; no entry-point discovery exists.[^registry]
- The draft `native-redis-driver` description specifies a hand-built Tokio RESP3 client, contradicting the PRD crate table (`redis` 1.8.1).[^prd]
<!-- truth: end -->

## Engine Status

| Engine | Executes through | Status |
| --- | --- | --- |
| PostgreSQL | `psycopg_pool` / `asyncpg` (undeclared), `memory://` fake | Real Python library |
| MySQL | `pymysql` / `asyncmy` (undeclared), `memory://` fake | Real Python library |
| SQLite | stdlib `sqlite3` | Real (stdlib) |
| Oracle | `sqlite3` in-memory | Stand-in |
| MSSQL | `sqlite3` in-memory | Stand-in |
| DuckDB | `duckdb` when importable, else `sqlite3` | Partial, silent fallback |
| Redis / Valkey | Python sockets + Python RESP3 parser, `memory://` fake | Real (pure Python) |
| DataFusion | `datafusion` PyPI package, no extra | Real Python library |
| BigQuery / Spanner | `vertebra._bigquery` / `vertebra._spanner` | Native |

## Rust Codec Disposition

The PRD builds MySQL on `mysql_async`, MSSQL on `tiberius`, Oracle on `oracledb`, SQLite on `rusqlite`, DuckDB on `duckdb`, and Redis on `redis`.[^prd] Each crate owns its framing and authentication, so the hand-written `mysql/`, `mssql/` (except the `@P1` rewriter), Oracle TTC piggyback, `sqlite/`, and `duckdb/` modules in `vertebra-db` have no consumer after cutover. Codecs that survive: PostgreSQL frames, PGCOPY, JSONB, pgvector, Oracle VECTOR, `tns.rs` descriptor parsing (`parse_tnsnames_ora`, `resolve_tns_alias`), and possibly RESP3 CRC16 slot hashing; `register_db_bindings` exposes the surviving codecs to Python, and Python wallet handling lives in `oracle/config.py::OracleWallet`.[^py-db-bindings] `connection.rs` is consumed by the PostgreSQL client; `tiberius` accepts any `AsyncRead + AsyncWrite` stream and can reuse `TransportStream`.

## Per-Engine Crates

Arguments that keep engine code in separate crates:

- DuckDB bundles a C++ amalgamation with multi-minute builds and a large binary.
- `oracledb` pins `arrow 59` while `duckdb` pins `arrow 60`; one crate would link two Arrow majors.
- HTTP-only installs carry no engine dependencies.

Costs:

- Each wheel multiplies the release matrix by the interpreter set (3.12–3.15 plus 3.14t/3.15t) and platform set.
- Each binary carries its own copy of `py_async.rs` static values, its Tokio runtime, and `ACTIVE_ARROW_SHM_REGIONS`; ADR-004 covers capsule ownership, and registry duplication needs explicit release tests per wheel.[^db-arrow-shm]

The pure-Rust network engines (PostgreSQL, MySQL, MSSQL, Redis) share Tokio, rustls, `connection.rs`, and GCP auth, with small dependency trees. Crate and wheel are separable: these engines can stay in separate crates while shipping in one wheel, leaving DuckDB and Oracle as their own wheels.

Applications typically load one or two engines, so the duplicated cost is roughly one extra Tokio runtime (two worker threads, `py_async.rs` multi-thread builder) and a few megabytes of shared Tokio, rustls, and PyO3 code per additional wheel. Per-engine wheels stay the packaging decision (PRD Q5); the release matrix is a CI cost, not an install cost.

## Decisions

- *(user, 2026-11-07)* Stand-ins and silent SQLite fallbacks raise `MissingDependencyError` in a dedicated spec ahead of the native driver cutovers, which also declares extras, makes conformance cases skip, and deletes dead pools and superseded codecs.

## Unplanned Gaps

1. Oracle and MSSQL stand-ins accept connection settings and return SQLite results; no spec makes them fail loudly before native cutover.
2. Silent SQLite fallbacks in DuckDB and the SQLAlchemy shim.
3. Missing per-engine extras and undeclared Python client dependencies.
4. Conformance cases that error instead of skipping when `vertebra._<engine>` is absent.
5. Closed registry with no third-party or per-wheel discovery.
6. Dead `*Pool` types and codecs superseded by client crates.
7. `lib.rs` and driver docstrings that describe native protocols.
8. DataFusion has no extra and no native plan.
9. `native-redis-driver` crate choice contradicts the PRD.

[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db re-exports and crate docs.
[^db-connection]: `src/rs/vertebra-db/src/connection.rs` — Transport stream, TLS, Connection and CancelHandle traits.
[^db-arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::ACTIVE_ARROW_SHM_REGIONS` — Per-binary VTBARROW region registry.
[^py-db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Codec and dialer exposure on vertebra._vertebra.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver backed by sqlite3 in-memory connection.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — MSSQL driver backed by sqlite3 in-memory connection.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver with silent sqlite3 fallback.
[^sqla-dialect]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — DB-API shim with SQLite fallback on URL parse failure.
[^registry]: `src/py/vertebra/db/registry.py::REGISTERED_CONFIG_TYPES` — Closed config registry.
[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance suite targeting vertebra._<engine> modules.
[^pyproject]: `pyproject.toml` — Optional dependency extras.
[^prd]: `.agents/bundles/specs/native-data-platform/spec.md` — Native data platform PRD and resolved Q5 crate table.
