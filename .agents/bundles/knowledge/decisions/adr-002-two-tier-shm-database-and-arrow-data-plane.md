---
type: Concept
title: "ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane"
description: "Architectural decision to combine pure-Rust native database drivers and SHM connection leasing with a universal broker proxy for SQLSpec and Advanced Alchemy."
status: stable
tags: [decisions, adr, database, arrow, sqlspec, alchemy]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-core-src-broker-rs
    resource: src/rs/vertebra-core/src/broker.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/broker.rs)"
  - id: src-vertebra-db-src-lib-rs
    resource: src/rs/vertebra-db/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/lib.rs)"
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: "Python implementation module (src/py/vertebra/db/__init__.py)"
  - id: src-extensions-sqlspec-init-py
    resource: src/py/vertebra/extensions/sqlspec/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py)"
  - id: src-extensions-alchemy-init-py
    resource: src/py/vertebra/extensions/alchemy/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py)"
  - id: src-extensions-sqlalchemy-dialects-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py)"
---

# ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane

<!-- truth: start -->
- **Decision**: Provide a dual-path shared-memory database architecture: (1) pure-Rust native wire drivers and cloud dialers in `vertebra-db` + `ShmLeaseTable` + `ArrowCursor` for PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, Redis, and Valkey, and (2) universal `ProxySyncConfig` / `ProxyAsyncConfig` broker wrappers that route any upstream SQLSpec or Advanced Alchemy driver across shared-memory rings with zero-copy Arrow IPC transfer.
- **Status**: Accepted (`v0.2.0`).
<!-- truth: end -->

## Context

Multi-worker Python servers traditionally maintain isolated connection pools per worker process, multiplying database session counts and serializing large result sets into Python `dict` objects under the GIL. At the same time, applications using Litestar, SQLSpec, and Advanced Alchemy rely on rich ecosystem adapters (including BigQuery, Spanner, ADBC, and asyncpg) and ORM session lifecycles.

## Alternatives Considered

| Option | Trade-offs | Outcome |
| :--- | :--- | :--- |
| **Pure-Python per-worker connection pools only** | Zero custom Rust driver code, but suffers from `W x pool_size` connection multiplication, C client library packaging friction (`libclntsh.so`, `msodbcsql18`), and high GIL object allocation overhead on analytical queries. | Rejected. |
| **Bespoke Vertebra-only ORM / query layer** | Fast wire execution, but fragments the Litestar / SQLSpec / Advanced Alchemy ecosystem and forces users to rewrite repositories and migrations. | Rejected. |
| **PyO3 per-row Arrow encoding from Python objects** | Iterating Python row dicts or tuples across PyO3 FFI to assemble Rust Arrow arrays benchmarked ~3x slower than C/native buffer paths due to per-element FFI overhead; `VTBARROW` Arrow IPC frames must materialize directly from Rust wire buffers without intermediate Python row objects. | Rejected. |
| **Pure-Rust SHM drivers + universal SQLSpec/Alchemy SHM broker proxy** | Eliminates C driver dependencies for core engines, coordinates cross-worker connection leases in `ShmLeaseTable`, streams large tabular results via zero-copy `ShmRegion` Arrow IPC (`ArrowCursor`), and plugs directly into `SQLSpecPlugin`, `AlchemyPlugin`, and SQLAlchemy `+vtb` dialects. | **Selected**. |

## Consequences

- `vertebra-db` implements pure-Rust protocol codecs and cloud dialers (`CloudSqlDialer` `:3307`, `AlloyDbDialer` `:5433`, Oracle Thin TNS/TTC + Wallet, TDS `@P1` rewriting, RESP3 client tracking) with zero external C client libraries required at runtime.
- `vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`, and `vertebra.extensions.sqlalchemy` expose drop-in adapter configs and `+vtb` dialects backed by `LeaseBroker`, `StatementCache`, and `hydrate_rows_vectorcall`.

Provenance sources: [^src-vertebra-core-src-broker-rs], [^src-vertebra-db-src-lib-rs], [^src-db-init-py], [^src-extensions-sqlspec-init-py], [^src-extensions-alchemy-init-py], [^src-extensions-sqlalchemy-dialects-py].

[^src-vertebra-core-src-broker-rs]: `src/rs/vertebra-core/src/broker.rs` — Rust implementation module (src/rs/vertebra-core/src/broker.rs).
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Rust implementation module (src/rs/vertebra-db/src/lib.rs).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-extensions-sqlspec-init-py]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py).
[^src-extensions-alchemy-init-py]: `src/py/vertebra/extensions/alchemy/__init__.py` — Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py).
[^src-extensions-sqlalchemy-dialects-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py).
