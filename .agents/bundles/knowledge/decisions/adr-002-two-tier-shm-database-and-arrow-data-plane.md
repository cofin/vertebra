---
type: Decision
title: "ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane"
description: "Architectural decision to combine pure-Rust native database drivers and SHM connection leasing with a universal broker proxy for SQLSpec and Advanced Alchemy."
status: stable
tags:
  - decisions
  - adr
  - database
  - arrow
  - sqlspec
  - alchemy
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-09T15:06:00Z"
    commit: af1b3d30
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:codex
    at: "2026-11-05T06:09:26Z"
    commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
- id: src-db-contracts
  resource: src/py/vertebra/db/driver/_common.py
  title: Generic configuration contract and broker dialect identifiers
- id: src-db-registry
  resource: src/py/vertebra/db/registry.py
  title: Declarative adapter configuration registration and lazy driver resolution
- id: src-db-boundary-tests
  resource: src/py/tests/unit/db/test_native_db_drivers.py
  title: Native DB import boundaries and channel capability regressions
- id: src-agent-tools
  resource: src/py/vertebra/agent/tools/__init__.py
  title: Generic typed ToolProvider adaptation and caller-bound confirmation
- id: src-tool-contracts
  resource: src/py/vertebra/_typing.py
  title: Neutral ToolProvider and ToolPolicyError contracts exported through vertebra.typing
- id: src-tool-typing-facade
  resource: src/py/vertebra/typing.py
  title: Public neutral provider contract exports
- id: src-bigquery-provider
  resource: src/py/vertebra/db/adapters/bigquery/tools.py
  title: Adapter-owned BigQuery operations and execution policy
- id: src-spanner-provider
  resource: src/py/vertebra/db/adapters/spanner/tools.py
  title: Adapter-owned Spanner operations and execution policy
- id: src-channel-bus
  resource: src/py/vertebra/server/_channels.py
  title: Engine-neutral channel fan-out and capability bridges
- id: src-vertebra-core-src-broker-rs
  resource: src/rs/vertebra-core/src/broker.rs
  title: Rust implementation module (src/rs/vertebra-core/src/broker.rs)
- id: src-vertebra-db-src-lib-rs
  resource: src/rs/vertebra-db/src/lib.rs
  title: Rust implementation module (src/rs/vertebra-db/src/lib.rs)
- id: src-db-init-py
  resource: src/py/vertebra/db/__init__.py
  title: Python implementation module (src/py/vertebra/db/__init__.py)
- id: src-extensions-sqlspec-init-py
  resource: src/py/vertebra/extensions/sqlspec/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py)
- id: src-extensions-alchemy-init-py
  resource: src/py/vertebra/extensions/alchemy/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py)
- id: src-extensions-sqlalchemy-dialects-py
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py)
---

# ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane

<!-- truth: start -->
- **Decision**: Provide a dual-path shared-memory database architecture: (1) pure-Rust native wire drivers and cloud dialers in `vertebra-db` + `ShmLeaseTable` + `ArrowCursor` for PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, Redis, and Valkey, and (2) universal `ProxySyncConfig` / `ProxyAsyncConfig` broker wrappers that route any upstream SQLSpec or Advanced Alchemy driver across shared-memory rings with zero-copy Arrow IPC transfer.
- **Status**: Accepted (`v0.2.0`).
<!-- truth: end -->

## Context

Multi-worker Python servers traditionally maintain isolated connection pools per worker process, multiplying database session counts and serializing large result sets into Python `dict` objects under the GIL. At the same time, applications using Litestar, SQLSpec, and Advanced Alchemy rely on rich ecosystem adapters (including BigQuery, Spanner, ADBC, and asyncpg) and ORM session lifecycles.

## Python Ownership Boundaries

`vertebra.db` owns generic driver/capability contracts, registry resolution, Arrow cursors, and engine-neutral change-feed/tenant primitives. Engine configuration, SQL and native operations live in `db/adapters/<engine>/`, including PostgreSQL notifications, Spanner change streams, and database embedding pushdown. SQLSpec wrappers consume those contracts from `extensions/sqlspec`. Native `BigQueryToolProvider` and `SpannerToolProvider` classes live in their adapters and expose bound operations through neutral `vertebra.typing.ToolProvider`[^src-tool-typing-facade]; they raise neutral `ToolPolicyError` for execution-policy denials. `agent/tools.build_tools` consumes that contract generically, preserving typed invocation and caller-bound confirmation without vendor imports or engine dispatch. A generic DB operation never imports agent, framework, extension, server, or embedding implementations.[^src-db-contracts][^src-db-registry][^src-db-boundary-tests][^src-tool-contracts][^src-bigquery-provider][^src-spanner-provider][^src-agent-tools]

The registry's declarative config registration and annotation-only driver imports are composition metadata. They do not authorize concrete driver imports or engine-class dispatch inside generic modules; construction still delegates polymorphically to the owning config. Broker dialect integers likewise identify protocol/cache domains; keep their numeric identities rather than treating every engine name as executable coupling.[^src-db-registry][^src-db-contracts][^src-db-boundary-tests]

`server/_channels.py::ChannelBus` supplies engine-neutral fan-out and replay to framework and extension consumers. Its optional notification/change-source bridges consume DB capability protocols; SQL, transport setup, and engine-specific configuration live on adapters. This preserves a downward dependency from application integration to DB contracts.[^src-channel-bus]

## Alternatives Considered

| Option | Trade-offs | Outcome |
| :--- | :--- | :--- |
| **Pure-Python per-worker connection pools only** | Zero custom Rust driver code, but suffers from `W x pool_size` connection multiplication, C client library packaging friction (`libclntsh.so`, `msodbcsql18`), and high GIL object allocation overhead on analytical queries. | Rejected. |
| **Bespoke Vertebra-only ORM / query layer** | Fast wire execution, but fragments the Litestar / SQLSpec / Advanced Alchemy ecosystem and forces users to rewrite repositories and migrations. | Rejected. |
| **PyO3 per-row Arrow encoding from Python objects** | Iterating Python row dicts or tuples across PyO3 FFI to assemble Rust Arrow arrays benchmarked ~3x slower than C/native buffer paths due to per-element FFI overhead; `VTBARROW` Arrow IPC frames must materialize directly from Rust wire buffers without intermediate Python row objects. | Rejected. |
| **Pure-Rust SHM drivers + universal SQLSpec/Alchemy SHM broker proxy** | Eliminates C driver dependencies for core engines, coordinates cross-worker connection leases in `ShmLeaseTable`, streams large tabular results via zero-copy `ShmRegion` Arrow IPC (`ArrowCursor`), and plugs directly into `SQLSpecPlugin`, `AlchemyPlugin`, and SQLAlchemy `+vtb` dialects. | **Selected**. |

## Consequences

- `vertebra-db` implements shared transport, framing, completion queues, `VTBARROW` SHM cursors, cloud dialers (`CloudSqlDialer` `:3307`, `AlloyDbDialer` `:5433`), and RESP3 codecs with zero external C client libraries required at runtime; `vertebra-bigquery`, `vertebra-spanner`, `vertebra-postgres`, `vertebra-mysql`, `vertebra-mssql`, `vertebra-oracle`, and `vertebra-duckdb` ship standalone PyO3 driver wheels (`vertebra._*`), `sqlite` uses Python stdlib `sqlite3`, and `redis`/`valkey` pair `vertebra-db` RESP3 codecs with Python network streams.
- `vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`, and `vertebra.extensions.sqlalchemy` expose drop-in adapter configs and `+vtb` dialects backed by `LeaseBroker`, `StatementCache`, and `hydrate_rows_vectorcall`.

Provenance sources: [^src-vertebra-core-src-broker-rs], [^src-vertebra-db-src-lib-rs], [^src-db-init-py], [^src-extensions-sqlspec-init-py], [^src-extensions-alchemy-init-py], [^src-extensions-sqlalchemy-dialects-py].

[^src-vertebra-core-src-broker-rs]: `src/rs/vertebra-core/src/broker.rs` — Rust implementation module (src/rs/vertebra-core/src/broker.rs).
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Rust implementation module (src/rs/vertebra-db/src/lib.rs).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-extensions-sqlspec-init-py]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py).
[^src-extensions-alchemy-init-py]: `src/py/vertebra/extensions/alchemy/__init__.py` — Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py).
[^src-extensions-sqlalchemy-dialects-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py).

[^src-db-contracts]: `src/py/vertebra/db/driver/_common.py` — Generic configuration contract and broker dialect identifiers.
[^src-db-registry]: `src/py/vertebra/db/registry.py` — Declarative adapter configuration registration and lazy driver resolution.
[^src-db-boundary-tests]: `src/py/tests/unit/db/test_native_db_drivers.py` — Native DB import boundaries and channel capability regressions.
[^src-agent-tools]: `src/py/vertebra/agent/tools/__init__.py` — Generic typed ToolProvider adaptation and caller-bound confirmation.
[^src-tool-contracts]: `src/py/vertebra/_typing.py` — Neutral ToolProvider and ToolPolicyError contracts exported through vertebra.typing.
[^src-tool-typing-facade]: `src/py/vertebra/typing.py` — Public neutral provider contract exports.
[^src-bigquery-provider]: `src/py/vertebra/db/adapters/bigquery/tools.py` — Adapter-owned BigQuery operations and execution policy.
[^src-spanner-provider]: `src/py/vertebra/db/adapters/spanner/tools.py` — Adapter-owned Spanner operations and execution policy.
[^src-channel-bus]: `src/py/vertebra/server/_channels.py` — Engine-neutral channel fan-out and capability bridges.
