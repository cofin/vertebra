---
type: Task
id: sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t03
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: Implement Alembic DefaultImpl, Rust ShmCache, and Sync/Async Native Cache Backends
description: Split Alembic DefaultImpl and DDL ops into alembic/{__init__,impl,ops}.py, implement Rust ShmCache with ShmLeaseTable ring-buffer L1 invalidation, and build configurable SyncCacheBackend and AsyncCacheBackend (shm://, memory://, valkey://, redis://, s3:///gs:///az:///r2:///oci:///file://) plus optional dogpile.cache backend plugins.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t02
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
  - src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
  - src/py/vertebra/extensions/sqlalchemy/alembic/ops.py
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/alchemy/__init__.py
tests:
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/_storage.pyi
estimated_loc: 245
tags:
  - alembic
  - sqlalchemy
  - ddl
  - advanced-alchemy
  - cache
  - shm
  - dogpile-cache
  - valkey
  - redis
  - storage
sources:
  - id: shm-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Rust POSIX shared-memory lease table and ring buffer
  - id: py-db-rs
    resource: src/rs/vertebra-py/src/db.rs
    title: PyO3 bindings for vertebra._vertebra database and SHM primitives
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: Type stub declarations for vertebra._vertebra
  - id: sa-alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: Monolithic Alembic DefaultImpl and vector/view DDL operations
  - id: db-common
    resource: src/py/vertebra/db/driver/_common.py
    title: BaseDatabaseConfig effective_direct_dsn metadata
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin
verification_strategy: regression_tdd
---

# Task `t03`: Implement Alembic DefaultImpl, Rust ShmCache, and Sync/Async Native Cache Backends

## Objective & Scope Boundary

Delete monolithic `extensions/sqlalchemy/alembic.py` and split it into `alembic/{__init__,impl,ops}.py`, implement cross-worker POSIX `ShmCache` in `src/rs/vertebra-core/src/broker.rs` + `src/rs/vertebra-py/src/db.rs` + `src/py/vertebra/_vertebra.pyi`, and build configurable `SyncCacheBackend` (`SyncCacheRegionProtocol`) and `AsyncCacheBackend` (`shm://`, `memory://`, `valkey://`, `redis://`, `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://`) plus `AsyncCacheManager`, `ZeroCopyServiceMixin`, and optional `dogpile.cache` backend plugins in `src/py/vertebra/extensions/alchemy/cache.py` per [`../spec.md`](../spec.md) [^shm-broker] [^py-db-rs] [^vertebra-pyi] [^sa-alembic] [^db-common] [^aa-cache]. Out of scope: Alembic autogenerate comparator hooks and `extensions/sqlalchemy/cache.py` re-exports (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseTable` ring buffer (`publish_channel` / `drain_channel`) and POSIX SHM segment primitives.
- `src/rs/vertebra-py/src/broker.rs` & `src/py/vertebra/_vertebra.pyi` — `LeaseBroker.publish_channel` / `drain_channel` / `drain_all_channels` bindings and stubs; the new `ShmCache` binding and stub land beside them.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — existing `DefaultImpl` subclasses and DDL ops to refactor and remove (`git rm`).
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig.effective_direct_dsn`.
- `src/py/vertebra/extensions/alchemy/cache.py` — cache backends, `AsyncCacheManager`, and `ZeroCopyServiceMixin`.
- `src/py/vertebra/db/adapters/valkey/driver.py`, `src/py/vertebra/db/adapters/redis/driver.py`, and `src/py/vertebra/_storage.pyi` — native Valkey, Redis, and ObjectStore drivers.

## Contracts & Edge Cases

- Implement `DefaultImpl` subclasses registered via `ImplMeta` (`PostgreSQLAlembicImpl`, `MySQLAlembicImpl`, `SQLiteAlembicImpl`, `OracleAlembicImpl`, `MSSQLAlembicImpl`, `DuckDBAlembicImpl`, `BigQueryAlembicImpl` with `transactional_ddl = False`, `SpannerAlembicImpl` and `SpangresAlembicImpl` with `transactional_ddl = False` and batched `update_ddl`) and resolve direct migration URLs from `BaseDatabaseConfig.effective_direct_dsn` with zero `isinstance(..., (PostgreSQLConfig, MySQLConfig))` checks.
- Implement Rust `ShmCache` in `vertebra-core` + `vertebra-py` (`vertebra._vertebra.ShmCache(name, capacity_bytes=..., max_entries=...)`) with lock-free seqlock reads, CLOCK eviction, monotonic `expiry_ns` TTL, atomic `incr`/`decr`, prefix invalidation, and cross-worker L1 invalidations published/drained via `ShmLeaseTable` ring buffer (`publish_channel` / `drain_channel` on `b"vtb:cache:inv"`).
- Implement `SyncCacheBackend` (satisfying Advanced Alchemy `SyncCacheRegionProtocol` directly: `get`, `get_or_create`, `set`, `delete`, `invalidate`, `configure`; returning `advanced_alchemy.cache._null.NO_VALUE` on misses) and `AsyncCacheBackend` in `src/py/vertebra/extensions/alchemy/cache.py` supporting:
  - `shm://<segment>`: `vertebra._vertebra.ShmCache` + `ShmLeaseTable` ring-buffer invalidation.
  - `memory://`: In-process monotonic TTL + LRU cache with optional `ShmLeaseTable` ring-buffer invalidation.
  - `valkey://` / `valkeys://`: Native `SyncValkeyDriver` / `AsyncValkeyDriver` (`vertebra._valkey`) with pipelined `get_multi`/`set_multi`/`delete_multi`, `SET ... PX [NX]`, `INCRBY`/`DECRBY`, prefix `SCAN`+`UNLINK`, and RESP3 `CLIENT TRACKING` L1 invalidation bridged into `ShmCache`/SHM ring buffer.
  - `redis://` / `rediss://`: Native `SyncRedisDriver` / `AsyncRedisDriver` (`vertebra._redis`) with identical capabilities.
  - `s3://`, `gs://`, `az://`, `r2://`, `oci://`, `file://`: Native `SyncObjectStore` / `AsyncObjectStore` (`vertebra._storage`) using the ADR-005 `VTBSTORE\x01` MessagePack `(deadline_ms, payload)` TTL envelope.
  - Optional tiered L1 (`l1_url="shm://..."` or `"memory://"`): reads check L1 before L2; writes/deletes/prefix-invalidations broadcast over the SHM ring buffer (`b"vtb:cache:inv"`).
- When `dogpile.cache` is installed (`register_dogpile_backends()`), register `"vertebra.shm"`, `"vertebra.memory"`, `"vertebra.valkey"`, `"vertebra.redis"`, and `"vertebra.storage"` `dogpile.cache.api.CacheBackend` classes delegating to `SyncCacheBackend`.

## Test Matrix

- Rust and Python `ShmCache` tests verifying cross-handle reads/writes, monotonic TTL expiry, CLOCK eviction, atomic `incr`/`decr`, prefix invalidation, and `ShmLeaseTable` ring-buffer invalidation (`b"vtb:cache:inv"`).
- Parametrized `SyncCacheBackend` and `AsyncCacheBackend` tests across `shm://`, `memory://`, `valkey://`, `redis://`, and `file://` (`VTBSTORE\x01`), L1+L2 tiered invalidation, `AsyncCacheManager` native async L2 execution (zero `asyncio.to_thread`), `ZeroCopyServiceMixin`, and `dogpile.cache` `make_region().configure("vertebra.shm" | "vertebra.memory" | "vertebra.valkey" | "vertebra.redis" | "vertebra.storage")` tests.
- Parametrized Alembic DDL rendering, `ImplMeta` dialect lookup, Spanner `update_ddl` batching, and `effective_direct_dsn` URL resolution tests.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q
```

## Acceptance Criteria

- [ ] Monolithic `extensions/sqlalchemy/alembic.py` is deleted and replaced by `alembic/{__init__,impl,ops}.py`.
- [ ] `vertebra._vertebra.ShmCache` passes Rust tests and `mypy.stubtest`.
- [ ] `SyncCacheBackend` and `AsyncCacheBackend` work across `shm://`, `memory://`, `valkey://`, `redis://`, and `file://`/`s3://`/`gs://`/`az://`/`r2://`/`oci://` with SHM ring-buffer L1 invalidation and optional `dogpile.cache` backend plugins.
- [ ] Zero regressions in the fast verification gate.

[^shm-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Rust POSIX shared-memory lease table and ring buffer.
[^py-db-rs]: `src/rs/vertebra-py/src/db.rs` — PyO3 bindings for vertebra._vertebra database and SHM primitives.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stub declarations for vertebra._vertebra.
[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic DefaultImpl and vector/view DDL operations.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig effective_direct_dsn metadata.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin.
