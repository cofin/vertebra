# Phase 3: ORM Extensions & Drivers

**Goal**: Plugin-based provider registration for SQLSpec; Advanced Alchemy integration comes last.
**Version Baseline**: Pin minimums to the latest stable SQLSpec + Advanced Alchemy at implementation time.

**Design Doc**: [`design/driver-ecosystem.md`](../design/driver-ecosystem.md)

## Architecture Summary

| Library | Integration Pattern | Broker Registration |
|---------|--------------------|--------------------|
| **SQLSpec** | `Vertebra(plugins=[SqlSpecProvider(...)])` | Full IPC (16+ adapters) |
| **Advanced Alchemy** | Provider plugin (last) | Full IPC (any SA driver) |
| **Plain SQLAlchemy** | Exposed drivers only | Deferred until last |

## Tasks

### 3.1 Core Registration API

- [x] **P3.1.1** Add plugin list support on `Vertebra(...)`.
- [x] **P3.1.2** Add `register_plugin(...)` for provider registration.
- [ ] **P3.1.3** Add plugin validation (ensure provider implements register/serve).
- [ ] **P3.1.4** Write registration unit tests.

### 3.2 SQLSpec Integration

- [x] **P3.2.1** Hook into SQLSpec's `provide_session()` to route through broker.
- [x] **P3.2.2** Support all 16+ SQLSpec adapters (asyncpg, psycopg, duckdb, etc.) via proxy config.
- [x] **P3.2.3** Implement Arrow result transport (shared memory).
- [ ] **P3.2.4** Implement `write_to_storage_async()` passthrough.
- [ ] **P3.2.5** Transaction boundary tracking for connection affinity.
- [ ] **P3.2.6** Write SQLSpec integration tests (multiple adapters).
- [x] **P3.2.7** Propagate SQLSpec correlation IDs into trace context and logs.

### 3.3 Advanced Alchemy Integration (Last)

- [ ] **P3.3.1** Hook into AA's session lifecycle to route through broker.
- [ ] **P3.3.2** Verify Repository pattern works with broker routing.
- [ ] **P3.3.3** Verify Service pattern works with broker routing.
- [ ] **P3.3.4** Implement File Object Storage routing via IPC.
- [ ] **P3.3.5** Write AA Repository/Service integration tests.

### 3.4 Internal Driver Enhancements

The Bun drivers (`src/ts/src/drivers/`) currently support basic `connect`, `execute`, `close`. They must expose the **full Bun API surface** for ORM compatibility.

**Reference**: [Bun SQL Docs](https://bun.com/docs/runtime/sql), [bun:sqlite Docs](https://bun.com/docs/runtime/sqlite), [Bun Redis Docs](https://bun.com/docs/runtime/redis)

#### 3.4.1 Postgres/MySQL Driver (Bun.SQL)

**Transactions**:
- [ ] **P3.4.1** Add `begin` method - wraps `sql.begin()` with scoped connection.
- [ ] **P3.4.2** Add `commit` method.
- [ ] **P3.4.3** Add `rollback` method.
- [ ] **P3.4.4** Add `beginDistributed` method - two-phase commit (PREPARE TRANSACTION).
- [ ] **P3.4.5** Implement savepoint support for nested transactions.

**Query Features**:
- [ ] **P3.4.6** Expose prepared statement caching (enabled by default in Bun).
- [ ] **P3.4.7** Expose query pipelining for batch operations.
- [ ] **P3.4.8** Add `unsafe()` method passthrough for raw SQL.

**Result Metadata**:
- [ ] **P3.4.9** Return `lastrowid` for INSERT operations.
- [ ] **P3.4.10** Return `rowcount` for UPDATE/DELETE operations.
- [ ] **P3.4.11** Return column type information from result description.

**Connection Metadata**:
- [ ] **P3.4.12** Return `server_version` on connect.
- [ ] **P3.4.13** Return `encoding` on connect.
- [ ] **P3.4.14** Expose pool statistics (`active`, `idle`, `waiting`).

#### 3.4.2 SQLite Driver (bun:sqlite)

**Transactions**:
- [ ] **P3.4.15** Add `transaction` method - wraps `db.transaction()` callable.
- [ ] **P3.4.16** Support `deferred`, `immediate`, `exclusive` transaction modes.
- [ ] **P3.4.17** Support nested transactions (savepoints).

**Query Methods**:
- [ ] **P3.4.18** Add `get` method - returns single row (`.get()`).
- [ ] **P3.4.19** Add `all` method - returns all rows (`.all()`).
- [ ] **P3.4.20** Add `values` method - returns arrays (`.values()`).
- [ ] **P3.4.21** Add `run` method - returns `{ lastInsertRowid, changes }`.
- [ ] **P3.4.22** Add `exec` method - execute raw SQL without results.

**Database Features**:
- [ ] **P3.4.23** Expose `serialize()` / `deserialize()` for backup/restore.
- [ ] **P3.4.24** Expose `pragma` passthrough for WAL mode, etc.
- [ ] **P3.4.25** Expose `loadExtension()` for SQLite extensions.

#### 3.4.3 Redis Driver (Bun.RedisClient)

**Pub/Sub**:
- [ ] **P3.4.26** Add `subscribe` method with channel + callback.
- [ ] **P3.4.27** Add `unsubscribe` method (channel-specific or all).
- [ ] **P3.4.28** Add `publish` method.
- [ ] **P3.4.29** Add `duplicate` method for subscriber connections.

**Pipelining**:
- [ ] **P3.4.30** Expose automatic pipelining (enabled by default).
- [ ] **P3.4.31** Add `pipeline` method for explicit batch operations.

**Connection Options**:
- [ ] **P3.4.32** Expose `connectionTimeout` option.
- [ ] **P3.4.33** Expose `idleTimeout` option.
- [ ] **P3.4.34** Expose `autoReconnect` option.
- [ ] **P3.4.35** Expose `enableOfflineQueue` option.
- [ ] **P3.4.36** Expose TLS configuration options.

**Commands**:
- [ ] **P3.4.37** Support all 66+ Redis commands via `send()`.
- [ ] **P3.4.38** Add typed helpers for common commands (GET, SET, HGET, LPUSH, etc.).

#### 3.4.4 Driver Tests

- [ ] **P3.4.39** Write Postgres transaction tests (begin/commit/rollback/savepoint).
- [ ] **P3.4.40** Write MySQL transaction tests.
- [ ] **P3.4.41** Write SQLite transaction tests (all modes).
- [ ] **P3.4.42** Write Redis pub/sub tests.
- [ ] **P3.4.43** Write metadata extraction tests (all drivers).
- [ ] **P3.4.44** Test connection pool behavior under load.
- [ ] **P3.4.45** Benchmark prepared statement performance.

### 3.5 Exposed Drivers (Plain SQLAlchemy - Last)

- [ ] **P3.5.1** Implement `vertebra+postgres://` dialect (uses Bun postgres driver).
- [ ] **P3.5.2** Implement `vertebra+mysql://` dialect (uses Bun mysql driver).
- [ ] **P3.5.3** Implement `vertebra+sqlite://` dialect (uses Bun sqlite driver).
- [ ] **P3.5.4** Register dialect entrypoints in `pyproject.toml`.
- [ ] **P3.5.5** Write dialect tests (basic CRUD operations).

### 3.6 Litestar Plugin Integration

- [ ] **P3.6.1** Add `sqlspec_configs` param to `VertebraPlugin`.
- [ ] **P3.6.2** Add `alchemy_configs` param to `VertebraPlugin`.
- [ ] **P3.6.3** Auto-register configs on plugin init.
- [ ] **P3.6.4** Integrate with AA's Litestar plugin for DI.
- [ ] **P3.6.5** Write Litestar integration tests.

### 3.7 Container Integration Tests

- [ ] **P3.7.1** Create container fixtures (Postgres, MySQL, Redis) using `tools/lib/container.py`.
- [ ] **P3.7.2** Test SQLSpec with asyncpg against real Postgres.
- [ ] **P3.7.3** Test SQLSpec with DuckDB (no container needed).
- [ ] **P3.7.4** Test Advanced Alchemy Repository against real Postgres.
- [ ] **P3.7.5** Verify Arrow zero-copy transport end-to-end.
- [ ] **P3.7.6** Test storage write operations.

### 3.8 fsspec Integration (Local-first)

- [ ] **P3.8.1** Implement `fsspec` adapter backed by brokered `vertebra.storage`.
- [ ] **P3.8.2** Support core filesystem methods (`open`, `ls`, `info`, `exists`, `mkdir`, `rm`).
- [ ] **P3.8.3** Local backend tests (default).
- [ ] **P3.8.4** Optional S3/GCS tests gated by env vars; mark xfail/skip when not configured.

## Verification

```bash
# Run integration tests
pytest tests/integration/

# Specific tests
pytest tests/integration/test_sqlspec_asyncpg.py
pytest tests/integration/test_alchemy_repository.py
pytest tests/integration/test_arrow_transport.py
```

**Success Criteria**:
- Internal drivers expose full Bun API surface (transactions, metadata, pub/sub)
- SQLSpec configs register and `provide_session()` works through broker
- AA Repository/Service patterns work with registered configs
- Arrow results transport via shared memory (zero-copy verified)
- Storage write operations complete successfully
- All driver tests pass with real databases (via container fixtures)
