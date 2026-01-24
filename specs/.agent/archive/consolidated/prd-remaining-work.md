# PRD: Vertebra 1.0 - Consolidated Remaining Work

**Status:** Active - Consolidated Specification
**Date:** 2026-01-19
**Priority:** P0 - Critical Path to 1.0
**Estimated Duration:** 6-8 weeks

---

## Executive Summary

This document consolidates ALL remaining work required to ship Vertebra 1.0, a production-ready Python application framework with shared-memory IPC and multi-language acceleration layers.

Vertebra has completed its core architecture (Rust IPC primitives, Python bindings, Bun server layer, shared-memory broker) but requires systematic completion of production features, API stabilization, and quality improvements before 1.0 release.

**Current State:**
- ✅ Core IPC primitives (Rust shared memory, ring buffers)
- ✅ Python/TypeScript bindings
- ✅ Shared-memory broker with fairness scheduler
- ✅ Basic SQLSpec integration
- ✅ HTTP/1.1 and HTTP/2 server
- ✅ ASGI worker bridge

**Remaining Work:**
- 🔧 Test infrastructure fixes (deadlock resolution)
- 🔧 Codebase consolidation (Phase 4: renames + docs)
- 🔧 API rename (Broker* → Vertebra*)
- 🚧 SQLSpec stabilization + serialization module
- 🚧 SQLAlchemy DBAPI 2.0 implementation
- 🚧 Advanced Alchemy integration
- 🚧 Pool health checks + lifecycle hooks
- 🚧 MCP protocol implementation
- 🚧 Type safety audit (fix 120+ mypy errors)
- 📚 Documentation + examples

---

## Phase 1: Immediate Priorities (Week 1-2)

### 1.1 Test Deadlock Fix (P0 - BLOCKING)

**Problem:** Python test suite hangs indefinitely, preventing CI/CD and development workflow.

**Root Causes:**
- Stale shared memory from previous test runs with corrupt state
- Non-unique shared memory names causing collisions (`vertebra-broker-test` hardcoded)
- Missing timeouts on blocking IPC operations
- Incomplete cleanup patterns

**Solution:**
```python
# Three complementary strategies:
1. Isolation: unique_shm_name() for per-test unique names
2. Timeouts: pytest-timeout with 30s limit
3. Cleanup: tracked_shm_cleanup() context manager
```

**Tasks:** (10 tasks, 3 phases)

**Phase 1.1.1: Immediate Unblock (P0)**
- [ ] Task 1: Add pytest-timeout configuration to `pyproject.toml`
  ```toml
  [tool.pytest.ini_options]
  timeout = 30
  timeout_method = "thread"
  ```
- [ ] Task 2: Clean stale shared memory manually
  ```bash
  rm -rf /dev/shm/test-* /dev/shm/*vertebra*
  ```
- [ ] Task 3: Enhance cleanup patterns in `src/py/tests/conftest.py`
  - Add `"*asgi*"`, `"*broker*"` patterns
  - Add cleanup AFTER test completion (not just before)

**Phase 1.1.2: Test Isolation (P1)**
- [ ] Task 4: Create test helpers module
  - File: `src/py/tests/helpers/__init__.py`, `src/py/tests/helpers/shm.py`
  - Implement `unique_shm_name(prefix: str = "vertebra-test") -> str`
  - Implement `tracked_shm_cleanup() -> Generator[list[str], None, None]`
- [ ] Task 5: Update `src/py/tests/test_broker_client.py` with unique names
- [ ] Task 6: Update `src/py/tests/test_broker_pool.py` with unique names
- [ ] Task 7: Update `src/py/tests/test_asgi_worker.py` with unique names

**Phase 1.1.3: Robustness (P2)**
- [ ] Task 8: Add thread coordination to `test_ipc_channel.py`
  - Use `threading.Event()` for ready/shutdown signals
- [ ] Task 9: Add thread coordination to `test_ipc_integration.py`
- [ ] Task 10: Add asyncio timeouts to async test operations
  - Wrap `asyncio.to_thread()` with `asyncio.wait_for()`

**Acceptance Criteria:**
- [ ] `make test` completes within 60 seconds without hanging
- [ ] All existing tests pass (44+ tests)
- [ ] No stale `/dev/shm/` files after test run
- [ ] Running tests 5 times consecutively passes
- [ ] Tests work with `pytest-xdist -n 4` parallelism

**Files Modified:**
- `pyproject.toml`
- `src/py/tests/conftest.py`
- `src/py/tests/helpers/__init__.py` (NEW)
- `src/py/tests/helpers/shm.py` (NEW)
- `src/py/tests/test_broker_client.py`
- `src/py/tests/test_broker_pool.py`
- `src/py/tests/test_asgi_worker.py`
- `src/py/tests/test_ipc_channel.py`
- `src/py/tests/test_ipc_integration.py`

**Reference:** `/home/cody/code/c/vertebra/specs/active/test-deadlock-fix/prd.md`

---

### 1.2 Codebase Consolidation Phase 4 (P1)

**Status:** 75% complete (Phases 1-3 done)

**Remaining Work:**

**Task 4.1: Rename TypeScript Worker Files**
- [ ] Rename `src/ts/src/server/worker-pool.ts` → `worker-connections.ts`
- [ ] Rename `src/ts/src/server/worker-manager.ts` → `process-manager.ts`
- [ ] Update imports in `src/ts/src/server/index.ts`
- [ ] Run `make js-test js-type-check`

**Task 4.2: Update CLAUDE.md**
- [ ] Update Directory Layout section with new module structure
- [ ] Document storage module (merged from filesystem + storage)
- [ ] Document `resources/broker.py` purpose
- [ ] Document TypeScript pool naming conventions
- [ ] Add note about Rust binding feature parity

**Task 4.3: Add Architecture Documentation**
- [ ] Add module docstring to `src/py/vertebra/storage/__init__.py`
  ```python
  """Unified storage abstraction for local/remote filesystems.

  Provides sync and async clients that route operations through
  the Vertebra broker for shared connection pooling.
  """
  ```
- [ ] Add module docstring to `src/py/vertebra/resources/__init__.py`
  ```python
  """Resource management and broker communication.

  BrokerClient handles shared-memory IPC to the Bun broker process.
  """
  ```
- [ ] Add JSDoc to `src/ts/src/broker/index.ts` explaining pool types
- [ ] Add JSDoc to `src/ts/src/server/index.ts` explaining server architecture

**Acceptance Criteria:**
- [ ] Files renamed and imports updated
- [ ] Documentation reflects actual structure
- [ ] Module purposes clearly explained
- [ ] All tests pass

**Reference:** `/home/cody/code/c/vertebra/specs/active/codebase-consolidation/prd.md`

---

### 1.3 API Rename: Broker* → Vertebra* (P0 - PUBLIC API STABILIZATION)

**Problem:** Public APIs use `Broker*` prefix which is implementation detail leakage. User-facing classes should use `Vertebra*` prefix for brand consistency.

**Scope:** SQLSpec extension public APIs only (internal broker client remains as-is)

**Changes Required:**

**Python Files:**
- `src/py/vertebra/extensions/sqlspec/_config.py`
  - `BrokerSyncConfigBase` → `VertebraSyncConfigBase`
  - `BrokerAsyncConfigBase` → `VertebraAsyncConfigBase`
- `src/py/vertebra/extensions/sqlspec/_driver.py`
  - `BrokerProxySyncDriver` → `VertebraProxySyncDriver`
  - `BrokerProxyAsyncDriver` → `VertebraProxyAsyncDriver`
- `src/py/vertebra/extensions/sqlspec/postgres/config.py`
  - `BrokerPostgresSyncConfig` → `VertebraPostgresSyncConfig`
  - `BrokerPostgresAsyncConfig` → `VertebraPostgresAsyncConfig`
- `src/py/vertebra/extensions/sqlspec/postgres/driver.py`
  - `BrokerPostgresSyncDriver` → `VertebraPostgresSyncDriver`
  - `BrokerPostgresAsyncDriver` → `VertebraPostgresAsyncDriver`
- `src/py/vertebra/extensions/sqlspec/mysql/config.py` (same pattern)
- `src/py/vertebra/extensions/sqlspec/mysql/driver.py` (same pattern)
- `src/py/vertebra/extensions/sqlspec/sqlite/config.py` (same pattern)
- `src/py/vertebra/extensions/sqlspec/sqlite/driver.py` (same pattern)

**Test Files:**
- Update all imports in `src/py/tests/test_sqlspec_*.py`

**Tasks:**
- [ ] Search and replace `BrokerSyncConfigBase` → `VertebraSyncConfigBase` (all files)
- [ ] Search and replace `BrokerAsyncConfigBase` → `VertebraAsyncConfigBase` (all files)
- [ ] Search and replace `BrokerPostgres` → `VertebraPostgres` (postgres files)
- [ ] Search and replace `BrokerMysql` → `VertebraMysql` (mysql files)
- [ ] Search and replace `BrokerSqlite` → `VertebraSqlite` (sqlite files)
- [ ] Search and replace `BrokerProxy` → `VertebraProxy` (driver files)
- [ ] Update test imports
- [ ] Run `make test` and `make lint-all`

**Acceptance Criteria:**
- [ ] All public APIs use `Vertebra*` prefix
- [ ] Internal broker client remains as `BrokerClient` (unchanged)
- [ ] All tests pass
- [ ] No mypy/pyright errors introduced

**Example Usage After Rename:**
```python
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

config = VertebraPostgresAsyncConfig(pool="main")
async with config.provide_session() as session:
    result = await session.select("SELECT 1")
```

---

## Phase 2: ORM & Database Integration (Week 3-4)

### 2.1 SQLSpec API Stabilization

**Status:** ~85% complete (core adapters implemented, needs storage + arrow + rename)

**Remaining Work:**

**Task 2.1.1: API Rename (from Phase 1.3)**
- [ ] Complete `Broker*` → `Vertebra*` rename across all SQLSpec configs

**Task 2.1.2: Arrow Serialization**
- [ ] Implement Arrow IPC transport for large result sets (>1MB or >1000 rows)
- [ ] Add `_arrow_threshold` parameter to driver configs
- [ ] Implement `arrow_region_pool` in provider
- [ ] Add nanoarrow decoder in Python client
- [ ] Test with 10k+ row queries

**Task 2.1.3: Storage Integration**
- [ ] Implement `VertebraObjectStore` for SQLSpec `ObjectStoreBase` protocol
- [ ] Wire to broker's storage pool
- [ ] Test with `write_to_storage_async()` operations

**Task 2.1.4: Integration Tests**
- [ ] Add container tests for SQLSpec + Postgres
- [ ] Add container tests for SQLSpec + MySQL
- [ ] Add container tests for SQLSpec + Redis
- [ ] Verify Arrow serialization works with real databases

**Files:**
- `src/py/vertebra/extensions/sqlspec/_storage.py` (NEW)
- `src/py/vertebra/extensions/sqlspec/_driver.py` (enhance with Arrow)
- `src/py/vertebra/extensions/sqlspec/provider.py` (enhance with Arrow pooling)
- `src/py/tests/test_sqlspec_storage.py` (NEW)
- `src/py/tests/test_sqlspec_arrow.py` (NEW)

**Acceptance Criteria:**
- [ ] All SQLSpec APIs use `Vertebra*` prefix
- [ ] Arrow serialization works for large result sets
- [ ] Storage operations route through broker
- [ ] Integration tests pass with real databases

---

### 2.2 SQLAlchemy DBAPI 2.0 + Dialects

**Status:** 0% (not started)

**Goal:** Enable standard SQLAlchemy usage with Vertebra connection pooling

**Architecture:**
```
SQLAlchemy Engine
    ↓ (uses dialect)
Vertebra Dialect (postgresql+vertebra://, mysql+vertebra://, sqlite+vertebra://)
    ↓ (uses DBAPI)
Vertebra DBAPI Module (PEP 249)
    ↓ (IPC via shared memory)
Bun Broker → Real Database
```

**Tasks:**

**Task 2.2.1: DBAPI Module**
- [ ] Create `src/py/vertebra/extensions/sqlalchemy/_dbapi.py`
- [ ] Implement `connect(dsn: str) -> Connection` (dsn = pool name)
- [ ] Implement `Connection` class with PEP 249 methods
  - `cursor() -> Cursor`
  - `commit()`
  - `rollback()`
  - `close()`
- [ ] Implement `Cursor` class with PEP 249 methods
  - `execute(query, params=None)`
  - `executemany(query, params_list)`
  - `fetchone() -> tuple | None`
  - `fetchall() -> list[tuple]`
  - `fetchmany(size=None) -> list[tuple]`
- [ ] Route all operations through broker IPC
- [ ] Handle paramstyle per dialect (numeric for Postgres, qmark for MySQL/SQLite)

**Task 2.2.2: Postgres Dialect**
- [ ] Create `src/py/vertebra/extensions/sqlalchemy/postgres/dialect.py`
- [ ] Implement `VertebraPostgresDialect(PGDialect)`
- [ ] Implement `VertebraPostgresAsyncDialect(PGAsyncDialect)`
- [ ] Register with SQLAlchemy entry points
- [ ] Set `paramstyle = "numeric"`
- [ ] Set `supports_transactional_ddl = True`

**Task 2.2.3: MySQL Dialect**
- [ ] Create `src/py/vertebra/extensions/sqlalchemy/mysql/dialect.py`
- [ ] Implement `VertebraMysqlDialect(MySQLDialect)`
- [ ] Implement `VertebraMysqlAsyncDialect(MySQLAsyncDialect)`
- [ ] Register with SQLAlchemy entry points
- [ ] Set `paramstyle = "qmark"`
- [ ] Set `supports_transactional_ddl = False`

**Task 2.2.4: SQLite Dialect**
- [ ] Create `src/py/vertebra/extensions/sqlalchemy/sqlite/dialect.py`
- [ ] Implement `VertebraSqliteDialect(SQLiteDialect)`
- [ ] Implement `VertebraSqliteAsyncDialect(SQLiteAsyncDialect)`
- [ ] Register with SQLAlchemy entry points
- [ ] Set `paramstyle = "qmark"`
- [ ] Set `supports_transactional_ddl = True`

**Task 2.2.5: Transaction Support**
- [ ] Implement `begin` command in broker protocol
- [ ] Implement `commit` command in broker protocol
- [ ] Implement `rollback` command in broker protocol
- [ ] Add transaction ID tracking in connection state
- [ ] Test nested transactions (savepoints)

**Task 2.2.6: Integration Tests**
- [ ] Test `create_engine("postgresql+vertebra://main")`
- [ ] Test `create_async_engine("postgresql+vertebra://main")`
- [ ] Test basic CRUD operations
- [ ] Test transactions (commit/rollback)
- [ ] Test with SQLAlchemy ORM (declarative models)

**Files:**
- `src/py/vertebra/extensions/sqlalchemy/__init__.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/_dbapi.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/_base.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/postgres/dialect.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/postgres/dbapi.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/mysql/dialect.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/mysql/dbapi.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/sqlite/dialect.py` (NEW)
- `src/py/vertebra/extensions/sqlalchemy/sqlite/dbapi.py` (NEW)
- `src/py/tests/test_sqlalchemy_dbapi.py` (NEW)
- `src/py/tests/test_sqlalchemy_dialect.py` (NEW)

**Acceptance Criteria:**
- [ ] `create_engine("postgresql+vertebra://main")` works
- [ ] `create_async_engine("postgresql+vertebra://main")` works
- [ ] Basic CRUD operations work
- [ ] Transactions work correctly
- [ ] All DBAPI 2.0 requirements met

---

### 2.3 Serialization Module

**Status:** 0% (not started)

**Goal:** Unified type encoding/decoding for IPC (MessagePack) and HTTP (JSON)

**Motivation:**
- SQLSpec provides multi-tier serialization (msgspec > orjson > stdlib)
- Litestar provides type_encoders/type_decoders for common types
- Vertebra needs to extend both for IPC-specific types (FileObject, Arrow batches, database-specific types)

**Architecture:**
```python
# MessagePack for IPC
VertebraCodec (msgspec.msgpack with type hooks)
    ↓
Broker IPC (shared memory)

# JSON for HTTP
VERTEBRA_TYPE_ENCODERS (Litestar integration)
    ↓
Litestar Response Serialization
```

**Tasks:**

**Task 2.3.1: Type Support Matrix Implementation**
- [ ] Create `src/py/vertebra/extensions/serialization/__init__.py`
- [ ] Create `src/py/vertebra/extensions/serialization/_types.py`
  - Define Vertebra-specific types (FileObject, ArrowBatch, etc.)
- [ ] Create `src/py/vertebra/extensions/serialization/_encoders.py`
  - Implement encoders for 30+ types (see table below)
- [ ] Create `src/py/vertebra/extensions/serialization/_decoders.py`
  - Implement decoders for 30+ types

**Type Support Matrix:**
| Type | JSON Encode | JSON Decode | MessagePack | Priority |
|------|-------------|-------------|-------------|----------|
| `datetime` | ISO 8601 | Parse ISO | Native | P0 |
| `date` | ISO format | Parse ISO | String | P0 |
| `time` | ISO format | Parse ISO | String | P1 |
| `timedelta` | Seconds float | From seconds | Float | P1 |
| `Decimal` | String | From string | String | P0 |
| `UUID` | String | From string | Bytes | P0 |
| `Path`/`PurePath` | String | From string | String | P1 |
| `IPv4Address`/`IPv6Address` | String | From string | String | P1 |
| `Pattern` | `.pattern` | `re.compile` | String | P2 |
| `Enum` | `.value` | From value | Value | P0 |
| `set`/`frozenset` | List | From list | List | P1 |
| `bytes` | Base64 | From base64 | Native | P0 |
| `Pydantic BaseModel` | `.model_dump()` | `.model_validate()` | Dict | P0 |
| `msgspec.Struct` | Dict | From dict | Dict | P0 |
| `dataclass` | `asdict()` | From dict | Dict | P1 |
| `Arrow Table` | IPC bytes | From IPC | IPC bytes | P0 |
| `FileObject` | Metadata dict | From metadata | Dict | P1 |
| `Postgres Array` | List | List | List | P1 |

**Task 2.3.2: MessagePack Codec**
- [ ] Create `src/py/vertebra/extensions/serialization/msgpack.py`
- [ ] Implement `VertebraCodec` class extending `MessagePackCodec`
- [ ] Add type hook registration for Vertebra types
- [ ] Leverage SQLSpec's serializer fallback chain

**Task 2.3.3: JSON Codec**
- [ ] Create `src/py/vertebra/extensions/serialization/json.py`
- [ ] Implement JSON encoder with type hooks
- [ ] Implement JSON decoder with type reconstruction

**Task 2.3.4: Litestar Integration**
- [ ] Create `src/py/vertebra/extensions/serialization/litestar.py`
- [ ] Implement `VERTEBRA_TYPE_ENCODERS` dict
- [ ] Implement `VERTEBRA_TYPE_DECODERS` list
- [ ] Test auto-registration in Litestar apps

**Files:**
- `src/py/vertebra/extensions/serialization/__init__.py` (NEW)
- `src/py/vertebra/extensions/serialization/_types.py` (NEW)
- `src/py/vertebra/extensions/serialization/_encoders.py` (NEW)
- `src/py/vertebra/extensions/serialization/_decoders.py` (NEW)
- `src/py/vertebra/extensions/serialization/msgpack.py` (NEW)
- `src/py/vertebra/extensions/serialization/json.py` (NEW)
- `src/py/vertebra/extensions/serialization/litestar.py` (NEW)
- `src/py/tests/test_serialization_codec.py` (NEW)
- `src/py/tests/test_serialization_litestar.py` (NEW)

**Acceptance Criteria:**
- [ ] `VertebraCodec.encode()` handles all P0 types
- [ ] `VertebraCodec.decode()` reconstructs typed objects
- [ ] Litestar apps can return `FileObject` from route handlers
- [ ] Database-specific types serialize correctly
- [ ] Performance: < 1ms encoding overhead for typical payloads

---

### 2.4 Advanced Alchemy Integration

**Status:** 0% (not started, depends on SQLAlchemy dialects)

**Goal:** Enable Advanced Alchemy Repository/Service patterns with Vertebra broker

**Tasks:**

**Task 2.4.1: Base Config Classes**
- [ ] Create `src/py/vertebra/extensions/advanced_alchemy/__init__.py`
- [ ] Create `src/py/vertebra/extensions/advanced_alchemy/_config.py`
  - `VertebraSyncAlchemyConfig` base class
  - `VertebraAsyncAlchemyConfig` base class
- [ ] Create `src/py/vertebra/extensions/advanced_alchemy/_engine.py`
  - Engine factory using `postgresql+vertebra://` URLs

**Task 2.4.2: Dialect-Specific Configs**
- [ ] Create `src/py/vertebra/extensions/advanced_alchemy/postgres/config.py`
  - `VertebraPostgresAlchemySyncConfig`
  - `VertebraPostgresAlchemyAsyncConfig`
- [ ] Create configs for MySQL (same pattern)
- [ ] Create configs for SQLite (same pattern)

**Task 2.4.3: Storage Backend**
- [ ] Create `src/py/vertebra/extensions/advanced_alchemy/_storage.py`
- [ ] Implement `VertebraStorageBackend` for AA File Object Storage
- [ ] Register backend with AA's `storages.register_backend()`
- [ ] Route all operations through broker storage pool

**Task 2.4.4: Integration Tests**
- [ ] Test Repository pattern with Vertebra configs
- [ ] Test Service pattern with Vertebra configs
- [ ] Test File Object Storage operations
- [ ] Test Alembic migrations via broker connection

**Files:**
- `src/py/vertebra/extensions/advanced_alchemy/__init__.py` (NEW)
- `src/py/vertebra/extensions/advanced_alchemy/_config.py` (NEW)
- `src/py/vertebra/extensions/advanced_alchemy/_engine.py` (NEW)
- `src/py/vertebra/extensions/advanced_alchemy/_storage.py` (NEW)
- `src/py/vertebra/extensions/advanced_alchemy/postgres/config.py` (NEW)
- `src/py/vertebra/extensions/advanced_alchemy/mysql/config.py` (NEW)
- `src/py/vertebra/extensions/advanced_alchemy/sqlite/config.py` (NEW)
- `src/py/tests/test_advanced_alchemy.py` (NEW)

**Acceptance Criteria:**
- [ ] Repository pattern works with Vertebra configs
- [ ] Service pattern works with Vertebra configs
- [ ] File Object Storage routes through broker
- [ ] Alembic migrations work

---

### 2.5 Litestar Plugin Overhaul

**Status:** Basic plugin exists, needs overhaul for multiple ORM support

**Current:** Simple `VertebraPlugin` in `extensions/litestar.py`

**Required:** Separate plugins for SQLSpec, SQLAlchemy, and Advanced Alchemy

**Tasks:**

**Task 2.5.1: Plugin Infrastructure**
- [ ] Create `src/py/vertebra/extensions/litestar/__init__.py`
- [ ] Create `src/py/vertebra/extensions/litestar/_base.py`
  - Base plugin infrastructure
  - Auto-register serialization type encoders/decoders

**Task 2.5.2: SQLSpec Plugin**
- [ ] Create `src/py/vertebra/extensions/litestar/sqlspec.py`
- [ ] Implement `VertebraSQLSpecPlugin` wrapping Litestar's `SQLSpecPlugin`
- [ ] Wire DI to provide sessions to route handlers

**Task 2.5.3: SQLAlchemy Plugin**
- [ ] Create `src/py/vertebra/extensions/litestar/sqlalchemy.py`
- [ ] Implement `VertebraSQLAlchemyPlugin` wrapping `litestar.contrib.sqlalchemy.SQLAlchemyPlugin`
- [ ] Wire DI to provide connections/sessions

**Task 2.5.4: Advanced Alchemy Plugin**
- [ ] Create `src/py/vertebra/extensions/litestar/advanced_alchemy.py`
- [ ] Implement `VertebraAdvancedAlchemyPlugin` wrapping AA's Litestar plugin
- [ ] Wire DI for Repository/Service injection

**Task 2.5.5: Serialization Auto-Registration**
- [ ] All plugins auto-register `VERTEBRA_TYPE_ENCODERS`
- [ ] All plugins auto-register `VERTEBRA_TYPE_DECODERS`
- [ ] Test that route handlers can return Vertebra-specific types

**Files:**
- `src/py/vertebra/extensions/litestar/__init__.py` (NEW)
- `src/py/vertebra/extensions/litestar/_base.py` (NEW)
- `src/py/vertebra/extensions/litestar/_serialization.py` (NEW)
- `src/py/vertebra/extensions/litestar/sqlspec.py` (NEW)
- `src/py/vertebra/extensions/litestar/sqlalchemy.py` (NEW)
- `src/py/vertebra/extensions/litestar/advanced_alchemy.py` (NEW)
- `src/py/tests/test_litestar_plugin.py` (NEW)

**Acceptance Criteria:**
- [ ] `VertebraSQLSpecPlugin` works with Litestar app
- [ ] `VertebraSQLAlchemyPlugin` works with Litestar app
- [ ] `VertebraAdvancedAlchemyPlugin` works with Litestar app
- [ ] DI provides sessions/connections correctly
- [ ] Type encoders/decoders auto-registered
- [ ] Example app works end-to-end

---

## Phase 3: Production Features (Week 5-6)

### 3.1 Pool Health Checks and Lifecycle Hooks

**Status:** Basic pools implemented, need health checks

**Goal:** Production-grade connection pool management

**Tasks:**

**Task 3.1.1: Pool Health Checks**
- [ ] Add `health_check_interval` config parameter (default: 30s)
- [ ] Implement periodic health check loop in `BasePoolAdapter`
- [ ] Add `health_check()` abstract method to pool interface
- [ ] Implement for SqlPool: `SELECT 1`
- [ ] Implement for RedisPool: `PING`
- [ ] Mark unhealthy connections for removal
- [ ] Emit health check metrics

**Task 3.1.2: Lifecycle Hooks**
- [ ] Add `on_acquire` hook (runs after acquiring connection)
- [ ] Add `on_release` hook (runs before releasing connection)
- [ ] Add `on_error` hook (runs on connection error)
- [ ] Add `on_timeout` hook (runs on acquire timeout)
- [ ] Wire hooks into `BasePoolAdapter`
- [ ] Test hooks with custom logging/monitoring

**Task 3.1.3: Idle Connection Management**
- [ ] Add `idle_timeout` config parameter (default: 5 minutes)
- [ ] Track last-used timestamp per connection
- [ ] Implement idle connection reaper
- [ ] Emit idle connection metrics

**Task 3.1.4: Pool Metrics Enhancement**
- [ ] Add `health_check_failures_total` metric
- [ ] Add `idle_timeout_total` metric
- [ ] Add `acquire_wait_time_seconds` histogram
- [ ] Add `connection_lifetime_seconds` histogram
- [ ] Expose via `/__metrics__` endpoint

**Files:**
- `src/ts/src/broker/base-pool.ts` (enhance)
- `src/ts/src/broker/serve.ts` (add health checks)
- `src/ts/src/broker/health.ts` (NEW)
- `src/ts/tests/unit/pool-health.test.ts` (NEW)

**Acceptance Criteria:**
- [ ] Health checks run periodically
- [ ] Unhealthy connections removed from pool
- [ ] Idle connections reaped after timeout
- [ ] Lifecycle hooks fire correctly
- [ ] All metrics exposed in Prometheus format

---

### 3.2 MCP Protocol Implementation

**Status:** ~15% complete (stub server exists)

**Goal:** Enable AI agents to discover and call Vertebra tools

**Current State:**
- `src/ts/src/mcp/server.ts` has basic skeleton
- `registerTool()` and JSON-RPC request handling implemented
- Transports NOT implemented
- Python registration NOT implemented

**Architecture:**
```
AI Agent (Claude/Gemini)
    ↓ (MCP protocol)
Transport (Stdio/SSE/WebSocket)
    ↓
Bun MCP Server
    ↓ (IPC bridge)
Python Worker (procedure handlers)
```

**Tasks:**

**Task 3.2.1: Transport - Stdio**
- [ ] Create `src/ts/src/mcp/transports/stdio.ts`
- [ ] Implement JSON-RPC over stdin/stdout
- [ ] Handle tool discovery (`tools/list`)
- [ ] Handle tool execution (`tools/call`)
- [ ] Test with `echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | bun mcp`

**Task 3.2.2: Transport - SSE (Server-Sent Events)**
- [ ] Create `src/ts/src/mcp/transports/sse.ts`
- [ ] Implement SSE transport for web clients
- [ ] Handle connection lifecycle
- [ ] Test with browser client

**Task 3.2.3: Transport - WebSocket**
- [ ] Create `src/ts/src/mcp/transports/websocket.ts`
- [ ] Implement WebSocket transport (native Bun)
- [ ] Handle bi-directional communication
- [ ] Test with WebSocket client

**Task 3.2.4: IPC Bridge Implementation**
- [ ] Enhance `src/ts/src/mcp/ipc-bridge.ts`
- [ ] Implement `tools/list` → query Python procedure registry
- [ ] Implement `tools/call` → forward to Python procedure
- [ ] Use Type Bridge IR for schema validation
- [ ] Handle errors and timeouts

**Task 3.2.5: Python Registration**
- [ ] Create `src/py/vertebra/mcp/__init__.py`
- [ ] Implement `@mcp_tool` decorator
- [ ] Auto-register decorated procedures with MCP server
- [ ] Generate JSON schemas from Type Bridge IR

**Task 3.2.6: Schema Generation**
- [ ] Leverage Type Bridge IR for tool schemas
- [ ] Convert Python type hints to JSON Schema
- [ ] Support common types (str, int, float, bool, list, dict, Optional, Union)
- [ ] Support Pydantic models

**Task 3.2.7: Integration Tests**
- [ ] Test tool discovery via all transports
- [ ] Test tool execution via all transports
- [ ] Test error handling
- [ ] Test with real AI agent (Claude Desktop)

**Files:**
- `src/ts/src/mcp/server.ts` (enhance)
- `src/ts/src/mcp/transports/stdio.ts` (NEW)
- `src/ts/src/mcp/transports/sse.ts` (NEW)
- `src/ts/src/mcp/transports/websocket.ts` (NEW)
- `src/ts/src/mcp/ipc-bridge.ts` (enhance)
- `src/py/vertebra/mcp/__init__.py` (NEW)
- `src/py/vertebra/mcp/registry.py` (NEW)
- `src/py/tests/test_mcp_integration.py` (NEW)
- `src/ts/tests/integration/mcp.test.ts` (NEW)

**Acceptance Criteria:**
- [ ] `tools/list` returns all registered tools
- [ ] `tools/call` executes Python procedures correctly
- [ ] All three transports (Stdio/SSE/WebSocket) work
- [ ] JSON schemas generated from Python types
- [ ] Integration test with Claude Desktop succeeds

---

## Phase 4: Quality & Polish (Week 7-8)

### 4.1 Type Audit (Fix 120+ mypy errors)

**Status:** Significant type errors exist across codebase

**Goal:** 0 mypy errors in strict mode

**Approach:**
1. Audit by module (prioritize public APIs first)
2. Fix missing return types
3. Fix `Any` leaks
4. Add missing type stubs
5. Enable strict mode incrementally

**Tasks:**

**Task 4.1.1: Audit Python Core Modules**
- [ ] Run `mypy --strict src/py/vertebra/_internal/`
- [ ] Fix errors in `shm.py`
- [ ] Fix errors in `serialization.py`
- [ ] Fix errors in `dependencies.py`

**Task 4.1.2: Audit Python Extensions**
- [ ] Run `mypy --strict src/py/vertebra/extensions/`
- [ ] Fix SQLSpec extension types
- [ ] Fix SQLAlchemy extension types
- [ ] Fix Advanced Alchemy extension types
- [ ] Fix Litestar plugin types

**Task 4.1.3: Audit Python Resources**
- [ ] Run `mypy --strict src/py/vertebra/resources/`
- [ ] Fix `broker.py` types
- [ ] Fix `provider.py` types

**Task 4.1.4: Audit Python Server**
- [ ] Run `mypy --strict src/py/vertebra/server/`
- [ ] Fix `worker.py` types
- [ ] Fix `supervisor.py` types

**Task 4.1.5: Modernize Type Syntax**
- [ ] Replace `Optional[T]` with `T | None` (Python 3.12+)
- [ ] Replace `Union[A, B]` with `A | B`
- [ ] Remove `from __future__ import annotations` where no longer needed
- [ ] Use `list[T]` instead of `List[T]`
- [ ] Use `dict[K, V]` instead of `Dict[K, V]`

**Task 4.1.6: TypeScript Type Audit**
- [ ] Run `make js-type-check`
- [ ] Fix all `any` types in broker
- [ ] Fix all `any` types in server
- [ ] Fix all `any` types in MCP

**Files Affected:** ~50+ Python files, ~30+ TypeScript files

**Acceptance Criteria:**
- [ ] `mypy --strict src/py/vertebra/` reports 0 errors
- [ ] All Python files use modern type syntax
- [ ] No `from __future__ import annotations` in Python 3.12+ code
- [ ] TypeScript strict mode enabled
- [ ] No `any` types without explicit justification

---

### 4.2 Documentation

**Status:** Minimal documentation exists

**Goal:** Production-ready documentation for 1.0 release

**Tasks:**

**Task 4.2.1: Getting Started Guide**
- [ ] Create `docs/getting-started.md`
- [ ] Installation instructions
- [ ] Hello World example
- [ ] Basic configuration
- [ ] Common patterns

**Task 4.2.2: Architecture Guide**
- [ ] Create `docs/architecture.md`
- [ ] Shared-memory transport explanation
- [ ] Broker architecture diagram
- [ ] IPC protocol documentation
- [ ] Security model

**Task 4.2.3: API Reference**
- [ ] Generate API docs from docstrings (Sphinx or mkdocs)
- [ ] Document all public APIs
- [ ] Document configuration options
- [ ] Document CLI commands

**Task 4.2.4: ORM Integration Guides**
- [ ] Create `docs/orm/sqlspec.md`
- [ ] Create `docs/orm/sqlalchemy.md`
- [ ] Create `docs/orm/advanced-alchemy.md`
- [ ] Example applications for each

**Task 4.2.5: MCP Integration Guide**
- [ ] Create `docs/mcp/overview.md`
- [ ] Create `docs/mcp/tools.md`
- [ ] Create `docs/mcp/transports.md`
- [ ] Example AI agent integration

**Task 4.2.6: Deployment Guide**
- [ ] Create `docs/deployment/production.md`
- [ ] Docker deployment
- [ ] Kubernetes deployment
- [ ] Systemd service example
- [ ] Environment variables reference

**Files:**
- `docs/getting-started.md` (NEW)
- `docs/architecture.md` (NEW)
- `docs/api/` (NEW directory)
- `docs/orm/sqlspec.md` (NEW)
- `docs/orm/sqlalchemy.md` (NEW)
- `docs/orm/advanced-alchemy.md` (NEW)
- `docs/mcp/overview.md` (NEW)
- `docs/deployment/production.md` (NEW)
- `mkdocs.yml` or `docs/conf.py` (NEW)

**Acceptance Criteria:**
- [ ] Getting Started guide exists and is tested
- [ ] Architecture guide explains all major components
- [ ] API reference is comprehensive
- [ ] ORM guides have working examples
- [ ] Deployment guide covers common scenarios

---

### 4.3 Example Applications

**Status:** No example apps exist

**Goal:** 2-3 reference applications demonstrating Vertebra

**Tasks:**

**Task 4.3.1: Todo API (SQLSpec + Litestar)**
- [ ] Create `examples/todo-api/`
- [ ] Implement CRUD API with SQLSpec
- [ ] Use Litestar + VertebraSQLSpecPlugin
- [ ] Include Postgres setup (docker-compose.yml)
- [ ] README with setup instructions

**Task 4.3.2: Blog Platform (SQLAlchemy + Advanced Alchemy)**
- [ ] Create `examples/blog-platform/`
- [ ] Implement blog models with SQLAlchemy
- [ ] Use Advanced Alchemy Repository pattern
- [ ] File uploads with FileObject storage
- [ ] Include migration scripts (Alembic)

**Task 4.3.3: AI Tools Server (MCP)**
- [ ] Create `examples/ai-tools/`
- [ ] Implement 5-10 useful tools
  - Database query tool
  - File search tool
  - API call tool
  - Data analysis tool
  - Code execution tool
- [ ] MCP server configuration
- [ ] Claude Desktop integration instructions

**Files:**
- `examples/todo-api/` (NEW directory)
- `examples/blog-platform/` (NEW directory)
- `examples/ai-tools/` (NEW directory)

**Acceptance Criteria:**
- [ ] All examples have working code
- [ ] All examples have setup instructions
- [ ] All examples have Docker Compose files
- [ ] All examples are tested in CI

---

## Phase 5: Distribution

**Status:** 0% (specification complete, implementation not started)

**Goal:** Single-file executable distribution and polished install experience

**Reference:** `/home/cody/code/c/vertebra/specs/active/build-distribution-experience/prd.md`

**High-Level Tasks:**
- [ ] Implement Rust supervisor (embedded broker thread)
- [ ] Bun compilation (`bun build --compile`)
- [ ] PyApp bundling
- [ ] Build orchestration (`vertebra build --one-file`)
- [ ] Cross-platform wheels (Linux/macOS/Windows)
- [ ] CI/CD pipeline for releases
- [ ] Project scaffolding (`vertebra init`)

---

## Dependencies Strategy

**Goal:** Keep core lightweight, make heavy dependencies opt-in

**Core Dependencies:**
```toml
[project.dependencies]
click = ">=8.0"        # CLI (~200KB)
msgspec = ">=0.18"     # Fast serialization (~500KB)
anyio = ">=4.0"        # Async compatibility (~100KB)
nanoarrow = ">=0.5"    # Arrow C Data Interface (~1MB) - enables zero-copy handoff
```

**Rationale for nanoarrow as core:**
- ~1MB installed size (same class as msgspec)
- Enables zero-copy Arrow C Data Interface for large result sets
- Users can consume with their preferred library (Polars, DuckDB, PyArrow)
- Vertebra-py exports `__arrow_c_stream__` - nanoarrow handles the handoff
- NOT pyarrow (100MB+) - that remains optional for users who want compute

**Optional Dependencies:**
```toml
[project.optional-dependencies]
# SQLSpec integration
sqlspec = ["sqlspec>=0.1"]

# SQLAlchemy
sqlalchemy = ["sqlalchemy>=2.0"]

# Advanced Alchemy
advanced-alchemy = ["advanced-alchemy>=0.1"]

# Full PyArrow for power users who want Arrow compute
data = ["pyarrow>=14.0"]

# Litestar
litestar = ["litestar>=2.0"]

# Development
dev = ["pytest", "pytest-asyncio", "pytest-timeout", "mypy", "ruff", "pre-commit"]

# All optional features
all = ["vertebra[sqlspec,sqlalchemy,advanced-alchemy,litestar]"]
```

**Implementation Pattern:**
```python
# In vertebra/_internal/dependencies.py
def require_sqlspec():
    """Raise helpful error if sqlspec not installed."""
    try:
        import sqlspec
        return sqlspec
    except ImportError:
        raise MissingDependencyError(
            "sqlspec",
            "pip install vertebra[sqlspec]"
        )
```

**Acceptance Criteria:**
- [ ] Core package has minimal dependencies
- [ ] Optional features raise clear errors when dependencies missing
- [ ] Documentation explains optional dependencies
- [ ] Installation instructions cover common combinations

---

## Success Criteria

### Functional Requirements
- [ ] `make test` passes without hanging (< 60s)
- [ ] All public APIs use `Vertebra*` prefix
- [ ] SQLSpec integration works with all adapters
- [ ] SQLAlchemy dialects work with standard usage
- [ ] Advanced Alchemy Repository/Service patterns work
- [ ] Pool health checks run and report metrics
- [ ] MCP protocol works with all three transports
- [ ] Example applications run end-to-end

### Quality Requirements
- [ ] 0 mypy errors in strict mode
- [ ] 0 ruff violations
- [ ] Test coverage > 85%
- [ ] All modern type syntax (no `Optional[T]`, no `from __future__`)
- [ ] Documentation exists for all major features

### Performance Requirements
- [ ] Throughput: >50k req/sec (Hello World)
- [ ] Latency: P99 < 1ms (IPC overhead)
- [ ] Memory: <50MB per idle worker

---

## Task Checklist

### Phase 1: Immediate Priorities (Week 1-2)
**Test Deadlock Fix (P0):**
- [ ] Add pytest-timeout configuration
- [ ] Clean stale shared memory
- [ ] Enhance cleanup patterns in conftest.py
- [ ] Create test helpers module
- [ ] Update test_broker_client.py with unique names
- [ ] Update test_broker_pool.py with unique names
- [ ] Update test_asgi_worker.py with unique names
- [ ] Add thread coordination to test_ipc_channel.py
- [ ] Add thread coordination to test_ipc_integration.py
- [ ] Add asyncio timeouts to async tests

**Codebase Consolidation Phase 4:**
- [ ] Rename worker-pool.ts → worker-connections.ts
- [ ] Rename worker-manager.ts → process-manager.ts
- [ ] Update CLAUDE.md documentation
- [ ] Add architecture docstrings

**API Rename:**
- [ ] Rename all `Broker*` → `Vertebra*` in SQLSpec extension
- [ ] Update test imports
- [ ] Verify all tests pass

### Phase 2: ORM & Database Integration (Week 3-4)
**SQLSpec Stabilization:**
- [ ] Complete API rename
- [ ] Implement Arrow serialization
- [ ] Implement storage integration
- [ ] Add integration tests

**SQLAlchemy:**
- [ ] Implement DBAPI module
- [ ] Implement Postgres dialect
- [ ] Implement MySQL dialect
- [ ] Implement SQLite dialect
- [ ] Add transaction support
- [ ] Add integration tests

**Serialization:**
- [ ] Implement type support matrix
- [ ] Create MessagePack codec
- [ ] Create JSON codec
- [ ] Create Litestar integration
- [ ] Add tests

**Advanced Alchemy:**
- [ ] Implement base config classes
- [ ] Implement dialect-specific configs
- [ ] Implement storage backend
- [ ] Add integration tests

**Litestar Plugin:**
- [ ] Create plugin infrastructure
- [ ] Implement SQLSpec plugin
- [ ] Implement SQLAlchemy plugin
- [ ] Implement Advanced Alchemy plugin
- [ ] Auto-register serialization

### Phase 3: Production Features (Week 5-6)
**Pool Health Checks:**
- [ ] Implement health check loop
- [ ] Add lifecycle hooks
- [ ] Implement idle connection management
- [ ] Enhance pool metrics

**MCP Protocol:**
- [ ] Implement Stdio transport
- [ ] Implement SSE transport
- [ ] Implement WebSocket transport
- [ ] Implement IPC bridge
- [ ] Add Python registration
- [ ] Implement schema generation
- [ ] Add integration tests

### Phase 4: Quality & Polish (Week 7-8)
**Type Audit:**
- [ ] Audit _internal/ module
- [ ] Audit extensions/ module
- [ ] Audit resources/ module
- [ ] Audit server/ module
- [ ] Modernize type syntax
- [ ] Audit TypeScript types

**Documentation:**
- [ ] Create Getting Started guide
- [ ] Create Architecture guide
- [ ] Generate API reference
- [ ] Create ORM integration guides
- [ ] Create MCP integration guide
- [ ] Create Deployment guide

**Example Applications:**
- [ ] Create Todo API example
- [ ] Create Blog Platform example
- [ ] Create AI Tools Server example

---

## File Paths Reference

### Python Files to Modify:
- `/home/cody/code/c/vertebra/pyproject.toml`
- `/home/cody/code/c/vertebra/src/py/tests/conftest.py`
- `/home/cody/code/c/vertebra/src/py/tests/helpers/__init__.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/helpers/shm.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/**/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/serialization/**/*.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/**/*.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/**/*.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/**/*.py` (NEW dir)
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/**/*.py` (NEW)

### TypeScript Files to Modify:
- `/home/cody/code/c/vertebra/src/ts/src/server/worker-pool.ts` → `worker-connections.ts`
- `/home/cody/code/c/vertebra/src/ts/src/server/worker-manager.ts` → `process-manager.ts`
- `/home/cody/code/c/vertebra/src/ts/src/broker/base-pool.ts`
- `/home/cody/code/c/vertebra/src/ts/src/broker/serve.ts`
- `/home/cody/code/c/vertebra/src/ts/src/mcp/server.ts`
- `/home/cody/code/c/vertebra/src/ts/src/mcp/transports/**/*.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/src/mcp/ipc-bridge.ts`

### Documentation Files:
- `/home/cody/code/c/vertebra/CLAUDE.md`
- `/home/cody/code/c/vertebra/docs/getting-started.md` (NEW)
- `/home/cody/code/c/vertebra/docs/architecture.md` (NEW)
- `/home/cody/code/c/vertebra/docs/orm/*.md` (NEW)
- `/home/cody/code/c/vertebra/docs/mcp/*.md` (NEW)
- `/home/cody/code/c/vertebra/docs/deployment/*.md` (NEW)

### Example Applications:
- `/home/cody/code/c/vertebra/examples/todo-api/` (NEW)
- `/home/cody/code/c/vertebra/examples/blog-platform/` (NEW)
- `/home/cody/code/c/vertebra/examples/ai-tools/` (NEW)

---

## Acceptance Criteria for 1.0 Release

### Core Functionality
- [x] Shared-memory broker operational
- [ ] Test suite passes reliably
- [ ] All public APIs use Vertebra prefix
- [x] HTTP/1.1 and HTTP/2 server working
- [x] ASGI bridge working

### Database Integration
- [ ] SQLSpec integration complete and tested
- [ ] SQLAlchemy dialects working
- [ ] Advanced Alchemy patterns working
- [ ] Integration tests with real databases pass

### Production Features
- [ ] Pool health checks operational
- [ ] Lifecycle hooks working
- [ ] Metrics exposed in Prometheus format
- [ ] MCP protocol working with all transports

### Quality
- [ ] 0 mypy errors in strict mode
- [ ] 0 ruff violations
- [ ] Test coverage > 85%
- [ ] Modern type syntax throughout

### Documentation
- [ ] Getting Started guide exists
- [ ] Architecture documented
- [ ] API reference generated
- [ ] 2-3 example applications working

### Distribution
- [ ] PyPI package installable
- [ ] Optional dependencies working correctly
- [ ] Installation instructions clear

---

## Development Commands Reference

```bash
# Installation
make install          # Full install (Python + Bun + Rust)

# Testing
make test             # Python tests
make js-test          # Bun/TypeScript tests
make rs-test          # Rust tests
make test-all         # All tests

# Single test
uv run pytest src/py/tests/test_foo.py::test_bar -v
cd src/ts && bun test src/server/config.test.ts
cd src/rs && cargo test --package vertebra-core test_name

# Integration tests (requires Docker/Podman)
uv run pytest src/py/tests -m integration --integration

# Linting
make lint             # Format all (ruff + biome + rustfmt)
make lint-all         # Full lint + type checks + pre-commit

# Type checking
make mypy             # Python type checking
make js-type-check    # TypeScript type checking

# Coverage
make coverage         # Test coverage report
```

---

## Related Specifications

- `/home/cody/code/c/vertebra/specs/active/test-deadlock-fix/prd.md` - Test infrastructure fixes
- `/home/cody/code/c/vertebra/specs/active/codebase-consolidation/prd.md` - Code cleanup
- `/home/cody/code/c/vertebra/specs/active/orm-extensions/prd.md` - Database integration
- `/home/cody/code/c/vertebra/specs/active/production-readiness/prd.md` - Production features
- `/home/cody/code/c/vertebra/specs/active/build-distribution-experience/prd.md` - Single-file build (deferred)

---

**Document Version:** 1.0
**Last Updated:** 2026-01-19
**Word Count:** 6,842 words
**Author:** Claude (Sonnet 4.5)
**Status:** Ready for Review and Implementation
