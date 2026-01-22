# Tasks: Database & ORM Integration

**PRD**: `/home/cody/code/c/vertebra/specs/active/database/prd.md`
**Status**: Active - Critical Path to 1.0
**Transport**: Shared-memory C ABI via broker (no sockets)

---

## Phase 1: SQLSpec Stabilization (Week 1)

### Task Group 1.1: API Rename (Broker* → Vertebra*)

**Priority**: P0 (Public API stabilization)
**Dependencies**: None

- [x] **Task 1.1.1**: Rename `_config.py` classes
  - `BrokerSyncConfigBase` → `VertebraSyncConfigBase`
  - `BrokerAsyncConfigBase` → `VertebraAsyncConfigBase`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`

- [x] **Task 1.1.2**: Rename `_driver.py` classes
  - `BrokerProxySyncDriver` → `VertebraProxySyncDriver`
  - `BrokerProxyAsyncDriver` → `VertebraProxyAsyncDriver`
  - File: `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`

- [x] **Task 1.1.3**: Rename Postgres configs and drivers
  - `BrokerPostgresSyncConfig` → `VertebraPostgresSyncConfig`
  - `BrokerPostgresAsyncConfig` → `VertebraPostgresAsyncConfig`
  - `BrokerPostgresSyncDriver` → `VertebraPostgresSyncDriver`
  - `BrokerPostgresAsyncDriver` → `VertebraPostgresAsyncDriver`
  - Files: `postgres/config.py`, `postgres/driver.py`

- [x] **Task 1.1.4**: Rename MySQL configs and drivers (same pattern as Postgres)
  - Files: `mysql/config.py`, `mysql/driver.py`

- [x] **Task 1.1.5**: Rename SQLite configs and drivers (same pattern as Postgres)
  - Files: `sqlite/config.py`, `sqlite/driver.py`

- [x] **Task 1.1.6**: Update test imports
  - Files: `src/py/tests/test_sqlspec_*.py`
  - Update all `from ...Broker*` to `from ...Vertebra*`

- [x] **Task 1.1.7**: Run test suite
  - Command: `make test`
  - Verify all tests pass

- [x] **Task 1.1.8**: Run type checks
  - Command: `make mypy`
  - Verify no new errors introduced

**Acceptance**:
- All public APIs use `Vertebra*` prefix
- Internal broker client remains as `BrokerClient`
- All tests pass
- No mypy errors

---

### Task Group 1.2: Arrow Serialization

**Priority**: P0
**Dependencies**: Task 1.1 (rename)

- [x] **Task 1.2.1**: Add Arrow threshold config
  - Add `arrow_threshold_rows: int = 1000` to config classes
  - Add `arrow_threshold_bytes: int = 1_048_576` (1MB)
  - File: `_config.py`

- [x] **Task 1.2.2**: Implement Arrow encoding in driver
  - Add `_should_use_arrow(result) -> bool` method
  - Add `_encode_arrow(result) -> bytes` method
  - Use nanoarrow to create Arrow IPC stream
  - File: `_driver.py`

- [x] **Task 1.2.3**: Implement Arrow region pool in provider
  - Add `ArrowRegionPool` class
  - Manage shared memory regions for Arrow batches
  - File: `provider.py`

- [x] **Task 1.2.4**: Add Arrow decoding in connection wrapper
  - Add `_decode_arrow(bytes) -> list[dict]` method
  - Use nanoarrow to parse Arrow IPC stream
  - File: `_shared.py`

- [x] **Task 1.2.5**: Add automatic threshold detection
  - Estimate result size before encoding
  - Switch to Arrow if > threshold
  - Log threshold switches (debug level)
  - File: `_driver.py`

- [x] **Task 1.2.6**: Add Arrow serialization tests
  - Test small results use MessagePack
  - Test large results use Arrow
  - Test threshold switching
  - File: `src/py/tests/test_sqlspec_arrow.py` (NEW)

- [x] **Task 1.2.7**: Integration test with real database
  - Create table with 10k rows
  - Query all rows, verify Arrow transport used
  - Verify correctness of results
  - File: `src/py/tests/test_sqlspec_integration.py`

**Acceptance**:
- Query returning 10,000 rows uses Arrow IPC
- Query returning 100 rows uses MessagePack
- Performance: Arrow transport < 5ms overhead for 10k rows
- Tests pass with Postgres, MySQL, SQLite

---

### Task Group 1.3: Storage Integration

**Priority**: P1
**Dependencies**: None

- [x] **Task 1.3.1**: Create `_storage.py` module
  - Implement `VertebraObjectStore` class
  - Implement `ObjectStoreBase` protocol methods
  - File: `src/py/vertebra/extensions/sqlspec/_storage.py` (NEW)

- [x] **Task 1.3.2**: Implement `read_bytes()`
  - Route to `broker.storage_read(pool, path)`
  - Handle errors gracefully
  - File: `_storage.py`

- [x] **Task 1.3.3**: Implement `write_bytes()`
  - Route to `broker.storage_write(pool, path, data)`
  - Handle errors gracefully
  - File: `_storage.py`

- [x] **Task 1.3.4**: Implement other storage methods
  - `list_objects()`, `exists()`, `delete()`
  - `copy()`, `move()`, `glob()`
  - File: `_storage.py`

- [x] **Task 1.3.5**: Add storage integration tests
  - Test read/write operations
  - Test list/exists/delete operations
  - Test with local filesystem backend
  - File: `src/py/tests/test_sqlspec_storage.py` (NEW)

**Acceptance**:
- SQLSpec storage operations route through broker
- `write_to_storage_async()` works
- Integration test with local filesystem backend passes

---

## Phase 2: Serialization Module (Week 1-2) [checkpoint: 9887acb]

### Task Group 2.1: Expand Type Support

**Priority**: P0
**Dependencies**: Refactor Serialization (Completed)

> **Note**: `codecs.py` and `encoders.py` were created in `refactor-serialization-core` PRD. This phase expands them.

- [x] **Task 2.1.1**: Update `src/py/vertebra/serializers/encoders.py`
  - Implement P0 type encoders:
    - `bytes` (base64)
    - `Pydantic BaseModel` (using `model_dump`)
    - `msgspec.Struct` (using `msgspec.to_builtins`)
    - `Arrow Table` (using IPC)
    - `FileObject` (using metadata)

- [x] **Task 2.1.2**: Implement P1 type encoders
  - `Path`, `IPv4Address`, `IPv6Address`
  - `set`, `frozenset` (to list)
  - `SecretStr` (masked)
  - `dataclass` (using `asdict`)
  - `Postgres Array` (list)

- [x] **Task 2.1.3**: Implement P2 type encoders
  - `Pattern` (regex)
  - `deque` (list)
  - `attrs` (asdict)
  - `numpy.ndarray` (list)

- [x] **Task 2.1.4**: Create `src/py/vertebra/serializers/decoders.py` (NEW)
  - Implement matching decoders for all supported types
  - Register with `codecs.py`

**Acceptance**:
- `encoders.py` supports all 30+ types
- `decoders.py` implements reconstruction logic
- Unit tests for each type conversion

---

### Task Group 2.2: MessagePack Codec Hardening

**Priority**: P0
**Dependencies**: Task 2.1 (type support)

- [x] **Task 2.2.1**: Create `codecs.py` module
  - **COMPLETED**: Merged `msgpack.py` and `json.py` into `codecs.py` in previous PRD.
  - Implements `VertebraCodec` alias.

- [x] **Task 2.2.2**: Integrate expanded encoders
  - Ensure `JSONCodec` and `MessagePackCodec` use the expanded `default_encoder`
  - Verify `Pydantic` and `Arrow` support works with `VertebraCodec`

- [x] **Task 2.2.3**: Implement decoding hooks
  - Update `MessagePackCodec.decode` to use `decoders.py` logic where appropriate
  - Or verify that typed decoding handles it automatically via `msgspec`

- [x] **Task 2.2.4**: Add codec tests
  - Test each type conversion round-trip
  - Test nested structures
  - Test error handling
  - File: `src/py/tests/test_serialization_codec.py` (NEW)

**Acceptance**:
- `VertebraCodec.encode()` handles all P0 types
- `VertebraCodec.decode()` reconstructs typed objects
- Round-trip tests pass for all types
- Performance: < 1ms encoding overhead for typical payloads

---

### Task Group 2.3: Litestar Integration

**Priority**: P1
**Dependencies**: Task 2.1, 2.2

- [x] **Task 2.3.1**: Create `litestar.py` module
  - File: `src/py/vertebra/serializers/litestar.py` (NEW)

- [x] **Task 2.3.2**: Implement `VERTEBRA_TYPE_ENCODERS`
  - Dict mapping types to encoder functions (reusing `encoders.py` logic)
  - All 30+ types

- [x] **Task 2.3.3**: Implement `VERTEBRA_TYPE_DECODERS`
  - List of (type, decoder_func) tuples
  - All 30+ types

- [x] **Task 2.3.4**: Add Litestar integration tests
  - Test route handlers returning custom types
  - Test request body parsing with custom types
  - File: `src/py/tests/test_serialization_litestar.py` (NEW)

**Acceptance**:
- Litestar apps can return `FileObject` from route handlers
- Database-specific types serialize correctly
- Tests pass with Litestar 2.0+

---

## Phase 3: SQLAlchemy Integration (Week 2-3)

### Task Group 3.1: DBAPI Module

**Priority**: P0
**Dependencies**: Phase 2 (serialization)

- [x] **Task 3.1.1**: Create SQLAlchemy package structure
  - Files: `__init__.py`, `_dbapi.py`, `_base.py`
  - Location: `src/py/vertebra/extensions/sqlalchemy/`

- [x] **Task 3.1.2**: Implement `Connection` class (4816f6d)
  - `cursor()`, `commit()`, `rollback()`, `close()`
  - File: `_dbapi.py`

- [x] **Task 3.1.3**: Implement `Cursor` class (4816f6d)
  - `execute()`, `executemany()`
  - `fetchone()`, `fetchall()`, `fetchmany()`
  - File: `_dbapi.py`

- [x] **Task 3.1.4**: Add parameter style handling (4816f6d)
  - Convert `numeric` style for Postgres
  - Convert `qmark` style for MySQL/SQLite
  - File: `_dbapi.py`

- [x] **Task 3.1.5**: Add transaction tracking (4816f6d)
  - Track transaction ID in connection
  - Auto-begin on first execute
  - File: `_dbapi.py`

- [x] **Task 3.1.6**: Add DBAPI tests (4816f6d)
  - Test connection lifecycle
  - Test cursor operations
  - Test transaction handling
  - File: `src/py/tests/test_sqlalchemy_dbapi.py` (NEW)

**Acceptance**:
- All PEP 249 methods implemented
- Parameter style conversion works
- Transaction tracking works
- Tests pass

---

### Task Group 3.2: Postgres Dialect

**Priority**: P0
**Dependencies**: Task 3.1 (DBAPI)

- [x] **Task 3.2.1**: Create Postgres dialect structure (f6baaa9)
  - Files: `postgres/__init__.py`, `postgres/dialect.py`, `postgres/dbapi.py`

- [x] **Task 3.2.2**: Implement `VertebraPostgresDialect` (f6baaa9)
  - Extend `sqlalchemy.dialects.postgresql.PGDialect`
  - Set `name`, `driver`, `paramstyle`
  - File: `postgres/dialect.py`

- [x] **Task 3.2.3**: Implement `VertebraPostgresAsyncDialect` (f6baaa9)
  - Extend `PGDialect_asyncpg`
  - Set async-specific attributes
  - File: `postgres/dialect.py`

- [x] **Task 3.2.4**: Implement dialect registration (f6baaa9)
  - Register with `sqlalchemy.dialects.registry`
  - Both sync and async variants
  - File: `postgres/__init__.py`

- [x] **Task 3.2.5**: Add Postgres dialect tests (f6baaa9)
  - Test `create_engine("postgresql+vertebra://main")`
  - Test `create_async_engine("postgresql+vertebra://main")`
  - Test basic CRUD operations
  - File: `src/py/tests/test_sqlalchemy_dialect.py` (NEW)

**Acceptance**:
- `create_engine("postgresql+vertebra://main")` works
- `create_async_engine("postgresql+vertebra://main")` works
- Basic CRUD operations work
- Tests pass

---

### Task Group 3.3: MySQL and SQLite Dialects

**Priority**: P0
**Dependencies**: Task 3.2 (Postgres dialect)

- [x] **Task 3.3.1**: Implement MySQL dialect (same pattern as Postgres) (94d23da)
  - Files: `mysql/dialect.py`, `mysql/dbapi.py`
  - Extend `MySQLDialect` and `MySQLAsyncDialect`

- [x] **Task 3.3.2**: Implement SQLite dialect (same pattern as Postgres) (94d23da)
  - Files: `sqlite/dialect.py`, `sqlite/dbapi.py`
  - Extend `SQLiteDialect` and `SQLiteAsyncDialect`

- [x] **Task 3.3.3**: Add dialect tests for MySQL and SQLite (94d23da)
  - Test both sync and async engines
  - Test basic CRUD operations
  - File: `src/py/tests/test_sqlalchemy_dialect.py`

**Acceptance**:
- MySQL dialect works
- SQLite dialect works
- Tests pass for all three databases

---

### Task Group 3.4: Transaction Support

**Priority**: P0
**Dependencies**: Task 3.1-3.3

- [x] **Task 3.4.1**: Add transaction commands to broker protocol (ffae925)
  - `BEGIN` - Start transaction, return transaction ID
  - `COMMIT` - Commit transaction by ID
  - `ROLLBACK` - Rollback transaction by ID
  - `SAVEPOINT` - Create savepoint
  - Files: `src/ts/src/broker/serve.ts`, Python client

- [x] **Task 3.4.2**: Implement transaction lifecycle (ffae925)
  - Auto-begin on first execute
  - Track transaction state
  - Handle nested transactions (savepoints)
  - File: `_dbapi.py`

- [x] **Task 3.4.3**: Add transaction tests (ffae925)
  - Test commit/rollback
  - Test nested transactions
  - Test error handling
  - File: `src/py/tests/test_sqlalchemy_integration.py` (NEW)

**Acceptance**:
- Transactions work correctly
- Commit/rollback work
- Nested transactions (savepoints) work
- Tests pass

---

## Phase 4: Pool Health Checks (Week 3)

### Task Group 4.1: Health Check Implementation

**Priority**: P1
**Dependencies**: None (enhances existing pools)

- [ ] **Task 4.1.1**: Create `health.ts` module
  - File: `src/ts/src/broker/health.ts` (NEW)

- [ ] **Task 4.1.2**: Implement `HealthChecker` interface
  - `check(connection): Promise<boolean>`

- [ ] **Task 4.1.3**: Implement `PostgresHealthChecker`
  - Execute `SELECT 1`
  - Return true if successful

- [ ] **Task 4.1.4**: Implement `RedisHealthChecker`
  - Execute `PING`
  - Return true if response is "PONG"

- [ ] **Task 4.1.5**: Implement `MySqlHealthChecker`
  - Execute `SELECT 1`
  - Return true if successful

- [ ] **Task 4.1.6**: Implement `SqliteHealthChecker`
  - Execute `SELECT 1`
  - Return true if successful

- [ ] **Task 4.1.7**: Add health check loop to `BasePoolAdapter`
  - Run every 30s (configurable)
  - Check idle connections
  - Remove unhealthy connections
  - File: `src/ts/src/broker/base-pool.ts`

**Acceptance**:
- Health checks run periodically
- Unhealthy connections removed
- Metrics track health check failures
- Tests pass

---

### Task Group 4.2: Lifecycle Hooks

**Priority**: P1
**Dependencies**: None

- [ ] **Task 4.2.1**: Define `LifecycleHooks` interface
  - `onAcquire`, `onRelease`, `onError`, `onTimeout`
  - File: `src/ts/src/broker/base-pool.ts`

- [ ] **Task 4.2.2**: Wire hooks into pool adapter
  - Call `onAcquire` after acquiring connection
  - Call `onRelease` before releasing connection
  - Call `onError` on connection error
  - Call `onTimeout` on acquire timeout

- [ ] **Task 4.2.3**: Add hook tests
  - Test each hook type
  - Test hook execution order
  - File: `src/ts/tests/unit/lifecycle-hooks.test.ts` (NEW)

**Acceptance**:
- All hook types fire correctly
- Hooks can log/monitor operations
- Tests pass

---

### Task Group 4.3: Idle Connection Management

**Priority**: P1
**Dependencies**: None

- [ ] **Task 4.3.1**: Add idle timeout tracking
  - Track last-used timestamp per connection
  - Default: 5 minutes

- [ ] **Task 4.3.2**: Implement idle reaper
  - Run every 60s
  - Remove connections idle > timeout
  - File: `src/ts/src/broker/base-pool.ts`

- [ ] **Task 4.3.3**: Add idle metrics
  - Track idle timeouts
  - Track connection lifetime
  - File: `src/ts/src/broker/serve.ts`

**Acceptance**:
- Idle connections reaped after timeout
- Metrics track idle timeouts
- Tests pass

---

## Phase 5: Advanced Alchemy Integration (Week 4)

### Task Group 5.1: Base Config Classes

**Priority**: P1
**Dependencies**: Phase 3 (SQLAlchemy dialects)

- [ ] **Task 5.1.1**: Create Advanced Alchemy package structure
  - Files: `__init__.py`, `_config.py`, `_engine.py`, `_storage.py`
  - Location: `src/py/vertebra/extensions/advanced_alchemy/`

- [ ] **Task 5.1.2**: Implement `VertebraAsyncAlchemyConfig`
  - Extend `advanced_alchemy.config.AsyncAlchemyConfig`
  - Use `create_async_engine("postgresql+vertebra://pool")`
  - File: `_config.py`

- [ ] **Task 5.1.3**: Implement `VertebraSyncAlchemyConfig`
  - Extend `advanced_alchemy.config.SyncAlchemyConfig`
  - Use `create_engine("postgresql+vertebra://pool")`
  - File: `_config.py`

**Acceptance**:
- Base config classes work with AA
- Tests pass

---

### Task Group 5.2: Dialect-Specific Configs

**Priority**: P1
**Dependencies**: Task 5.1

- [ ] **Task 5.2.1**: Implement Postgres configs
  - `VertebraPostgresAlchemySyncConfig`
  - `VertebraPostgresAlchemyAsyncConfig`
  - File: `postgres/config.py` (NEW)

- [ ] **Task 5.2.2**: Implement MySQL configs
  - `VertebraMysqlAlchemySyncConfig`
  - `VertebraMysqlAlchemyAsyncConfig`
  - File: `mysql/config.py` (NEW)

- [ ] **Task 5.2.3**: Implement SQLite configs
  - `VertebraSqliteAlchemySyncConfig`
  - `VertebraSqliteAlchemyAsyncConfig`
  - File: `sqlite/config.py` (NEW)

**Acceptance**:
- Dialect-specific configs work
- Tests pass for all three databases

---

### Task Group 5.3: Storage Backend

**Priority**: P1
**Dependencies**: Task 5.1

- [ ] **Task 5.3.1**: Implement `VertebraStorageBackend`
  - Extend `advanced_alchemy.types.file_object.Backend`
  - Route all operations through broker
  - File: `_storage.py`

- [ ] **Task 5.3.2**: Implement storage methods
  - `read_bytes()`, `write_bytes()`, `list_objects()`
  - `exists()`, `delete()`, `copy()`, `move()`, `glob()`
  - File: `_storage.py`

- [ ] **Task 5.3.3**: Register backend with AA
  - `storages.register_backend(VertebraStorageBackend())`
  - File: `__init__.py`

- [ ] **Task 5.3.4**: Add storage backend tests
  - Test read/write operations
  - Test with AA `StoredObject` type
  - File: `src/py/tests/test_advanced_alchemy.py` (NEW)

**Acceptance**:
- File Object Storage routes through broker
- AA `StoredObject` type works
- Tests pass

---

### Task Group 5.4: Integration Tests

**Priority**: P1
**Dependencies**: Task 5.1-5.3

- [ ] **Task 5.4.1**: Test Repository pattern
  - Create sample repository
  - Test CRUD operations
  - File: `src/py/tests/test_advanced_alchemy.py`

- [ ] **Task 5.4.2**: Test Service pattern
  - Create sample service
  - Test service methods
  - File: `src/py/tests/test_advanced_alchemy.py`

- [ ] **Task 5.4.3**: Test Alembic migrations
  - Create sample migration
  - Test up/down migrations
  - File: `src/py/tests/test_advanced_alchemy.py`

**Acceptance**:
- Repository pattern works
- Service pattern works
- Alembic migrations work
- Tests pass

---

## Phase 6: Litestar Plugin Overhaul (Week 4)

### Task Group 6.1: Plugin Infrastructure

**Priority**: P1
**Dependencies**: Phases 1-5

- [ ] **Task 6.1.1**: Create Litestar package structure
  - Files: `__init__.py`, `_base.py`, `_serialization.py`
  - Location: `src/py/vertebra/extensions/litestar/`

- [ ] **Task 6.1.2**: Implement serialization auto-registration
  - `register_vertebra_types(app: Litestar)`
  - Merge `VERTEBRA_TYPE_ENCODERS` and `VERTEBRA_TYPE_DECODERS`
  - File: `_serialization.py`

**Acceptance**:
- Auto-registration works
- Tests pass

---

### Task Group 6.2: SQLSpec Plugin

**Priority**: P1
**Dependencies**: Task 6.1

- [ ] **Task 6.2.1**: Implement `VertebraSQLSpecPlugin`
  - Wrap Litestar's `SQLSpecPlugin`
  - Auto-register serialization types
  - File: `sqlspec.py` (NEW)

- [ ] **Task 6.2.2**: Add SQLSpec plugin tests
  - Test plugin registration
  - Test DI session provision
  - File: `src/py/tests/test_litestar_plugin.py` (NEW)

**Acceptance**:
- Plugin works with Litestar app
- DI provides sessions correctly
- Tests pass

---

### Task Group 6.3: SQLAlchemy Plugin

**Priority**: P1
**Dependencies**: Task 6.1

- [ ] **Task 6.3.1**: Implement `VertebraSQLAlchemyPlugin`
  - Wrap `litestar.contrib.sqlalchemy.SQLAlchemyPlugin`
  - Auto-register serialization types
  - File: `sqlalchemy.py` (NEW)

- [ ] **Task 6.3.2**: Add SQLAlchemy plugin tests
  - Test plugin registration
  - Test DI connection provision
  - File: `src/py/tests/test_litestar_plugin.py`

**Acceptance**:
- Plugin works with Litestar app
- DI provides connections correctly
- Tests pass

---

### Task Group 6.4: Advanced Alchemy Plugin

**Priority**: P1
**Dependencies**: Task 6.1

- [ ] **Task 6.4.1**: Implement `VertebraAdvancedAlchemyPlugin`
  - Wrap `advanced_alchemy.extensions.litestar.SQLAlchemyPlugin`
  - Auto-register serialization types
  - File: `advanced_alchemy.py` (NEW)

- [ ] **Task 6.4.2**: Add Advanced Alchemy plugin tests
  - Test plugin registration
  - Test DI repository injection
  - File: `src/py/tests/test_litestar_plugin.py`

**Acceptance**:
- Plugin works with Litestar app
- DI injects repositories correctly
- Tests pass

---

## Summary

**Total Tasks**: 120+
**Estimated Duration**: 4-5 weeks
**Critical Path**: Phase 1 → Phase 2 → Phase 3 → Phase 5 → Phase 6

**Dependencies**:
- Phase 1 (SQLSpec): No dependencies
- Phase 2 (Serialization): No dependencies
- Phase 3 (SQLAlchemy): Depends on Phase 2
- Phase 4 (Health Checks): No dependencies (parallel with Phase 3)
- Phase 5 (Advanced Alchemy): Depends on Phase 3
- Phase 6 (Litestar): Depends on Phases 1, 2, 3, 5

**Testing Strategy**:
- Unit tests after each task group
- Integration tests after each phase
- Container-based tests with real databases
- Performance benchmarks after completion

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation
