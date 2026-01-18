# PRD-4: Bun-Backed Python Drivers - Implementation Tasks

**Status**: In Progress
**Created**: 2026-01-17
**Last Updated**: 2026-01-18
**Est. Duration**: 8 weeks

---

## Phase 1: Foundation Layer (Weeks 1-2)

### 1.1 IPC Protocol Definition

- [x] Define TypeScript interfaces for driver requests/responses
- [x] Define message types (DriverRequest, DriverResponse, StreamChunk)
- [x] Define column metadata format (ColumnDesc)
- [x] Define error response format
- [x] Document protocol specification

**Files**:
- `src/ts/src/protocol/messages.ts`
- `docs/protocol.md`

### 1.2 Serialization Layer

- [x] Implement MessagePack serialization in Python
- [x] Implement MessagePack deserialization in Python
- [x] Implement MessagePack serialization in TypeScript
- [x] Implement MessagePack deserialization in TypeScript
- [x] Add binary data handling (Uint8Array ↔ bytes)
- [x] Write serialization unit tests

**Files**:
- `src/py/vertebra/_internal/serialization.py`
- `src/ts/src/protocol/serialization.ts`
- `src/py/tests/test_driver_serialization.py`

**Dependencies**:
- `msgspec` (Python)
- `@msgpack/msgpack` (TypeScript)

### 1.3 Base Driver Interface

- [x] Create SyncDriver / AsyncDriver base classes
- [x] Define SyncConnection interface
- [x] Define SyncCursor interface
- [x] Define AsyncConnection interface
- [x] Define SyncPool / AsyncPool interfaces
- [x] Document driver interface contract

**Files**:
- `src/py/vertebra/drivers/base.py`
- `docs/api/drivers.md`

### 1.4 Exception Hierarchy

- [x] Implement DB-API 2.0 exception classes
- [x] Create exception hierarchy (Error, DatabaseError, etc.)
- [x] Implement error code mapping
- [x] Add error message formatting
- [x] Write exception tests

**Files**:
- `src/py/vertebra/drivers/exceptions.py`
- `src/py/tests/test_driver_exceptions.py`

**Exceptions**:
- `Error`
- `InterfaceError`
- `DatabaseError`
  - `DataError`
  - `OperationalError`
  - `IntegrityError`
  - `InternalError`
  - `ProgrammingError`
  - `NotSupportedError`

### 1.5 Type Conversion

- [x] Implement Python → PostgreSQL type conversion
- [x] Implement PostgreSQL → Python type conversion
- [x] Implement Python → MySQL type conversion
- [x] Implement MySQL → Python type conversion
- [x] Implement SQLite type conversions
- [x] Handle special types (JSON, UUID, arrays, etc.)
- [x] Write type conversion tests

**Files**:
- `src/py/vertebra/drivers/types.py`
- `src/py/tests/test_driver_types.py`

**Types to Handle**:
- Primitives: int, float, str, bytes, bool, None
- Date/Time: date, datetime, time, timedelta
- PostgreSQL: UUID, JSON/JSONB, arrays, hstore
- MySQL: DECIMAL, ENUM, SET
- Binary: bytea, BLOB

### 1.6 IPC Communication

- [x] Implement IPC client (Python side)
- [x] Implement request/response handling
- [x] Add connection retry logic
- [x] Implement timeout handling
- [x] Add connection pooling for IPC sockets
- [x] Write IPC tests

### 1.7 Bun-Only Runtime Guard

- [x] Detect Bun runtime availability on startup
- [x] Raise clear "unsupported runtime" errors when Bun is absent
- [x] Add tests for runtime guard behavior

**Files**:
- `src/py/vertebra/_internal/ipc.py`
- `src/py/tests/test_ipc_guard.py`

**Deliverables**:
- Working IPC layer between Python and Bun
- MessagePack serialization operational
- Base driver interfaces defined
- Exception hierarchy complete
- Type conversion utilities ready

---

## Phase 2: Database Drivers (Weeks 3-5)

### 2.1 PostgreSQL Driver - Core

- [x] Implement Connection class (DB-API 2.0)
- [x] Implement Cursor class (DB-API 2.0)
- [x] Implement execute() method
- [x] Implement executemany() method
- [x] Implement fetchone() method
- [x] Implement fetchmany() method
- [x] Implement fetchall() method
- [ ] Add module-level attributes (apilevel, threadsafety, paramstyle)
- [ ] Write DB-API 2.0 compliance tests

**Files**:
- `src/py/vertebra/drivers/postgres.py`
- `tests/compliance/test_dbapi_postgres.py`

### 2.2 PostgreSQL Driver - Async Interface

- [x] Implement AsyncConnection class
- [x] Implement async fetch() method
- [x] Implement async fetchrow() method
- [x] Implement async fetchval() method
- [x] Implement async execute() method
- [x] Implement async executemany() method
- [x] Write async interface tests

**Files**:
- `src/py/vertebra/drivers/postgres.py`
- `tests/unit/test_postgres_async.py`

### 2.3 PostgreSQL Driver - Connection Pool

- [x] Implement Pool class
- [x] Implement pool.acquire() method
- [x] Implement pool.release() method
- [x] Add pool statistics (get_size, get_idle_size)
- [x] Implement pool.close() method
- [x] Add pool configuration (min_size, max_size, timeouts)
- [ ] Write pool tests

**Files**:
- `src/py/vertebra/drivers/postgres.py`
- `src/ts/src/drivers/postgres.ts` (Bun server)
- `tests/integration/test_pooling.py`

### 2.4 PostgreSQL Driver - Transactions

- [x] Implement Transaction context manager
- [x] Add transaction isolation levels
- [ ] Implement savepoints (nested transactions)
- [ ] Add transaction status tracking
- [x] Write transaction tests

**Files**:
- `src/py/vertebra/drivers/postgres.py`
- `tests/integration/test_postgres_transactions.py`

### 2.5 PostgreSQL Driver - Advanced Features

- [ ] Implement prepared statements
- [ ] Add COPY support (bulk operations)
- [ ] Implement connection lifecycle callbacks
- [ ] Add query cancellation
- [ ] Write advanced feature tests

**Files**:
- `src/py/vertebra/drivers/postgres.py`
- `tests/integration/test_postgres_advanced.py`

### 2.6 PostgreSQL - Bun Server Integration

- [x] Implement PostgreSQL connection pooling in Bun
- [x] Add query execution handler
- [ ] Implement transaction management
- [ ] Add prepared statement caching
- [ ] Handle connection lifecycle
- [ ] Write Bun server tests

**Files**:
- `src/ts/src/drivers/postgres.ts`
- `tests/integration/test_postgres_integration.py`

**Dependencies**:
- `postgres` package (Bun)

### 2.7 MySQL Driver

- [ ] Implement Connection class (DB-API 2.0)
- [ ] Implement Cursor class (DB-API 2.0)
- [ ] Implement AsyncConnection class
- [ ] Implement Pool class
- [ ] Add MySQL-specific type conversions
- [ ] Implement transaction management
- [ ] Write MySQL tests

**Files**:
- `src/py/vertebra/drivers/mysql.py`
- `src/ts/src/drivers/mysql.ts`
- `tests/compliance/test_dbapi_mysql.py`
- `tests/integration/test_mysql_integration.py`

**Dependencies**:
- `mysql2` package (Bun)

### 2.8 SQLite Driver

- [x] Implement Connection class (DB-API 2.0)
- [x] Implement Cursor class (DB-API 2.0)
- [x] Implement AsyncConnection class
- [x] Add SQLite-specific type conversions
- [ ] Implement transaction management
- [x] Write SQLite tests

**Files**:
- `src/py/vertebra/drivers/sqlite.py`
- `src/ts/src/drivers/sqlite.ts`
- `tests/compliance/test_dbapi_sqlite.py`
- `tests/integration/test_sqlite_integration.py`

**Dependencies**:
- `better-sqlite3` package (Bun)

### 2.9 SQLSpec Adapters

- [ ] Implement SyncPostgresDriver SQLSpec adapter
- [ ] Implement SyncMySQLDriver SQLSpec adapter
- [ ] Implement SyncSqliteDriver SQLSpec adapter
- [ ] Add get_connection() method
- [ ] Add release_connection() method
- [ ] Write SQLSpec integration tests

**Files**:
- `src/py/vertebra/drivers/postgres.py`
- `src/py/vertebra/drivers/mysql.py`
- `src/py/vertebra/drivers/sqlite.py`
- `tests/compliance/test_sqlspec_adapter.py`

**Deliverables**:
- PostgreSQL driver (DB-API 2.0 + async)
- MySQL driver (DB-API 2.0 + async)
- SQLite driver (DB-API 2.0 + async)
- SQLSpec adapters for all drivers
- Connection pooling operational
- Transaction management complete

---

## Phase 3: File System Layer (Weeks 4-6)

### 3.1 VertebraFileSystem Base

- [ ] Create VertebraFileSystem class (extends AbstractFileSystem)
- [ ] Implement \_\_init\_\_ method
- [ ] Set protocol attribute
- [ ] Add backend configuration (local, s3, gcs)
- [ ] Implement instance caching
- [ ] Write base class tests

**Files**:
- `src/py/vertebra/filesystem/fsspec.py`
- `src/py/vertebra/filesystem/__init__.py`
- `tests/unit/test_filesystem_base.py`

### 3.2 Object Store Protocol (obstore-style)

- [x] Define SyncObjectStore / AsyncObjectStore protocols
- [x] Implement Local/S3/GCS stores backed by Bun filesystem client
- [x] Add URL-based store registry (store_from_url/async_store_from_url)
- [x] Add byte-range reads + list_with_delimiter support
- [x] Add streaming helpers (iter_chunks/put_stream + upload/download to path)
- [x] Add signed URL support for S3 (graceful fallback when unsupported)
- [ ] Defer GCS signed URLs until a signing strategy is chosen
- [ ] Write store protocol unit tests

**Files**:
- `src/py/vertebra/storage/store.py`
- `src/py/vertebra/filesystem/client.py`
- `src/ts/src/drivers/filesystem.ts`
- `tests/unit/test_storage_store.py`

### 3.3 Required File Operations

- [ ] Implement \_ls() method
- [ ] Implement \_cat_file() method
- [ ] Implement \_put_file() method
- [ ] Implement \_get_file() method
- [ ] Implement \_rm_file() method
- [ ] Implement \_exists() method
- [ ] Implement \_info() method
- [ ] Write file operation tests

**Files**:
- `src/py/vertebra/filesystem/client.py`
- `tests/unit/test_filesystem_ops.py`

### 3.4 Async File Operations

- [ ] Implement \_ls_async() method
- [ ] Implement \_cat_file_async() method
- [ ] Implement \_put_file_async() method
- [ ] Implement \_get_file_async() method
- [ ] Implement \_rm_file_async() method
- [ ] Implement batch operations
- [ ] Write async operation tests

**Files**:
- `src/py/vertebra/filesystem/client.py`
- `tests/unit/test_filesystem_async.py`

### 3.5 Streaming Support

- [ ] Implement \_open() method
- [ ] Create VertebraFile class (file-like object)
- [ ] Implement read() method
- [ ] Implement write() method
- [ ] Implement seek() method
- [ ] Add context manager support
- [ ] Write streaming tests

**Files**:
- `src/py/vertebra/filesystem/fsspec.py`
- `tests/unit/test_filesystem_streaming.py`

### 3.6 Local Filesystem Backend

- [ ] Implement local filesystem operations
- [ ] Use Bun I/O for file operations
- [ ] Add directory operations
- [ ] Implement file metadata
- [ ] Write local filesystem tests

**Files**:
- `src/py/vertebra/filesystem/__init__.py`
- `src/ts/src/drivers/filesystem.ts`
- `tests/integration/test_fs_local.py`

### 3.7 S3 Filesystem Backend

- [ ] Implement S3 list operations
- [ ] Implement S3 read operations
- [ ] Implement S3 write operations
- [ ] Implement S3 delete operations
- [ ] Add S3 metadata handling
- [ ] Write S3 filesystem tests

**Files**:
- `src/py/vertebra/filesystem/__init__.py`
- `src/ts/src/drivers/filesystem.ts`
- `tests/integration/test_fs_s3.py`

**Dependencies**:
- Bun `S3Client` (native)

### 3.8 GCS Filesystem Backend

- [ ] Implement GCS list operations
- [ ] Implement GCS read operations
- [ ] Implement GCS write operations
- [ ] Implement GCS delete operations
- [ ] Add GCS metadata handling
- [ ] Write GCS filesystem tests

**Files**:
- `src/py/vertebra/filesystem/__init__.py`
- `src/ts/src/drivers/filesystem.ts`
- `tests/integration/test_fs_gcs.py`

**Dependencies**:
- Bun `fetch` + GCS REST API (native)

### 3.9 fsspec Integration

- [ ] Register VertebraFileSystem with fsspec
- [ ] Test with pandas.read_parquet()
- [ ] Test with pandas.read_csv()
- [ ] Test with dask.dataframe
- [ ] Write fsspec compliance tests

**Files**:
- `src/py/vertebra/filesystem/__init__.py`
- `src/py/vertebra/filesystem/fsspec.py`
- `tests/compliance/test_fsspec_compliance.py`

**Deliverables**:
- fsspec-compatible filesystem interface
- Local, S3, and GCS backends
- Streaming file operations
- Integration with pandas and dask
- Compliance with fsspec contract

---

## Phase 4: Cloud Storage Clients (Weeks 6-7)

### 4.1 S3Client - Core

- [ ] Implement S3Client class
- [ ] Add initialization with credentials
- [ ] Implement upload() method
- [ ] Implement download() method
- [ ] Implement exists() method
- [ ] Implement delete() method
- [ ] Write S3Client tests

**Files**:
- `src/py/vertebra/storage/s3.py`
- `tests/unit/test_s3_client.py`

### 4.2 S3Client - Streaming

- [ ] Implement upload_stream() method
- [ ] Implement download_stream() method
- [ ] Add chunked streaming
- [ ] Implement progress callbacks
- [ ] Write streaming tests

**Files**:
- `src/py/vertebra/storage/s3.py`
- `tests/integration/test_s3_streaming.py`

### 4.3 S3Client - Multipart Upload

- [ ] Implement multipart upload initialization
- [ ] Add part upload handling
- [ ] Implement upload completion
- [ ] Add abort handling
- [ ] Implement resumable uploads
- [ ] Write multipart tests

**Files**:
- `src/py/vertebra/storage/s3.py`
- `src/ts/src/storage/s3.ts`
- `tests/integration/test_s3_multipart.py`

### 4.4 S3Client - Metadata and Listing

- [ ] Implement get_metadata() method
- [ ] Implement list_objects() method
- [ ] Add pagination support
- [ ] Implement prefix filtering
- [ ] Write metadata/listing tests

**Files**:
- `src/py/vertebra/storage/s3.py`
- `tests/integration/test_s3_metadata.py`

### 4.5 GCSClient

- [ ] Implement GCSClient class (same interface as S3Client)
- [ ] Implement upload() method
- [ ] Implement download() method
- [ ] Implement streaming methods
- [ ] Implement multipart upload
- [ ] Implement metadata/listing
- [ ] Write GCSClient tests

**Files**:
- `src/py/vertebra/storage/gcs.py`
- `src/ts/src/storage/gcs.ts`
- `tests/integration/test_gcs_client.py`

### 4.6 Error Handling

- [ ] Add retry logic for transient errors
- [ ] Implement exponential backoff
- [ ] Add timeout handling
- [ ] Implement graceful degradation
- [ ] Write error handling tests

**Files**:
- `src/py/vertebra/storage/s3.py`
- `src/py/vertebra/storage/gcs.py`
- `tests/integration/test_storage_errors.py`

**Deliverables**:
- S3Client with full feature set
- GCSClient with same interface
- Streaming upload/download
- Progress tracking
- Multipart upload support
- Resumable uploads

---

## Phase 5: Testing and Optimization (Weeks 7-8)

### 5.1 Test Infrastructure

- [ ] Create Docker Compose for test databases
- [ ] Add PostgreSQL test database
- [ ] Add MySQL test database
- [ ] Add MinIO (S3-compatible storage)
- [ ] Add test fixtures
- [ ] Document test setup

**Files**:
- `tests/docker-compose.yml`
- `tests/README.md`

### 5.2 Unit Tests

- [ ] Complete unit test coverage (>90%)
- [ ] Mock all Bun server responses
- [ ] Test parameter validation
- [ ] Test type conversions
- [ ] Test error handling
- [ ] Generate coverage report

**Files**:
- `tests/unit/**/*.py`

### 5.3 Integration Tests

- [ ] PostgreSQL integration tests
- [ ] MySQL integration tests
- [ ] SQLite integration tests
- [ ] S3 integration tests
- [ ] GCS integration tests
- [ ] Connection pool tests
- [ ] Transaction tests

**Files**:
- `tests/integration/**/*.py`

### 5.4 Compliance Tests

- [ ] DB-API 2.0 compliance suite
- [ ] Test all required methods
- [ ] Test exception hierarchy
- [ ] Test module attributes
- [ ] fsspec compliance tests
- [ ] SQLSpec adapter tests

**Files**:
- `tests/compliance/**/*.py`

### 5.5 Performance Benchmarks

- [ ] Benchmark PostgreSQL vs asyncpg
- [ ] Benchmark MySQL vs aiomysql
- [ ] Benchmark SQLite vs aiosqlite
- [ ] Benchmark S3 operations
- [ ] Measure IPC overhead
- [ ] Profile memory usage
- [ ] Generate benchmark report

**Files**:
- `tests/performance/**/*.py`
- `docs/performance.md`

**Metrics**:
- Queries per second (QPS)
- Query latency (p50, p95, p99)
- Connection establishment time
- Result set deserialization time
- Memory usage
- IPC overhead

### 5.6 Stress Tests

- [ ] Connection pool limit tests
- [ ] Concurrent query tests
- [ ] Long-running transaction tests
- [ ] Network failure simulation
- [ ] Bun server restart tests
- [ ] Memory leak detection
- [ ] Generate stress test report

**Files**:
- `tests/stress/**/*.py`

### 5.7 Performance Optimization

- [ ] Optimize MessagePack serialization
- [ ] Optimize type conversions
- [ ] Reduce IPC overhead
- [ ] Optimize connection pooling
- [ ] Profile and fix bottlenecks
- [ ] Document optimizations

**Files**:
- `docs/performance-tuning.md`

### 5.8 CI/CD Pipeline

- [ ] Configure GitHub Actions workflow
- [ ] Add unit test job
- [ ] Add integration test job
- [ ] Add compliance test job
- [ ] Add performance benchmark job
- [ ] Add coverage reporting
- [ ] Add stress test job (nightly)

**Files**:
- `.github/workflows/test.yml`

**Deliverables**:
- Comprehensive test suite (>90% coverage)
- All compliance tests passing
- Performance benchmarks documented
- CI/CD pipeline operational
- Stress tests validating reliability

---

## Phase 6: Documentation (Weeks 7-8)

### 6.1 API Documentation

- [ ] Generate API reference from docstrings
- [ ] Document all driver methods
- [ ] Document filesystem methods
- [ ] Document storage client methods
- [ ] Add code examples
- [ ] Configure Sphinx

**Files**:
- `docs/api/drivers.md`
- `docs/api/filesystem.md`
- `docs/api/storage.md`
- `docs/conf.py`

### 6.2 User Guides

- [ ] Write quickstart guide
- [ ] Write connection pooling guide
- [ ] Write async programming guide
- [ ] Write error handling guide
- [ ] Write performance tuning guide
- [ ] Add diagrams and examples

**Files**:
- `docs/guides/quickstart.md`
- `docs/guides/connection-pooling.md`
- `docs/guides/async-programming.md`
- `docs/guides/error-handling.md`
- `docs/guides/performance-tuning.md`

### 6.3 Driver Documentation

- [ ] Write PostgreSQL driver guide
- [ ] Write MySQL driver guide
- [ ] Write SQLite driver guide
- [ ] Document DB-API 2.0 compliance
- [ ] Document SQLSpec integration
- [ ] Add usage examples

**Files**:
- `docs/drivers/index.md`
- `docs/drivers/postgres.md`
- `docs/drivers/mysql.md`
- `docs/drivers/sqlite.md`
- `docs/drivers/db-api-compliance.md`
- `docs/drivers/sqlspec-integration.md`

### 6.4 Filesystem Documentation

- [ ] Write filesystem overview
- [ ] Write local filesystem guide
- [ ] Write S3 filesystem guide
- [ ] Write GCS filesystem guide
- [ ] Document fsspec integration
- [ ] Add pandas/dask examples

**Files**:
- `docs/filesystem/index.md`
- `docs/filesystem/local.md`
- `docs/filesystem/s3.md`
- `docs/filesystem/gcs.md`
- `docs/filesystem/fsspec-integration.md`

### 6.5 Storage Client Documentation

- [ ] Write S3Client API reference
- [ ] Write GCSClient API reference
- [ ] Document streaming best practices
- [ ] Add upload/download examples
- [ ] Document progress callbacks

**Files**:
- `docs/storage/s3-client.md`
- `docs/storage/gcs-client.md`
- `docs/storage/streaming.md`

### 6.6 Migration Guides

- [ ] Write asyncpg migration guide
- [ ] Write aiomysql migration guide
- [ ] Write aiosqlite migration guide
- [ ] Write boto3 migration guide
- [ ] Add before/after examples

**Files**:
- `docs/migration/from-asyncpg.md`
- `docs/migration/from-aiomysql.md`
- `docs/migration/from-aiosqlite.md`
- `docs/migration/from-boto3.md`

### 6.7 Examples

- [ ] Create basic PostgreSQL example
- [ ] Create basic MySQL example
- [ ] Create basic SQLite example
- [ ] Create SQLSpec integration example
- [ ] Create filesystem usage example
- [ ] Create S3 streaming example
- [ ] Create GCS streaming example

**Files**:
- `examples/basic_postgres.py`
- `examples/basic_mysql.py`
- `examples/basic_sqlite.py`
- `examples/sqlspec_integration.py`
- `examples/filesystem_usage.py`
- `examples/s3_streaming.py`
- `examples/gcs_streaming.py`

**Deliverables**:
- Complete API reference
- User guides for all components
- Migration guides from native drivers
- Working examples for all features
- Performance benchmarks documented

---

## Quality Gates

### Code Quality

- [ ] All code passes type checking (mypy, pyright)
- [ ] All code passes linting (ruff)
- [ ] All code formatted correctly (ruff format)
- [ ] No code smells (slotscheck)
- [ ] Pre-commit hooks pass

### Test Quality

- [ ] Unit test coverage >90%
- [ ] Integration tests pass
- [ ] Compliance tests pass (DB-API 2.0, fsspec)
- [ ] Performance benchmarks meet targets (2-5x improvement)
- [ ] Stress tests show no memory leaks

### Documentation Quality

- [ ] All public APIs documented
- [ ] All guides have working examples
- [ ] Migration guides complete
- [ ] README updated
- [ ] CHANGELOG maintained

### Performance Targets

- [ ] PostgreSQL: 2-3x faster than asyncpg
- [ ] MySQL: 2-3x faster than aiomysql
- [ ] SQLite: 1.5-2x faster than aiosqlite
- [ ] S3 upload: 1.5-2x faster than boto3
- [ ] S3 download: 1.5-2x faster than boto3
- [ ] IPC overhead: <1ms for small queries
- [ ] Memory overhead: <10% vs native

---

## Release Checklist

- [ ] All tasks complete
- [ ] All tests passing
- [ ] Documentation complete
- [ ] Examples working
- [ ] Performance targets met
- [ ] Security review complete
- [ ] License files in place
- [ ] CHANGELOG updated
- [ ] Version bumped
- [ ] Git tags created
- [ ] PyPI package built
- [ ] PyPI package published
- [ ] GitHub release created
- [ ] Announcement prepared

---

**Task Tracking**: Use GitHub Issues/Projects to track individual tasks
**Definition of Done**: Code complete, tests passing, documented, reviewed
**Success Criteria**: All quality gates passed, performance targets met
