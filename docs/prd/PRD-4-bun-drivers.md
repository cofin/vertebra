# PRD-4: Bun-Backed Python Drivers

**Status**: In Progress
**Created**: 2026-01-17
**Owner**: Vertebra Core Team
**Dependencies**: PRD-2 (Python Bindings), PRD-3 (Bun Server)

---

## Executive Summary

PRD-4 defines the implementation of high-performance database drivers, file systems, and cloud storage clients for Python that leverage Bun's fast I/O capabilities through a client-server architecture. By routing I/O operations through Bun, Vertebra bypasses Python's Global Interpreter Lock (GIL) limitations and provides 2-5x performance improvements over native Python drivers while maintaining full compatibility with industry standards (DB-API 2.0, fsspec).

**Key Goals**:
- Provide DB-API 2.0 compliant database drivers (PostgreSQL, MySQL, SQLite)
- Implement fsspec-compatible file systems (local, S3, GCS)
- Deliver high-level cloud storage clients with streaming support
- Achieve 2-5x performance improvement over native Python drivers
- Maintain seamless integration with existing Python ecosystem (SQLSpec, pandas, dask)

---

## Problem Statement

### Current Limitations

Python's database drivers and file I/O operations face significant performance constraints:

1. **GIL Bottleneck**: Python's Global Interpreter Lock prevents true parallelism, limiting I/O throughput
2. **Blocking I/O**: Native drivers (psycopg2, PyMySQL) use blocking I/O, reducing concurrency
3. **Context Switching Overhead**: Even async drivers (asyncpg, aiomysql) suffer from Python's async event loop overhead
4. **Memory Inefficiency**: Large result sets and file transfers consume excessive memory in Python

### Opportunity

Bun provides excellent I/O performance through its native implementation (written in Zig, optimized for I/O). By offloading I/O operations to a Bun server:
- Bypass Python's GIL entirely for I/O operations
- Leverage Bun's superior connection pooling and concurrency
- Reduce memory overhead through efficient binary serialization
- Maintain compatibility with existing Python APIs and tools

---

## Scope

### In Scope

**Database Drivers**:
- PostgreSQL driver with DB-API 2.0 and asyncpg-like async interface
- MySQL driver with DB-API 2.0 and aiomysql-like async interface
- SQLite driver with DB-API 2.0 and aiosqlite-like async interface
- SQLSpec adapter integration for all drivers
- Connection pooling (managed by Bun server)
- Transaction management
- Prepared statements
- Streaming large result sets

**File System Layer**:
- fsspec AbstractFileSystem implementation
- Local filesystem backend (Bun I/O)
- S3 filesystem backend
- GCS filesystem backend
- Integration with pandas, dask, and other fsspec consumers
- Caching and buffering strategies

**Cloud Storage Clients**:
- S3Client with streaming upload/download
- GCSClient with streaming upload/download
- Progress tracking and callbacks
- Multipart upload handling
- Resumable uploads

**Cross-Cutting Concerns**:
- Efficient IPC protocol (MessagePack serialization)
- Type conversion (Python ↔ Database types)
- Error handling and exception mapping
- Both sync and async Python interfaces
- Comprehensive test suite (>90% coverage)
- Performance benchmarks

### Out of Scope

- ORM implementations (compatible with SQLAlchemy/SQLSpec but not implementing)
- Database migration tools
- Schema introspection utilities
- Advanced caching strategies (basic caching only)
- Multi-region replication
- Custom query builders
- Direct wire protocol implementations (using Bun's database libraries)

### Assumptions and Constraints

- **Bun SQL / bun:sqlite** are the primary data access layers; PostgreSQL/MySQL support must be validated against Bun's SQL APIs.
- **DB-API 2.0 (PEP 249)** compliance is required for sync drivers; async APIs aim for asyncpg/aiomysql compatibility.
- **Cross-platform target**: Linux/macOS/Windows parity for MVP if feasible; Linux-first implementation.
- **Node runtime** is not supported for drivers by default; enable only if performance parity can be proven.

> **See PRD-0 Cross-PRD Architectural Decisions** for canonical project-wide choices:
> - **MessagePack for driver data plane** (consistent with this PRD)
> - Python 3.12+ baseline
> - Directory layout: `src/ts/` for TypeScript/Bun code (not `src/js/`)

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Python Application                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Python API (sync/async)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Vertebra Python Drivers Layer                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Database   │  │  Filesystem  │  │   Storage    │      │
│  │   Drivers    │  │    (fsspec)  │  │   Clients    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ IPC (MessagePack over socket)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Bun Server (PRD-3)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Connection  │  │  File I/O    │  │  Cloud SDK   │      │
│  │    Pools     │  │   Handler    │  │   (AWS/GCP)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Native drivers & APIs
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        PostgreSQL  │  MySQL  │  SQLite  │  S3  │  GCS       │
└─────────────────────────────────────────────────────────────┘
```

### Design Philosophy: Thin Python Wrapper

**Selected Approach**: Thin Python wrappers that serialize requests to Bun server

**Rationale**:
1. **Maximum Performance**: All I/O, pooling, and state management in Bun/TypeScript
2. **Simpler Python Code**: Python layer only handles API compliance and type conversion
3. **Easier Maintenance**: Single source of truth for pooling and connection logic
4. **Better Resource Management**: Bun manages all database connections and file handles

**Trade-offs Accepted**:
- Network overhead for IPC (mitigated by efficient binary serialization)
- More complex protocol design (mitigated by using MessagePack)
- Dependency on Bun server availability (mitigated by connection retry and graceful degradation)

**Runtime Constraint**:
Drivers are **Bun-only** for performance. If running under Node (or without Bun), modules must raise a clear "unsupported runtime" error rather than falling back to slower implementations.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Separate sync/async interfaces** | DB-API 2.0 requires sync, modern Python prefers async. Sync interface uses thread pool to call async methods internally. |
| **Server-side connection pooling** | Bun manages all pools, Python just requests connections. Simplifies state management and improves performance. |
| **MessagePack serialization** | Fast binary format, schema-less (flexible), well-supported in both Python and TypeScript. Protocol Buffers considered for future optimization. |
| **Streaming for large results** | Results >10MB or >10000 rows sent in chunks to avoid memory issues. Python can iterate results without loading all into memory. |
| **Type conversion in Python** | Python layer converts between Python types and database wire format. Keeps serialization simple (JSON-compatible types). |
| **Bun-only enforcement** | Preserve performance by disabling drivers under Node; allow future Node support only if parity is demonstrated. |

---

## Database Drivers

### DB-API 2.0 Compliance

All database drivers implement [PEP 249 (Python Database API Specification v2.0)](https://peps.python.org/pep-0249/), ensuring compatibility with existing Python database tools and libraries.

**Required Module Attributes**:
```python
apilevel = "2.0"              # DB-API version
threadsafety = 2              # Threads may share module and connections
paramstyle = "pyformat"       # %(name)s style parameters
```

**Exception Hierarchy**:
```python
Exception
└── Error
    ├── InterfaceError
    ├── DatabaseError
    │   ├── DataError
    │   ├── OperationalError
    │   ├── IntegrityError
    │   ├── InternalError
    │   ├── ProgrammingError
    │   └── NotSupportedError
```

**Connection Interface**:
```python
class Connection:
    def close(self) -> None:
        """Close the connection to the database."""

    def commit(self) -> None:
        """Commit pending transaction."""

    def rollback(self) -> None:
        """Rollback pending transaction."""

    def cursor(self) -> Cursor:
        """Return new Cursor object using the connection."""
```

**Cursor Interface**:
```python
class Cursor:
    description: tuple[tuple]  # Column metadata
    rowcount: int              # Number of rows affected

    def execute(self, operation: str, parameters=None) -> Self:
        """Execute a database operation (query or command)."""

    def executemany(self, operation: str, seq_of_params) -> Self:
        """Execute operation against all parameter sequences."""

    def fetchone(self) -> tuple | None:
        """Fetch next row of a query result set."""

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch next set of rows of a query result."""

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows of a query result."""

    def close(self) -> None:
        """Close the cursor."""
```

### Async Interface (asyncpg-compatible)

Modern Python applications prefer async/await syntax. Vertebra drivers provide an asyncpg-compatible async interface:

```python
class AsyncConnection:
    async def fetch(self, query: str, *args) -> list[Record]:
        """Execute query and return all rows."""

    async def fetchrow(self, query: str, *args) -> Record | None:
        """Execute query and return first row."""

    async def fetchval(self, query: str, *args) -> Any:
        """Execute query and return single value."""

    async def execute(self, query: str, *args) -> str:
        """Execute command and return status message."""

    async def executemany(self, query: str, args: list) -> None:
        """Execute command with multiple parameter sets."""

    def transaction(self) -> AsyncTransaction:
        """Create new transaction context manager."""

    async def close(self) -> None:
        """Close the connection."""
```

**Connection Pool**:
```python
class Pool:
    async def acquire(self) -> AsyncConnection:
        """Acquire connection from pool."""

    async def release(self, connection: AsyncConnection) -> None:
        """Release connection back to pool."""

    async def close(self) -> None:
        """Close all connections in pool."""

    def get_size(self) -> int:
        """Get current pool size."""

    def get_idle_size(self) -> int:
        """Get number of idle connections."""
```

### PostgreSQL Driver

**Priority**: 1 (Most Common)

**Features**:
- Full DB-API 2.0 compliance
- Asyncpg-compatible async interface
- Connection pooling via Bun server (using Bun's PostgreSQL driver)
- Prepared statements for frequently executed queries
- Transaction management with savepoints
- COPY support for bulk operations
- Type conversion for PostgreSQL types (arrays, JSON, UUID, etc.)

**Example Usage**:
```python
# Sync (DB-API 2.0)
from vertebra.drivers import PostgresDriver

conn = PostgresDriver.connect("postgresql://localhost/mydb")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = %s", (123,))
users = cursor.fetchall()
conn.close()

# Async (asyncpg-like)
import asyncio

async def main():
    pool = await PostgresDriver.create_pool(
        "postgresql://localhost/mydb",
        min_size=10,
        max_size=20
    )

    async with pool.acquire() as conn:
        # Simple query
        users = await conn.fetch("SELECT * FROM users WHERE active = $1", True)

        # Transaction
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO users (name, email) VALUES ($1, $2)",
                "Alice", "alice@example.com"
            )

    await pool.close()

asyncio.run(main())
```

### MySQL Driver

**Priority**: 2

**Features**:
- Full DB-API 2.0 compliance
- Aiomysql-compatible async interface
- Connection pooling via Bun server (using Bun's MySQL driver)
- Prepared statements
- Transaction management
- Type conversion for MySQL types

**Example Usage**:
```python
# Sync (DB-API 2.0)
from vertebra.drivers import MySQLDriver

conn = MySQLDriver.connect("mysql://user:pass@localhost/mydb")
cursor = conn.cursor()
cursor.execute("SELECT * FROM products WHERE price > %s", (100,))
products = cursor.fetchall()
conn.close()

# Async (aiomysql-like)
async def main():
    pool = await MySQLDriver.create_pool(
        "mysql://user:pass@localhost/mydb",
        minsize=5,
        maxsize=15
    )

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM products")
            products = await cur.fetchall()

    pool.close()
    await pool.wait_closed()
```

### SQLite Driver

**Priority**: 3 (Simpler, local only)

**Features**:
- Full DB-API 2.0 compliance
- Aiosqlite-compatible async interface
- File-based (no connection pooling needed)
- Transaction management
- Type conversion

**Example Usage**:
```python
# Sync (DB-API 2.0)
from vertebra.drivers import SQLiteDriver

conn = SQLiteDriver.connect("database.db")
cursor = conn.cursor()
cursor.execute("SELECT * FROM logs")
logs = cursor.fetchall()
conn.close()

# Async (aiosqlite-like)
async def main():
    async with SQLiteDriver.connect("database.db") as db:
        await db.execute("INSERT INTO logs (message) VALUES (?)", ("test",))
        await db.commit()

        async with db.execute("SELECT * FROM logs") as cursor:
            async for row in cursor:
                print(row)
```

### SQLSpec Integration

Vertebra drivers integrate seamlessly with SQLSpec for multi-database support:

```python
from vertebra.drivers import PostgresDriver, MySQLDriver
from sqlspec import SQLSpec

sql = SQLSpec()

# Add multiple database configurations
sql.add_config(PostgresDriver(dsn="postgresql://localhost/analytics"))
sql.add_config(MySQLDriver(dsn="mysql://localhost/legacy"))

async with sql.provide_session() as session:
    # SQLSpec routes queries to appropriate database
    users = await session.select_all("SELECT * FROM users")
    await session.execute("INSERT INTO audit_log (action) VALUES (?)", ("login",))
```

**SQLSpec Adapter Interface**:
```python
class PostgresDriver:
    """SQLSpec-compatible driver adapter"""

    def __init__(self, dsn: str, **pool_config):
        self.dsn = dsn
        self.pool_config = pool_config
        self._pool = None

    async def get_connection(self):
        """SQLSpec calls this to get connection"""
        if not self._pool:
            self._pool = await create_pool(self.dsn, **self.pool_config)
        return await self._pool.acquire()

    async def release_connection(self, conn):
        """Release connection back to pool"""
        await self._pool.release(conn)
```

---

## File System Layer

### fsspec Integration

[fsspec](https://filesystem-spec.readthedocs.io/) provides a unified Pythonic interface to local, remote, and embedded file systems. Vertebra implements fsspec's AbstractFileSystem interface, enabling use with pandas, dask, and other data science tools.

**Benefits**:
- Seamless integration with existing data tools
- Consistent API across storage backends
- Caching and buffering support
- Async/await support

### VertebraFileSystem

**Base Class**: `fsspec.AbstractFileSystem`

**Supported Backends**:
- `local`: Local filesystem with Bun I/O
- `s3`: AWS S3 storage
- `gcs`: Google Cloud Storage

**Required Methods**:
```python
class VertebraFileSystem(AbstractFileSystem):
    protocol = "vertebra"
    async_impl = True

    def __init__(self, backend: str = "local", **storage_options):
        """
        Args:
            backend: "local", "s3", or "gcs"
            storage_options: Backend-specific configuration
        """
        super().__init__(**storage_options)
        self.backend = backend

    # File operations
    def _ls(self, path: str, detail: bool = True) -> list:
        """List files in directory"""

    def _cat_file(self, path: str) -> bytes:
        """Get contents of file"""

    def _put_file(self, lpath: str, rpath: str) -> None:
        """Upload file from local to remote"""

    def _get_file(self, rpath: str, lpath: str) -> None:
        """Download file from remote to local"""

    def _rm_file(self, path: str) -> None:
        """Remove file"""

    def _exists(self, path: str) -> bool:
        """Check if path exists"""

    def _info(self, path: str) -> dict:
        """Get file metadata"""

    # Streaming support
    def _open(self, path: str, mode: str = "rb", **kwargs):
        """Return file-like object for streaming"""
        return VertebraFile(self, path, mode, **kwargs)
```

**Usage Examples**:

**With fsspec registration**:
```python
from vertebra.fs import VertebraFileSystem
import fsspec

# Register protocol
fsspec.register_implementation("vertebra", VertebraFileSystem)

# Use with any fsspec-compatible tool
import pandas as pd
df = pd.read_parquet("vertebra://s3/bucket/data.parquet")

# Use with dask
import dask.dataframe as dd
ddf = dd.read_csv("vertebra://s3/bucket/*.csv")
```

**Direct usage**:
```python
from vertebra.fs import VertebraFileSystem

# Local filesystem
fs_local = VertebraFileSystem(backend="local")
with fs_local.open("/tmp/data.txt", "r") as f:
    content = f.read()

# S3 filesystem
fs_s3 = VertebraFileSystem(
    backend="s3",
    bucket="my-bucket",
    region="us-east-1"
)
files = fs_s3.ls("path/to/directory/")
fs_s3.put_file("local.csv", "remote/path/file.csv")
```

### Async File Operations

For async applications, use the async methods:

```python
import asyncio
from vertebra.fs import VertebraFileSystem

async def process_files():
    fs = VertebraFileSystem(backend="s3", bucket="my-bucket")

    # List files asynchronously
    files = await fs._ls_async("data/")

    # Read file contents
    for file_info in files:
        content = await fs._cat_file_async(file_info["name"])
        process(content)

asyncio.run(process_files())
```

---

## Cloud Storage Clients

### Design Philosophy

While fsspec provides low-level file operations, high-level clients (S3Client, GCSClient) offer better UX for common cloud storage tasks:

- Streaming uploads/downloads with progress tracking
- Multipart upload handling
- Resumable uploads
- Simplified authentication
- Intuitive API

### S3Client

**Features**:
- Streaming upload/download
- Progress callbacks
- Multipart upload (automatic for large files)
- Object metadata management
- List/delete operations

**Interface**:
```python
class S3Client:
    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        credentials: dict | None = None
    ):
        """
        Initialize S3 client.

        Args:
            bucket: S3 bucket name
            region: AWS region
            endpoint_url: Custom endpoint (for S3-compatible services)
            credentials: AWS credentials dict with 'access_key_id' and 'secret_access_key'
        """

    async def upload(
        self,
        key: str,
        file_path: str,
        on_progress: Callable[[int, int], None] | None = None,
        multipart_threshold: int = 8 * 1024 * 1024
    ) -> str:
        """
        Upload file to S3.

        Args:
            key: S3 object key
            file_path: Local file path
            on_progress: Callback(uploaded_bytes, total_bytes)
            multipart_threshold: Size threshold for multipart upload

        Returns:
            Object ETag
        """

    async def download(
        self,
        key: str,
        file_path: str,
        on_progress: Callable[[int, int], None] | None = None
    ) -> None:
        """Download file from S3"""

    async def download_stream(
        self,
        key: str,
        chunk_size: int = 64 * 1024
    ) -> AsyncIterator[bytes]:
        """Stream download in chunks"""

    async def upload_stream(
        self,
        key: str,
        stream: AsyncIterator[bytes],
        content_type: str = "application/octet-stream"
    ) -> str:
        """Upload from async iterator"""

    async def list_objects(
        self,
        prefix: str = "",
        max_keys: int = 1000
    ) -> list[dict]:
        """List objects with pagination"""

    async def delete(self, key: str) -> None:
        """Delete object"""

    async def exists(self, key: str) -> bool:
        """Check if object exists"""

    async def get_metadata(self, key: str) -> dict:
        """Get object metadata"""
```

**Usage Examples**:

**Simple upload/download**:
```python
from vertebra.storage import S3Client

async def main():
    s3 = S3Client(bucket="my-bucket", region="us-east-1")

    # Upload
    await s3.upload("data/file.csv", "/local/path/file.csv")

    # Download
    await s3.download("data/file.csv", "/local/path/downloaded.csv")

    # Check existence
    if await s3.exists("data/file.csv"):
        print("File exists")
```

**Streaming with progress**:
```python
async def upload_with_progress():
    s3 = S3Client(bucket="my-bucket")

    def progress_callback(uploaded: int, total: int):
        percent = (uploaded / total) * 100
        print(f"Progress: {percent:.1f}% ({uploaded}/{total} bytes)")

    await s3.upload(
        "large-file.zip",
        "/local/large-file.zip",
        on_progress=progress_callback
    )

async def stream_download():
    s3 = S3Client(bucket="my-bucket")

    async for chunk in s3.download_stream("large-file.zip"):
        # Process chunk without loading entire file into memory
        process_chunk(chunk)
```

**List and delete**:
```python
async def cleanup_old_files():
    s3 = S3Client(bucket="my-bucket")

    # List objects with prefix
    objects = await s3.list_objects(prefix="logs/2025/")

    for obj in objects:
        # Delete old log files
        await s3.delete(obj["Key"])
```

### GCSClient

**Features**: Identical to S3Client (consistent API across cloud providers)

**Interface**: Same as S3Client, using Google Cloud Storage backend

**Usage Example**:
```python
from vertebra.storage import GCSClient

async def main():
    gcs = GCSClient(
        bucket="my-gcs-bucket",
        project="my-project",
        credentials="/path/to/service-account.json"
    )

    # Same API as S3Client
    await gcs.upload("data/file.csv", "/local/file.csv")
    await gcs.download("data/file.csv", "/local/downloaded.csv")
```

---

## IPC Protocol and Serialization

### Message Format

**Request Message**:
```typescript
interface DriverRequest {
  requestId: string;        // UUID for matching responses
  driver: "postgres" | "mysql" | "sqlite" | "s3" | "gcs" | "fs";
  operation: string;        // "connect", "execute", "fetch", etc.
  params: Record<string, any>;
}
```

**Response Message**:
```typescript
interface DriverResponse {
  requestId: string;
  status: "success" | "error" | "streaming";
  data?: any;              // Serialized result
  error?: {
    type: string;          // Exception type
    message: string;
    code?: string;         // DB error code
  };
  metadata?: {
    rowCount?: number;
    description?: ColumnDesc[];
    affectedRows?: number;
  };
}
```

**Column Metadata**:
```typescript
interface ColumnDesc {
  name: string;
  typeOid: number;         // PostgreSQL type OID
  typeName: string;        // "integer", "varchar", etc.
  nullable: boolean;
}
```

### Serialization Format: MessagePack

**Selected**: MessagePack for Phase 1

**Rationale**:
- Fast binary serialization
- Schema-less (flexible for evolving protocol)
- Well-supported in Python (msgpack) and TypeScript (@msgpack/msgpack)
- Smaller than JSON, faster than Protocol Buffers for our use case

**Future**: Evaluate Protocol Buffers for optimization if needed

**Python Implementation**:
```python
import msgpack
import uuid

def serialize_request(driver: str, operation: str, **params) -> bytes:
    request = {
        "requestId": str(uuid.uuid4()),
        "driver": driver,
        "operation": operation,
        "params": params
    }
    return msgpack.packb(request, use_bin_type=True)

def deserialize_response(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)
```

### Streaming Large Results

For result sets exceeding thresholds (10MB or 10000 rows):

1. **Server detects large result**: Checks size before sending
2. **Initial response**: Sends metadata + first chunk
3. **Client requests more**: Python requests additional chunks
4. **Server streams**: Sends remaining data in chunks
5. **Client assembles**: Python assembles full result or yields incrementally

**Stream Chunk Format**:
```typescript
interface StreamChunk {
  requestId: string;
  chunkIndex: number;
  data: Uint8Array;        // Binary data chunk
  isLast: boolean;
}
```

### Connection Pooling Protocol

**Pool Configuration**:
```typescript
interface PoolConfig {
  driver: "postgres" | "mysql";
  dsn: string;
  minSize: number;
  maxSize: number;
  maxIdleTime: number;      // seconds
  commandTimeout: number;   // seconds
}
```

**Pool Operations**:
```typescript
// Pool lifecycle
pool.create(config: PoolConfig) -> poolId: string
pool.close(poolId: string) -> void

// Connection management
pool.acquire(poolId: string) -> connectionId: string
pool.release(poolId: string, connectionId: string) -> void

// Query execution (auto-acquire/release)
pool.execute(poolId: string, query: string, params: any[]) -> result

// Pool statistics
pool.stats(poolId: string) -> PoolStats

interface PoolStats {
  totalConnections: number;
  idleConnections: number;
  activeConnections: number;
  waitingRequests: number;
}
```

### Error Mapping

Database errors are mapped to DB-API 2.0 exception hierarchy:

| Database Error | Python Exception |
|----------------|------------------|
| Connection failure | `OperationalError` |
| Timeout | `OperationalError` |
| Constraint violation | `IntegrityError` |
| SQL syntax error | `ProgrammingError` |
| Data type error | `DataError` |
| Internal database error | `InternalError` |
| Unsupported operation | `NotSupportedError` |

---

## Implementation Plan

### Phase 1: Foundation Layer (Weeks 1-2)

**Deliverables**:
- Base driver interface and abstract classes
- IPC protocol definitions (TypeScript interfaces)
- MessagePack serialization/deserialization
- Error handling and exception hierarchy
- Type conversion utilities (Python ↔ Database types)
- Basic connection lifecycle management

**Dependencies**:
- PRD-2 IPC primitives
- PRD-3 Bun server endpoints

**Success Criteria**:
- Can send/receive messages between Python and Bun
- Type conversions tested for common types
- Error mapping verified

### Phase 2: Database Drivers (Weeks 3-5)

**2.1 PostgreSQL Driver** (Priority 1):
- DB-API 2.0 Connection and Cursor classes
- Async Connection and Pool classes
- Connection pooling integration with Bun server
- Transaction management (begin, commit, rollback, savepoints)
- Prepared statements
- Type conversion for PostgreSQL types (arrays, JSON, UUID, timestamptz, etc.)

**2.2 MySQL Driver** (Priority 2):
- Same interface pattern as PostgreSQL
- MySQL-specific type conversions
- Transaction isolation levels

**2.3 SQLite Driver** (Priority 3):
- Simpler implementation (no pooling)
- File-based connection management

**2.4 SQLSpec Adapters**:
- Adapter classes for each driver
- Integration tests with SQLSpec

**Critical Path**: PostgreSQL must be complete and tested before MySQL

**Success Criteria**:
- All DB-API 2.0 compliance tests pass
- Performance benchmarks show 2-5x improvement
- SQLSpec integration works

### Phase 3: File System Layer (Weeks 4-6, parallel with Phase 2)

**Deliverables**:
- fsspec AbstractFileSystem base class
- Local filesystem implementation (Bun I/O)
- S3 filesystem backend
- GCS filesystem backend
- Caching and buffering strategies
- File-like streaming objects

**Dependencies**: Phase 1 complete

**Success Criteria**:
- fsspec compliance tests pass
- Works with pandas.read_parquet/read_csv
- Works with dask
- Performance tests show improvement over native fsspec

### Phase 4: Cloud Storage Clients (Weeks 6-7)

**Deliverables**:
- S3Client implementation
- GCSClient implementation
- Streaming upload/download
- Progress callbacks
- Multipart upload handling
- Resumable uploads

**Dependencies**: Phase 3 filesystem patterns established

**Success Criteria**:
- Can upload/download large files (>1GB) efficiently
- Progress callbacks work correctly
- Handles network interruptions gracefully

### Phase 5: Testing and Optimization (Weeks 7-8)

**Deliverables**:
- Comprehensive test suite:
  - Unit tests (mocked Bun responses)
  - Integration tests (real databases)
  - Compliance tests (DB-API 2.0, fsspec)
  - Performance benchmarks
  - Stress tests (connection pool limits, concurrent queries)
- Performance optimization based on benchmarks
- Documentation (API reference, guides, examples)
- CI/CD pipeline configuration

**Success Criteria**:
- Test coverage >90%
- All compliance tests pass
- Performance targets met (2-5x improvement)
- No memory leaks in stress tests
- Documentation complete

---

## Testing Strategy

### Test Levels

**1. Unit Tests** (Fast, no external dependencies):
```
tests/unit/
├── test_postgres_driver.py       # Mock Bun responses
├── test_mysql_driver.py
├── test_sqlite_driver.py
├── test_filesystem.py
├── test_s3_client.py
├── test_serialization.py
└── test_exceptions.py
```

**2. Integration Tests** (Requires running Bun server + databases):
```
tests/integration/
├── test_postgres_integration.py  # Real PostgreSQL
├── test_mysql_integration.py     # Real MySQL
├── test_sqlite_integration.py    # Local SQLite file
├── test_s3_integration.py        # MinIO/LocalStack
├── test_gcs_integration.py       # GCS emulator
└── test_pooling.py               # Connection pool behavior
```

**3. Compliance Tests** (Standards conformance):
```
tests/compliance/
├── test_dbapi_compliance.py      # PEP 249
├── test_sqlspec_adapter.py       # SQLSpec
└── test_fsspec_compliance.py     # fsspec contract
```

**4. Performance Tests** (Benchmarks):
```
tests/performance/
├── bench_postgres.py             # vs asyncpg
├── bench_mysql.py                # vs aiomysql
├── bench_sqlite.py               # vs aiosqlite
├── bench_s3_streaming.py         # Large files
└── bench_connection_pool.py      # Pool overhead
```

**Metrics**:
- Queries per second (QPS)
- Query latency (p50, p95, p99)
- Connection establishment time
- Result set deserialization time
- Memory usage
- IPC overhead

**Target**: 2-5x faster than native Python drivers

**5. Stress Tests** (Reliability):
```
tests/stress/
├── test_connection_pool_limits.py
├── test_concurrent_queries.py
├── test_long_running_transactions.py
├── test_network_failures.py
└── test_bun_server_restart.py
```

**Scenarios**:
- 1000+ concurrent connections
- Connection pool exhaustion
- Network interruptions
- Bun server crashes (graceful degradation)
- Memory leaks (long-running tests)

### Test Infrastructure

**Docker Compose** for test databases:
```yaml
# tests/docker-compose.yml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: test

  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: test

  minio:  # S3-compatible storage
    image: minio/minio
    command: server /data
```

**CI/CD Pipeline**:
1. Unit tests (fast, parallel)
2. Integration tests (parallel across databases)
3. Compliance tests
4. Performance benchmarks (track regressions)
5. Stress tests (nightly builds only)

**Coverage Requirements**:
- Overall: >90%
- Critical paths (connection, execute, fetch): 100%
- Error handling: >95%

**Quality Gates**:
- All tests pass
- No performance regressions >10%
- Code coverage maintained
- Type checking passes (mypy, pyright)
- Linting passes (ruff)

---

## Performance Targets

### Benchmarks vs Native Drivers

| Operation | Native Driver | Vertebra Target | Improvement |
|-----------|--------------|----------------|-------------|
| **PostgreSQL** |
| Simple SELECT | asyncpg | 2-3x faster | Reduced GIL overhead |
| Bulk INSERT | asyncpg | 2-5x faster | Better pooling |
| Large result set | asyncpg | 1.5-2x faster | Streaming + binary |
| Connection pool | asyncpg | 1.5-2x faster | Bun native pools |
| **MySQL** |
| Simple SELECT | aiomysql | 2-3x faster | Reduced GIL overhead |
| Bulk INSERT | aiomysql | 2-5x faster | Better pooling |
| **SQLite** |
| Simple SELECT | aiosqlite | 1.5-2x faster | Bun I/O |
| Bulk INSERT | aiosqlite | 2-3x faster | Bun I/O |
| **S3** |
| Upload 100MB | boto3 | 1.5-2x faster | Streaming + Bun I/O |
| Download 100MB | boto3 | 1.5-2x faster | Streaming + Bun I/O |
| List 10k objects | boto3 | 2-3x faster | Parallel requests |

### Resource Usage Targets

- **Memory**: <10% overhead vs native drivers
- **CPU**: <5% overhead for IPC serialization
- **Latency**: <1ms added latency for small queries (<1KB result)
- **Latency**: <5ms added latency for large queries (>1MB result)

---

## Documentation and Examples

### Documentation Structure

```
docs/drivers/
├── index.md                  # Overview
├── postgres.md              # PostgreSQL guide
├── mysql.md                 # MySQL guide
├── sqlite.md                # SQLite guide
├── db-api-compliance.md     # DB-API 2.0 details
└── sqlspec-integration.md   # SQLSpec usage

docs/filesystem/
├── index.md                 # Filesystem overview
├── local.md                 # Local filesystem
├── s3.md                    # S3 filesystem
└── fsspec-integration.md    # pandas, dask integration

docs/storage/
├── s3-client.md             # S3Client API
├── gcs-client.md            # GCSClient API
└── streaming.md             # Streaming best practices

docs/guides/
├── quickstart.md
├── connection-pooling.md
├── async-programming.md
├── error-handling.md
└── performance-tuning.md

docs/api/
├── drivers.md               # Full API reference
├── filesystem.md
└── storage.md
```

### Example Code

**examples/basic_postgres.py**:
```python
from vertebra.drivers import PostgresDriver

# Sync usage (DB-API 2.0)
conn = PostgresDriver.connect("postgresql://localhost/mydb")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = %s", (123,))
users = cursor.fetchall()
conn.close()

# Async usage
import asyncio

async def main():
    pool = await PostgresDriver.create_pool("postgresql://localhost/mydb")
    async with pool.acquire() as conn:
        users = await conn.fetch("SELECT * FROM users")
    await pool.close()

asyncio.run(main())
```

**examples/sqlspec_integration.py**:
```python
from vertebra.drivers import PostgresDriver
from sqlspec import SQLSpec

sql = SQLSpec()
sql.add_config(PostgresDriver(dsn="postgresql://localhost/mydb"))

async with sql.provide_session() as session:
    users = await session.select_all("SELECT * FROM users")
    await session.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
```

**examples/filesystem_usage.py**:
```python
from vertebra.fs import VertebraFileSystem
import pandas as pd
import fsspec

# Register with fsspec
fsspec.register_implementation("vertebra", VertebraFileSystem)

# Use with pandas
df = pd.read_parquet("vertebra://s3/bucket/data.parquet")

# Direct usage
fs = VertebraFileSystem(backend="s3", bucket="my-bucket")
with fs.open("path/to/file.txt", "r") as f:
    content = f.read()
```

**examples/s3_streaming.py**:
```python
from vertebra.storage import S3Client

async def upload_large_file():
    s3 = S3Client(bucket="my-bucket", region="us-east-1")

    # Upload with progress
    def progress(uploaded: int, total: int):
        pct = (uploaded / total) * 100
        print(f"Progress: {uploaded}/{total} bytes ({pct:.1f}%)")

    await s3.upload("large-file.zip", "/local/file.zip", on_progress=progress)

    # Stream download
    async for chunk in s3.download_stream("large-file.zip"):
        process_chunk(chunk)
```

### Migration Guides

**From asyncpg**:
```markdown
# Migrating from asyncpg

## Before
```python
import asyncpg
pool = await asyncpg.create_pool(dsn)
async with pool.acquire() as conn:
    result = await conn.fetch("SELECT * FROM users")
```

## After
```python
from vertebra.drivers import PostgresDriver
pool = await PostgresDriver.create_pool(dsn)
async with pool.acquire() as conn:
    result = await conn.fetch("SELECT * FROM users")
```

## Benefits
- 2-3x faster queries
- Better connection pooling
- SQLSpec multi-database support
```

---

## Dependencies

### Python Dependencies

```toml
[project.dependencies]
msgpack = "^1.0.7"           # IPC serialization
typing-extensions = "^4.9"   # Type hints

[project.optional-dependencies]
drivers = [
    "vertebra[bindings]",    # PRD-2 Python bindings
]
filesystem = [
    "fsspec >=2024.1",       # Filesystem interface
]
storage = [
    "vertebra[filesystem]",
]
```

### Bun Dependencies

```json
{
  "dependencies": {
    "postgres": "^3.4",      // PostgreSQL driver
    "mysql2": "^3.6",        // MySQL driver
    "better-sqlite3": "^9.2", // SQLite driver
    "@aws-sdk/client-s3": "^3.478",     // S3 client
    "@google-cloud/storage": "^7.7",    // GCS client
    "@msgpack/msgpack": "^3.0"          // Serialization
  }
}
```

### External Dependencies

**Runtime**:
- PRD-2: Python Bindings (IPC layer)
- PRD-3: Bun Server (I/O operations)
- PostgreSQL 12+ (optional, for PostgreSQL driver)
- MySQL 8+ (optional, for MySQL driver)
- SQLite 3.35+ (optional, for SQLite driver)

**Development**:
- Docker & Docker Compose (test infrastructure)
- pytest (Python testing)
- pytest-databases (database fixtures)
- pytest-asyncio (async test support)

---

## Success Metrics

### Performance Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Query performance | 2-5x faster than native | Benchmark suite (p50, p95, p99) |
| Connection pool overhead | <5% vs native | Pool stress tests |
| IPC latency | <1ms for small queries | Latency measurements |
| Memory overhead | <10% vs native | Memory profiling |
| Throughput (QPS) | 2x native drivers | Load testing |

### Quality Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Test coverage | >90% | pytest-cov |
| DB-API 2.0 compliance | 100% | PEP 249 test suite |
| fsspec compliance | 100% | fsspec test suite |
| Type safety | 100% | mypy, pyright |
| Documentation coverage | 100% API | Sphinx coverage |

### Adoption Metrics

| Metric | Target | Timeline |
|--------|--------|----------|
| Initial users | 10 projects | Month 1 |
| GitHub stars | 100+ | Month 3 |
| PyPI downloads | 1000+/month | Month 6 |
| Community PRs | 5+ | Month 6 |

---

## Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| **IPC serialization overhead** | High | Medium | Use MessagePack (fast binary). Consider Protocol Buffers if needed. Benchmark early. |
| **Connection pool edge cases** | High | Medium | Extensive stress testing. Reference asyncpg implementation. Gradual rollout. |
| **Type conversion bugs** | Medium | High | Comprehensive test suite for all PostgreSQL types. Fuzzing. |
| **Bun server availability** | High | Low | Connection retry logic. Graceful degradation. Health checks. |
| **Version compatibility** | Medium | Medium | Test against multiple database versions. CI matrix testing. |
| **Memory leaks** | High | Low | Long-running stress tests. Memory profiling. Reference counting checks. |
| **Breaking API changes** | Medium | Low | Semantic versioning. Deprecation warnings. Migration guides. |

---

## Open Questions

1. **Protocol Buffers vs MessagePack**: Should we implement both and benchmark?
   - **Answer**: Start with MessagePack (simpler). Benchmark in Phase 5.

2. **Prepared statement caching**: Should Python or Bun cache prepared statements?
   - **Answer**: Bun server caches (better memory management).

3. **Transaction isolation levels**: How to expose PostgreSQL/MySQL-specific isolation levels?
   - **Answer**: Driver-specific methods + common interface.

4. **Result set format**: Tuples (DB-API 2.0) vs Records (asyncpg)?
   - **Answer**: Support both. Tuples for sync, Records for async.

5. **Connection pool per-pool config**: Should each pool have different min/max sizes?
   - **Answer**: Yes, pass pool config to create_pool().

---

## Future Enhancements

### Post-MVP Features

1. **Additional Drivers**:
   - SQL Server driver
   - Oracle driver
   - MongoDB driver (NoSQL)
   - Redis driver (caching)

2. **Advanced Features**:
   - Query result caching (Redis-backed)
   - Read replicas support (automatic routing)
   - Connection pool metrics and monitoring
   - Distributed tracing integration

3. **Performance Optimizations**:
   - Protocol Buffers serialization option
   - Zero-copy data transfer (shared memory)
   - Batch query optimization
   - Prepared statement pooling

4. **Developer Experience**:
   - Type stubs generation from database schema
   - Query builder integration
   - ORM adapters (SQLAlchemy, Tortoise ORM)
   - CLI tools for connection testing

5. **Cloud Integrations**:
   - Azure Blob Storage client
   - Cloud-specific optimizations (IAM roles, VPC endpoints)
   - Multi-region replication

---

## References

### Standards and Specifications

- [PEP 249: Python Database API Specification v2.0](https://peps.python.org/pep-0249/)
- [fsspec Documentation](https://filesystem-spec.readthedocs.io/)
- [asyncpg Documentation](https://github.com/magicstack/asyncpg)
- [aiomysql Documentation](https://aiomysql.readthedocs.io/)
- [aiosqlite Documentation](https://aiosqlite.omnilib.dev/)

### Implementation References

- [asyncpg Connection Pooling](https://github.com/magicstack/asyncpg/blob/master/docs/usage.rst)
- [fsspec AbstractFileSystem](https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/spec)
- [MessagePack Specification](https://msgpack.org/)
- [Bun SQL Documentation](https://bun.sh/docs/api/sql)
- [Bun File I/O](https://bun.sh/docs/api/file-io)
- [Bun S3 Client](https://bun.sh/docs/api/s3)
- [Bun Redis Client](https://bun.sh/docs/api/redis)

### Research Sources

- [aiobotocore Documentation](https://aiobotocore.readthedocs.io/)
- [S3 Streaming Best Practices](https://github.com/aio-libs/aiobotocore)

---

## Appendix

### File Structure

```
src/py/vertebra/
├── drivers/
│   ├── __init__.py
│   ├── base.py              # Base driver interface
│   ├── postgres.py          # PostgreSQL driver
│   ├── mysql.py             # MySQL driver
│   ├── sqlite.py            # SQLite driver
│   ├── exceptions.py        # DB-API 2.0 exceptions
│   └── types.py             # Type conversion utilities
├── fs/
│   ├── __init__.py
│   ├── base.py              # VertebraFileSystem base
│   ├── local.py             # Local filesystem
│   ├── s3.py                # S3 filesystem
│   └── gcs.py               # GCS filesystem
├── storage/
│   ├── __init__.py
│   ├── s3.py                # S3Client
│   └── gcs.py               # GCSClient
└── _internal/
    ├── ipc.py               # IPC protocol
    ├── serialization.py     # MessagePack utils
    └── pool.py              # Pool management

src/ts/src/
├── drivers/
│   ├── postgres.ts          # PostgreSQL pool
│   ├── mysql.ts             # MySQL pool
│   └── sqlite.ts            # SQLite handler
├── fs/
│   ├── local.ts             # Local filesystem
│   ├── s3.ts                # S3 operations
│   └── gcs.ts               # GCS operations
└── protocol/
    ├── messages.ts          # Message types
    └── serialization.ts     # MessagePack

tests/
├── unit/
├── integration/
├── compliance/
├── performance/
├── stress/
└── docker-compose.yml

docs/
├── drivers/
├── filesystem/
├── storage/
├── guides/
└── api/

examples/
├── basic_postgres.py
├── basic_mysql.py
├── basic_sqlite.py
├── sqlspec_integration.py
├── filesystem_usage.py
├── s3_streaming.py
└── gcs_streaming.py
```

### Glossary

- **DB-API 2.0**: Python Database API Specification (PEP 249)
- **fsspec**: Filesystem Spec - Pythonic filesystem interface
- **IPC**: Inter-Process Communication
- **MessagePack**: Efficient binary serialization format
- **GIL**: Global Interpreter Lock (Python concurrency limitation)
- **DSN**: Data Source Name (connection string)
- **ORM**: Object-Relational Mapping
- **QPS**: Queries Per Second
- **COPY**: PostgreSQL bulk data transfer protocol

---

**Document Version**: 1.0
**Last Updated**: 2026-01-17
**Next Review**: Implementation kickoff
