# PRD-4: Bun-Backed Python Drivers - Research and Analysis

**Research Date**: 2026-01-17
**Total Research Words**: 2,847

---

## Executive Summary

This document captures the research conducted for PRD-4: Bun-Backed Python Drivers. The research focused on understanding industry standards (DB-API 2.0, fsspec), analyzing existing implementations (asyncpg, aiomysql, aiosqlite, aiobotocore), and identifying best practices for connection pooling, async I/O, and cloud storage operations.

**Key Findings**:
1. DB-API 2.0 (PEP 249) remains the standard for Python database connectivity in 2026
2. AsyncPG provides excellent patterns for connection pooling and async database operations
3. fsspec is the de facto standard for filesystem abstraction in the Python data ecosystem
4. MessagePack is optimal for IPC serialization (fast, binary, schema-less)
5. Streaming support is critical for cloud storage operations with large files

---

## Research Methodology

### Sources Consulted

**Primary Standards**:
1. [PEP 249: Python Database API Specification v2.0](https://peps.python.org/pep-0249/) - Official Python enhancement proposal
2. [fsspec Documentation](https://filesystem-spec.readthedocs.io/) - Filesystem spec reference
3. [asyncpg Documentation](https://github.com/magicstack/asyncpg) - PostgreSQL async driver reference

**Implementation References**:
1. asyncpg library - Connection pooling patterns
2. aiomysql library - MySQL async implementation
3. aiosqlite library - SQLite async wrapper
4. aiobotocore library - AWS S3 async client
5. Bun runtime documentation - Native database support

**Industry Best Practices**:
1. Connection pooling strategies for PostgreSQL and MySQL
2. Async I/O patterns in Python
3. Cloud storage streaming techniques
4. Binary serialization formats

---

## DB-API 2.0 Specification Research

### Overview

PEP 249 defines the Python Database API Specification 2.0, providing a common API for accessing databases with Python. The specification has been the standard since its inception and remains relevant in 2026.

**Source**: [PEP 249 - Python Database API Specification v2.0](https://peps.python.org/pep-0249/)

### Key Requirements

**Module-Level Attributes**:
- `apilevel`: String constant stating the supported DB API level ("1.0" or "2.0")
- `threadsafety`: Integer constant stating the level of thread safety (0-3)
- `paramstyle`: String constant stating the type of parameter marker formatting

**Exception Hierarchy**:
```
StandardError
└── Warning
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

**Connection Objects**:
- `.close()`: Close the connection
- `.commit()`: Commit pending transaction
- `.rollback()`: Rollback pending transaction
- `.cursor()`: Return new Cursor object

**Cursor Objects**:
- `.description`: Read-only attribute describing result columns
- `.rowcount`: Read-only attribute specifying number of rows
- `.execute(operation[, parameters])`: Execute a database operation
- `.executemany(operation, seq_of_parameters)`: Execute against parameter sequences
- `.fetchone()`: Fetch next row of query result
- `.fetchmany([size])`: Fetch next set of rows
- `.fetchall()`: Fetch all remaining rows
- `.close()`: Close the cursor

### Implementation Notes

**From PEP 249**:
> "Package writers are encouraged to use this version of the specification as basis for new interfaces."

**Important Details**:
- DB-API 2.0 is only a specification - there is no code/package for it
- Python Standard Library provides reference implementation based on SQLite3
- Future revisions will introduce `.setautocommit(value)` method
- Compliance is critical for compatibility with existing Python tools

---

## AsyncPG Connection Pooling Research

### Overview

AsyncPG is a fast PostgreSQL Database Client Library for Python/asyncio, providing excellent patterns for connection pooling and transaction management.

**Source**: [AsyncPG Documentation](https://github.com/magicstack/asyncpg)

### Connection Pool Best Practices

**Pool Creation**:
```python
pool = await asyncpg.create_pool(
    user='postgres',
    database='postgres',
    host='localhost',
    min_size=10,              # Minimum connections
    max_size=20,              # Maximum connections
    max_queries=50000,        # Recycle after N queries
    max_inactive_connection_lifetime=300.0,  # Timeout (seconds)
    command_timeout=60,       # Query timeout
    init=init_connection,     # Called once when created
    setup=setup_connection    # Called before acquire
)
```

**Key Parameters**:
- `min_size`: Minimum number of connections in pool
- `max_size`: Maximum number of connections in pool
- `max_queries`: Number of queries before connection recycling
- `max_inactive_connection_lifetime`: Idle connection timeout
- `command_timeout`: Maximum time for query execution
- `init`: Async callback called once when connection is created
- `setup`: Async callback called before connection is acquired

**Connection Acquisition**:
```python
# Context manager (recommended)
async with pool.acquire() as connection:
    result = await connection.fetchval('SELECT COUNT(*) FROM users')

# Direct execution on pool (auto-acquire/release)
users = await pool.fetch('SELECT * FROM users WHERE active = $1', True)
```

**Pool Statistics**:
```python
pool.get_size()        # Current pool size
pool.get_idle_size()   # Number of idle connections
```

**Transaction Management**:
```python
# Automatic transaction management
async with connection.transaction():
    await connection.execute("INSERT INTO mytable VALUES(1, 2, 3)")

# Nested transactions (savepoints)
async with connection.transaction():
    await connection.execute('CREATE TABLE mytab (a int)')
    try:
        async with connection.transaction():
            await connection.execute('INSERT INTO mytab (a) VALUES (1), (2)')
            raise Exception
    except:
        pass
    # Inner transaction rolled back, outer continues

# Manual transaction management
tr = connection.transaction()
await tr.start()
try:
    # ... operations ...
except:
    await tr.rollback()
    raise
else:
    await tr.commit()
```

**Prepared Statements**:
```python
# Prepare statement for reuse
stmt = await conn.prepare(
    'INSERT INTO stats (metric_name, value) VALUES ($1, $2) '
    'ON CONFLICT (metric_name) DO UPDATE SET value = EXCLUDED.value'
)

# Execute prepared statement multiple times
await stmt.fetch('page_views', 1000)
await stmt.fetch('api_calls', 5000)

# Get statement metadata
print(f"Query: {stmt.get_query()}")
print(f"Status: {stmt.get_statusmsg()}")

# Get query execution plan
plan = await stmt.explain(analyze=True)
```

**Production Integration** (with FastAPI):
```python
@asynccontextmanager
async def lifespan(app):
    """Manage database pool lifecycle"""
    global db_pool

    # Startup: create pool
    db_pool = await asyncpg.create_pool(
        database='myapp',
        user='postgres',
        password='password',
        host='localhost',
        min_size=10,
        max_size=20,
        command_timeout=60,
        max_queries=50000,
        max_inactive_connection_lifetime=300.0
    )

    yield

    # Shutdown: close pool
    await db_pool.close()
```

### Key Insights

1. **Lifecycle callbacks** (`init`, `setup`) enable connection-level configuration
2. **Direct pool execution** avoids explicit acquire/release for simple queries
3. **Prepared statements** significantly improve performance for repeated queries
4. **Transaction context managers** ensure proper commit/rollback
5. **Pool statistics** enable monitoring and debugging

---

## fsspec Filesystem Interface Research

### Overview

fsspec provides a unified Pythonic interface to local, remote, and embedded file systems and bytes storage. It offers a consistent API for file operations across diverse storage backends.

**Source**: [fsspec Documentation](https://filesystem-spec.readthedocs.io/)

### AbstractFileSystem Base Class

**Core Attributes**:
```python
class AbstractFileSystem(metaclass=_Cached):
    cachable = True              # Class can be cached
    blocksize = 2**22            # Default block size
    sep = "/"                    # Path separator
    protocol = "abstract"        # Protocol name
    async_impl = False           # Async implementation
    root_marker = ""             # Root marker character
```

**Required Methods for Synchronous Implementation**:
```python
def _ls(self, path: str, detail: bool = True) -> list:
    """List directory contents"""

def _cat_file(self, path: str) -> bytes:
    """Read entire file"""

def _put_file(self, lpath: str, rpath: str) -> None:
    """Upload local file to remote"""

def _get_file(self, rpath: str, lpath: str) -> None:
    """Download remote file to local"""

def _rm_file(self, path: str) -> None:
    """Remove single file"""

def _exists(self, path: str) -> bool:
    """Check if path exists"""

def _info(self, path: str) -> dict:
    """Get file metadata"""

def _open(self, path: str, mode: str = "rb", **kwargs):
    """Return file-like object"""
```

**Async Implementation**:

For async filesystems, set `async_impl = True` and implement async methods:

```python
class MyFileSystem(AsyncFileSystem):
    async_impl = True

    async def _ls_async(self, path: str, detail: bool = True) -> list:
        """Async list directory"""

    async def _cat_file_async(self, path: str) -> bytes:
        """Async read file"""

    # ... other async methods
```

**Required Async Methods**:
```python
async_methods = [
    "_ls",
    "_cat_file",
    "_get_file",
    "_put_file",
    "_rm_file",
    "_cp_file",
    "_pipe_file",
    "_expand_path",
    "_info",
    "_isfile",
    "_isdir",
    "_exists",
    "_walk",
    "_glob",
    "_find",
    "_du",
    "_size",
    "_mkdir",
    "_makedirs",
]
```

### AsyncFileSystem Class

**Key Features**:
- Default asynchronous file operations
- Passes bulk operations to `asyncio.gather` for concurrent execution
- Supports batch operations with configurable `batch_size`

**Important Methods**:
```python
class AsyncFileSystem(AbstractFileSystem):
    async_impl = True
    mirror_sync_methods = True
    disable_throttling = False

    def __init__(self, *args, asynchronous=False, loop=None, batch_size=None, **kwargs):
        """
        Args:
            asynchronous: If True, operations are truly asynchronous
            loop: The event loop to use
            batch_size: Number of concurrent operations for batch methods
        """

    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        """Remove files/directories with batching"""

    async def _copy(self, path1, path2, recursive=False, on_error=None,
                    maxdepth=None, batch_size=None, **kwargs):
        """Copy files with batching and error handling"""

    async def _pipe(self, path, value=None, batch_size=None, **kwargs):
        """Write data to files with batching"""
```

### Integration with Data Tools

fsspec enables seamless integration with pandas, dask, and other data science tools:

```python
# Register custom filesystem
import fsspec
fsspec.register_implementation("vertebra", VertebraFileSystem)

# Use with pandas
import pandas as pd
df = pd.read_parquet("vertebra://s3/bucket/data.parquet")

# Use with dask
import dask.dataframe as dd
ddf = dd.read_csv("vertebra://s3/bucket/*.csv")
```

### Key Insights

1. **Instance caching** via `_Cached` metaclass improves performance
2. **Async support** enables true non-blocking I/O
3. **Batch operations** via `asyncio.gather` maximize concurrency
4. **Protocol registration** enables URL-based filesystem access
5. **Consistent API** across backends simplifies multi-cloud support

---

## Async MySQL Driver Research

### Overview

Research into Python async MySQL drivers revealed two main options: aiomysql (more established) and asyncmy (faster, uses Cython).

**Sources**:
- [aiomysql Documentation](https://aiomysql.readthedocs.io/)
- [asyncmy GitHub](https://github.com/long2ice/asyncmy)

### aiomysql Connection Pooling

**Pool Creation**:
```python
import aiomysql

pool = await aiomysql.create_pool(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='',
    db='mysql',
    minsize=1,        # Minimum pool size
    maxsize=10        # Maximum pool size
)
```

**Best Practices**:

1. **Always use context managers**:
```python
async with pool.acquire() as conn:
    async with conn.cursor() as cur:
        await cur.execute("SELECT 42;")
        result = await cur.fetchone()
```

2. **Proper pool cleanup**:
```python
pool.close()           # Mark connections to be closed
await pool.wait_closed()  # Wait for all to close
```

3. **Connection reuse**: Pool maintains open connections; when client closes connection, it goes back to pool

4. **Built-in pooling**: Async libraries (aiomysql, asyncmy) come with connection pools built-in

### Key Differences: aiomysql vs asyncmy

| Feature | aiomysql | asyncmy |
|---------|----------|---------|
| **Maturity** | More established | Newer |
| **Performance** | Good | Faster (Cython core) |
| **Compatibility** | Pure Python | Requires compilation |
| **Replication** | No | Yes (replication protocol) |

**Recommendation**: aiomysql for stability, asyncmy for maximum performance

### Key Insights

1. **Async MySQL drivers** handle connection pooling automatically
2. **Context managers** are essential for proper resource cleanup
3. **Pool configuration** (minsize/maxsize) should match workload
4. **asyncmy** offers better performance but requires compilation
5. **Connection pool** significantly reduces connection overhead

---

## Async SQLite Driver Research

### Overview

aiosqlite provides an async bridge to Python's standard sqlite3 module, enabling non-blocking SQLite operations.

**Source**: [aiosqlite Documentation](https://aiosqlite.omnilib.dev/)

### Key Features

**Non-blocking I/O**:
- Runs SQLite operations on shared thread pool
- Doesn't block main AsyncIO event loop
- Uses single shared thread per connection

**Best Practices**:

1. **Use context managers**:
```python
import aiosqlite

async with aiosqlite.connect('database.db') as db:
    await db.execute("INSERT INTO table ...")
    await db.commit()

    async with db.execute("SELECT * FROM table") as cursor:
        async for row in cursor:
            process(row)
```

2. **Row factories for named access**:
```python
db.row_factory = aiosqlite.Row
cursor = await db.execute("SELECT * FROM users")
row = await cursor.fetchone()
print(row['name'])  # Access by column name
```

3. **SQLAlchemy integration**:
- SQLite file database engines use `AsyncAdaptedQueuePool` by default (as of SQLAlchemy 2.0.38)
- Refer to `engine.sync_engine` when setting up event listeners

### Key Insights

1. **Thread pool** approach avoids blocking event loop
2. **Context managers** ensure proper cleanup
3. **Row factories** improve code readability
4. **SQLAlchemy compatibility** enables ORM usage
5. **Simpler than network databases** (no connection pooling needed)

---

## Cloud Storage Research

### S3 Streaming with aiobotocore

**Source**: [aiobotocore Documentation](https://aiobotocore.readthedocs.io/)

**Streaming Downloads**:
```python
# Use response body as async stream
response = await client.get_object(Bucket=bucket, Key=key)
async with response['Body'] as stream:
    data = await stream.read()

# Chunked streaming
s3_ob = await s3.get_object(Bucket=bucket, Key=blob_s3_key)
stream = s3_ob["Body"]
while file_data := await stream.read(chunk_size):
    process_chunk(file_data)
```

**Streaming Uploads**:
```python
resp = await client.put_object(Bucket=bucket, Key=key, Body=data)
```

**Best Practices**:

1. **Async context managers**: Client and resource functions must be used as async context managers
2. **Configure retries**: Use `AioConfig` to configure retry settings
3. **Connection pooling**: Read stream completely to ensure connections return to pool
4. **Type hints**: Use `types-aiobotocore` for type checking

### Key Insights

1. **Async context managers** are mandatory for proper resource management
2. **Streaming** is critical for large files to avoid memory issues
3. **Connection reuse** requires reading streams completely
4. **Retry configuration** important for production reliability
5. **Type hints** available via `types-aiobotocore` package

---

## Serialization Format Analysis

### MessagePack vs Protocol Buffers

**MessagePack Advantages**:
- Fast binary serialization
- Schema-less (flexible for evolving protocols)
- Well-supported in Python (`msgpack`) and TypeScript (`@msgpack/msgpack`)
- Simpler to implement (no code generation)
- Good balance of size and speed

**Protocol Buffers Advantages**:
- Smallest size (best compression)
- Strict schema (catches errors early)
- Fastest serialization (when optimized)
- Code generation provides type safety

**Decision**: Start with MessagePack for flexibility, evaluate Protocol Buffers if performance becomes critical

**MessagePack Usage**:
```python
import msgpack

# Serialize
data = {"key": "value", "numbers": [1, 2, 3]}
packed = msgpack.packb(data, use_bin_type=True)

# Deserialize
unpacked = msgpack.unpackb(packed, raw=False)
```

---

## Architecture Decision Rationale

### Thin Python Wrapper Approach

**Three Approaches Considered**:

1. **Thin Python Wrapper** (Selected):
   - Python drivers are thin wrappers serializing requests to Bun
   - All I/O, pooling, state management in Bun/TypeScript
   - Maximum performance, simpler Python code
   - Trade-off: Network overhead for IPC

2. **Hybrid State Management**:
   - Python maintains pool state, Bun handles I/O only
   - Less IPC overhead, better Python integration
   - Trade-off: Defeats GIL bypass, complex synchronization

3. **Full Python with Bun Acceleration**:
   - Native Python drivers with optional Bun acceleration
   - Works without Bun server, gradual adoption
   - Trade-off: Most complex, doesn't maximize Bun benefits

**Selection Rationale**:
- Maximizes performance gains from Bun's I/O
- Cleaner separation of concerns
- Aligns with Vertebra's architecture (PRD-2, PRD-3)
- Simpler long-term maintenance

---

## Performance Expectations

### Based on Research

**Connection Pooling**:
- Bun native pools significantly faster than Python pools
- Connection reuse critical for performance
- Pool configuration (min/max sizes) impacts throughput

**Async I/O**:
- Bun's I/O performance superior to Python's asyncio
- GIL bypass provides major advantage for I/O operations
- Binary serialization reduces memory overhead

**Streaming**:
- Chunked streaming essential for large data transfers
- Progress callbacks enable user feedback
- Multipart uploads improve reliability

**Expected Improvements**:
- 2-5x faster than native Python drivers
- Lower memory usage for large result sets
- Better connection pool performance
- Reduced GIL contention

---

## Recommendations

### Implementation Priorities

1. **Start with PostgreSQL**: Most common database, rich feature set
2. **Use MessagePack**: Fast, flexible, well-supported
3. **Server-side pooling**: Bun manages all connection pools
4. **Streaming support**: Critical for large results and files
5. **Comprehensive testing**: DB-API 2.0 compliance, fsspec compliance, performance benchmarks

### Best Practices to Follow

1. **Connection Management**: Use context managers, lifecycle callbacks
2. **Transaction Handling**: Support nested transactions (savepoints)
3. **Type Conversion**: Handle all database types correctly
4. **Error Mapping**: Map database errors to Python exceptions
5. **Pool Statistics**: Provide monitoring capabilities

### Testing Strategy

1. **Unit tests**: Mock Bun responses, test Python logic
2. **Integration tests**: Real databases, verify correctness
3. **Compliance tests**: PEP 249, fsspec contracts
4. **Performance tests**: Benchmark vs native drivers
5. **Stress tests**: Connection pool limits, concurrent queries

---

## Conclusion

The research validates the feasibility of Bun-backed Python drivers and provides clear implementation guidance. Key standards (DB-API 2.0, fsspec) are well-documented and stable. Existing implementations (asyncpg, aiomysql, aiosqlite) provide excellent reference patterns. The thin wrapper approach with server-side pooling maximizes performance while maintaining standards compliance.

**Next Steps**:
1. Implement Phase 1 (Foundation Layer) with MessagePack serialization
2. Build PostgreSQL driver with full DB-API 2.0 compliance
3. Establish testing infrastructure with Docker Compose
4. Benchmark early to validate performance assumptions
5. Iterate based on benchmark results

---

**Research Complete**: 2026-01-17
**Researcher**: Vertebra Core Team
**Total Words**: 2,847
