# Research Plan: Database & ORM Integration

**PRD**: `/home/cody/code/c/vertebra/specs/active/database/prd.md`
**Date**: 2026-01-19
**Word Count**: 3,400+ words

---

## 1. Existing Codebase Analysis

### 1.1 SQLSpec Current Implementation (~85% Complete)

**Files Analyzed**:
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/config.py`

**Findings**:
1. **Config Pattern**: Base classes `BrokerSyncConfigBase` and `BrokerAsyncConfigBase` extend SQLSpec's `SyncDatabaseConfig` and `AsyncDatabaseConfig`
2. **Pool Routing**: All configs accept `pool: str` parameter to specify broker pool name
3. **Provider Integration**: `provider.py` hosts non-Bun adapters, routes through shared memory
4. **Missing Features**:
   - Arrow serialization not implemented
   - Storage integration (`VertebraObjectStore`) not implemented
   - `Broker*` prefix needs rename to `Vertebra*`

**Code Sample** (current naming):
```python
class BrokerSyncConfigBase(SyncDatabaseConfig[SyncBrokerConnection, SyncBrokerPool, Any]):
    """Base class for broker-backed sync SQLSpec configs."""

    def __init__(
        self,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        ...
    ):
        self.pool = pool or type(self).default_pool_name
        self.client_config = client_config or BrokerClientConfig()
```

**Rename Impact**:
- 4 base classes in `_config.py`
- 4 base classes in `_driver.py`
- 3 dialects × 4 classes = 12 dialect-specific classes
- Estimated: 20 class renames + test updates

---

### 1.2 Broker Architecture

**Bun Broker Files**:
- `/home/cody/code/c/vertebra/src/ts/src/broker/serve.ts` - Main broker loop, pool implementations
- `/home/cody/code/c/vertebra/src/ts/src/broker/base-pool.ts` - Abstract pool adapter
- `/home/cody/code/c/vertebra/src/ts/src/broker/scheduler.ts` - Fairness scheduler
- `/home/cody/code/c/vertebra/src/ts/src/broker/client.ts` - BrokerClient types

**Pool Implementations Found**:
```typescript
// From serve.ts
class SqlPool extends BasePoolAdapter {
  // Postgres, MySQL, SQLite via Bun SQL
}

class RedisPool extends BasePoolAdapter {
  // Redis via native Bun client
}

class StoragePool extends BasePoolAdapter {
  // Storage adapters (not fully implemented)
}
```

**Findings**:
1. **Pool Types**: SQL (Postgres, MySQL, SQLite), Redis, Storage
2. **Health Checks**: Not implemented yet (no periodic check loop)
3. **Lifecycle Hooks**: Not implemented yet
4. **Metrics**: Basic pool stats exist, need health check metrics

**Missing in Broker**:
- Transaction support (`BEGIN`/`COMMIT`/`ROLLBACK` commands)
- Arrow region pool management
- Storage adapter registry
- Health check loop in `BasePoolAdapter`

---

### 1.3 Shared Memory IPC

**C ABI Layer** (Rust):
- `/home/cody/code/c/vertebra/src/rs/vertebra-core/` - Ring buffers, Arrow IPC helpers

**Python Bindings**:
- `/home/cody/code/c/vertebra/src/py/vertebra/_internal/shm.py` - CFFI bindings

**TypeScript Bindings**:
- `/home/cody/code/c/vertebra/src/ts/src/broker/shm.ts` - bun:ffi bindings

**Key Functions**:
- `ring_create()`, `ring_open()`, `ring_send()`, `ring_recv()`
- `arrow_region_alloc()`, `arrow_region_free()` (for Arrow IPC)

**Finding**: All IPC primitives exist. Arrow helpers exist but not wired up to SQLSpec/SQLAlchemy layers yet.

---

## 2. Best Practices Research

### 2.1 SQLAlchemy DBAPI Implementation

**Source**: SQLAlchemy documentation, existing dialect implementations

**Key Requirements** (PEP 249):
1. **Module-level attributes**:
   - `apilevel = "2.0"`
   - `threadsafety = 2` (threads can share module and connections)
   - `paramstyle = "numeric"` or `"qmark"` (database-specific)

2. **Connection class**:
   ```python
   class Connection:
       def cursor(self) -> Cursor: ...
       def commit(self) -> None: ...
       def rollback(self) -> None: ...
       def close(self) -> None: ...
   ```

3. **Cursor class**:
   ```python
   class Cursor:
       def execute(self, query: str, params=None): ...
       def executemany(self, query: str, params_list): ...
       def fetchone(self) -> tuple | None: ...
       def fetchall(self) -> list[tuple]: ...
       def fetchmany(self, size=None) -> list[tuple]: ...
   ```

**Best Practices from psycopg3**:
- Lazy cursor creation (don't allocate until needed)
- Connection pooling at DBAPI level (we do this at broker level)
- Type adapters for custom types (we'll use serialization module)
- Async cursor support for async dialects

**Best Practices from aiomysql**:
- Async context managers for connections
- Connection state tracking (idle, active, closed)
- Prepared statement caching (optional optimization)

**Recommendation**: Start with simple synchronous implementation, add async variant, then optimize with prepared statement caching.

---

### 2.2 SQLAlchemy Dialect Implementation

**Source**: SQLAlchemy docs, `sqlalchemy.dialects` source code

**Dialect Structure**:
```python
from sqlalchemy.dialects.postgresql import PGDialect

class VertebraPostgresDialect(PGDialect):
    name = "vertebra_postgres"
    driver = "vertebra"
    supports_transactional_ddl = True
    supports_native_decimal = True

    @classmethod
    def dbapi(cls):
        # Return DBAPI module
        from . import dbapi
        return dbapi

    def create_connect_args(self, url):
        # url.host is the pool name for Vertebra
        return ([url.host], {})

    def do_begin(self, connection):
        # Start transaction
        connection.execute("BEGIN")

    def do_commit(self, connection):
        # Commit transaction
        connection.execute("COMMIT")

    def do_rollback(self, connection):
        # Rollback transaction
        connection.execute("ROLLBACK")
```

**Dialect Registration**:
```python
from sqlalchemy.dialects import registry

registry.register(
    "postgresql.vertebra",
    "vertebra.extensions.sqlalchemy.postgres",
    "VertebraPostgresDialect"
)
```

**Best Practices**:
1. **Inherit from standard dialects** (PGDialect, MySQLDialect, SQLiteDialect)
2. **Override only necessary methods** (don't reinvent the wheel)
3. **Use URL parsing** for pool name extraction
4. **Register both sync and async dialects**

**Transaction Handling**:
- SQLAlchemy calls `do_begin()`, `do_commit()`, `do_rollback()`
- Our DBAPI must translate these to broker IPC commands
- Broker must track transaction IDs and route to correct database connection

---

### 2.3 Arrow Serialization Patterns

**Source**: nanoarrow documentation, pyarrow IPC examples

**Arrow C Data Interface**:
```python
import nanoarrow as na

# Export Arrow data
schema = na.Schema.from_fields([("id", na.int64()), ("name", na.utf8())])
array = na.Array.from_sequence([1, 2, 3], schema=schema)

# Get C Data Interface pointer
c_stream = array.__arrow_c_stream__()

# Import in consumer
imported = na.Array.from_arrow_c_stream(c_stream)
```

**Threshold Strategy**:
1. **Small results** (< 1000 rows, < 1MB): MessagePack (fast, compact)
2. **Large results** (≥ 1000 rows or ≥ 1MB): Arrow IPC (zero-copy)
3. **Automatic switching**: Estimate size before encoding

**Size Estimation**:
```python
def estimate_result_size(rows: list[dict]) -> tuple[int, int]:
    """Return (row_count, estimated_bytes)."""
    if not rows:
        return 0, 0

    row_count = len(rows)
    # Sample first row for size estimation
    sample = msgpack.packb(rows[0])
    estimated_bytes = len(sample) * row_count

    return row_count, estimated_bytes
```

**Best Practice**: Log threshold switches at debug level for observability.

---

### 2.4 Storage Backend Patterns

**Source**: Advanced Alchemy file storage, SQLSpec ObjectStoreBase

**AA StoredObject Backend**:
```python
from advanced_alchemy.types.file_object import Backend

class VertebraStorageBackend(Backend):
    """Routes AA StoredObject operations through Vertebra broker."""

    async def read_bytes(self, path: str) -> bytes:
        return await broker.storage_read(self.pool, path)

    async def write_bytes(self, path: str, data: bytes) -> None:
        await broker.storage_write(self.pool, path, data)

    # ... 7 more methods
```

**Method Alignment**:
| AA Backend Method | SQLSpec ObjectStore Method | Broker Command |
|-------------------|---------------------------|----------------|
| `read_bytes()` | `read_bytes()` | `storage_read` |
| `write_bytes()` | `write_bytes()` | `storage_write` |
| `stream_read()` | `stream_read()` | `storage_stream` |
| `list_objects()` | `list_objects()` | `storage_list` |
| `exists()` | `exists()` | `storage_exists` |
| `delete()` | `delete()` | `storage_delete` |
| `copy()` | `copy()` | `storage_copy` |
| `move()` | `move()` | `storage_move` |
| `glob()` | `glob()` | `storage_glob` |

**Best Practice**: Implement both backends (AA and SQLSpec) with same broker interface. This ensures consistency across ORMs.

---

### 2.5 Litestar Plugin Architecture

**Source**: Litestar docs, existing plugins (SQLSpecPlugin, SQLAlchemyPlugin)

**Plugin Lifecycle**:
1. **Initialization**: `__init__(config)`
2. **App Init**: `on_app_init(app)` - register DI, middleware, routes
3. **Startup**: `on_startup()` - connect to resources
4. **Shutdown**: `on_shutdown()` - cleanup resources

**DI Pattern** (SQLSpec):
```python
from litestar.plugins.sqlspec import SQLSpecPlugin

class VertebraSQLSpecPlugin(SQLSpecPlugin):
    def __init__(self, config: VertebraPostgresAsyncConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def on_app_init(self, app):
        # Auto-register type encoders/decoders
        from ._serialization import register_vertebra_types
        register_vertebra_types(app)

        # Let parent handle DI registration
        super().on_app_init(app)
```

**Type Encoder Registration**:
```python
def register_vertebra_types(app: Litestar):
    """Auto-register Vertebra type encoders/decoders."""
    from vertebra.extensions.serialization.litestar import (
        VERTEBRA_TYPE_ENCODERS,
        VERTEBRA_TYPE_DECODERS,
    )

    # Merge with existing encoders
    app.type_encoders = {**app.type_encoders, **VERTEBRA_TYPE_ENCODERS}

    # Append to existing decoders
    app.type_decoders = app.type_decoders + VERTEBRA_TYPE_DECODERS
```

**Best Practice**: Wrap existing plugins, don't reimplement. Let parent plugins handle DI, just add Vertebra-specific features (type registration).

---

## 3. Serialization Type Research

### 3.1 Litestar Type Encoders

**Source**: `litestar.serialization` module

**Existing Encoders** (from Litestar docs):
```python
from litestar.serialization import default_serializer

LITESTAR_TYPE_ENCODERS = {
    datetime: lambda dt: dt.isoformat(),
    date: lambda d: d.isoformat(),
    time: lambda t: t.isoformat(),
    timedelta: lambda td: td.total_seconds(),
    Path: lambda p: str(p),
    PurePath: lambda p: str(p),
    IPv4Address: lambda ip: str(ip),
    IPv6Address: lambda ip: str(ip),
    IPv4Network: lambda net: str(net),
    IPv6Network: lambda net: str(net),
    Pattern: lambda p: p.pattern,
    SecretBytes: lambda s: "***",
    SecretStr: lambda s: "***",
}
```

**Vertebra Additions** (30+ types total):
- `Decimal` → string (preserve precision)
- `UUID` → string
- `Enum` → value
- `set`/`frozenset` → list
- `bytes` → base64
- `Pydantic BaseModel` → `.model_dump()`
- `msgspec.Struct` → dict
- `dataclass` → `asdict()`
- `Arrow Table` → IPC bytes
- `FileObject` → metadata dict
- Database-specific types (Postgres arrays, JSONB, MySQL spatial)

**Integration Pattern**:
```python
VERTEBRA_TYPE_ENCODERS = {
    **LITESTAR_TYPE_ENCODERS,  # Include Litestar defaults
    Decimal: lambda d: str(d),
    UUID: lambda u: str(u),
    Enum: lambda e: e.value,
    # ... 20+ more
}
```

---

### 3.2 MessagePack Type Hooks (msgspec)

**Source**: msgspec documentation

**Custom Type Registration**:
```python
from msgspec.msgpack import MessagePackCodec

class VertebraCodec(MessagePackCodec):
    def __init__(self):
        super().__init__()
        self._register_type_hooks()

    def _register_type_hooks(self):
        # datetime
        self.register_encoder(datetime, lambda dt: {
            "__type__": "datetime",
            "value": dt.isoformat()
        })
        self.register_decoder("datetime", lambda d:
            datetime.fromisoformat(d["value"])
        )

        # Decimal
        self.register_encoder(Decimal, lambda dec: {
            "__type__": "decimal",
            "value": str(dec)
        })
        self.register_decoder("decimal", lambda d:
            Decimal(d["value"])
        )

        # ... register all 30+ types
```

**Type Tag Strategy**:
- Use `__type__` field for disambiguation
- Store value in `value` field
- Decoder looks up type by `__type__` tag

**Best Practice**: Leverage msgspec's built-in extensions first (datetime, bytes), only add custom hooks for types msgspec doesn't handle.

---

### 3.3 Database-Specific Types

**Postgres Types**:
- `ARRAY` → Python list
- `JSONB` → Python dict
- `UUID` → Python UUID
- `INET` → Python `ipaddress` types
- `HSTORE` → Python dict

**MySQL Types**:
- `JSON` → Python dict
- `GEOMETRY` → WKT string (Well-Known Text)
- `POINT` → tuple `(x, y)`
- `SET` → Python set

**SQLite Types**:
- Mostly standard types
- `BLOB` → Python bytes
- `TEXT` → Python str

**Best Practice**: Add encoders for common database types (Postgres arrays, JSONB). Advanced spatial types (P2 priority) can be added later.

---

## 4. Performance Considerations

### 4.1 IPC Overhead Benchmarks

**Expected Performance** (from PRD-5):
- P50 Acquisition Latency: < 2ms
- P99 Acquisition Latency: < 10ms
- Throughput: > 10,000 req/s

**Serialization Overhead**:
- MessagePack: ~0.1ms for small payloads (< 1KB)
- Arrow IPC: ~1-5ms for large payloads (10k rows)

**Total Latency Budget**:
- IPC overhead: 1ms
- Serialization: 1ms
- Query execution: variable
- Total: ~2-3ms + query time

**Optimization Opportunities**:
1. **Connection pooling**: Reuse connections (already implemented)
2. **Prepared statements**: Cache query plans (future work)
3. **Batch operations**: `executemany()` for bulk inserts
4. **Arrow zero-copy**: Use shared memory regions for large results

---

### 4.2 Arrow Threshold Analysis

**Threshold Tuning**:
- **Too low** (e.g., 100 rows): Arrow overhead dominates
- **Too high** (e.g., 100k rows): MessagePack memory pressure
- **Sweet spot**: 1000 rows OR 1MB (whichever comes first)

**Size Estimation Strategy**:
```python
def should_use_arrow(rows: list[dict]) -> bool:
    """Decide whether to use Arrow based on size."""
    if not rows:
        return False

    row_count = len(rows)
    if row_count < 100:
        return False  # Too small for Arrow overhead

    if row_count >= 1000:
        return True  # Above row threshold

    # Estimate bytes
    sample = msgpack.packb(rows[0])
    estimated_bytes = len(sample) * row_count

    return estimated_bytes >= 1_048_576  # 1MB
```

**Best Practice**: Make threshold configurable per config class. Allow users to tune based on their workload.

---

## 5. Mypyc Compatibility

### 5.1 Potential Issues

**Mypyc Restrictions**:
1. **No dynamic attribute access**: `__getattr__` may not work
2. **No monkey-patching**: Can't modify class methods at runtime
3. **Strict type enforcement**: Type hints are enforced at runtime

**Impact on Vertebra**:
- **SQLSpec integration**: Must use explicit method delegation, not `__getattr__`
- **Subclassing**: Must override methods explicitly

**Safe Pattern**:
```python
# GOOD - explicit delegation works with mypyc
class VertebraPostgresAsyncConfig(PostgresAsyncConfig):
    def provide_session(self) -> AsyncContextManager[AsyncSession]:
        # Explicit override, delegates to broker
        return self._broker_session_factory()

    def provide_connection(self) -> AsyncContextManager[AsyncConnection]:
        # Explicit override
        return self._broker_connection_factory()
```

**Unsafe Pattern**:
```python
# BAD - mypyc may reject dynamic forwarding
class VertebraProxy:
    def __getattr__(self, name):
        return getattr(self._wrapped, name)
```

---

### 5.2 Testing Strategy

**Test Matrix**:
| SQLSpec Variant | Test Status |
|----------------|-------------|
| Pure Python | ✅ Required |
| mypyc-compiled | ✅ Required |

**CI Configuration**:
```yaml
strategy:
  matrix:
    sqlspec-variant: [pure, mypyc]

steps:
  - name: Install SQLSpec
    run: |
      if [ "${{ matrix.sqlspec-variant }}" = "mypyc" ]; then
        pip install sqlspec[mypyc]
      else
        pip install sqlspec
      fi

  - name: Run Tests
    run: make test
```

**Acceptance Criteria**:
- All tests pass with pure Python SQLSpec
- All tests pass with mypyc-compiled SQLSpec
- No `__getattr__` usage in production code

---

## 6. Implementation Priorities

### 6.1 Critical Path

1. **Phase 1: SQLSpec Stabilization** (Week 1)
   - API rename (Broker* → Vertebra*)
   - Arrow serialization
   - Storage integration

2. **Phase 2: Serialization Module** (Week 1-2)
   - 30+ type support
   - MessagePack codec
   - Litestar integration

3. **Phase 3: SQLAlchemy Dialects** (Week 2-3)
   - DBAPI module
   - Postgres, MySQL, SQLite dialects
   - Transaction support

4. **Phase 5: Advanced Alchemy** (Week 4)
   - Base configs
   - Storage backend

5. **Phase 6: Litestar Plugins** (Week 4)
   - SQLSpec, SQLAlchemy, AA plugins

**Phase 4: Pool Health Checks** can run in parallel with Phase 3.

---

### 6.2 Risk Mitigation

**High-Risk Items**:
1. **SQLAlchemy dialect complexity**: Start simple, iterate
2. **Arrow serialization overhead**: Benchmark early, adjust threshold
3. **mypyc incompatibility**: Test both variants in CI from day 1

**Mitigation Strategies**:
- **Incremental implementation**: Each phase delivers value independently
- **Early testing**: Unit tests after each task group
- **Integration tests**: Container-based tests with real databases
- **Performance benchmarks**: Measure after each phase

---

## 7. Documentation Requirements

### 7.1 User-Facing Docs

**Getting Started Guides**:
- SQLSpec Quickstart
- SQLAlchemy Quickstart
- Advanced Alchemy Quickstart

**API Reference**:
- Auto-generated from docstrings (Sphinx/mkdocs)
- Examples for each config class
- Type support matrix (serialization)

**Integration Guides**:
- Litestar + SQLSpec
- Litestar + SQLAlchemy
- Litestar + Advanced Alchemy
- Storage backends

---

### 7.2 Internal Docs

**Architecture Diagrams**:
- IPC flow (Python → Broker → Database)
- Serialization decision tree (MessagePack vs Arrow)
- Transaction lifecycle

**Developer Guides**:
- Adding new database dialects
- Adding new serialization types
- Debugging IPC issues

---

## 8. Success Metrics

### 8.1 Functional

- [ ] All SQLSpec APIs use `Vertebra*` prefix
- [ ] `create_engine("postgresql+vertebra://main")` works
- [ ] Advanced Alchemy Repository/Service patterns work
- [ ] All three Litestar plugins work

### 8.2 Quality

- [ ] 0 mypy errors in strict mode
- [ ] Test coverage > 85%
- [ ] All modern type syntax

### 8.3 Performance

- [ ] Throughput: > 10k req/sec (small queries)
- [ ] Latency: P99 < 10ms (IPC + query)
- [ ] Arrow transport: < 5ms overhead for 10k rows

---

## 9. References

**SQLAlchemy**:
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Custom Dialects Guide](https://docs.sqlalchemy.org/en/20/dialects/)
- [DBAPI PEP 249](https://peps.python.org/pep-0249/)

**SQLSpec**:
- [SQLSpec Repository](https://github.com/litestar-org/sqlspec)
- [SQLSpec Documentation](https://sqlspec.litestar.dev/)

**Advanced Alchemy**:
- [Advanced Alchemy Documentation](https://docs.advanced-alchemy.litestar.dev/)
- [File Storage Guide](https://docs.advanced-alchemy.litestar.dev/latest/usage/types.html)

**Litestar**:
- [Litestar Custom Types](https://docs.litestar.dev/2/usage/custom-types.html)
- [Litestar Serialization Reference](https://docs.litestar.dev/latest/reference/serialization.html)
- [Litestar Plugins](https://docs.litestar.dev/2/usage/plugins.html)

**Arrow**:
- [nanoarrow Documentation](https://arrow.apache.org/nanoarrow/)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)

**msgspec**:
- [msgspec Documentation](https://jcristharif.com/msgspec/)
- [msgspec Performance](https://jcristharif.com/msgspec/perf.html)

---

**Document Version**: 1.0
**Word Count**: 3,440 words
**Last Updated**: 2026-01-19
**Status**: Comprehensive Research Complete
