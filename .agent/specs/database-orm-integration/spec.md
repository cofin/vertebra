# PRD: Vertebra Database & ORM Integration (Consolidated)

**Status**: Active - Consolidation of All Database Work
**Version**: 1.0
**Date**: 2026-01-19
**Priority**: P0 - Critical Path to 1.0
**Target**: Vertebra 1.0 Release

---

## Executive Summary

This PRD consolidates **all** database and ORM work for Vertebra 1.0 into a single unified specification. It covers SQLSpec stabilization, SQLAlchemy DBAPI 2.0 implementation, Advanced Alchemy integration, serialization infrastructure, Litestar plugin architecture, and production-grade connection pooling.

**Architectural Foundation**: All database access routes through Vertebra's shared-memory broker (no sockets). The broker is a Bun process that owns all connection pools and communicates with Python workers via ring buffers using the C ABI.

### Current State (2026-01-19)

**Completed (✅)**:
- Shared-memory broker operational (Bun process)
- Ring buffer IPC primitives (Rust C ABI)
- Python PyO3 bindings to shared memory
- Basic SQLSpec integration (~85% complete)
- Connection pool implementations (Postgres, MySQL, SQLite, Redis)
- Fairness scheduler implementation
- Basic Litestar plugin

**In Progress (🔧)**:
- SQLSpec API rename (Broker* → Vertebra*)
- Arrow serialization for large result sets
- Storage backend integration

**Not Started (📋)**:
- SQLAlchemy DBAPI 2.0 + dialects
- Advanced Alchemy integration
- Comprehensive serialization module
- Litestar plugin overhaul (separate plugins per ORM)
- Pool health checks and lifecycle hooks
- Integration tests with real databases

### Scope

This PRD covers:
1. **SQLSpec Stabilization** - Complete existing implementation
2. **SQLAlchemy Integration** - New DBAPI + dialect implementation
3. **Advanced Alchemy Integration** - Broker-backed configs
4. **Serialization Module** - Unified encoding for IPC and HTTP (30+ types)
5. **Litestar Plugin Overhaul** - Separate plugins for each ORM
6. **Pool Management** - Health checks and lifecycle hooks
7. **Storage Integration** - Unified storage abstraction

---

## 1. Architecture Overview

### 1.1 System Architecture

```
Python Application (Worker Process)
    ↓
ORM Layer (SQLSpec / SQLAlchemy / Advanced Alchemy)
    ↓
Vertebra Extension (Config + Proxy Driver)
    ↓
C ABI Shared Memory (Ring Buffers)
    ↓
Bun Broker Process
    ├─→ SQL Pools (Postgres, MySQL, SQLite)
    ├─→ Redis Pool
    └─→ Storage Pool (fsspec-compatible)
```

### 1.2 Key Principles

1. **Zero-Socket Data Plane**: No TCP, UDS, or HTTP for database queries
2. **Central Authority**: Bun broker owns all connection pools
3. **Fairness**: Scheduler distributes connections fairly across workers
4. **Type Safety**: Arrow IPC for large results, MessagePack for small results
5. **Storage Unification**: Object storage routes through same broker

### 1.3 Serialization Strategy

| Context | Codec | Threshold |
|---------|-------|-----------|
| Small results (<1000 rows, <1MB) | MessagePack | Default |
| Large results (≥1000 rows or ≥1MB) | Arrow IPC | Automatic |
| Storage operations | MessagePack | N/A |
| HTTP responses | JSON | Litestar |

### 1.4 Dependencies

**Core** (always installed):
- `nanoarrow >= 0.5` - Arrow C Data Interface (~1MB)
- `msgspec >= 0.18` - Fast serialization (~500KB)

**Optional** (user choice):
- `sqlspec >= 0.1` - For SQLSpec extension
- `sqlalchemy >= 2.0` - For SQLAlchemy extension
- `advanced-alchemy >= 0.1` - For AA extension
- `pyarrow >= 14.0` - For Arrow compute (optional, 100MB+)
- `litestar >= 2.0` - For Litestar integration

**Rationale for nanoarrow**: Small (~1MB), enables zero-copy Arrow handoff. Users can consume with Polars, DuckDB, or PyArrow. Vertebra exports `__arrow_c_stream__` protocol.

---

## 2. Phase 1: SQLSpec Stabilization

### 2.1 Current State

**Implemented**:
- `vertebra.extensions.sqlspec` package
- Dialect-specific configs (Postgres, MySQL, SQLite)
- Broker-backed drivers (sync + async)
- Proxy wrapper for client-side routing
- Provider host for external adapters

**File Structure**:
```
src/py/vertebra/extensions/sqlspec/
├── __init__.py
├── _config.py          # BrokerSyncConfigBase, BrokerAsyncConfigBase (RENAME)
├── _driver.py          # BrokerProxySyncDriver, BrokerProxyAsyncDriver (RENAME)
├── _shared.py          # Connection/pool wrappers
├── _core.py            # Profile and statement config builders
├── _dictionary.py      # Data dictionary resolvers
├── proxy.py            # Proxy wrapper
├── provider.py         # Provider host
├── postgres/
│   ├── __init__.py
│   ├── config.py       # BrokerPostgres* configs (RENAME)
│   └── driver.py       # BrokerPostgres* drivers (RENAME)
├── mysql/
│   ├── __init__.py
│   ├── config.py       # BrokerMysql* configs (RENAME)
│   └── driver.py       # BrokerMysql* drivers (RENAME)
└── sqlite/
    ├── __init__.py
    ├── config.py       # BrokerSqlite* configs (RENAME)
    └── driver.py       # BrokerSqlite* drivers (RENAME)
```

### 2.2 Required Work

#### Task 2.2.1: API Rename (Broker* → Vertebra*)

**Motivation**: `Broker*` prefix leaks implementation details. User-facing APIs should use `Vertebra*` prefix for brand consistency.

**Files to Modify**:
- `_config.py`: `BrokerSyncConfigBase` → `VertebraSyncConfigBase`
- `_config.py`: `BrokerAsyncConfigBase` → `VertebraAsyncConfigBase`
- `_driver.py`: `BrokerProxySyncDriver` → `VertebraProxySyncDriver`
- `_driver.py`: `BrokerProxyAsyncDriver` → `VertebraProxyAsyncDriver`
- `postgres/config.py`: `BrokerPostgresSyncConfig` → `VertebraPostgresSyncConfig`
- `postgres/config.py`: `BrokerPostgresAsyncConfig` → `VertebraPostgresAsyncConfig`
- `postgres/driver.py`: Similar renames
- `mysql/*`: Similar renames
- `sqlite/*`: Similar renames
- All test files: Update imports

**Acceptance Criteria**:
- All public APIs use `Vertebra*` prefix
- Internal broker client remains as `BrokerClient` (unchanged)
- All tests pass
- No mypy/pyright errors introduced

**Example After Rename**:
```python
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

config = VertebraPostgresAsyncConfig(pool="main")
async with config.provide_session() as session:
    result = await session.select("SELECT 1")
```

#### Task 2.2.2: Arrow Serialization

**Motivation**: Large result sets (≥1000 rows or ≥1MB) should use Arrow IPC for efficient transport.

**Implementation**:
1. Add `_arrow_threshold` parameter to driver configs (default: 1000 rows OR 1MB)
2. Implement Arrow region pool in provider (`provider.py`)
3. Add nanoarrow decoder in Python client
4. Automatic threshold detection (row count + size estimate)
5. Fallback to MessagePack for small results

**Files**:
- `_driver.py`: Add Arrow encoding logic
- `provider.py`: Add `arrow_region_pool` management
- `_shared.py`: Add Arrow decoding in connection wrappers

**Acceptance Criteria**:
- Query returning 10,000 rows uses Arrow IPC
- Query returning 100 rows uses MessagePack
- Performance: Arrow transport < 5ms overhead for 10k rows
- Tests with real Postgres/MySQL/SQLite databases

#### Task 2.2.3: Storage Integration

**Motivation**: SQLSpec defines `ObjectStoreBase` protocol. Vertebra must implement it to route storage operations through the broker.

**Implementation**:
```python
# src/py/vertebra/extensions/sqlspec/_storage.py

from sqlspec.storage import ObjectStoreBase

class VertebraObjectStore(ObjectStoreBase):
    """Routes SQLSpec storage operations through Vertebra broker."""

    def __init__(self, pool: str = "storage"):
        self.pool = pool

    async def read_bytes(self, path: str) -> bytes:
        return await broker.storage_read(self.pool, path)

    async def write_bytes(self, path: str, data: bytes) -> None:
        await broker.storage_write(self.pool, path, data)

    # ... other methods: list_objects, exists, delete, copy, move, glob
```

**Files**:
- `_storage.py` (NEW)
- Tests: `src/py/tests/test_sqlspec_storage.py` (NEW)

**Acceptance Criteria**:
- SQLSpec storage operations route through broker
- `write_to_storage_async()` works
- Integration test with local filesystem backend

#### Task 2.2.4: Integration Tests

**Requirements**:
- Container-based tests (Docker/Podman)
- Real database instances (Postgres, MySQL, SQLite)
- Test Arrow threshold switching
- Test storage integration
- Test with mypyc-compiled SQLSpec

**Files**:
- `src/py/tests/test_sqlspec_integration.py` (enhance)
- `src/py/tests/test_sqlspec_arrow.py` (NEW)
- `src/py/tests/test_sqlspec_storage.py` (NEW)

---

## 3. Phase 2: SQLAlchemy Integration (New)

### 3.1 Overview

Implement native SQLAlchemy dialects that route through Vertebra broker. This enables standard SQLAlchemy usage with connection pooling benefits.

**Dialects**:
- `postgresql+vertebra://pool_name`
- `mysql+vertebra://pool_name`
- `sqlite+vertebra://pool_name`

### 3.2 Architecture

```
SQLAlchemy Engine
    ↓ (uses dialect)
Vertebra Dialect (postgresql+vertebra://, etc.)
    ↓ (uses DBAPI)
Vertebra DBAPI Module (PEP 249)
    ↓ (IPC via shared memory)
Bun Broker → Real Database
```

### 3.3 File Structure

```
src/py/vertebra/extensions/sqlalchemy/
├── __init__.py           # Dialect registration, exports
├── _dbapi.py             # PEP 249 DBAPI implementation (shared)
├── _base.py              # Base dialect classes (sync + async)
├── postgres/
│   ├── __init__.py
│   ├── dialect.py        # VertebraPostgresDialect, VertebraPostgresAsyncDialect
│   └── dbapi.py          # Postgres-specific DBAPI module
├── mysql/
│   ├── __init__.py
│   ├── dialect.py        # VertebraMysqlDialect, VertebraMysqlAsyncDialect
│   └── dbapi.py          # MySQL-specific DBAPI module
└── sqlite/
    ├── __init__.py
    ├── dialect.py        # VertebraSqliteDialect, VertebraSqliteAsyncDialect
    └── dbapi.py          # SQLite-specific DBAPI module
```

### 3.4 DBAPI Module (PEP 249)

**Requirements**:
- `connect(dsn: str) -> Connection` (dsn = pool name)
- `Connection` class with:
  - `cursor() -> Cursor`
  - `commit()`
  - `rollback()`
  - `close()`
- `Cursor` class with:
  - `execute(query, params=None)`
  - `executemany(query, params_list)`
  - `fetchone() -> tuple | None`
  - `fetchall() -> list[tuple]`
  - `fetchmany(size=None) -> list[tuple]`

**Parameter Styles**:
- Postgres: `numeric` (`$1`, `$2`, ...)
- MySQL: `qmark` (`?`, `?`, ...)
- SQLite: `qmark` (`?`, `?`, ...)

**Implementation**:
```python
# src/py/vertebra/extensions/sqlalchemy/_dbapi.py

class Connection:
    def __init__(self, pool_name: str):
        self.pool_name = pool_name
        self._broker = BrokerClient()
        self._transaction_id = None

    def cursor(self) -> Cursor:
        return Cursor(self)

    def commit(self):
        if self._transaction_id:
            self._broker.commit(self._transaction_id)
            self._transaction_id = None

    def rollback(self):
        if self._transaction_id:
            self._broker.rollback(self._transaction_id)
            self._transaction_id = None

    def close(self):
        # Release any held resources
        pass

class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self._result = None

    def execute(self, query: str, params=None):
        self._result = self.connection._broker.execute(
            self.connection.pool_name,
            query,
            params or []
        )
        self.rowcount = len(self._result)
        return self

    def fetchone(self) -> tuple | None:
        if not self._result:
            return None
        return self._result.pop(0) if self._result else None

    def fetchall(self) -> list[tuple]:
        result = self._result or []
        self._result = []
        return result

    def fetchmany(self, size=None) -> list[tuple]:
        size = size or self.arraysize
        result = self._result[:size]
        self._result = self._result[size:]
        return result
```

### 3.5 Dialect Implementation

**Postgres Dialect**:
```python
# src/py/vertebra/extensions/sqlalchemy/postgres/dialect.py

from sqlalchemy.dialects.postgresql import PGDialect
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from .._dbapi import connect

class VertebraPostgresDialect(PGDialect):
    name = "vertebra_postgres"
    driver = "vertebra"
    supports_transactional_ddl = True

    @classmethod
    def dbapi(cls):
        return connect

    def create_connect_args(self, url):
        # url.host is the pool name
        return ([url.host], {})

class VertebraPostgresAsyncDialect(PGDialect_asyncpg):
    name = "vertebra_postgres_async"
    driver = "vertebra"
    supports_transactional_ddl = True

    @classmethod
    def dbapi(cls):
        return connect
```

**Registration** (in `__init__.py`):
```python
from sqlalchemy.dialects import registry

registry.register("postgresql.vertebra", "vertebra.extensions.sqlalchemy.postgres", "VertebraPostgresDialect")
registry.register("postgresql+vertebra", "vertebra.extensions.sqlalchemy.postgres", "VertebraPostgresAsyncDialect")
```

### 3.6 Transaction Support

**Broker Protocol Commands** (add to broker):
- `BEGIN` - Start transaction, return transaction ID
- `COMMIT` - Commit transaction by ID
- `ROLLBACK` - Rollback transaction by ID
- `SAVEPOINT` - Create savepoint (nested transactions)

**Connection Tracking**:
- Track transaction ID in `Connection._transaction_id`
- Auto-begin on first execute if no transaction
- Raise error if commit/rollback without transaction

### 3.7 Acceptance Criteria

- `create_engine("postgresql+vertebra://main")` works
- `create_async_engine("postgresql+vertebra://main")` works
- Basic CRUD operations work
- Transactions work correctly (commit/rollback)
- All DBAPI 2.0 requirements met
- SQLAlchemy ORM works with declarative models
- Integration tests with real databases pass

---

## 4. Phase 3: Serialization Module

### 4.1 Overview

Unified type encoding/decoding for IPC (MessagePack) and HTTP (JSON). Follows the `vertebra.typing` pattern for optional dependency handling.

**Motivation**:
- SQLSpec provides multi-tier serialization (msgspec > orjson > stdlib)
- Vertebra needs to extend this for IPC-specific types (Arrow, Pydantic, attrs)
- Type registration for Litestar happens in `VertebraPlugin`, not serializers

### 4.2 File Structure

```
src/py/vertebra/serializers/
├── __init__.py           # Unified exports
├── codecs.py             # JSON + MessagePack codecs
├── encoders.py           # default_encoder hook (uses vertebra.typing)
└── decoders.py           # Type-specific decoders
```

**Note**: Type registration for Litestar moved to `vertebra.extensions.litestar._plugin`. Serializers are framework-agnostic.

### 4.2.1 Required Changes to `vertebra/_typing.py`

Add missing dependency flags and type stubs following existing pattern:

```python
# Add to vertebra/_typing.py

ATTRS_INSTALLED = dependency_flag("attrs")

# Attrs
try:
    from attrs import AttrsInstance
except ImportError:
    @runtime_checkable
    class AttrsInstance(Protocol):  # type: ignore[no-redef]
        __attrs_attrs__: ClassVar[tuple[Any, ...]]

# Pydantic (already have PYDANTIC_INSTALLED flag, add type stub)
try:
    from pydantic import BaseModel as PydanticBaseModel
except ImportError:
    @runtime_checkable
    class PydanticBaseModel(Protocol):  # type: ignore[no-redef]
        def model_dump(self) -> dict[str, Any]: ...
        def model_dump_json(self) -> str: ...
```

### 4.2.2 Updated `encoders.py` Pattern

Import from `vertebra.typing`, not inline try/except:

```python
# src/py/vertebra/serializers/encoders.py

from vertebra.typing import (
    ATTRS_INSTALLED,
    NUMPY_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
    ArrowTable,
    AttrsInstance,
    NumpyArray,
    PydanticBaseModel,
)

def default_encoder(value: Any) -> Any:
    """Default encoder for types not natively supported by msgspec."""
    # Standard types (no deps)
    if isinstance(value, datetime.datetime):
        ...

    # Optional deps - use flags from vertebra.typing
    if PYDANTIC_INSTALLED and isinstance(value, PydanticBaseModel):
        return value.model_dump()
    if ATTRS_INSTALLED and isinstance(value, AttrsInstance):
        import attrs
        return attrs.asdict(value)
    if NUMPY_INSTALLED and isinstance(value, NumpyArray):
        return value.tolist()
    if PYARROW_INSTALLED and isinstance(value, ArrowTable):
        ...
```

### 4.3 Type Support Matrix (30+ Types)

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
| `IPv4Network`/`IPv6Network` | String | From string | String | P2 |
| `Pattern` | `.pattern` | `re.compile` | String | P2 |
| `Enum` | `.value` | From value | Value | P0 |
| `set`/`frozenset` | List | From list | List | P1 |
| `deque` | List | From list | List | P2 |
| `bytes` | Base64 | From base64 | Native | P0 |
| `SecretStr`/`SecretBytes` | Masked | N/A | Masked | P1 |
| `Pydantic BaseModel` | `.model_dump()` | `.model_validate()` | Dict | P0 |
| `msgspec.Struct` | Dict | From dict | Dict | P0 |
| `dataclass` | `asdict()` | From dict | Dict | P1 |
| `attrs` | `asdict()` | From dict | Dict | P2 |
| `numpy.ndarray` | List | `np.array()` | List | P2 |
| `Arrow Table` | IPC bytes | From IPC | IPC bytes | P0 |
| `FileObject` | Metadata dict | From metadata | Dict | P1 |
| `Postgres JSONB` | Pass-through | Pass-through | Dict | P1 |
| `Postgres Array` | List | List | List | P1 |
| `MySQL Spatial` | WKT string | From WKT | String | P2 |

### 4.4 MessagePack Codec

```python
# src/py/vertebra/serializers/msgpack.py

from msgspec.msgpack import MessagePackCodec
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from uuid import UUID

class VertebraCodec(MessagePackCodec):
    """Extended MessagePack codec with Vertebra type support."""

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
        self.register_encoder(Decimal, lambda d: {
            "__type__": "decimal",
            "value": str(d)
        })
        self.register_decoder("decimal", lambda d:
            Decimal(d["value"])
        )

        # UUID
        self.register_encoder(UUID, lambda u: {
            "__type__": "uuid",
            "value": u.bytes
        })
        self.register_decoder("uuid", lambda d:
            UUID(bytes=d["value"])
        )

        # ... register all 30+ types
```

### 4.5 Litestar Integration

```python
# src/py/vertebra/serializers/litestar.py

from litestar.serialization import default_serializer
from typing import Any

VERTEBRA_TYPE_ENCODERS = {
    datetime: lambda dt: dt.isoformat(),
    Decimal: lambda d: str(d),
    UUID: lambda u: str(u),
    Path: lambda p: str(p),
    IPv4Address: lambda ip: str(ip),
    IPv6Address: lambda ip: str(ip),
    # ... all 30+ types
}

VERTEBRA_TYPE_DECODERS = [
    (datetime, lambda x: datetime.fromisoformat(x) if isinstance(x, str) else x),
    (Decimal, lambda x: Decimal(x) if isinstance(x, str) else x),
    (UUID, lambda x: UUID(x) if isinstance(x, str) else x),
    # ... all 30+ types
]
```

### 4.6 Acceptance Criteria

- `VertebraCodec.encode()` handles all P0 types
- `VertebraCodec.decode()` reconstructs typed objects correctly
- Litestar apps can return `FileObject` from route handlers
- Database-specific types serialize correctly
- Performance: < 1ms encoding overhead for typical payloads
- Tests cover all type conversions

---

## 5. Phase 4: Advanced Alchemy Integration

### 5.1 Overview

Broker-backed Advanced Alchemy configs following the same pattern as SQLSpec. This enables Repository/Service patterns with connection pooling.

### 5.2 File Structure

```
src/py/vertebra/extensions/advanced_alchemy/
├── __init__.py           # Import guards, exports
├── _config.py            # VertebraSyncAlchemyConfig, VertebraAsyncAlchemyConfig
├── _engine.py            # Engine factory using vertebra dialects
├── _storage.py           # StoredObject backend routing through broker
├── postgres/
│   ├── __init__.py
│   └── config.py         # VertebraPostgresAlchemySyncConfig, VertebraPostgresAlchemyAsyncConfig
├── mysql/
│   ├── __init__.py
│   └── config.py         # VertebraMysqlAlchemySyncConfig, VertebraMysqlAlchemyAsyncConfig
└── sqlite/
    ├── __init__.py
    └── config.py         # VertebraSqliteAlchemySyncConfig, VertebraSqliteAlchemyAsyncConfig
```

### 5.3 Base Config Classes

```python
# src/py/vertebra/extensions/advanced_alchemy/_config.py

from advanced_alchemy.config import AsyncAlchemyConfig, SyncAlchemyConfig
from sqlalchemy.ext.asyncio import create_async_engine

class VertebraAsyncAlchemyConfig(AsyncAlchemyConfig):
    """Base config for broker-backed Advanced Alchemy."""

    def __init__(self, pool: str, **kwargs):
        self.pool = pool
        # Create engine with vertebra dialect
        engine = create_async_engine(f"postgresql+vertebra://{pool}")
        super().__init__(engine=engine, **kwargs)
```

### 5.4 Storage Backend

Advanced Alchemy uses `StoredObject` type for file storage. Vertebra provides a backend that routes operations through the broker.

```python
# src/py/vertebra/extensions/advanced_alchemy/_storage.py

from advanced_alchemy.types.file_object import storages, Backend

class VertebraStorageBackend(Backend):
    """Routes AA StoredObject operations through Vertebra broker."""

    def __init__(self, pool: str = "storage", key: str = "vertebra"):
        self.pool = pool
        self.key = key

    async def read_bytes(self, path: str) -> bytes:
        return await broker.storage_read(self.pool, path)

    async def write_bytes(self, path: str, data: bytes) -> None:
        await broker.storage_write(self.pool, path, data)

    async def list_objects(self, prefix: str) -> list[str]:
        return await broker.storage_list(self.pool, prefix)

    async def exists(self, path: str) -> bool:
        return await broker.storage_exists(self.pool, path)

    async def delete(self, path: str) -> None:
        await broker.storage_delete(self.pool, path)

    async def copy(self, src: str, dst: str) -> None:
        await broker.storage_copy(self.pool, src, dst)

    async def move(self, src: str, dst: str) -> None:
        await broker.storage_move(self.pool, src, dst)

    async def glob(self, pattern: str) -> list[str]:
        return await broker.storage_glob(self.pool, pattern)

# Registration
storages.register_backend(VertebraStorageBackend())
```

### 5.5 Usage Example

```python
from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService

config = VertebraPostgresAlchemyAsyncConfig(pool="main")

class UserService(SQLAlchemyAsyncRepositoryService[User]):
    # Works unchanged - config handles broker routing
    pass

# File storage
class Document(UUIDBase):
    attachment: Mapped[FileObject | None] = mapped_column(
        StoredObject(backend="vertebra"), nullable=True
    )
```

### 5.6 Acceptance Criteria

- Repository pattern works with Vertebra configs
- Service pattern works with Vertebra configs
- File Object Storage routes through broker
- Alembic migrations work via broker connection
- Integration tests with AA's test suite pass

---

## 6. Phase 5: Litestar Plugin Overhaul

### 6.1 Overview

**NO wrapper plugins needed.** Users use Vertebra configs directly with Litestar's native plugins:

- SQLSpec adapters directly (`VertebraPostgresAsyncConfig`)
- SQLAlchemy dialects directly (`postgresql+vertebra://`)
- Advanced Alchemy configs directly (`VertebraPostgresAlchemyAsyncConfig`)

The **ONE** `VertebraPlugin` handles type encoder/decoder registration for all of them.

### 6.2 Required Structure

```
src/py/vertebra/extensions/litestar/
├── __init__.py           # Exports VertebraPlugin
└── _plugin.py            # Main VertebraPlugin with type registration
```

### 6.3 Plugin Implementation

```python
# src/py/vertebra/extensions/litestar/_plugin.py

from dataclasses import dataclass
from typing import Any

from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

from vertebra.serializers import default_encoder
from vertebra.serializers.decoders import (
    decode_datetime,
    decode_date,
    decode_time,
    decode_timedelta,
    decode_uuid,
    decode_decimal,
    decode_path,
    decode_ip,
)


@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    """Litestar plugin providing Vertebra integration.

    Automatically registers type encoders/decoders for:
    - datetime, date, time, timedelta
    - UUID, Decimal, Path
    - IPv4Address, IPv6Address
    - Pydantic BaseModel (if installed)
    - attrs classes (if installed)
    - Arrow tables (if installed)
    """

    log_level: str | None = None
    register_type_encoders: bool = True

    def on_app_init(self, app_config: Any) -> Any:
        """Register Vertebra with Litestar app."""
        app_config.state.setdefault("vertebra", self)

        if self.register_type_encoders:
            self._register_type_encoders(app_config)

        return app_config

    def _register_type_encoders(self, app_config: Any) -> None:
        """Register Vertebra type encoders/decoders."""
        import datetime
        import decimal
        import ipaddress
        import pathlib
        import uuid

        from vertebra.typing import PYDANTIC_INSTALLED

        # Build encoder map - all use default_encoder
        type_encoders: dict[type, Any] = {
            datetime.datetime: default_encoder,
            datetime.date: default_encoder,
            datetime.time: default_encoder,
            datetime.timedelta: default_encoder,
            uuid.UUID: default_encoder,
            decimal.Decimal: default_encoder,
            pathlib.Path: default_encoder,
            pathlib.PurePath: default_encoder,
            ipaddress.IPv4Address: default_encoder,
            ipaddress.IPv6Address: default_encoder,
            ipaddress.IPv4Network: default_encoder,
            ipaddress.IPv6Network: default_encoder,
            bytes: default_encoder,
            set: default_encoder,
            frozenset: default_encoder,
        }

        # Optional deps
        if PYDANTIC_INSTALLED:
            from pydantic import BaseModel
            type_encoders[BaseModel] = default_encoder

        # Decoder list - ordered (type, decoder_func) tuples
        type_decoders: list[tuple[type, Any]] = [
            (datetime.datetime, decode_datetime),
            (datetime.date, decode_date),
            (datetime.time, decode_time),
            (datetime.timedelta, decode_timedelta),
            (uuid.UUID, decode_uuid),
            (decimal.Decimal, decode_decimal),
            (pathlib.Path, decode_path),
            (ipaddress.IPv4Address, decode_ip),
            (ipaddress.IPv6Address, decode_ip),
        ]

        # Merge with existing
        existing_encoders = getattr(app_config, 'type_encoders', None) or {}
        app_config.type_encoders = {**existing_encoders, **type_encoders}

        existing_decoders = getattr(app_config, 'type_decoders', None) or []
        app_config.type_decoders = list(existing_decoders) + type_decoders

    def on_cli_init(self, cli: Any) -> Any:
        """Register CLI commands (existing implementation)."""
        # ... existing CLI code ...
        return cli
```

### 6.4 Usage Example

```python
from litestar import Litestar
from litestar.plugins.sqlspec import SQLSpecPlugin  # Litestar's native plugin

from vertebra.extensions.litestar import VertebraPlugin
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

# Vertebra configs work with Litestar's native plugins
app = Litestar(
    route_handlers=[...],
    plugins=[
        VertebraPlugin(),  # Registers type encoders/decoders
        SQLSpecPlugin(config=VertebraPostgresAsyncConfig(pool="main")),
    ],
)
```

### 6.5 Acceptance Criteria

- `VertebraPlugin` registers type encoders/decoders on app init
- Vertebra configs work with Litestar's native `SQLSpecPlugin`
- Vertebra SQLAlchemy dialects work with Litestar's native `SQLAlchemyPlugin`
- Vertebra AA configs work with AA's native Litestar plugin
- All 30+ types serializable in route handler responses
- Optional deps (pydantic, attrs, pyarrow) handled gracefully

---

## 7. Phase 6: Pool Health Checks and Lifecycle Hooks

### 7.1 Overview

Production-grade connection pool management with health checks, idle timeout, and lifecycle hooks.

**Currently**: Basic pools implemented in `src/ts/src/broker/serve.ts`

### 7.2 Health Check Implementation

**TypeScript (Broker)**:
```typescript
// src/ts/src/broker/health.ts

export interface HealthChecker {
  check(connection: any): Promise<boolean>;
}

export class PostgresHealthChecker implements HealthChecker {
  async check(connection: any): Promise<boolean> {
    try {
      await connection.execute("SELECT 1");
      return true;
    } catch (error) {
      console.warn("Postgres health check failed:", error);
      return false;
    }
  }
}

export class RedisHealthChecker implements HealthChecker {
  async check(connection: any): Promise<boolean> {
    try {
      const result = await connection.ping();
      return result === "PONG";
    } catch (error) {
      console.warn("Redis health check failed:", error);
      return false;
    }
  }
}
```

**Pool Enhancement**:
```typescript
// src/ts/src/broker/base-pool.ts

export abstract class BasePoolAdapter {
  protected healthCheckInterval: number = 30000; // 30s
  protected idleTimeout: number = 300000; // 5min
  protected healthChecker: HealthChecker;

  async startHealthCheckLoop() {
    setInterval(async () => {
      for (const conn of this.idleConnections()) {
        const healthy = await this.healthChecker.check(conn);
        if (!healthy) {
          console.warn(`Removing unhealthy connection`);
          await this.removeConnection(conn);
          await this.createNewConnection();
        }
      }
    }, this.healthCheckInterval);
  }

  async startIdleReaper() {
    setInterval(async () => {
      const now = Date.now();
      for (const [conn, lastUsed] of this.connectionTimestamps.entries()) {
        if (now - lastUsed > this.idleTimeout) {
          console.info(`Reaping idle connection`);
          await this.removeConnection(conn);
        }
      }
    }, 60000); // Check every minute
  }
}
```

### 7.3 Lifecycle Hooks

**Hook Types**:
- `on_acquire`: Runs after acquiring connection
- `on_release`: Runs before releasing connection
- `on_error`: Runs on connection error
- `on_timeout`: Runs on acquire timeout

**Implementation**:
```typescript
export interface LifecycleHooks {
  onAcquire?: (connection: any) => Promise<void>;
  onRelease?: (connection: any) => Promise<void>;
  onError?: (connection: any, error: Error) => Promise<void>;
  onTimeout?: (poolName: string) => Promise<void>;
}

export class BasePoolAdapter {
  constructor(
    name: string,
    config: PoolConfig,
    hooks: LifecycleHooks = {}
  ) {
    this.name = name;
    this.config = config;
    this.hooks = hooks;
  }

  async acquire(): Promise<Connection> {
    const conn = await this.pool.acquire();

    if (this.hooks.onAcquire) {
      await this.hooks.onAcquire(conn);
    }

    return conn;
  }
}
```

### 7.4 Metrics Enhancement

**Additional Metrics**:
```typescript
export const poolMetrics = {
  health_check_failures_total: new Counter({
    name: "vertebra_pool_health_check_failures_total",
    help: "Total number of failed health checks",
    labelNames: ["pool", "database"],
  }),
  idle_timeout_total: new Counter({
    name: "vertebra_pool_idle_timeout_total",
    help: "Total number of idle connection timeouts",
    labelNames: ["pool"],
  }),
  acquire_wait_time_seconds: new Histogram({
    name: "vertebra_pool_acquire_wait_time_seconds",
    help: "Time spent waiting for connection acquisition",
    labelNames: ["pool"],
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5],
  }),
  connection_lifetime_seconds: new Histogram({
    name: "vertebra_pool_connection_lifetime_seconds",
    help: "Time a connection was alive before removal",
    labelNames: ["pool", "reason"],
    buckets: [60, 300, 600, 1800, 3600],
  }),
};
```

### 7.5 Acceptance Criteria

- Health checks run periodically (30s default)
- Unhealthy connections removed from pool
- Idle connections reaped after timeout (5min default)
- Lifecycle hooks fire correctly
- All metrics exposed in Prometheus format
- Tests for all hook types

---

## 8. Testing Strategy

### 8.1 Unit Tests

**Python Tests**:
- `test_sqlspec_config.py` - Config creation and validation
- `test_sqlspec_driver.py` - Driver execute methods
- `test_sqlalchemy_dbapi.py` - DBAPI compliance
- `test_sqlalchemy_dialect.py` - Dialect behavior
- `test_advanced_alchemy_config.py` - AA config creation
- `test_serialization_codec.py` - Type encoding/decoding
- `test_litestar_plugin.py` - Plugin registration

**TypeScript Tests**:
- `pool-health.test.ts` - Health check logic
- `lifecycle-hooks.test.ts` - Hook execution

### 8.2 Integration Tests (Container-Based)

**Requirements**:
- Docker/Podman for real databases
- Test fixtures with sample data
- Test Arrow threshold switching
- Test storage operations

**Test Files**:
- `test_sqlspec_integration.py` - Full SQLSpec flow
- `test_sqlalchemy_integration.py` - Full SQLAlchemy flow
- `test_advanced_alchemy_integration.py` - Repository/Service patterns
- `test_storage_integration.py` - Storage backend

**Test Matrix**:
| Database | SQLSpec | SQLAlchemy | Advanced Alchemy |
|----------|---------|------------|------------------|
| Postgres | ✅ | ✅ | ✅ |
| MySQL | ✅ | ✅ | ✅ |
| SQLite | ✅ | ✅ | ✅ |
| Redis | ✅ | ❌ | ❌ |

### 8.3 Performance Benchmarks

**Metrics**:
- IPC overhead: < 1ms for small queries
- Arrow transport: < 5ms overhead for 10k rows
- Throughput: > 10k queries/sec
- Pool efficiency: 80%+ connection utilization

**Benchmark Suite**:
```python
# benchmarks/bench_sqlspec.py

import time
import asyncio
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

async def bench_small_query(iterations: int = 10000):
    config = VertebraPostgresAsyncConfig(pool="bench")

    start = time.time()
    async with config.provide_session() as session:
        for _ in range(iterations):
            await session.select("SELECT 1")

    duration = time.time() - start
    print(f"Small queries: {iterations / duration:.0f} qps")
    print(f"Average latency: {duration / iterations * 1000:.2f}ms")

async def bench_large_query():
    config = VertebraPostgresAsyncConfig(pool="bench")

    start = time.time()
    async with config.provide_session() as session:
        # 10k rows
        await session.select("SELECT generate_series(1, 10000)")

    duration = time.time() - start
    print(f"Large query (10k rows): {duration * 1000:.2f}ms")
```

### 8.4 Mypyc Compatibility Testing

**Test both variants**:
- Pure Python SQLSpec
- mypyc-compiled SQLSpec

**CI Matrix**:
```yaml
strategy:
  matrix:
    sqlspec-variant: [pure, mypyc]
```

---

## 9. Implementation Roadmap

### Phase 1: SQLSpec Stabilization (Week 1)
- [ ] Task 1.1: API Rename (Broker* → Vertebra*)
- [ ] Task 1.2: Arrow Serialization Implementation
- [ ] Task 1.3: Storage Integration (VertebraObjectStore)
- [ ] Task 1.4: Integration Tests

**Dependencies**: None (builds on existing work)

### Phase 2: Serialization Module (Week 1-2)
- [ ] Task 2.1: Type Support Matrix Implementation
- [ ] Task 2.2: MessagePack Codec (VertebraCodec)
- [ ] Task 2.3: JSON Codec
- [ ] Task 2.4: Litestar Integration

**Dependencies**: None (standalone module)

### Phase 3: SQLAlchemy Dialects (Week 2-3)
- [ ] Task 3.1: DBAPI Module
- [ ] Task 3.2: Postgres Dialect
- [ ] Task 3.3: MySQL Dialect
- [ ] Task 3.4: SQLite Dialect
- [ ] Task 3.5: Transaction Support
- [ ] Task 3.6: Integration Tests

**Dependencies**: Phase 2 (serialization)

### Phase 4: Pool Health Checks (Week 3)
- [ ] Task 4.1: Health Check Loop
- [ ] Task 4.2: Lifecycle Hooks
- [ ] Task 4.3: Idle Connection Management
- [ ] Task 4.4: Metrics Enhancement

**Dependencies**: None (enhances existing pools)

### Phase 5: Advanced Alchemy (Week 4)
- [ ] Task 5.1: Base Config Classes
- [ ] Task 5.2: Dialect-Specific Configs
- [ ] Task 5.3: Storage Backend
- [ ] Task 5.4: Integration Tests

**Dependencies**: Phase 3 (SQLAlchemy dialects)

### Phase 6: Litestar Plugin Overhaul (Week 4)
- [ ] Task 6.1: Plugin Infrastructure
- [ ] Task 6.2: SQLSpec Plugin
- [ ] Task 6.3: SQLAlchemy Plugin
- [ ] Task 6.4: Advanced Alchemy Plugin
- [ ] Task 6.5: Serialization Auto-Registration

**Dependencies**: Phases 1, 2, 3, 5

**Total Duration**: 4-5 weeks

---

## 10. Success Metrics

### 10.1 Functional Requirements

- [ ] `VertebraPostgresAsyncConfig` works with SQLSpec
- [ ] `create_engine("postgresql+vertebra://main")` works
- [ ] `create_async_engine("postgresql+vertebra://main")` works
- [ ] Advanced Alchemy Repository/Service patterns work
- [ ] Pool health checks run and report metrics
- [ ] All three Litestar plugins work
- [ ] Example applications run end-to-end

### 10.2 Quality Requirements

- [ ] 0 mypy errors in strict mode
- [ ] 0 ruff violations
- [ ] Test coverage > 85%
- [ ] All modern type syntax (no `Optional[T]`)
- [ ] Documentation exists for all major features

### 10.3 Performance Requirements

- [ ] Throughput: > 10k req/sec (small queries)
- [ ] Latency: P99 < 10ms (IPC + query)
- [ ] Arrow transport: < 5ms overhead for 10k rows
- [ ] Memory: < 50MB per idle worker

### 10.4 Compatibility Requirements

- [ ] Works with pure Python SQLSpec
- [ ] Works with mypyc-compiled SQLSpec
- [ ] Works with SQLAlchemy 2.0+
- [ ] Works with Advanced Alchemy latest
- [ ] Works with Litestar 2.0+

---

## 11. Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| SQLAlchemy dialect complexity | High | Medium | Start with simple DBAPI, iterate |
| Arrow serialization overhead | Medium | Low | Benchmark early, adjust threshold |
| mypyc incompatibility | Medium | Low | Test both variants in CI |
| Storage backend complexity | Medium | Medium | Start with local filesystem, iterate |
| Transaction support complexity | High | Medium | Implement minimal viable first |

---

## 12. File Paths Reference

### Python Files to Create/Modify:

**SQLSpec** (modify for rename):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/*.py`

**SQLSpec** (new files):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_storage.py`

**Serialization** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/_types.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/_encoders.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/_decoders.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/msgpack.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/json.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/serializers/litestar.py`

**SQLAlchemy** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/_dbapi.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/_base.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/postgres/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/mysql/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/sqlite/*.py`

**Advanced Alchemy** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/_config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/_engine.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/_storage.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/postgres/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/mysql/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/sqlite/*.py`

**Litestar** (overhaul):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/_base.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/_serialization.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/sqlspec.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/sqlalchemy.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/advanced_alchemy.py`

### TypeScript Files to Modify:

**Broker**:
- `/home/cody/code/c/vertebra/src/ts/src/broker/base-pool.ts`
- `/home/cody/code/c/vertebra/src/ts/src/broker/serve.ts`
- `/home/cody/code/c/vertebra/src/ts/src/broker/health.ts` (NEW)

### Test Files:

**Python**:
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlspec_integration.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlspec_arrow.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlspec_storage.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlalchemy_dbapi.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlalchemy_dialect.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlalchemy_integration.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_advanced_alchemy.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_serialization_codec.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_litestar_plugin.py` (NEW)

**TypeScript**:
- `/home/cody/code/c/vertebra/src/ts/tests/unit/pool-health.test.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/tests/unit/lifecycle-hooks.test.ts` (NEW)

---

## 13. Related Specifications

- `/home/cody/code/c/vertebra/specs/archive/consolidated/PRD-5-resource-management.md` - Pool fairness
- `/home/cody/code/c/vertebra/specs/archive/consolidated/prd-remaining-work.md` - Remaining 1.0 work
- `/home/cody/code/c/vertebra/specs/archive/shared-memory-broker/` - Broker architecture

---

## 14. Conclusion

This consolidated PRD defines a comprehensive database and ORM integration strategy for Vertebra 1.0. By completing this work, Vertebra will support:

1. **SQLSpec** - Broker-backed adapters with Arrow serialization
2. **SQLAlchemy** - Native dialects with standard usage patterns
3. **Advanced Alchemy** - Repository/Service patterns with file storage
4. **Serialization** - Unified encoding for 30+ types across IPC and HTTP
5. **Litestar** - Separate plugins for each ORM with auto-registration
6. **Production** - Health checks, lifecycle hooks, and comprehensive metrics

**Success means**: Users can choose their preferred ORM, write standard Python code, and automatically benefit from Vertebra's connection pooling and shared-memory transport.

---

**Document Version**: 1.0
**Word Count**: 7,842 words
**Last Updated**: 2026-01-19
**Author**: Vertebra Team
**Status**: Ready for Implementation
