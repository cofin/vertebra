# PRD: Vertebra SQLAlchemy & Advanced Alchemy Integration

## 1. Context & Problem Statement
Vertebra requires a standard SQL interface to interact with its IPC-based data sources. Currently, interaction is limited to raw IPC primitives. To enable the use of high-level ORMs and frameworks (like Litestar and Advanced Alchemy) and to facilitate standard database operations, we need a compliant Python DBAPI 2.0 layer and a SQLAlchemy Dialect.

The key challenge is to maintain the performance benefits of Vertebra's zero-copy shared memory architecture while conforming to the synchronous, copy-heavy expectations of standard DBAPIs.

## 2. Goals
- **DBAPI 2.0 Compliance**: Implement a custom DBAPI module (`vertebra.dbapi`) that satisfies PEP 249.
- **SQLAlchemy Dialect**: Create a `vertebra+ipc://` dialect to allow SQLAlchemy to inspect, query, and manage transactions over Vertebra IPC.
- **Advanced Alchemy Provider**: Implement an extension for Advanced Alchemy to allow seamless integration with Litestar applications.
- **Zero-Copy Optimization**: Ensure data retrieval uses shared memory references where possible, minimizing copying.
- **Transaction Support**: robust `begin`, `commit`, and `rollback` handling over IPC.

## 3. Scope & Out of Scope

### In Scope
- `vertebra.dbapi`: `connect()`, `Connection`, `Cursor`, `TypeObjects`.
- `vertebra.sqlalchemy`: `VertebraDialect`, `VertebraExecutionContext`.
- `vertebra.advanced_alchemy`: `VertebraAsyncConfig`, `VertebraSyncConfig` (if applicable).
- Transaction state management via IPC opcodes.
- Basic type mapping (String, Integer, Float, Boolean, Bytes).
- Zero-copy buffer protocol support for result sets (e.g., Apache Arrow integration if supported by transport, or raw shared memory buffers).

### Out of Scope
- DDL generation (unless basic reflection is needed).
- Migration support (Alembic) for now, unless critical.
- AsyncIO support for the *synchronous* DBAPI (we will implement a synchronous DBAPI first, as SQLAlchemy Core is sync-first, though we may consider an async driver `vertebra+asyncipc` later if needed. For now, we assume the standard sync `dialect`). *Correction*: Advanced Alchemy is async-first. We should ensure the DBAPI can potentially work in an async context or provide an async driver. However, standard SQLAlchemy dialects are sync. We will target the sync dialect `vertebra+ipc`.

## 4. Technical Design

### 4.1 Custom DBAPI (`vertebra.dbapi`)
- **Connection**:
  - Establishes a UDS (Unix) or Named Pipe (Windows) connection to the Vertebra kernel.
  - Handshake protocol to establish session.
- **Cursor**:
  - `execute(operation, parameters)`: Serializes SQL and params, sends via IPC `CMD_SQL_EXEC`.
  - `fetchall()`: Waits for `RESP_SQL_RESULT`.
    - **Zero-Copy**: The response will contain a handle to a Shared Memory block. The cursor will map this block and expose it as a Python buffer or Arrow Table without copying to a Python list until iterated.
- **Transactions**:
  - `Connection.commit()` -> IPC `CMD_TX_COMMIT`.
  - `Connection.rollback()` -> IPC `CMD_TX_ROLLBACK`.
  - Auto-begin behavior: The kernel should start a transaction implicitly on the first command, or the DBAPI should send `CMD_TX_BEGIN`.

### 4.2 SQLAlchemy Dialect (`vertebra.sqlalchemy`)
- **Name**: `vertebra`
- **Driver**: `ipc`
- **Class**: `VertebraDialect`
- **Entry Point**: `sqlalchemy.dialects` -> `vertebra.ipc = vertebra.sqlalchemy:VertebraDialect`
- **Features**:
  - `supports_native_boolean = True`
  - `supports_sequences = False`
  - `supports_sane_rowcount = True`
  - `supports_statement_cache = True`

### 4.3 Advanced Alchemy Integration
- Create a configuration class `VertebraAlchemyConfig` that inherits from `AdvancedAlchemyAsyncConfig` (if using async driver) or `AdvancedAlchemySyncConfig`.
- Since we are building a standard sync dialect first, we will likely use the `SQLAlchemyAsyncConfig` with a sync driver (standard SQLAlchemy behavior, running in a threadpool) or implement an `asyncio` compatible driver (`vertebra+aiosql://`) if true async is required.
- **Decision**: For high performance IPC, an async driver is preferred.
  - **Plan B**: Implement `vertebra.dbapi` as a blocking wrapper around the IPC, and let SQLAlchemy handle the threading.
  - **Plan A (Preferred)**: Implement a true `async` dialect `vertebra+asyncipc` if the Vertebra IPC client is async.
  - **Selected Approach**: We will implement the **Synchronous DBAPI** first (`vertebra+ipc://`) to ensure broad compatibility, and rely on SQLAlchemy's `asyncio` engine (which wraps sync drivers) for Advanced Alchemy integration.

## 5. Implementation Plan

### Phase 1: Core DBAPI (Skeleton)
- Define `Connection` and `Cursor` classes.
- Implement stubbed IPC calls.
- Define Exceptions (PEP 249).

### Phase 2: IPC Integration & Zero-Copy
- Integrate `vertebra.ipc` client.
- Implement `execute` serialization.
- Implement shared memory reading in `fetchall`.

### Phase 3: SQLAlchemy Dialect
- Subclass `DefaultDialect`.
- Implement `dbapi()` classmethod.
- Register entry point.
- Test with basic `create_engine("vertebra+ipc://...")`.

### Phase 4: Advanced Alchemy
- Create `extensions/advanced_alchemy.py`.
- Add integration tests with Litestar.

## 6. Acceptance Criteria
1.  Can connect using `sqlalchemy.create_engine("vertebra+ipc://...")`.
2.  Can execute `SELECT 1` and receive `1`.
3.  `connection.commit()` sends correct IPC opcode.
4.  Large result sets do not cause double memory usage (zero-copy validation).
5.  Advanced Alchemy service can list/create records.
