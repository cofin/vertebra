# Research: ORM Extensions for Vertebra

## Goal
Enable seamless integration of `advanced-alchemy` (and `sqlalchemy`) and `sqlspec` with Vertebra's IPC-based "DB Node" architecture. Applications running in Vertebra workers should be able to use these libraries as if they were connecting to a local database, but all queries should be tunneled to the central DB Node via shared memory IPC.

## 1. SQLSpec Integration

### Target Abstraction
`sqlspec.driver.AsyncDriverAdapterBase` is the correct integration point. It provides a high-level async interface (`dispatch_execute`, `begin`, `commit`, etc.) that we can map to IPC calls.

### Implementation Plan
- **Adapter**: `sqlspec.adapters.ipc.AsyncIPCDriver`
- **Configuration**: `IPCConfig` (inheriting from `AsyncConfig`)
- **Execution**:
  - `dispatch_execute(cursor, statement)`: Serializes `(sql, parameters)` using MsgPack. Sends to DB Node via `VertebraClientConnection`.
  - **Result Handling**: The DB Node executes the query using the *real* driver (e.g., `asyncpg`) and serializes the result.
  - **Zero-Copy**: If possible, the DB Node should return Arrow data in shared memory. `sqlspec` has native `select_to_arrow` support which we can leverage.
- **Transactions**:
  - `begin()`: Sends `BEGIN` control message. DB Node allocates a transaction-bound session.
  - `commit()`/`rollback()`: Sends corresponding control messages.

## 2. SQLAlchemy / Advanced Alchemy Integration

### Target Abstraction
SQLAlchemy requires a PEP 249 (DBAPI 2.0) compatible layer and a `Dialect`.

### Implementation Plan
- **DBAPI**: `vertebra.sqlalchemy.dbapi`
  - `connect()`: Returns `VertebraConnection`.
  - `VertebraConnection`: Manages IPC channel to DB Node.
  - `VertebraCursor`: `execute()` sends `(sql, params)` to DB Node.
- **Dialect**: `VertebraDialect` (subclass of `pg.dialect` or `DefaultDialect` depending on backend).
  - URL Scheme: `vertebra+ipc://<pool_name>`
  - This allows `create_async_engine("vertebra+ipc://main")`.
- **Advanced Alchemy**:
  - AA builds on top of SQLAlchemy. Once the `VertebraDialect` is working, AA should work out of the box.
  - We can provide a `VertebraPlugin` for Litestar that automatically configures AA to use the IPC engine.

## 3. Configuration & Registration

### Extension System
We will introduce a `vertebra.extensions` namespace.
- Users register extensions in `vertebra.config.py` or `vertebra.toml`.
- **Initialization**: When a worker starts, it initializes registered extensions.
  - `sqlspec` extension: Adds the `ipc` adapter to the global `sqlspec` registry (if needed) or provides a pre-configured `SQLSpec` instance.
  - `sqlalchemy` extension: Registers the `vertebra+ipc` entrypoint (if not using setuptools entrypoints) and sets up the engine.

### Protocol
- **Request**: `[MSG_TYPE, REQUEST_ID, PAYLOAD]`
  - `MSG_TYPE`: `EXECUTE`, `EXECUTE_MANY`, `BEGIN`, `COMMIT`, `ROLLBACK`, `FETCH`.
  - `PAYLOAD`: `(sql, params, timeout)`.
- **Response**: `[MSG_TYPE, REQUEST_ID, STATUS, DATA]`
  - `STATUS`: `OK`, `ERROR`.
  - `DATA`: Serialized rows (MsgPack) or Arrow Buffer Handle (for zero-copy).

## 4. Typing Shims
We will reuse the `_typing.py` pattern found in `sqlspec` to ensure we don't hard-crash if optional dependencies (like `sqlalchemy` or `sqlspec`) are missing from the worker environment.

## 5. File Structure
```
src/py/vertebra/
  extensions/
    sqlspec/
      __init__.py
      adapter.py  # AsyncIPCDriver
    sqlalchemy/
      __init__.py
      dialect.py  # VertebraDialect
      dbapi.py    # Fake DBAPI
```
