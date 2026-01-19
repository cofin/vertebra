# Vertebra Drivers & Integrations

This document summarizes the Python-facing driver interfaces and ORM integrations provided by Vertebra.

## High-Level Integrations

Vertebra provides first-class support for modern Python ORMs and database tools. These integrations are the recommended way to consume Vertebra's shared-memory data plane.

### SQLSpec (Recommended)

Vertebra provides fully compliant SQLSpec adapters that route queries through the shared-memory broker.

*   **Prefix**: `Vertebra*` (e.g., `VertebraPostgresSyncConfig`)
*   **Package**: `vertebra.extensions.sqlspec`

**Supported Adapters:**
*   `VertebraPostgresSyncConfig` / `VertebraPostgresAsyncConfig`
*   `VertebraMysqlSyncConfig` / `VertebraMysqlAsyncConfig`
*   `VertebraSqliteSyncConfig` / `VertebraSqliteAsyncConfig`

### SQLAlchemy

Native SQLAlchemy Dialects are provided to allow standard `create_engine` usage with Vertebra's pooling and IPC.

*   **Dialects**: `postgresql+vertebra://`, `mysql+vertebra://`, `sqlite+vertebra://`
*   **Package**: `vertebra.extensions.sqlalchemy`

### Advanced Alchemy

Integration for Advanced Alchemy Repositories and Services, leveraging the SQLAlchemy dialects and Vertebra's storage subsystem.

*   **Package**: `vertebra.extensions.advanced_alchemy`

---

## Low-Level Driver API

The foundation for the above integrations are the Bun-backed drivers. These implement a subset of DB-API 2.0.

### SyncDriver / AsyncDriver

- `SyncDriver.connect()` -> `SyncConnection`
- `SyncDriver.pool(min_size=1, max_size=10)` -> `SyncPool`
- `AsyncDriver.connect()` -> `AsyncConnection`
- `AsyncDriver.pool(min_size=1, max_size=10)` -> `AsyncPool`

### SyncConnection (DB-API 2.0)

- `cursor()` -> `SyncCursor`
- `commit()`
- `rollback()`
- `close()`

## SQLite Driver

`vertebra.drivers.sqlite.SyncSqliteDriver` provides a Bun-backed SQLite interface over IPC. Configure with `SyncSqliteDriverConfig` (database path + IPC config). Use `SyncSqliteDriver.connect()` for sync connections and `AsyncSqliteDriver.connect()` for async connections.

## PostgreSQL Driver

`vertebra.drivers.postgres.SyncPostgresDriver` provides a Bun-backed PostgreSQL interface over IPC. Configure with `SyncPostgresDriverConfig` (connection string + IPC config). Use `SyncPostgresDriver.connect()` for sync connections and `AsyncPostgresDriver.connect()` for async connections.

Transactions are available via `connection.transaction()` / `async_connection.transaction()` with optional isolation levels. The sync context is exposed as `SyncPostgresTransaction` to parallel `AsyncPostgresTransaction`.

## MySQL Driver

`vertebra.drivers.mysql.SyncMySQLDriver` provides a Bun-backed MySQL interface over IPC. Configure with `SyncMySQLDriverConfig` (connection string + IPC config). Use `SyncMySQLDriver.connect()` for sync connections and `AsyncMySQLDriver.connect()` for async connections.

## Redis Driver

`vertebra.drivers.redis.SyncRedisDriver` provides a Bun-backed Redis interface over IPC. Configure with `SyncRedisDriverConfig` (URL + IPC config). Use `SyncRedisDriver.connect()` for sync connections and `AsyncRedisDriver.connect()` for async connections.

## Type Conversions

Helper functions for Postgres/MySQL/SQLite are available in `vertebra.drivers.types`:

- `encode_postgres`, `decode_postgres`
- `encode_mysql`, `decode_mysql`
- `encode_sqlite`, `decode_sqlite`