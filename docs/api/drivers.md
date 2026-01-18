# Bun-Backed Drivers API

This document summarizes the Python-facing driver interfaces provided by Vertebra.

## SyncDriver / AsyncDriver

- `SyncDriver.connect()` -> `SyncConnection`
- `SyncDriver.pool(min_size=1, max_size=10)` -> `SyncPool`
- `AsyncDriver.connect()` -> `AsyncConnection`
- `AsyncDriver.pool(min_size=1, max_size=10)` -> `AsyncPool`

## SyncConnection (DB-API 2.0)

- `cursor()` -> `SyncCursor`
- `commit()`
- `rollback()`
- `close()`

## SyncCursor

- `execute(query, params=None)`
- `executemany(query, params)`
- `fetchone()`
- `fetchmany(size=None)`
- `fetchall()`
- `close()`

## AsyncConnection

- `fetch(query, params=None)`
- `fetchrow(query, params=None)`
- `fetchval(query, params=None)`
- `execute(query, params=None)`
- `executemany(query, params)`
- `close()`

## SyncPool

- `acquire()`
- `release(connection)`
- `close()`
- `get_size()`
- `get_idle_size()`

## AsyncPool

- `acquire()`
- `release(connection)`
- `close()`
- `get_size()`
- `get_idle_size()`

## Exceptions

Vertebra exposes a DB-API compatible exception hierarchy in `vertebra.drivers.exceptions`.

## Backend Registry

Custom backends can be registered for bridge/plugin scenarios:

- `register_driver_backend(name, factory)` registers a backend factory.
- `available_driver_backends()` lists registered backends.
- `get_driver_backend(config)` resolves a backend for a `DriverConfig`.

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
