# Bun-Backed Drivers API

This document summarizes the Python-facing driver interfaces provided by Vertebra.

## BaseDriver

- `connect()` -> `Connection`
- `connect_async()` -> `AsyncConnection`
- `pool(min_size=1, max_size=10)` -> `Pool`

## Connection (DB-API 2.0)

- `cursor()` -> `Cursor`
- `commit()`
- `rollback()`
- `close()`

## Cursor

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

## Pool

- `acquire()`
- `release(connection)`
- `close()`
- `get_size()`
- `get_idle_size()`

## Exceptions

Vertebra exposes a DB-API compatible exception hierarchy in `vertebra.drivers.exceptions`.

## SQLite Driver

`vertebra.drivers.sqlite.SqliteDriver` provides a Bun-backed SQLite interface over IPC. Configure with `SqliteDriverConfig` (database path + IPC config).

## PostgreSQL Driver

`vertebra.drivers.postgres.PostgresDriver` provides a Bun-backed PostgreSQL interface over IPC. Configure with `PostgresDriverConfig` (connection string + IPC config).

## Type Conversions

Helper functions for Postgres/MySQL/SQLite are available in `vertebra.drivers.types`:

- `encode_postgres`, `decode_postgres`
- `encode_mysql`, `decode_mysql`
- `encode_sqlite`, `decode_sqlite`
