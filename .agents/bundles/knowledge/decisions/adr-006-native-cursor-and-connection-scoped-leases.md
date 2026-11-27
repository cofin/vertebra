---
type: Decision
title: 'ADR-006: Native Cursor and Connection-Scoped Leases'
description: "Each PyO3 SQL extension owns a PEP 249-shaped cursor, and broker leases are taken per connection checkout."
status: stable
tags:
- decisions
- adr
- database
- dbapi
- leases
generated:
  by: flow/0.30.1
  at: '2026-11-21T01:16:00Z'
sources:
- id: sqlite-cursor
  resource: src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor
  title: Reference native cursor implementation
- id: dbapi-module
  resource: src/py/vertebra/db/dbapi/_sync.py
  title: DB-API exception classes, module globals, and connection
- id: sqlspec-base
  resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
  title: SQLSpec adapter base yielding the native cursor
- id: driver-common
  resource: src/py/vertebra/db/driver/_common.py::CommonDriverMixin
  title: Lease acquisition at connection checkout
verified:
- by: agent:flow-executor
  at: '2026-11-21T21:08:00Z'
  commit: 06e99fbf
- by: agent:flow-executor
  at: '2026-11-21T01:16:00Z'
  commit: 06e99fbf
---
# ADR-006: Native Cursor and Connection-Scoped Leases

<!-- truth: start -->
- Each of the eight SQL extensions exposes a sync and an async cursor with the PEP 249 shape; rows are tuples built once in Rust [^sqlite-cursor].
- The cursor and the single-call methods (`fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`) share one statement runner in each extension; neither is a second execution path [^sqlite-cursor].
- `description`, `rowcount`, and `lastrowid` come from the engine reply; Python does not scan SQL text [^sqlite-cursor] [^sqlspec-base].
- There is one exception hierarchy. The DB-API module binds its PEP 249 names to the `vertebra._vertebra` classes, and each extension raises them through its existing error conversion [^dbapi-module].
- Deviation from PEP 249: `InterfaceError` is a subclass of `DatabaseError`, and `Error` is the same class as `DatabaseError` [^dbapi-module].
- Network pools take one broker lease per connection checkout; embedded engines take none [^driver-common].
<!-- truth: end -->

## Context

Higher-level wrappers validate statements, convert parameters, and cache metadata before handing work to a database driver. The intermediate Python layers between those wrappers and the Rust extensions repeat that work on every query: scanning SQL text, wrapping native fetch calls in Python cursor objects, translating exceptions, and acquiring per-statement broker leases. On embedded engines, one interpreted Python frame costs about as much as the native engine call itself, so each extra wrapper layer doubles per-query latency. See the [Database Driver Limitations](../domains/data/database-driver-limitations.md) chapter for measured results.

## Decision

Each of the eight PyO3 SQL extensions owns a sync and an async cursor class that implements the PEP 249 shape directly in Rust [^sqlite-cursor]. Rows are materialized once as Python tuples inside the extension, and `description`, `rowcount`, and `lastrowid` come straight from the engine reply without Python SQL scanning. SQLSpec adapters yield the native cursor directly instead of wrapping it in a Python cursor layer [^sqlspec-base].

Each extension has one statement runner. The cursor stores the runner's outcome as cursor state, while the single-call methods (`fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`) return the same outcome directly and store no cursor state. Native-driver query methods call the single-call methods on SQLite, DuckDB, PostgreSQL, and Oracle, and the DB-API module and SQLSpec hook paths call the cursor. Bind planning, statement caching, and error conversion happen inside the shared statement runner, so they remain identical across both entry points [^sqlite-cursor].

Broker leases follow connection checkout rather than individual statements [^driver-common]. Network connection pools acquire one shared-memory lease when checking out a connection and release it when the connection or client closes, while embedded engines take no broker lease at all.

Vertebra keeps a single exception hierarchy rooted in the `DatabaseError` tree from `vertebra._vertebra` [^dbapi-module]. The DB-API module binds its PEP 249 exception names directly to those classes and defines only `Warning` itself, so each extension raises them through its existing error conversion. As an explicit deviation from PEP 249, `InterfaceError` is a subclass of `DatabaseError`, and `Error` is the same class as `DatabaseError`; specific subclasses such as `QueryCancelledError` and `SerializationFailure` reach DB-API callers unchanged.

## Alternatives Rejected

| Alternative | Trade-off |
| --- | --- |
| A slim Python cursor class over native `fetch_all` | Keeps one interpreted frame per `execute` and per `fetchall`, leaving wrapper overhead on every query. |
| Keeping the Python helpers as fallbacks | Requires maintaining two execution paths and keeping their edge cases aligned. |
| Per-statement leases with a cheaper lookup | Leaves lease overhead on every statement even though embedded engines need no lease. |
| Separate DB-API exception classes with a per-call translator, or a second error table in each extension | Adds work to every error and flattens specific classes to their PEP 249 parent. |
| Separate fast-path functions beside the cursor, each with its own bind and decode code | Two implementations of statement execution per extension that drift apart; a fix to one path misses the other. |

## Consequences

- Each SQL extension adds one public cursor class pair to its type stubs and stubtest surface [^sqlite-cursor].
- A change to how a statement runs is made once in the shared statement runner, and both the cursor tests and the single-call tests cover it [^sqlite-cursor].
- A worker holds its broker lease for as long as it holds the connection; fairness is decided at checkout [^driver-common].
- A Rust guard on the checked-out connection or client owns the lease, and dropping the guard returns it. Close, an error, task cancellation, and a driver dropped without `close()` all release the lease.
- Lease acquisition is best effort: it waits up to the broker timeout, and a refused or timed-out acquire lets the connection proceed without a lease.
- A single-client driver (MySQL, SQL Server, unpooled Oracle) holds its lease for the life of the client, so an idle open connection occupies a broker lease slot.
- The DB-API connection preserves begin-on-execute through the cursor's lazy-begin mode, so the transaction opens inside the extension [^dbapi-module].
- Native-driver conveniences with no wrapper above them (PostgreSQL named parameters, Oracle out-binds) live in Rust.
- Per-statement work the wire protocol requires is cached per SQL text inside the extension.
- The Oracle relay stays on by default and keeps a configuration switch.
- `except dbapi.DatabaseError` also catches interface errors, and `dbapi.Error` cannot be distinguished from `dbapi.DatabaseError`. Callers that need to separate them catch `InterfaceError` first [^dbapi-module].

[^sqlite-cursor]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor` — Reference native cursor implementation.
[^dbapi-module]: `src/py/vertebra/db/dbapi/_sync.py` — DB-API exception classes, module globals, and connection.
[^sqlspec-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver` — SQLSpec adapter base yielding the native cursor.
[^driver-common]: `src/py/vertebra/db/driver/_common.py::CommonDriverMixin` — Lease acquisition at connection checkout.
