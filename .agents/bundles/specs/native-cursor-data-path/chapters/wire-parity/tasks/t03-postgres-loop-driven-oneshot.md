---
type: Task
id: "native-cursor-data-path:wire:t03"
chapter_id: wire-parity
title: "PostgreSQL async loop-driven one-shot fast path"
description: "Drive cached one-shot PostgreSQL async pool queries on the calling Python event loop thread for plain TCP and Unix sockets, removing both thread crossings while preserving fallback, cancellation, and pool hygiene."
worksheet_format: compact
status: stable
state: completed
priority: P1
verification_strategy: targeted_unit_and_integration
depends_on:
  - "native-cursor-data-path:wire:t01"
read_set:
  - "src/rs/vertebra-db/src/connection.rs:272-315"
  - "src/rs/vertebra-postgres/src/pool.rs:296-336"
  - "src/rs/vertebra-postgres/src/pool.rs:410-460"
  - "src/rs/vertebra-postgres/src/pool.rs:835-880"
  - "src/rs/vertebra-postgres/src/session.rs:2063-2230"
  - "src/rs/vertebra-postgres/src/python_pool.rs:765-865"
  - "src/rs/vertebra-postgres/src/cursor.rs:951-1040"
files:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/metadata.rs
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/cursor.rs
  - src/rs/vertebra-postgres/src/python_pool.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/src/pool.rs
  - src/py/tests/unit/db/test_postgres_driver.py
estimated_loc: 280
plan_revision: 5
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T20:40:00Z
sources:
  - id: wire-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md
    title: "PostgreSQL hop budget and charter Q1 to Q8"
  - id: transport-stream
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: "Plain TCP, TLS, and Unix stream variants"
  - id: pg-pool
    resource: src/rs/vertebra-postgres/src/pool.rs::PostgresPool
    title: "Physical PostgreSQL connection pool and idle queue"
  - id: pg-session
    resource: src/rs/vertebra-postgres/src/session.rs::PostgresConnection
    title: "Cached named statement execution and wire frame handling"
  - id: pg-python-pool
    resource: src/rs/vertebra-postgres/src/python_pool.rs::PyAsyncPostgresPool
    title: "Async pool single-call Python methods"
tags: [performance, postgres, asyncio, rust, pyo3]
---

# t03 — PostgreSQL async loop-driven one-shot fast path

## Objective & Scope Boundary

`wire:t01` measured two thread crossings and `4.0` wake syscalls per query on `PyAsyncPostgresPool` (`95.60 us` caller-to-Tokio plus `44.94 us` Tokio-to-loop in the traced run). This task eliminates both crossings for cached one-shot queries on idle plain TCP or Unix pooled connections by encoding and writing on the calling event-loop thread and reading and resolving the future inside an `asyncio` loop reader callback.[^wire-spec]

In scope:

- Non-blocking plain-socket helpers (`raw_fd_if_plain`, `try_write_plain`, `try_read_buf_plain`) on `TransportStream` and `ConnectionStream`.[^transport-stream]
- Synchronous idle checkout (`PostgresPool::try_acquire_idle`) with stale-connection retirement dispatched via `db_runtime_handle().spawn`.[^pg-pool]
- Synchronous parameter metadata resolution (`try_resolve_parameter_metadata_sync`) for built-in types whose OIDs require no catalog discovery round trip.
- Synchronous cached-statement begin/read helpers on `PostgresConnection` sharing the protocol framing and decode logic of `execute_cached_named_statement`.[^pg-session]
- Loop-driven one-shot path inside `PyAsyncPostgresPool::{fetch_rows, fetch_dicts, fetch_value, execute_dml}` with automatic fallback to `spawn_single_call` on any unsupported case, clean cancellation (`remove_reader` plus `CancelRequest` and socket retirement), and clean fallback on event loops without `add_reader`.[^pg-python-pool]

Out of scope: TLS streams, uncached statements, transactions, pinned sessions, COPY, LISTEN/NOTIFY, pipelines, portals, Arrow streams, replication, and queries with explicit `timeout` or `parameter_oids`. All of those stay on `spawn_single_call`.

## Bounded Read Manifest

- `src/rs/vertebra-db/src/connection.rs:272-315` for `TransportStream` variants.
- `src/rs/vertebra-postgres/src/pool.rs:296-336`, `410-460`, `835-880` for `IdleEntry`, `PooledConnection`, and `PostgresPool`.
- `src/rs/vertebra-postgres/src/session.rs:2063-2230` for cached named statement execution.
- `src/rs/vertebra-postgres/src/python_pool.rs:765-865` and `cursor.rs:951-1040` for `PyAsyncPostgresPool` single-call entry points.

## Contracts & Edge Cases

1. **Preconditions for loop-driven execution**: `timeout.is_none()`, `parameter_oids.is_none()`, Unix target (`#[cfg(unix)]`), running event loop supports `add_reader`, `PostgresPool::try_acquire_idle` returns an idle recently-used `PlainTcp` or `Unix` connection whose `read_buf` and `write_buf` are empty, `exchange_state == Idle`, `transaction_status == Idle`, `open_portals.is_empty()`, `statement_cache.evicted_names().is_empty()`, `try_resolve_parameter_metadata_sync` succeeds, and `statement_cache.get_by_key` hits.
2. **Lossless fallback ordering**: register `loop.add_reader(fd, driver)` before `try_write_plain`. If `add_reader` fails (for example `NotImplementedError` on loops without reader support), mark the loop so future calls skip directly to `spawn_single_call`, return the untouched lease via `return_clean()`, and fall back. If `try_write_plain` returns `WouldBlock`, call `remove_reader(fd)`, clear `write_buf`, call `return_clean()`, and fall back. If `try_write_plain` performs a partial write (`0 < n < len`), call `remove_reader(fd)`, advance `write_buf`, set `InExtendedExchange`, and finish flushing and reading on a Tokio task via `db_runtime_handle().spawn` without corrupting wire state.
3. **Reader callback**: drain socket bytes via `try_read_buf_plain` and parse frames via `parse_buffered_message` until `ReadyForQuery` or `WouldBlock`. On `ReadyForQuery`, call `loop.remove_reader(fd)`, call `fut.remove_done_callback(driver)`, return the lease via `return_clean()` when clean (no `ParameterStatus` drift, no notifications, `is_clean_oneshot_query` true, `Idle` state) or spawn `lease.release()` on `db_runtime_handle()` otherwise, materialize the Python value with the requested builder (`rows_and_names`, `row_dicts`, `first_value`, `dml_outcome`), and resolve the future.
4. **Cancellation**: if `fut.cancel()` runs before `ReadyForQuery`, remove the loop reader, mark the lease for retirement, and spawn a background Tokio task via `db_runtime_handle()` that triggers `PostgresCancelHandle::cancel()` and `lease.release()`. Never return a cancelled in-flight socket to the idle queue.
5. **Multi-loop and free-threaded safety**: `add_reader` is removed before the lease returns to `PostgresPool`, so any other loop or thread can borrow the connection without cross-loop selector state. All shared state in the reader callback lives behind a `Mutex<Option<...>>` on a `frozen, immutable_type` `#[pyclass]`.

## Test Matrix

| Check | Expected |
| --- | --- |
| Cached one-shot query on plain socket | Returns `asyncio.Future` directly on the event-loop thread |
| Error recovery and cancellation | Removes reader, retires socket on cancel, and leaves pool healthy |
| Fallback paths (timeout, `parameter_oids`, TLS, miss) | Delegates cleanly to `spawn_single_call` |
| Live PostgreSQL release benchmark | `vertebra-async` median ratio `<= 1.0` against `asyncpg` |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --lib
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_driver.py -p no:xdist -q
```

Live acceptance verification (with `postgres:17-alpine` fixture on a release build):

- `vertebra-async` median on `point_select_1` beats `asyncpg` (`ratio <= 1.0`).
- `strace -f` confirms `0.0` wake syscalls per cached query and sends/receives on the event-loop thread.

## Acceptance Criteria

- [x] `TransportStream` and `ConnectionStream` expose plain-socket non-blocking read/write and Unix raw fd helpers.
- [x] `PostgresPool::try_acquire_idle` pops a fresh idle lease synchronously without entering Tokio.
- [x] `PyAsyncPostgresPool` single-call methods run cached plain-socket one-shot queries on the event-loop thread and fall back cleanly on cache miss, TLS, partial write, `WouldBlock`, timeout, `parameter_oids`, and loops without `add_reader`.
- [x] Cancelling an in-flight loop-driven future removes the reader, sends `CancelRequest`, and retires the socket.
- [x] Live release benchmark shows `vertebra-async` at or below `asyncpg` on `point_select_1`.

[^wire-spec]: `.agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md`
[^transport-stream]: `src/rs/vertebra-db/src/connection.rs::TransportStream`
[^pg-pool]: `src/rs/vertebra-postgres/src/pool.rs::PostgresPool`
[^pg-session]: `src/rs/vertebra-postgres/src/session.rs::PostgresConnection`
[^pg-python-pool]: `src/rs/vertebra-postgres/src/python_pool.rs::PyAsyncPostgresPool`
