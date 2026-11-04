---
type: Spec
flow_id: native-postgres-driver
title: Rust Postgres Client
description: Complete Rust PostgreSQL wire client in vertebra-db with auth, TLS, extended protocol, binary codecs, statement cache, COPY, LISTEN/NOTIFY, pipeline mode, cancellation, and Arrow results, replacing psycopg/asyncpg in the Python driver.
state: draft
parent_prd: native-data-platform
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [rust, postgres, driver, pipeline]
---

# Flow Charter: Rust Postgres Client

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Base.** `postgres-protocol` 0.6.12 supplies the frontend/backend codecs and SASL, including SCRAM-SHA-256-PLUS. On top of it sits a Vertebra-owned connection state machine that reuses the existing Parse/Bind/Execute/Sync encoders and `PostgresPool`.
- **TLS.** `tokio-postgres-rustls` 0.14 (`ring`).
- **Test oracle.** `tokio-postgres` 0.7.18 is a dev-dependency used as the reference implementation.
- **Why not wrap `tokio-postgres`.** Its `Client` sends one `Sync` per request and blocks raw frame injection, so it cannot do the required single-`Sync` batch pipeline.
- **Packaging.** Optional, behind the `postgres` Cargo feature and Python extra.

## Scope

1. **Connection.**
   - Startup message (existing `encode_startup_message`).
   - Auth: cleartext, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS.
   - TLS through `tokio-rustls` with `sslmode` semantics.
   - Cloud SQL / AlloyDB connectors through the existing `dialer.rs`.
   - IAM token passwords through `GcpTokenCache`.
   - Parameter status, backend key data, keepalive.
2. **Query protocols.**
   - Simple query.
   - Extended protocol: Parse/Bind/Describe/Execute/Sync (existing encoders), portal row limits for streaming, multi-result handling, and error/notice decoding with SQLSTATE.
3. **Codecs.**
   - Binary send/receive for core types: bool, int2/4/8, float4/8, numeric, text/varchar, bytea, uuid, date/time/timestamp/timestamptz/interval, json/jsonb (existing JSONB header codec), arrays, and composites.
   - pgvector `vector`/`halfvec`/`bit` (existing codecs).
   - Text fallback for unknown OIDs.
   - Python value mapping through msgspec.
4. **Statements and pooling.**
   - Named prepared statements through the existing `StatementCache`, with pooler-safe modes (`PoolerMode`, PgBouncer transaction mode).
   - Leases through `PostgresPool`.
5. **Bulk.** `COPY FROM STDIN`/`COPY TO STDOUT` binary (existing pgcopy codec), `load_from_arrow`, and `select_to_arrow`/`stream_arrow_batches` into VTBARROW.
6. **Notifications.** LISTEN/NOTIFY implementing `NotificationSource`, and `pg_logical_emit_message`.
7. **Pipelining.** `pipeline()` queues Parse/Bind/Execute for N statements with a single Sync and one flush, then demultiplexes results and errors per statement. Sync and async forms.
8. **Cancellation.** CancelRequest with the backend key, wired to `cancel()` and to asyncio cancellation.
9. **Python driver.** `db/adapters/postgres/driver.py` runs entirely on the Rust client. `psycopg_pool`, the asyncpg duck-typing, the `memory://` path, and `_in_memory_rows` are removed.

## Public Seams

`SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, `driver.pipeline()`, and the `NotificationSource` implementation.

## Dependencies

`db-layout-and-contract`.

## Acceptance Boundary

- The conformance suite passes for Postgres 14–18 through `pytest-databases`.
- Golden-byte fixtures cover startup, SCRAM, the extended protocol, COPY, and pipelining.
- The pipeline test shows one flush per pipeline.
- `make rs-bench`/`make bench-check` are at or above the psycopg baseline.

## Draft Task Outline

- t01 Connection, auth, and TLS.
- t02 Simple and extended query with error decoding.
- t03 Binary codecs.
- t04 Statement cache and pooler modes.
- t05 COPY and Arrow.
- t06 LISTEN/NOTIFY.
- t07 Pipeline mode and cancellation.
- t08 Python driver cutover.
