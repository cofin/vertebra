---
type: Task
id: "native-postgres-driver:ch21:t01"
chapter_id: ch21-live-notifications
title: Pump PostgreSQL notifications on a dedicated bounded session with reconnect gap state
description: Implement PostgresListener in vertebra-postgres/src/notifications.rs owning a dedicated PostgresConnection, single receive/control pump, 1024-frame and 8 MiB overflow termination, quoted LISTEN/UNLISTEN commands, and optional reconnect gap tracking.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch20:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch21-live-notifications/spec.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/config.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/notifications.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs
estimated_loc: 235
plan_revision: 4
plan_commit: 45ff9563
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Dedicated notification session ownership, closed snapshot, and at-most-once reconnect rules
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL NotificationResponse wire framing and LISTEN/UNLISTEN ReadyForQuery lifecycle
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Dedicated TCP/TLS/Unix TransportStream read/write pump seam
tags:
  - postgres
  - rust
  - notifications
  - wire-protocol
  - backpressure
---

# Task t01: Pump PostgreSQL Notifications on a Dedicated Bounded Session with Reconnect Gap State

## Objective & Scope Boundary

Implement `PostgresListener` in `src/rs/vertebra-postgres/src/notifications.rs` (wired through `lib.rs` and `session.rs`) to pump unsolicited PostgreSQL `NotificationResponse` (`'A'`) frames on a dedicated direct/session `PostgresConnection` with a bounded `1024`-frame + `8 MiB` delivery queue, separate control channel for `LISTEN`/`UNLISTEN`/`close`, and optional recoverable-transport reconnect (`0.1s..5.0s` jitter, max `8` attempts) with `reconnected_gap` tagging [^pg-contracts] [^pg-protocol-research] [^pg-conn-rs]. PyO3 `AsyncPostgresListener` bindings land in `ch21:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 21 dedicated notification session, bounded overflow, and reconnect contracts.
- `src/rs/vertebra-postgres/src/session.rs` — Direct `PostgresConnection::connect` (including Cloud SQL / AlloyDB / direct-IAM refresh), `SessionHandle` (carrying `PoolerMode` from `crate::session::statements::PoolerMode`), and `NoticeResponse`/`ParameterStatus` handling (`ch02`–`ch06`, `ch13:t03`).
- `src/rs/vertebra-postgres/src/protocol.rs` — Bounded `NotificationResponse` (`'A'`, `{ pid: i32, channel: String, payload: Bytes }`) wire frame decoder (`ch02:t01`).
- `src/rs/vertebra-postgres/src/config.rs` — `crate::config::PostgresConfig` direct connection configuration (raw `PostgresConfig` does not carry `pooler_mode`) (`ch01:t04`).
- [contracts.md](../../../contracts.md) — Dedicated event session ownership, `closed` snapshot, and reconnect rules [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `NotificationResponse` push delivery and identifier quoting [^pg-protocol-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod notifications;`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Expose internal raw-frame read/simple-command helper on `PostgresConnection` (decoded via `src/rs/vertebra-postgres/src/protocol.rs`) so `notifications::PostgresListener` drives the sole socket reader without borrowing any `PhysicalPool` lease.
- `src/rs/vertebra-postgres/src/notifications.rs`:
  - `pub const DEFAULT_NOTIFICATION_QUEUE_CAPACITY: usize = 1024;`
  - `pub const DEFAULT_NOTIFICATION_MAX_BYTES: usize = 8 * 1024 * 1024;`
  - `pub const MAX_RECONNECT_ATTEMPTS: u32 = 8;`
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct NotificationFrame { pub pid: i32, pub channel: String, pub payload: Bytes, pub reconnected_gap: bool }`
  - `#[derive(Clone, Copy, Debug)] pub struct ListenerOptions { pub queue_capacity: usize, pub max_queue_bytes: usize, pub reconnect: bool }`
  - `pub fn validate_and_quote_channel(channel: &str) -> Result<String, crate::errors::PostgresError>`:
    - Rejects empty strings, embedded `\0`, dots/whitespace/invalid identifier syntax (`crate::errors::PostgresError::Data`), and returns `"\"<escaped>\""` (`"` -> `""`) with zero SQL value interpolation.
  - `pub struct PostgresListener`:
    - Validates positive `queue_capacity > 0` and `max_queue_bytes > 0` before connecting via `crate::config::PostgresConfig` (note `PoolerMode` lives in `crate::session::statements::PoolerMode` / `SessionHandle` rather than raw `PostgresConfig`, so `PoolerMode::Transaction` / `PoolerMode::Statement` rejection on listeners happens on pooled `SessionHandle` and Python `PostgreSQLConfig.derive_direct_config()` in `ch21:t03`).
    - Opens its own `PostgresConnection` (empty initial channel list sends zero `LISTEN` commands and remains cleanly closable; non-empty deduplicated list issues `LISTEN "<ch>"` and awaits `ReadyForQuery` before resolving `open`).
    - Spawns ONE Tokio pump task selecting over socket reads and a separate bounded control `mpsc::channel` (`Subscribe`, `Unsubscribe`, `Close`) so high-rate notifications cannot starve control commands or vice versa; `NotificationResponse` frames arriving during `LISTEN`/`UNLISTEN` responses enqueue through the same pump in wire order; duplicated `subscribe`/`unsubscribe` calls for already-subscribed or unsubscribed channels are idempotent.
    - Enforces both `queued_frames <= queue_capacity` and `queued_payload_bytes <= max_queue_bytes`: on breach, discards the socket, sets `closed = true` (`AtomicBool`), and terminates `next()` with explicit `crate::errors::PostgresError::Operational("notification queue overflow")` — never silently dropping oldest or newest frames.
    - Single-waiter guard on `pub async fn next(&self) -> Result<Option<NotificationFrame>, crate::errors::PostgresError>`: concurrent `next()` calls return `Err(crate::errors::PostgresError::Interface)` immediately.
    - Optional `reconnect = true`: retries recoverable I/O / unexpected EOF drops only (never authentication `28*`, protocol, SQL error, or overflow failures) with bounded exponential backoff `100ms..5000ms` + jitter up to `8` attempts, reconnecting via `crate::config::PostgresConfig` (refreshing IAM login tokens/cloud certs when configured), resubscribing the current active channel snapshot through `ReadyForQuery` (preserving existing queued count/byte caps and rejecting stale-generation responses), and marking the next delivered `NotificationFrame` with `reconnected_gap = true`.
    - `pub fn is_closed(&self) -> bool` (non-blocking `AtomicBool` snapshot: `false` while active or in recoverable reconnect backoff; `true` after clean EOF with `reconnect = false`, terminal error, exhausted retries, or `close()`).
    - `pub async fn close(&self) -> Result<(), crate::errors::PostgresError>` and `Drop`: cancels receive/backoff/control waits immediately, closes socket (`Terminate` best-effort), wakes any pending `next()` with `Ok(None)`, clears queued frames, sets `closed = true`, and joins the pump task within `connect_timeout` with zero Python callbacks on Tokio workers.

## Edge Cases & Error Semantics

- Invalid channel in a multi-channel `open`, `subscribe`, or `unsubscribe` batch fails atomically before emitting any `LISTEN`/`UNLISTEN` wire bytes.
- `NoticeResponse` and `ParameterStatus` frames interleaved between idle `NotificationResponse` frames or during `LISTEN` `ReadyForQuery` handshakes are absorbed cleanly without disrupting notification order or payload bytes (including empty `b""`, valid UTF-8, and duplicate back-to-back notifications).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs`
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Idle push delivery, byte-by-byte framing, interleaved `NoticeResponse` / `ParameterStatus`, and `max_size = 1` query pool independence | Listener open on `"orders"` while a `max_size = 1` `PhysicalPool` runs concurrent `SELECT 1`; peer pushes byte-split `NotificationResponse` (empty `b""`, UTF-8 `"{\"id\":42}"`, and duplicate frames) + `NoticeResponse` | `next()` yields exact ordered `NotificationFrame`s (`reconnected_gap == false`) without client queries; `max_size = 1` pool executes queries with zero contention. |
| Notification arriving mid-`LISTEN` handshake, empty initial channels, and idempotent/invalid channels | (a) Open with `[]`, `subscribe(["a", "a"])` while peer sends `NotificationResponse` before `CommandComplete` + `ReadyForQuery`; (b) `subscribe(["good", "bad;drop"])` | (a) `open([])` sends no `LISTEN`, `subscribe` sends one `LISTEN "a"`, resolves after `ReadyForQuery`, and delivers the interleaved notification; (b) fails before writing any bytes. |
| Bounded frame/byte overflow fail-closed termination | Configure `queue_capacity = 3` (or `max_queue_bytes = 64`), push `4` frames without calling `next()` | Listener closes socket, sets `is_closed() == true`, and `next()` returns `Err(PostgresError::Operational)` (never silently dropping frames). |
| `reconnect = false` EOF vs `reconnect = true` backoff, resubscribe, `reconnected_gap`, and `close()` during backoff | (a) Peer drops idle socket with `reconnect = false`; (b) peer drops idle/subscribe socket with `reconnect = true`, accepts 2nd attempt, receives `LISTEN` for active channels, then pushes event; (c) call `close()` mid-backoff | (a) Surfaces transport error immediately and sets `is_closed() == true`; (b) `is_closed()` stays `false` during backoff, resubscribes active set, and yields next frame with `reconnected_gap == true`; (c) `close()` aborts backoff within `100ms` and sets `is_closed() == true`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): own dedicated notification receive sessions`

## Acceptance Criteria

- [ ] `PostgresListener` owns a dedicated non-pooled `PostgresConnection` and sole Tokio receive/control task, validates and double-quotes channel identifiers, and delivers idle and interleaved `NotificationResponse` frames without starving `max_size = 1` query pools (R21).
- [ ] Bounded `1024`-frame and `8 MiB` byte queues terminate with explicit `PostgresError::Operational` on overflow rather than silently dropping events, and `close()` / `Drop` shut down the socket and pump task boundedly (R21).
- [ ] `is_closed()`, single-waiter `next()`, and optional `reconnect = true` (`0.1s..5.0s` jitter, max `8` attempts, refreshed credentials, active-channel resubscribe through `ReadyForQuery`, and `reconnected_gap = true`) satisfy all state and wire invariants (R21).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Dedicated notification session ownership, closed snapshot, and at-most-once reconnect rules.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL NotificationResponse wire framing and LISTEN/UNLISTEN ReadyForQuery lifecycle.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Dedicated TCP/TLS/Unix TransportStream read/write pump seam.
