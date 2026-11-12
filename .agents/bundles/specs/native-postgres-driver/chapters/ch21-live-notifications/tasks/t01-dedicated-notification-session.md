---
type: Task
id: "native-postgres-driver:ch21:t01"
chapter_id: ch21-live-notifications
title: Pump PostgreSQL notifications on a dedicated bounded session with reconnect gap state
description: Implement PostgresListener in vertebra-postgres/src/notifications.rs owning a dedicated PostgresConnection, single receive/control pump, 1024-frame and 8 MiB overflow termination, quoted LISTEN/UNLISTEN commands, and optional reconnect gap tracking.
status: stable
state: completed
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
plan_revision: 6
plan_commit: c8722c76
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T22:35:18Z"
last_verified_commit: c8722c76
verification_evidence: "cargo test -p vertebra-postgres --test postgres_notification_wire_tests (11 passed), cargo test --lib (12 passed), cargo fmt --check, and cargo clippy --all-targets -- -D warnings exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T22:35:18Z"
commit: e0ff8d78
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: c8722c76
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

Implement `PostgresListener` in `src/rs/vertebra-postgres/src/notifications.rs` (wired through `lib.rs` and `session.rs`) to pump unsolicited PostgreSQL `NotificationResponse` (`'A'`) frames on a dedicated direct/session `PostgresConnection` with a bounded `1024`-frame + `8 MiB` delivery queue, separate control channel for `LISTEN`/`UNLISTEN` alongside `close_notify` shutdown signaling, and optional recoverable-transport reconnect (`0.1s..5.0s` jitter, max `8` attempts) with `reconnected_gap` tagging [^pg-contracts] [^pg-protocol-research] [^pg-conn-rs]. PyO3 `AsyncPostgresListener` bindings land in `ch21:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 21 dedicated notification session, bounded overflow, and reconnect contracts.
- `src/rs/vertebra-postgres/src/session.rs` — `establish_postgres_connection` (Cloud SQL / AlloyDB / direct-IAM / TLS / SCRAM-SHA-256-PLUS / Unix / `connect_timeout`), `PoolerMode`, and `NoticeResponse`/`ParameterStatus` handling (`ch02`–`ch06`, `ch13:t03`).
- `src/rs/vertebra-postgres/src/protocol.rs` — `protocol::read_message` decoding `postgres_protocol::message::backend::Message::NotificationResponse` (`'A'`) with `connect_timeout` deadlines (`ch02:t01`).
- `src/rs/vertebra-postgres/src/config.rs` — `crate::config::PostgresConfig` carrying `pub pooler_mode: PoolerMode` (`ch01:t04`).
- [contracts.md](../../../contracts.md) — Dedicated event session ownership, `PoolerMode` exclusion, `closed` snapshot, and reconnect rules [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `NotificationResponse` push delivery, `NAMEDATALEN - 1` (`63`-byte) channel bounds, and identifier quoting [^pg-protocol-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod notifications;`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Expose `pub(crate) async fn establish_standalone_connection(config: &PostgresConfig, replication_database: bool) -> Result<PostgresConnection, PostgresError>`: reuses full Cloud SQL / AlloyDB / direct-IAM token refresh / TLS / SCRAM-SHA-256-PLUS / Unix / `connect_timeout` startup, encoding `"replication" => "database"` in `StartupMessage` (`postgres_protocol::message::frontend::startup_message`) and skipping post-authentication `SET` GUC queries incompatible with walsender startup when `replication_database == true` (so both `ch21` notifications with `false` and `ch22` replication preflight/stream with `false`/`true` share one standalone dialer).
  - Expose internal raw-frame read/simple-command accessors on `PostgresConnection` (reading `postgres_protocol::message::backend::Message::NotificationResponse` via `protocol::read_message`) so `notifications::PostgresListener` drives the sole socket reader without borrowing any `PhysicalPool` lease.
- `src/rs/vertebra-postgres/src/notifications.rs`:
  - `pub const DEFAULT_NOTIFICATION_QUEUE_CAPACITY: usize = 1024;`
  - `pub const DEFAULT_NOTIFICATION_MAX_BYTES: usize = 8 * 1024 * 1024;`
  - `pub const MAX_CHANNEL_BYTES: usize = 63;`
  - `pub const MAX_RECONNECT_ATTEMPTS: u32 = 8;`
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct NotificationFrame { pub pid: i32, pub channel: String, pub payload: Bytes, pub reconnected_gap: bool }`
  - `#[derive(Clone, Copy, Debug)] pub struct ListenerOptions { pub queue_capacity: usize, pub max_queue_bytes: usize, pub reconnect: bool }`
  - `pub fn validate_and_quote_channel(channel: &str) -> Result<String, crate::errors::PostgresError>`:
    - Enforces PostgreSQL identifier syntax AND `1 <= channel.len() <= 63` UTF-8 bytes (`NAMEDATALEN - 1`, rejecting empty strings, `> 63`-byte UTF-8 names, embedded `\0`, dots/whitespace/invalid syntax with `crate::errors::PostgresError::Data`), and returns `"\"<escaped>\""` (`"` -> `""`) with zero SQL value interpolation.
  - `pub struct PostgresListener`:
    - Validates `queue_capacity > 0`, `max_queue_bytes > 0`, and explicit `PoolerMode::Direct | PoolerMode::Session` on `config.pooler_mode` (returning `Err(PostgresError::Interface)` immediately before dialing when `config.pooler_mode` is `PoolerMode::Transaction`, `PoolerMode::Statement`, or `PoolerMode::Auto`, matching `derive_direct_config()`), then connects via `crate::session::establish_standalone_connection(&config, false)`.
    - Empty initial channel list sends zero `LISTEN` commands and remains cleanly closable; non-empty deduplicated list issues `LISTEN "<ch>"` and awaits `ReadyForQuery` before resolving `open`.
    - Spawns ONE Tokio pump task selecting over `close_notify` shutdown wakeup, a separate bounded control `mpsc::channel` (`Subscribe`, `Unsubscribe`), and socket reads (`Message::NotificationResponse`) so high-rate notifications never starve control commands; `Message::NotificationResponse` frames arriving during `LISTEN`/`UNLISTEN` responses enqueue in wire order; duplicated `subscribe`/`unsubscribe` calls for already-subscribed or unsubscribed channels are idempotent.
    - Maintains a `1`-slot unconsumed front buffer (`pub(crate) fn push_front_unconsumed(&self, frame: NotificationFrame)`) in front of the bounded `VecDeque<NotificationFrame>`: if a Tokio `next()` waiter drops after popping/receiving a frame or an async PyO3 `PendingDeliveryGuard` (`ch21:t02`) drops before Python receives the tuple while the listener remains open, the unconsumed `NotificationFrame` (including `reconnected_gap`) returns to the front slot and is yielded first on the next `next()` call without overflow or reorder (`close()` clears both the front slot and queue).
    - Enforces both `queued_frames <= queue_capacity` and `queued_payload_bytes <= max_queue_bytes`: on breach, discards the socket, sets `closed = true` (`AtomicBool`), and terminates `next()` with explicit `crate::errors::PostgresError::Operational("notification queue overflow")` — never silently dropping oldest or newest frames.
    - Single-waiter guard on `pub async fn next(&self) -> Result<Option<NotificationFrame>, crate::errors::PostgresError>`: concurrent `next()` calls return `Err(crate::errors::PostgresError::Interface)` immediately.
    - Optional `reconnect = true`: retries recoverable I/O / unexpected EOF drops only (never authentication `28*`, protocol, SQL error, or overflow failures) with bounded exponential backoff `100ms..5000ms` + jitter up to `8` attempts via `establish_standalone_connection(&config, false)` (refreshing IAM login tokens/cloud certs when configured), resubscribing the current active channel snapshot through `ReadyForQuery` and marking the next delivered `NotificationFrame` with `reconnected_gap = true`.
    - `pub fn is_closed(&self) -> bool` (non-blocking `AtomicBool` snapshot: `false` while active or in recoverable reconnect backoff; `true` after clean EOF with `reconnect = false`, terminal error, exhausted retries, or `close()`).
    - `pub async fn close(&self) -> Result<(), crate::errors::PostgresError>` and `Drop`: cancels receive/backoff/control waits immediately, closes socket (`Terminate` best-effort), wakes any pending `next()` with `Ok(None)`, clears front slot and queued frames, sets `closed = true`, and joins the pump task within `connect_timeout` with zero Python callbacks on Tokio workers.

## Edge Cases & Error Semantics

- Invalid channel in a multi-channel `open`, `subscribe`, or `unsubscribe` batch fails atomically before emitting any `LISTEN`/`UNLISTEN` wire bytes.
- `NoticeResponse` and `ParameterStatus` frames interleaved between idle `NotificationResponse` frames or during `LISTEN` `ReadyForQuery` handshakes are absorbed cleanly without disrupting notification order or payload bytes (including empty `b""`, valid UTF-8, and duplicate back-to-back notifications).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs`
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Idle push delivery, byte-by-byte framing, interleaved `NoticeResponse` / `ParameterStatus`, and `max_size = 1` query pool independence | Listener open on `"orders"` while a `max_size = 1` `PhysicalPool` runs concurrent `SELECT 1`; peer pushes byte-split `NotificationResponse` (empty `b""`, UTF-8 `"{\"id\":42}"`, and duplicate frames) + `NoticeResponse` | `next()` yields exact ordered `NotificationFrame`s (`reconnected_gap == false`) without client queries; `max_size = 1` pool executes queries with zero contention. |
| Notification arriving mid-`LISTEN` handshake, `PoolerMode` rejection, `63`-byte channel cap, and `push_front_unconsumed` retention | (a) Open with `[]`, `subscribe(["a", "a"])` while peer sends `Message::NotificationResponse` before `CommandComplete` + `ReadyForQuery`, plus `push_front_unconsumed` / cancelled `next()` retention; (b) `config.pooler_mode = PoolerMode::Transaction | Statement | Auto`, `"a".repeat(64)`, or `subscribe(["good", "bad;drop"])` | (a) `open([])` sends no `LISTEN`, `subscribe` sends one `LISTEN "a"`, resolves after `ReadyForQuery`, and retained/interleaved frame yields first on next `next()`; (b) rejects with `PostgresError::Interface` (`PoolerMode::Transaction | Statement | Auto`, `0` listener dials) or `PostgresError::Data` (`> 63` bytes / syntax) before dialing or writing SQL. |
| Bounded frame/byte overflow fail-closed termination & `replication_database` startup helper | (a) Configure `queue_capacity = 3` (or `max_queue_bytes = 64`), push `4` frames without calling `next()`; (b) call `establish_standalone_connection(&cfg, true)` against scripted peer | (a) Listener closes socket, sets `is_closed() == true`, and `next()` returns `Err(PostgresError::Operational)`; (b) StartupMessage includes `"replication" => "database"` and skips post-auth `SET` GUCs. |
| `reconnect = false` EOF vs `reconnect = true` backoff, resubscribe, `reconnected_gap`, and `close()` during backoff | (a) Peer drops idle socket with `reconnect = false`; (b) peer drops idle/subscribe socket with `reconnect = true`, accepts 2nd attempt, receives `LISTEN` for active channels, then pushes event; (c) call `close()` mid-backoff | (a) Surfaces transport error immediately and sets `is_closed() == true`; (b) `is_closed()` stays `false` during backoff, resubscribes active set, and yields next frame with `reconnected_gap == true`; (c) `close()` aborts backoff within `100ms` and sets `is_closed() == true`. |

## Verification

- Initial Red baseline: `PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests`
- Final Green + Clippy gate:
  ```bash
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): own dedicated notification receive sessions`

## Acceptance Criteria

- [x] `session::establish_standalone_connection(config, replication_database)` supports both ordinary standalone sessions and `replication=database` startup, and `PostgresListener::open` rejects `PoolerMode::Transaction | PoolerMode::Statement | PoolerMode::Auto` (requiring explicit `PoolerMode::Direct | PoolerMode::Session`) with `PostgresError::Interface`, enforces `1..=63` UTF-8 channel bytes + double-quoting, and pumps `Message::NotificationResponse` without starving `max_size = 1` pools (R21).
- [x] Bounded `1024`-frame and `8 MiB` byte queues terminate with explicit `PostgresError::Operational` on overflow, `push_front_unconsumed` preserves cancelled/unconsumed frames in order while open, and `close()` / `Drop` shut down the socket and pump task boundedly (R21).
- [x] `is_closed()`, single-waiter `next()`, and optional `reconnect = true` (`0.1s..5.0s` jitter, max `8` attempts, refreshed credentials, active-channel resubscribe through `ReadyForQuery`, and `reconnected_gap = true`) satisfy all state and wire invariants (R21).

## Notes & Discoveries

`establish_standalone_connection(config, replication_database)` omits session GUC parameters and post-authentication `SET` commands when `replication_database == true`, providing the shared dialer seam for `ch22` logical replication alongside `ch21` standalone listeners.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Dedicated notification session ownership, closed snapshot, and at-most-once reconnect rules.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL NotificationResponse wire framing and LISTEN/UNLISTEN ReadyForQuery lifecycle.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Dedicated TCP/TLS/Unix TransportStream read/write pump seam.
