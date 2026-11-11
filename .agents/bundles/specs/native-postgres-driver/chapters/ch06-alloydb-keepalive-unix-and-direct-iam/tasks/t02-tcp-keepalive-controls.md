---
type: Task
id: "native-postgres-driver:ch06:t02"
chapter_id: ch06-alloydb-keepalive-unix-and-direct-iam
title: Apply validated TCP keepalive controls to physical sessions
description: Validate keepalives, keepalives_idle, keepalives_interval, and keepalives_count in PostgresConfig, implement TransportStream::set_tcp_keepalive via socket2 on PlainTcp and TlsTcp sockets, and apply keepalive settings once before PostgreSQL startup.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch06:t01"
read_set:
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/session.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 210
plan_revision: 5
plan_commit: 0147d80c
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
updated_at: "2026-11-08T02:05:22Z"
commit: 2bcade62
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: PlainTcp, TlsTcp, and Unix variants on TransportStream
  - id: db-cargo
    resource: src/rs/vertebra-db/Cargo.toml
    title: vertebra-db Cargo dependencies
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native constructor keepalives, keepalives_idle, keepalives_interval, and keepalives_count contract
tags:
  - postgres
  - keepalive
  - tcp
  - transport
---

# Task t02: Apply Validated TCP Keepalive Controls to Physical Sessions

## Objective & Scope Boundary

Implement `TcpKeepaliveConfig` and `TransportStream::set_tcp_keepalive` in `src/rs/vertebra-db/src/connection.rs` using `socket2 = "=0.6.5"`, validate `keepalives`, `keepalives_idle`, `keepalives_interval`, and `keepalives_count` in `src/rs/vertebra-postgres/src/config.rs`, and apply keepalive configuration once to the underlying physical TCP socket (both `PlainTcp` and `TlsTcp.get_ref().0`, never `Unix`) at `session.rs`'s unified pre-`StartupMessage` seam across direct TCP, direct TLS, Cloud SQL, and AlloyDB streams [^db-conn] [^pg-contracts]. Keep `ConnectOptions` struct fields unchanged so existing callers stay source-compatible. Windows CI `cargo check` wiring in `.github/workflows/release.yaml` is owned by `native-postgres-driver:ch08:t04`.

## Bounded Read Manifest

- `src/rs/vertebra-db/Cargo.toml` — `socket2 = { version = "=0.6.5", features = ["all"] }` dependency declared on `vertebra-db` and locked in `src/rs/Cargo.lock` by `ch01:t02` [^db-cargo].
- `src/rs/vertebra-db/src/connection.rs` — `TransportStream::{PlainTcp, TlsTcp, Unix}` and `TransportError::InvalidConfig` [^db-conn].
- `src/rs/vertebra-postgres/src/config.rs` — `PostgresConfig` scalar fields (`keepalives: bool`, `keepalives_idle: Option<i64>`, `keepalives_interval: Option<i64>`, `keepalives_count: Option<i64>`) from `ch01:t04`.
- `src/rs/vertebra-postgres/src/session.rs` — Unified pre-`StartupMessage` connect seam receiving direct `DialedTransport` (`tls::connect`) and cloud `DialedCloudTransport` (`cloud::dial_cloudsql` from `ch05:t04` and `cloud::dial_alloydb` from `ch06:t01`).
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Connection constructors` keepalive defaults and Unix exclusions [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/connection.rs`:
  - Define `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct TcpKeepaliveConfig`:
    - `pub enabled: bool`
    - `pub idle: Option<Duration>`
    - `pub interval: Option<Duration>`
    - `pub retries: Option<u32>`
    - `impl Default for TcpKeepaliveConfig` -> `{ enabled: true, idle: None, interval: None, retries: None }`.
  - Add `pub fn set_tcp_keepalive(&self, config: TcpKeepaliveConfig) -> Result<(), TransportError>` on `TransportStream`:
    - Extract `&tokio::net::TcpStream` from `Self::PlainTcp(tcp)` or `Self::TlsTcp(tls)` via `tls.get_ref().0`; return `Err(TransportError::InvalidConfig("TCP keepalive cannot be applied to a Unix domain socket".into()))` on `Self::Unix(_)`.
    - Reject `config.enabled == false` combined with any `Some(_)` in `idle`, `interval`, or `retries` (`TransportError::InvalidConfig`), and reject zero `Duration::ZERO` or `retries == Some(0)` (`TransportError::InvalidConfig`).
    - Obtain `socket2::SockRef::from(tcp)`. When `!config.enabled`, call `sock_ref.set_keepalive(false)` and return `Ok(())`.
    - When `config.enabled`, call `sock_ref.set_keepalive(true)?`; if any of `idle`, `interval`, or `retries` is `Some(_)`, construct `socket2::TcpKeepalive::new()` with `.with_time(idle)` and `.with_interval(interval)`, plus `.with_retries(count)` under `#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", target_os = "illumos", target_os = "solaris", target_os = "windows"))]` (returning `Err(TransportError::InvalidConfig(_))` on targets lacking `with_retries` rather than silently ignoring requested retries), and call `sock_ref.set_tcp_keepalive(&ka)?`.
- `src/rs/vertebra-postgres/src/config.rs`:
  - Validate constructor defaults (`keepalives = true`, `keepalives_idle = None`, `keepalives_interval = None`, `keepalives_count = None`):
    - Each `Some(v)` in `keepalives_idle`, `keepalives_interval`, and `keepalives_count` must satisfy `1 <= v <= i32::MAX as i64` (`1..=2_147_483_647` seconds / retry count); `<= 0` or `> i32::MAX` fails purely with `PostgresError::Config`.
    - `keepalives == false` with any `Some(_)` tuning parameter fails purely with `PostgresError::Config`.
    - Produce `TcpKeepaliveConfig` on `PostgresConfig` (`Duration::from_secs(v as u64)`, `retries: count.map(|c| c as u32)`).
- `src/rs/vertebra-postgres/src/session.rs`:
  - Because `cloud::dial_cloudsql` (`ch05:t04`) and `cloud::dial_alloydb` (`ch06:t01`) return pre-startup `DialedCloudTransport` to `session.rs` before `StartupMessage`, apply `stream.set_tcp_keepalive(config.tcp_keepalive())` once in `session.rs` immediately before encoding/writing `StartupMessage` across all physical TCP and TLS `TransportStream` paths (direct `PlainTcp`, post-`SSLRequest` `TlsTcp`, Cloud SQL `TlsTcp`, and AlloyDB `TlsTcp`, with zero edits to `cloud/mod.rs`), within the existing `ConnectAttempt` `absolute_deadline`.

## Edge Cases & Error Semantics

- `keepalives = false` with `keepalives_idle = None, keepalives_interval = None, keepalives_count = None` succeeds and clears `SO_KEEPALIVE` (`sock_ref.keepalive()? == false`) on the live TCP socket.
- `keepalives = false` with `keepalives_idle = Some(60)` fails purely in `PostgresConfig` (`PostgresError::Config` -> `ValueError`) and in `TransportStream::set_tcp_keepalive` (`TransportError::InvalidConfig`).
- `keepalives_idle = Some(0)`, `keepalives_interval = Some(-5)`, or `keepalives_count = Some(0)` fails purely with `PostgresError::Config`.
- Calling `set_tcp_keepalive` on `TransportStream::Unix` returns `Err(TransportError::InvalidConfig(_))` (Unix sessions in `ch06:t03` skip `set_tcp_keepalive` when `keepalives = true` with no tuning and reject explicit tuning in `config.rs`).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-db/src/connection.rs` (`tcp_keepalive_*`), `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` (`tcp_keepalive_*`), and `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (`tcp_keepalive_cloud_*`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Enabled default & explicit idle/interval/retries readback | `TcpKeepaliveConfig { enabled: true, idle: Some(Duration::from_secs(45)), interval: Some(Duration::from_secs(15)), retries: Some(4) }` on `PlainTcp` and `TlsTcp` | `set_tcp_keepalive` returns `Ok(())`; `SockRef::from(tcp).keepalive().unwrap() == true` (plus platform `KEEPIDLE`/`KEEPINTVL`/`KEEPCNT` `getsockopt` readback on Linux equals `45`, `15`, `4`). |
| Disabled keepalive clears `SO_KEEPALIVE` | `TcpKeepaliveConfig { enabled: false, idle: None, interval: None, retries: None }` after pre-enabling `SO_KEEPALIVE` | `set_tcp_keepalive` returns `Ok(())` and `SockRef::from(tcp).keepalive().unwrap() == false`. |
| Disabled with tuning or non-positive values rejected | `keepalives = false, keepalives_idle = Some(30)`, or `keepalives_idle = Some(0)`, `keepalives_interval = Some(-1)`, `keepalives_count = Some(0)`, `keepalives_idle = Some(i64::from(i32::MAX) + 1)` | Pure `PostgresConfig` validation returns `Err(PostgresError::Config(_))`; `set_tcp_keepalive` returns `Err(TransportError::InvalidConfig(_))`. |
| Unix stream rejection | `TransportStream::Unix` + `set_tcp_keepalive(TcpKeepaliveConfig::default())` | Returns `Err(TransportError::InvalidConfig(_))`. |
| Direct TCP & TLS startup session socket readback | Connect `SessionHandle` over loopback TCP and TLS (`SSLRequest`) with `keepalives = true, keepalives_idle = Some(30), keepalives_interval = Some(10), keepalives_count = Some(3)` | Session reaches `TransactionStatus::Idle` (`ReadyForQuery` `b'I'`) and underlying client socket reports `SO_KEEPALIVE == true` (and `30`/`10`/`3` on Linux) before `StartupMessage`. |
| Cloud-stream (`DialedCloudTransport`) TCP keepalive readback (`tcp_keepalive_cloud_streams`) | Connect `SessionHandle` over mock Cloud SQL `:3307` and AlloyDB `:5433` mTLS listeners with `keepalives = true, keepalives_idle = Some(30), keepalives_interval = Some(10), keepalives_count = Some(3)` (holding server `StartupMessage` read to inspect client `TlsTcp` socket) | `session.rs` applies `set_tcp_keepalive` on `DialedCloudTransport` before `StartupMessage`; client `TlsTcp` socket reports `SO_KEEPALIVE == true` (and `30`/`10`/`3` on Linux) and session reaches `TransactionStatus::Idle`. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib tcp_keepalive_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests tcp_keepalive_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests tcp_keepalive_cloud_`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib tcp_keepalive_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests tcp_keepalive_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests tcp_keepalive_cloud_`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] `PostgresConfig` validates `keepalives`, `keepalives_idle`, `keepalives_interval`, and `keepalives_count` purely against `contracts.md` bounds without accepting Python socket objects (R2, R3).
- [x] `TransportStream::set_tcp_keepalive` applies `socket2` keepalive parameters to `PlainTcp` and `TlsTcp` sockets and rejects disabled-with-tuning, zero/out-of-range values, or `Unix` streams via `TransportError::InvalidConfig` (R2, R3).
- [x] `session.rs` applies keepalive once before `StartupMessage` across direct TCP, direct TLS, Cloud SQL, and AlloyDB streams under `absolute_deadline` without touching `cloud/mod.rs`, and commit `feat(postgres): configure physical TCP keepalive` passes all `tcp_keepalive_*` tests (R2, R3).

## Notes & Discoveries

- `socket2 = { version = "=0.6.5", features = ["all"] }` is declared in `src/rs/vertebra-db/Cargo.toml` and locked in `src/rs/Cargo.lock` by `ch01:t02`. Windows MSVC `cargo check -p vertebra-db -p vertebra-postgres --all-targets` CI verification is wired in `ch08:t04` where `.github/workflows/release.yaml` is owned.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared `PlainTcp`, `TlsTcp`, and `Unix` transport stream variants.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — `vertebra-db` dependency manifest.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native constructor TCP keepalive parameters and defaults.
