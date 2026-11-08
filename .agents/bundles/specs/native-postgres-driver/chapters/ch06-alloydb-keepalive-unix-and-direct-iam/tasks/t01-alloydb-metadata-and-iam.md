---
type: Task
id: "native-postgres-driver:ch06:t01"
chapter_id: ch06-alloydb-keepalive-unix-and-direct-iam
title: Authenticate automatic AlloyDB metadata and IAM sessions across split TLS and metadata stages
description: Extract TransportStream::connect_alloydb_tls and finish_alloydb_metadata in vertebra-db, verify ScopedLoginSnapshot::ensure_usable immediately after AlloyDB TLS before emitting metadata bytes, and establish AlloyDB :5433 built-in and IAM sessions.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch05:t04"
read_set:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-db/src/dialer.rs
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/session.rs
  - .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 245
plan_revision: 4
plan_commit: e8901de4
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
commit: null
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared AlloyDB :5433 mTLS connection and peer verification
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::exchange_alloydb_metadata_over_stream
    title: AlloyDB length-prefixed protobuf MetadataExchangeRequest exchange
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Official AlloyDB Go connector v1alpha, :5433 TLS 1.3, and metadata auth_type rules
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Split connect_alloydb_tls and finish_alloydb_metadata post-TLS token guard contract
tags:
  - postgres
  - alloydb
  - mtls
  - iam
---

# Task t01: Authenticate Automatic AlloyDB Metadata and IAM Sessions Across Split TLS and Metadata Stages

## Objective & Scope Boundary

Extract `TransportStream::connect_alloydb_tls` and module-level `finish_alloydb_metadata` in `src/rs/vertebra-db/src/connection.rs` (refactoring `TransportStream::connect` to compose both stages under a single `absolute_deadline`), and implement `dial_alloydb` returning pre-startup `DialedCloudTransport` in `src/rs/vertebra-postgres/src/cloud/mod.rs` wired into `src/rs/vertebra-postgres/src/session.rs` [^db-conn] [^db-dialer] [^pg-contracts]. Immediately after `connect_alloydb_tls` verifies the server's TLS 1.3 IP/PSC DNS SAN identity and before `finish_alloydb_metadata` transmits any token-bearing `MetadataExchangeRequest` bytes, `dial_alloydb` invokes `login_snapshot.ensure_usable(clock.now_ms())`, dropping the stream with `0` metadata bytes and `PostgresError::Auth` (`GoogleCloudAuthError`) if the token expired or entered refresh skew during TLS, and returns `DialedCloudTransport` **before** PostgreSQL `StartupMessage` so `session.rs` executes `StartupMessage` and authentication uniformly across Cloud SQL and AlloyDB [^cloud-research] [^pg-contracts].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/connection.rs` — `TransportStream::connect`, `connect_inner`, `EndpointTarget::AlloyDb`, and `CloudPeerIdentity::AlloyDb` from `ch05:t01` [^db-conn].
- `src/rs/vertebra-db/src/dialer.rs` — `AlloyDbClusterUri`, `AuthType` (`Password = 1`, `Iam = 2`), and `exchange_alloydb_metadata_over_stream` [^db-dialer].
- `src/rs/vertebra-postgres/src/cloud/mod.rs` — `DialedCloudTransport`, `dial_cloudsql`, and `CloudConnector` integration from `ch04:t04` and `ch05:t04`.
- `src/rs/vertebra-postgres/src/session.rs` — `ConnectAttempt::run`, `PendingSession`, and `TransactionStatus::Idle` startup lifecycle from `ch05:t04`.
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — AlloyDB `:5433` metadata exchange (`alloydb.login` required in both `DB_NATIVE` and `AUTO_IAM` modes) [^cloud-research].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) AlloyDB split stage contract [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/connection.rs`:
  - `pub async fn connect_alloydb_tls(options: &ConnectOptions) -> Result<TransportStream, TransportError>` (on `TransportStream`):
    - Accept only `EndpointTarget::AlloyDb { .. }` (returning `Err(TransportError::InvalidConfig(_))` for any other `EndpointTarget`).
    - Perform TCP connect (`tcp_nodelay`) and `build_cloud_mtls_config` TLS 1.3 + `CloudPeerIdentity::AlloyDb` SAN handshake, returning `TransportStream::TlsTcp` **before** sending `MetadataExchangeRequest`. Callers wrap or bound this call under their own `absolute_deadline` without starting an independent second timeout budget.
  - `pub async fn finish_alloydb_metadata(stream: &mut TransportStream, options: &ConnectOptions, absolute_deadline: tokio::time::Instant) -> Result<(), TransportError>`:
    - Require `EndpointTarget::AlloyDb { user_agent, oauth2_token, auth_type, .. }` and `TransportStream::TlsTcp(tls_stream)` (else `Err(TransportError::InvalidConfig(_))`).
    - Check `tokio::time::Instant::now() >= absolute_deadline` before writing so an already-elapsed deadline returns `Err(TransportError::Timeout(options.connect_timeout))` with `0` bytes written; otherwise drive `tokio::time::timeout_at(absolute_deadline, exchange_alloydb_metadata_over_stream(tls_stream, user_agent, oauth2_token, *auth_type))`.
  - Refactor `TransportStream::connect` / `connect_inner` for `EndpointTarget::AlloyDb` to compose `connect_alloydb_tls` followed by `finish_alloydb_metadata` under the single `deadline` computed at `connect` entry, keeping `cloudsql_mtls_verifies_instance_cn_and_alloydb_runs_metadata_exchange` behavior unchanged.
- `src/rs/vertebra-postgres/src/cloud/mod.rs` & `src/rs/vertebra-postgres/src/session.rs`:
  - `pub(crate) async fn dial_alloydb(config: &PostgresConfig, connector: &CloudConnector, absolute_deadline: tokio::time::Instant) -> Result<DialedCloudTransport, PostgresError>`:
    - Obtain `PreparedCloudMaterial` from `connector.prepare(..., absolute_deadline)` (port `5433`, `ALLOYDB_LOGIN_SCOPE` `https://www.googleapis.com/auth/alloydb.login` fetched for **both** `vertebra_db::dialer::AuthType::Password` and `vertebra_db::dialer::AuthType::Iam`).
    - Drive `tokio::time::timeout_at(absolute_deadline, TransportStream::connect_alloydb_tls(&connect_opts))`.
    - Immediately call `login_snapshot.ensure_usable(connector.clock().now_ms())?`. On failure, drop `stream` immediately and return `PostgresError::Auth` (zero `MetadataExchangeRequest` bytes written).
    - Call `finish_alloydb_metadata(&mut stream, &connect_opts, absolute_deadline).await?`.
    - Compute `tls_server_end_point` from `stream.peer_leaf_der()` for `SCRAM-SHA-256-PLUS` and return `DialedCloudTransport { stream, tls_server_end_point, login_snapshot: config.enable_iam_auth.then_some(login_snapshot) }` **before** emitting PostgreSQL `StartupMessage` (and never emitting `SSLRequest`).
  - Route `ConnectAttempt::run` in `session.rs` to `cloud::dial_alloydb` when `config.alloydb_instance_uri` is `Some(_)`, then run `session.rs`'s unified `StartupMessage` + `auth.rs` engine (`AuthType::Iam` uses `dialed.login_snapshot` as password, re-checking `ensure_usable` in `auth.rs` before password/SASL write; `AuthType::Password` has `login_snapshot == None` and uses `config.password`) to `TransactionStatus::Idle`.

## Edge Cases & Error Semantics

- Non-zero `MetadataExchangeResponse` code, truncated/malformed protobuf frame, or EOF during metadata exchange returns `TransportError::Dialer` -> `PostgresError::Transport` (`ConnectionFailure`) without fallback to direct `:5432` or a second metadata handshake.
- Held TLS handshake crossing `now + refresh_skew_ms >= expires_at_unix_ms` (including exact equality `now + skew == expiry`) fails via `PostgresError::Auth` -> `GoogleCloudAuthError` with `0` metadata bytes observed by the TLS peer.
- Held `MetadataExchangeResponse` exceeding `absolute_deadline` fails with timeout under the original `ConnectAttempt` deadline without resetting the timeout budget after TLS.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-db/src/connection.rs` (`alloydb_split_transport_stages` and `cloudsql_mtls_verifies_instance_cn_and_alloydb_runs_metadata_exchange`) and `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (`alloydb_token_expires_during_tls` and `alloydb_*`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Split `connect_alloydb_tls` vs `finish_alloydb_metadata` | Verified TLS 1.3 AlloyDB listener recording post-handshake bytes | `connect_alloydb_tls` returns `Ok(stream)` with `0` post-handshake bytes received by server; `finish_alloydb_metadata` writes `MetadataExchangeRequest` exactly once; composed `TransportStream::connect` preserves legacy single-exchange behavior. |
| Token crosses skew or expires during held AlloyDB TLS (`alloydb_token_expires_during_tls`) | Finite login token (`expires_at_unix_ms = 1_500_000`, `skew = 300_000`, initial `now = 1_100_000`); hold server TLS handshake at barrier, advance clock to `1_200_000` (exact `expiry - skew`) and in sibling case `1_500_001`, release TLS | TLS handshake completes, `ensure_usable` fails with `PostgresError::Auth`, server reads immediate EOF (`0` `MetadataExchangeRequest` bytes, `0` PostgreSQL startup bytes, `0` retries). |
| Sibling valid boundary & AlloyDB IAM + built-in sessions | Same fixture with clock at `1_199_999` (`1 ms` before skew) in `AUTO_IAM` (`AuthType::Iam`) and `DB_NATIVE` (`AuthType::Password`) modes | Sends `1` `MetadataExchangeRequest` with `alloydb.login` token, returns `DialedCloudTransport` to `session.rs`, sends `StartupMessage` without `SSLRequest`, authenticates (`alloydb.login` token in IAM mode, `config.password` in built-in mode), and reaches `TransactionStatus::Idle` (`ReadyForQuery` `b'I'`). |
| Non-zero/malformed metadata & shared deadline exhaustion | Server returns non-OK `MetadataExchangeResponse` or holds metadata response past `absolute_deadline` | Fails with `PostgresError::Transport` / timeout within the single original `connect_timeout` budget. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib alloydb_split_transport_stages`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_token_expires_during_tls`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib alloydb_split_transport_stages`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloudsql_mtls_verifies_instance_cn_and_alloydb_runs_metadata_exchange`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_token_expires_during_tls`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `TransportStream::connect_alloydb_tls` and `finish_alloydb_metadata` split AlloyDB TLS 1.3 SAN verification from `MetadataExchangeRequest` while `TransportStream::connect` composes both under one deadline (R5, R9).
- [ ] `dial_alloydb` verifies `ScopedLoginSnapshot::ensure_usable` immediately after TLS (dropping expired/inside-skew streams with zero metadata bytes and `PostgresError::Auth`), completes `finish_alloydb_metadata`, and returns pre-startup `DialedCloudTransport` to `session.rs` (R5, R9).
- [ ] Built-in (`AuthType::Password`) and IAM (`AuthType::Iam`) AlloyDB `:5433` sessions pass all `alloydb_*` wire tests without `SSLRequest` or `:5432` fallback, committed as `feat(postgres): guard AlloyDB credentials after shared TLS setup` (R5, R9).

## Notes & Discoveries

- None.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared AlloyDB `:5433` mTLS transport.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::exchange_alloydb_metadata_over_stream` — AlloyDB protobuf `MetadataExchangeRequest` helper.
[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — AlloyDB v1alpha and `:5433` metadata protocol details.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Post-TLS `ensure_usable` guard before AlloyDB metadata exchange.
