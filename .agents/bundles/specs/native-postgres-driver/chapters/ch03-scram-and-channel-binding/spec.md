---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch03-scram-and-channel-binding
title: "Chapter 03: SCRAM-SHA-256 and Peer Certificate Channel Binding"
description: Ordered SCRAM-SHA-256 SASL state machine, RFC 5929 X.509 leaf tls-server-end-point digest derivation, and policy-driven SCRAM-SHA-256-PLUS channel binding with downgrade signaling.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch02-wire-codec-and-session-startup
replan_trigger: null
current_task: "native-postgres-driver:ch03:t01"
plan_revision: 5
plan_commit: fd571e3a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:40:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL channel_binding policies and startup error classification
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol 0.6.12 SASL SCRAM and x509-parser 0.18.1 channel-binding research
  - id: rs-transport
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared rustls TlsTcp peer certificate chain access
tags:
  - postgres
  - scram
  - channel-binding
  - tls
  - x509
---

# Chapter 03: SCRAM-SHA-256 and Peer Certificate Channel Binding

<!-- truth: start -->
**Bounded-Truth Summary**

- **Scope:** Extend `vertebra-postgres` startup authentication from password/MD5 to ordered `SCRAM-SHA-256` (`ch03:t01`), RFC 5929 `tls-server-end-point` X.509 leaf certificate digest extraction (`ch03:t02`), and policy-governed `SCRAM-SHA-256-PLUS` selection with RFC 5802 `GS2` `y,,` downgrade protection (`ch03:t03`).
- **Key Invariants:**
  - Production SCRAM framing, SASLprep, PBKDF2, and proof verification delegate exclusively to pinned `postgres_protocol::authentication::sasl::ScramSha256` (`=0.6.12`) with client-enforced iteration bounds `1..=100_000` and strict server-nonce extension (`server_nonce.len() > client_nonce.len() && server_nonce.starts_with(client_nonce)`).
  - `AuthenticationOk` is accepted after SASL only from `ScramVerifiedAwaitingOk` (after `AuthenticationSaslFinal` passes `ScramSha256::finish`), and `ScramSha256::message()` is never invoked after `finish()`.
  - `binding::tls_server_end_point` hashes the entire verified peer leaf certificate DER via `x509-parser = "=0.18.1"` (`default-features = false`), mapping SHA-1/MD5 and omitted RSA-PSS hash parameters to SHA-256 while treating no-hash Ed25519/Ed448 (`EdDSA`) or unknown OIDs as unavailable channel binding (`tls_server_end_point: None`).
  - `ChannelBindingPolicy::Prefer` over TLS with usable binding emits `p=tls-server-end-point,,` when `SCRAM-SHA-256-PLUS` is offered and `y,,` (`ChannelBinding::unrequested()`) when only plain `SCRAM-SHA-256` is offered, failing terminally without retry on `e=server-does-support-channel-binding`.
- **Verification Gate:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapter 03 completes direct startup authentication for `vertebra-postgres` (`R4`, `R5`) under the parent specification ([../../spec.md](../../spec.md)) and frozen API contracts ([../../contracts.md](../../contracts.md)) [^pg-contracts]. Building on Chapter 02's bounded frame codec, `SessionHandle` publication lifecycle, `tls::connect` handshake, and `auth::Authentication` state machine, this chapter adds SASL `SCRAM-SHA-256`, peer leaf X.509 `tls-server-end-point` digest derivation from `TransportStream::TlsTcp`, and full `ChannelBindingPolicy` (`disable`, `prefer`, `require`) enforcement for `SCRAM-SHA-256-PLUS` [^proto-research] [^rs-transport].

### 1.2 Non-Goals

- Google Cloud SQL and AlloyDB control-plane discovery, ephemeral RSA client certificate issuance, and IAM token refresh (owned by Chapters 04–06).
- GSSAPI, SSPI, or Kerberos v5 startup authentication (explicitly rejected with `PostgresError::UnsupportedAuth`).
- RFC 9266 `tls-exporter` channel binding (PostgreSQL `SCRAM-SHA-256-PLUS` specifies `tls-server-end-point` only).

### 1.3 Upstream Baseline & Code Analysis

Applicable repository guidance: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), and [pyo3-bridge SKILL.md](../../../../../skills/pyo3-bridge/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

```rust
// src/rs/vertebra-postgres/src/binding.rs
pub fn tls_server_end_point(der: &[u8]) -> Result<Vec<u8>, PostgresError>;

// src/rs/vertebra-postgres/src/auth.rs
pub(crate) struct Authentication { /* Initial | PasswordSent | ScramStarted(ScramSha256) | ScramFinalSent(ScramSha256) | ScramVerifiedAwaitingOk | Complete */ }
impl Authentication {
    pub(crate) fn new() -> Self;
    pub(crate) fn is_complete(&self) -> bool;
    pub(crate) fn handle_message(
        &mut self,
        msg: &postgres_protocol::message::backend::Message,
        config: &PostgresConfig,
        tls_server_end_point: Option<&[u8]>,
        out: &mut bytes::BytesMut,
    ) -> Result<bool, PostgresError>;
}
```

### Caller -> Seam -> Collaborator Map

- `SessionHandle::begin_connect` -> `ConnectAttempt::run` (`session.rs`) -> `tls::connect` (`tls.rs`), which extracts leaf DER from `TransportStream::TlsTcp` (`peer_certificates().and_then(|c| c.first())`) and invokes `binding::tls_server_end_point` (`binding.rs`) to populate `DialedTransport.tls_server_end_point`.
- `ConnectAttempt::run` -> `Authentication::handle_message` (`auth.rs`) -> `postgres_protocol::authentication::sasl::{ScramSha256, ChannelBinding, SCRAM_SHA_256, SCRAM_SHA_256_PLUS}` and `postgres_protocol::message::frontend::{sasl_initial_response, sasl_response}`, rechecking `ConnectAttempt`'s single outer deadline after bounded PBKDF2 before flushing or committing `PendingSession`.

### Edge-Case & Invariant Matrix

| Scenario | Input / Wire Condition | Required Behavior |
| --- | --- | --- |
| Malformed SASL iteration or nonce | `AuthenticationSaslContinue` with `i=0`, `i=100001`, non-decimal/duplicate `i=`, or `r=` equal to / not prefixed by client nonce | Reject before `ScramSha256::update` with `PostgresError::Protocol`, close socket, never emit `SASLResponse`. |
| Premature or out-of-order SASL frames | `AuthenticationOk` in `ScramStarted`/`ScramFinalSent`, or duplicate `AuthenticationSaslContinue`/`AuthenticationSaslFinal` | Fail with `PostgresError::Protocol`; only `ScramVerifiedAwaitingOk` transitions to `Complete` on `AuthenticationOk`. |
| Certificate signature hash selection | RSA/ECDSA SHA-224/256/384/512, DSA SHA-224/256, SHA-1/MD5, RSA-PSS explicit or omitted default (SHA-1), Ed25519/Ed448 | Hash full leaf DER; substitute SHA-256 for SHA-1/MD5/default-PSS; return `Err` (binding unavailable) for Ed25519/Ed448, unknown OIDs, or trailing DER. |
| Plain-only offer over bound TLS under `Prefer` | Usable `tls_server_end_point` + server offers `["SCRAM-SHA-256"]` only | Construct `ScramSha256` with `ChannelBinding::unrequested()` emitting `GS2` `"y,,"`; if stripped PLUS server replies `e=server-does-support-channel-binding`, fail terminally without `"n,,"` retry. |
| `ChannelBindingPolicy::Require` | Trust, cleartext, MD5, plain-only SCRAM, plaintext transport, or Ed25519 unavailable binding | Fail locally with `PostgresError::UnsupportedAuth` / `Transport` before transmitting any credential bytes. |

### Alternatives Comparison

| Option | Decision & Rationale |
| --- | --- |
| Upstream `postgres-protocol` `ScramSha256` + pre-update bounds vs custom Rust SCRAM implementation | **Chosen:** Delegate crypto, SASLprep, and wire proof checks to `postgres_protocol = "=0.6.12"`, adding only strict `i in 1..=100_000`, strict nonce extension, and explicit phase gates. Eliminates custom client crypto while closing upstream's prefix-only nonce check and unbounded PBKDF2 cost. |
| `x509-parser` public `SignatureAlgorithm::RSASSA_PSS` inspection vs unconditional SHA-256 or TLS exporter | **Chosen:** Parse leaf DER via `x509-parser = "=0.18.1"` (`default-features = false`) per RFC 5929 Section 4.1 (`tls-server-end-point`). PostgreSQL `SCRAM-SHA-256-PLUS` mandates `tls-server-end-point` (not RFC 9266 `tls-exporter`), and EdDSA has no RFC 5929 SHA-256 substitution. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch03]`

- Knowledge updates are deferred to Chapter 30 (`ch30:t02`), which records the RFC 5929 RSA-PSS / EdDSA channel-binding rules and `GS2` `y,,` downgrade invariants in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task(s) | Public Seam / Test Verification |
| --- | --- | --- |
| R4 (TLS peer certificate channel-binding material) | `ch03:t02`, `ch03:t03` | `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` |
| R5 (Ordered SCRAM-SHA-256, SCRAM-SHA-256-PLUS, and `channel_binding` policy enforcement) | `ch03:t01`, `ch03:t02`, `ch03:t03` | `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs`, `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` |

## 5. Implementation Plan

- [x] native-postgres-driver:ch03:t01 — Authenticate ordered SCRAM-SHA-256 SASL exchanges (tasks/t01-scram-sha256.md)
- [x] native-postgres-driver:ch03:t02 — Derive RFC 5929 tls-server-end-point digests from verified peer certificates (tasks/t02-peer-certificate-binding.md)
- [x] native-postgres-driver:ch03:t03 — Enforce SCRAM-SHA-256-PLUS channel-binding policies and GS2 downgrade protection (tasks/t03-scram-sha256-plus.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `e8901de4`; chapter `state: planned`, `planning_status: complete`, depends on `ch02-wire-codec-and-session-startup`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending), awaiting `native-postgres-driver:ch02:t04`.
- **Documentation & OKF Status**: `[0/0 files documented in ch03]`; RFC 5929 channel-binding OKF curation scheduled in `ch30:t02`.
- **Downstream Dependents**: Unblocks `ch04-cloud-material-and-admin-discovery` (`ch04:t01` depends on `ch03:t03`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `channel_binding` constructor policy and startup error contracts.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol` SASL SCRAM and `x509-parser` signature algorithm research.
[^rs-transport]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared `rustls` peer certificate chain access on `TlsTcp`.
