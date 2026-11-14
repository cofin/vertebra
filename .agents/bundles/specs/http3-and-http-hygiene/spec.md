---
type: Spec
flow_id: http3-and-http-hygiene
title: HTTP/3 QUIC Listener Wiring and HTTP Ingress Hygiene
description: "Wire the feature-gated QUIC HTTP/3 UDP listener into vertebra-http server startup, gate Alt-Svc on active QUIC sockets, remove dead McpStdioBridge code, and enforce --features http3 in Make and CI."
status: stable
state: in_progress
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
current_task: null
parent_prd: integration-remediation
depends_on_flows:
  - server-runtime-correctness
plan_revision: 1
plan_commit: 18baadcd
created_at: "2026-11-12T23:45:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - http3
  - quic
  - rust
  - ingress
  - ci
sources:
  - id: http3-rs
    resource: "src/rs/vertebra-http/src/http3.rs::serve_http3_endpoint"
    title: QUIC HTTP/3 accept loop and Alt-Svc header formatter
  - id: server-rs
    resource: "src/rs/vertebra-http/src/server.rs::run_with_listener"
    title: TCP/TLS Hyper accept loop currently lacking QUIC UDP listener startup
  - id: response-rs
    resource: "src/rs/vertebra-http/src/response.rs::handle"
    title: HTTP request handler currently injecting Alt-Svc without an active QUIC socket
  - id: config-rs
    resource: "src/rs/vertebra-http/src/config.rs::ServerConfig"
    title: ServerConfig holding Http3Config and environment parsing
  - id: tls-rs
    resource: "src/rs/vertebra-http/src/tls.rs::TlsAcceptor.rustls_config"
    title: "Reusable rustls::ServerConfig accessor for HTTP/3 QUIC endpoints"
  - id: mcp-bridge-rs
    resource: "src/rs/vertebra-http/src/mcp_bridge.rs::McpStdioBridge"
    title: Unwired Rust McpStdioBridge and McpBridgeConfig alongside active header extractor
  - id: makefile
    resource: Makefile
    title: Rust workspace lint and test targets currently omitting http3 feature
  - id: protocol-okf
    resource: .agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md
    title: HTTP protocol and offload modules architecture OKF chapter
  - id: ingress-okf
    resource: .agents/bundles/knowledge/architecture/http/ingress-and-routing.md
    title: HTTP ingress and routing architecture OKF chapter
---

# Specification: HTTP/3 QUIC Listener Wiring and HTTP Ingress Hygiene

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `vertebra-http/src/http3.rs` implements `build_quic_server_config` and `serve_http3_endpoint` (`quinn`, `h3`, `h3-quinn`), and `response.rs` injects `Alt-Svc: h3=":<port>"`, while `server.rs` never binds a UDP QUIC socket and `Makefile`/`ci.yml` never compile `--features http3` [^http3-rs] [^server-rs] [^response-rs] [^makefile].
- **Live QUIC Listener & Honest `Alt-Svc`**: When compiled with `--features http3` and `ServerConfig` enables HTTP/3 (`VERTEBRA_HTTP3=1` / `config.http3.enabled` with TLS configured), `server::run_with_listener` binds a UDP `quinn::Endpoint` reusing `TlsAcceptor::rustls_config()`, routes HTTP/3 streams through the shared `response::handle_buffered` pipeline, drains on shutdown, and injects `Alt-Svc` strictly when a live QUIC listener is active (failing loud if HTTP/3 is requested without TLS or without the `http3` feature) [^config-rs] [^tls-rs].
- **Ingress Hygiene & Feature Gates**: Unwired `McpBridgeConfig` and `McpStdioBridge` in `mcp_bridge.rs` are removed (retaining `McpObservabilityHeaders` and `extract_mcp_observability_headers`), `Makefile` and `.github/workflows/ci.yml` enforce `cargo test` and `cargo clippy` on `vertebra-http --features tls,http3,compression`, and HTTP architecture OKF chapters stay synchronized (`<= 200` lines) [^mcp-bridge-rs] [^protocol-okf] [^ingress-okf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Baseline (`18baadcd`)

`src/rs/vertebra-http/src/http3.rs` implements `Http3Config`, `build_quic_server_config`, `serve_http3_endpoint`, and `handle_h3_request` over `quinn 0.11.12`, `h3 0.0.8`, and `h3-quinn 0.0.10` [^http3-rs], and `TlsAcceptor::rustls_config()` in `tls.rs` already exposes the underlying `Arc<rustls::ServerConfig>` for QUIC reuse [^tls-rs]. However:
1. `ServerConfig::from_env()` in `config.rs` never parses `VERTEBRA_HTTP3` or QUIC transport knobs [^config-rs].
2. `server::run_with_listener` (invoked by `main.rs::run_server` via `run_with_collector`) binds only the TCP listener and never spawns `serve_http3_endpoint`, while `response.rs:471` unconditionally injects `Alt-Svc: h3=":<port>"` whenever `state.config.http3.enabled` is true—advertising an unreachable UDP port [^server-rs] [^response-rs].
3. `src/rs/vertebra-http/src/mcp_bridge.rs` carries dead `McpBridgeConfig` and `McpStdioBridge` structs with zero production callers (`vtb ai mcp bridge` lives in Python `vertebra.agent.mcp.bridge`; Rust ingress only calls `extract_mcp_observability_headers`) [^mcp-bridge-rs].
4. Neither `Makefile` (`--features tls,compression,gcp`) nor `.github/workflows/ci.yml` compiles or runs `tests/it/http3_quic.rs` under `--features http3` [^makefile].

### 1.2 Non-Goals

- Changing TCP cleartext HTTP/1.1 zero-peek fastpath or `h2c` prior-knowledge behavior in `server.rs`.
- Implementing WebTransport over HTTP/3 (`h3` datagrams/extended CONNECT remain out of scope).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **`R1` QUIC HTTP/3 Server Listener & Shared Request Pipeline (`t01`)**:
  - Parse `VERTEBRA_HTTP3` (`1`/`true`), `VERTEBRA_HTTP3_ALT_SVC_MAX_AGE`, `VERTEBRA_HTTP3_MAX_STREAMS`, and `VERTEBRA_HTTP3_KEEP_ALIVE_MS` in `ServerConfig::from_env()`, and add `ServerConfig::enable_http3(&self) -> bool` + `ServerConfig::validate_http3(&self) -> Result<(), String>` failing loud when HTTP/3 is enabled without `config.tls` or when compiled without `feature = "http3"`.
  - Factor `pub async fn handle_buffered(req: Request<Bytes>, state: Arc<ServerState>) -> Result<Response<ResponseBody>, Infallible>` in `response.rs` so both TCP `handle(req: Request<Incoming>, state)` and QUIC `serve_http3_endpoint` execute identical routing, static asset, OpenAPI, discovery, CORS, compression, backpressure, and batching/streaming pipelines.
  - Track `pub http3_port: Option<u16>` on `ServerState`; in `server::run_with_listener`, when `#[cfg(feature = "http3")]` and `config.enable_http3()`, build `quinn::ServerConfig` from `(*tls_acc.rustls_config()).clone()`, bind a UDP `quinn::Endpoint` on `SocketAddr::new(bound_addr.ip(), bound_addr.port())`, set `state.http3_port = Some(quic_port)`, spawn `serve_http3_endpoint` wired to a `watch` shutdown channel, await clean QUIC drain on shutdown, and inject `Alt-Svc` in `response.rs` strictly when `state.http3_port` is `Some(port)`.
- **`R2` `vertebra-http` Dead-Code Hygiene & `--features http3` Make/CI Gates (`t02`)**:
  - Delete unwired `McpBridgeConfig` and `McpStdioBridge` from `src/rs/vertebra-http/src/mcp_bridge.rs` (retaining `McpObservabilityHeaders` and `extract_mcp_observability_headers`) and remove the dead-struct test in `tests/it/mcp_discovery_bridge.rs`, verifying all remaining `vertebra-http` modules compile warning-free under `-D warnings`.
  - Add `--features tls,http3,compression` checks for `vertebra-http` in `Makefile` (`lint`, `lint-all`, `rs-test`) and `.github/workflows/ci.yml` so `tests/it/http3_quic.rs` executes in local and CI gates.
- **`R3` HTTP Architecture OKF Synchronization (`t03`)**:
  - Synchronize [protocol-and-offload-modules.md](../../knowledge/architecture/http/protocol-and-offload-modules.md) and [ingress-and-routing.md](../../knowledge/architecture/http/ingress-and-routing.md) (`<= 200` lines each, `<= 40`-line truth blocks) with live UDP `quinn::Endpoint` listener startup, `handle_buffered` pipeline sharing, `Alt-Svc` gating, `McpStdioBridge` removal, and `http3` feature gates.

### 2.2 Concrete Rust Contracts

```rust
// src/rs/vertebra-http/src/config.rs
impl ServerConfig {
    pub fn enable_http3(&self) -> bool { self.http3.enabled }
    pub fn validate_http3(&self) -> Result<(), String>;
}

// src/rs/vertebra-http/src/response.rs
pub async fn handle_buffered(
    req: http::Request<bytes::Bytes>,
    state: Arc<ServerState>,
) -> Result<http::Response<ResponseBody>, std::convert::Infallible>;
```

## 3. Documentation & OKF Knowledge Plan

`t03-http3-architecture-okf-sync.md` updates both HTTP architecture OKF chapters in place (`<= 200` lines each):
- `.agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md` [^protocol-okf]
- `.agents/bundles/knowledge/architecture/http/ingress-and-routing.md` [^ingress-okf]

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` QUIC listener & `Alt-Svc` | `t01` | `src/rs/vertebra-http/src/{config,response,server}.rs` | `cargo test -p vertebra-http --features tls,http3,compression --test it http3_quic` |
| `R2` Dead-code removal & CI gates | `t02` | `src/rs/vertebra-http/src/mcp_bridge.rs`, `Makefile`, `.github/workflows/ci.yml` | `cargo clippy -p vertebra-http --all-targets --features tls,http3,compression -- -D warnings` + `rs-test` |
| `R3` HTTP OKF sync | `t03` | `architecture/http/{protocol-and-offload-modules,ingress-and-routing}.md` | `make review-check` |

## 5. Implementation Plan

- [x] [9b44d409] [t01-quic-http3-server-listener-wiring.md](tasks/t01-quic-http3-server-listener-wiring.md) — Wire UDP QUIC `serve_http3_endpoint` into `server.rs`, share `response::handle_buffered`, and gate `Alt-Svc` on active QUIC binding
- [x] [afa74da5] [t02-http-dead-code-hygiene-and-feature-gates.md](tasks/t02-http-dead-code-hygiene-and-feature-gates.md) — Delete dead `McpStdioBridge` in `mcp_bridge.rs` and enable `--features tls,http3,compression` in `Makefile` and `ci.yml`
- [x] [f20adf9d] [t03-http3-architecture-okf-sync.md](tasks/t03-http3-architecture-okf-sync.md) — Synchronize `protocol-and-offload-modules.md` and `ingress-and-routing.md` (`<= 200L`)

## 6. Current State & Remaining Plan Snapshot

- **State**: `in_progress` (`3/3` tasks completed at `9b44d409`, `afa74da5`, `f20adf9d`; `[2/2 files documented]`; awaiting zero-context review and `/flow:finish` + `/flow:archive`).
- **Full Flow Finish Gate (`/flow:finish`)**: `make lint-all && make rs-test && make review-check`.

[^http3-rs]: `src/rs/vertebra-http/src/http3.rs::serve_http3_endpoint` — QUIC HTTP/3 accept loop and Alt-Svc header formatter.
[^server-rs]: `src/rs/vertebra-http/src/server.rs::run_with_listener` — TCP/TLS Hyper accept loop currently lacking QUIC UDP listener startup.
[^response-rs]: `src/rs/vertebra-http/src/response.rs::handle` — HTTP request handler currently injecting Alt-Svc without an active QUIC socket.
[^config-rs]: `src/rs/vertebra-http/src/config.rs::ServerConfig` — ServerConfig holding Http3Config and environment parsing.
[^tls-rs]: `src/rs/vertebra-http/src/tls.rs::TlsAcceptor.rustls_config` — Reusable rustls::ServerConfig accessor for HTTP/3 QUIC endpoints.
[^mcp-bridge-rs]: `src/rs/vertebra-http/src/mcp_bridge.rs::McpStdioBridge` — Unwired Rust McpStdioBridge and McpBridgeConfig alongside active header extractor.
[^makefile]: `Makefile` — Rust workspace lint and test targets currently omitting http3 feature.
[^protocol-okf]: `.agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md` — HTTP protocol and offload modules architecture OKF chapter.
[^ingress-okf]: `.agents/bundles/knowledge/architecture/http/ingress-and-routing.md` — HTTP ingress and routing architecture OKF chapter.
