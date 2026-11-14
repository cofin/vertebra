---
type: Task
id: "http3-and-http-hygiene:t01"
flow_id: http3-and-http-hygiene
title: Wire QUIC HTTP/3 UDP Listener into server.rs and Gate Alt-Svc Advertisement
description: "Parse VERTEBRA_HTTP3 config, share response::handle_buffered across TCP and QUIC, spawn serve_http3_endpoint in run_with_listener, and gate Alt-Svc on live QUIC binding."
status: stable
state: completed
commit: 9b44d409
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-http/src/response.rs
  - src/rs/vertebra-http/src/server.rs
tests:
  - src/rs/vertebra-http/tests/it/http3_quic.rs
read_set:
  - src/rs/vertebra-http/src/http3.rs
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-http/src/response.rs
  - src/rs/vertebra-http/src/server.rs
  - src/rs/vertebra-http/src/tls.rs
  - src/rs/vertebra-http/tests/it/http3_quic.rs
estimated_loc: 215
tags:
  - http3
  - quic
  - rust
  - ingress
sources:
  - id: http3-rs
    resource: "src/rs/vertebra-http/src/http3.rs::serve_http3_endpoint"
    title: QUIC HTTP/3 endpoint runner
  - id: server-rs
    resource: "src/rs/vertebra-http/src/server.rs::run_with_listener"
    title: Server listener startup and shutdown loop
  - id: response-rs
    resource: "src/rs/vertebra-http/src/response.rs::handle"
    title: HTTP request pipeline and Alt-Svc header injection
---

# Task `t01`: Wire QUIC HTTP/3 UDP Listener into server.rs and Gate Alt-Svc Advertisement

## Objective & Scope Boundary

Bind and serve live HTTP/3 QUIC traffic from `server::run_with_listener` (powering `main.rs::run_server`) when `--features http3` and `ServerConfig::enable_http3()` are active per [`../spec.md` §2](../spec.md) [^http3-rs] [^server-rs] [^response-rs].

## Bounded Read Manifest

- `src/rs/vertebra-http/src/{http3,config,response,server,tls}.rs` & `tests/it/http3_quic.rs`.

## Contracts & Edge Cases

- `ServerConfig::from_env()` parses `VERTEBRA_HTTP3`, `VERTEBRA_HTTP3_ALT_SVC_MAX_AGE`, `VERTEBRA_HTTP3_MAX_STREAMS`, and `VERTEBRA_HTTP3_KEEP_ALIVE_MS`; `validate_http3()` returns `Err` when `http3.enabled` is true without `tls` or when built without `feature = 'http3'`.
- `response::handle_buffered(req: Request<Bytes>, state: Arc<ServerState>)` shares full routing, offload, payload-limit, and unary/streaming dispatch with `handle(req: Request<Incoming>, state)`, injecting `Alt-Svc` strictly when `state.http3_port` is `Some(port)`.
- `server::run_with_listener` validates `config.validate_http3()?`, binds UDP `quinn::Endpoint::server(build_quic_server_config((*tls_acc.rustls_config()).clone(), &config.http3)?, udp_addr)` when `#[cfg(feature = 'http3')]` and `config.enable_http3()`, spawns `serve_http3_endpoint`, and signals + awaits clean QUIC shutdown when `shutdown` resolves.

## Test Matrix

- Integration tests in `tests/it/http3_quic.rs` exercising `run_with_listener` end-to-end: HTTPS TCP response advertising `Alt-Svc: h3=":<port>"`, live QUIC `h3` unary + progressive streaming roundtrips through `Router`/`Dispatcher`, absence of `Alt-Svc` when `http3.enabled == false`, and fail-loud error when `http3.enabled == true` with `tls == None`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,http3,compression --test it http3_quic && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-http --all-targets --features tls,http3,compression -- -D warnings
```

## Acceptance Criteria

- [x] `run_with_listener` serves live HTTP/3 QUIC unary and streaming requests alongside HTTPS TCP.
- [x] `Alt-Svc` is never emitted without a bound UDP QUIC listener, and invalid `http3` configs fail loud.

[^http3-rs]: `src/rs/vertebra-http/src/http3.rs::serve_http3_endpoint` — QUIC HTTP/3 endpoint runner.
[^server-rs]: `src/rs/vertebra-http/src/server.rs::run_with_listener` — Server listener startup and shutdown loop.
[^response-rs]: `src/rs/vertebra-http/src/response.rs::handle` — HTTP request pipeline and Alt-Svc header injection.
