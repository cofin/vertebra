---
type: Task
id: "http3-and-http-hygiene:t02"
flow_id: http3-and-http-hygiene
title: Remove Unwired McpStdioBridge and Enable --features http3 in Make and CI
description: "Delete dead McpBridgeConfig and McpStdioBridge in mcp_bridge.rs and add --features tls,http3,compression verification to Makefile and .github/workflows/ci.yml."
status: stable
state: completed
commit: 2fb2ec5b
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "http3-and-http-hygiene:t01"
files:
  - src/rs/vertebra-http/src/mcp_bridge.rs
  - Makefile
  - .github/workflows/ci.yml
tests:
  - src/rs/vertebra-http/tests/it/mcp_discovery_bridge.rs
  - src/rs/vertebra-http/tests/it/http3_quic.rs
read_set:
  - src/rs/vertebra-http/src/mcp_bridge.rs
  - src/rs/vertebra-http/src/response.rs
  - src/rs/vertebra-http/tests/it/mcp_discovery_bridge.rs
  - Makefile
  - .github/workflows/ci.yml
estimated_loc: 95
tags:
  - http3
  - rust
  - cleanup
  - ci
sources:
  - id: mcp-bridge-rs
    resource: "src/rs/vertebra-http/src/mcp_bridge.rs::McpStdioBridge"
    title: Dead McpStdioBridge and active extract_mcp_observability_headers
  - id: makefile
    resource: Makefile
    title: Rust workspace lint and test Make targets
  - id: ci-yaml
    resource: .github/workflows/ci.yml
    title: Pull request Rust CI workflow
---

# Task `t02`: Remove Unwired McpStdioBridge and Enable --features http3 in Make and CI

## Objective & Scope Boundary

Delete unwired `McpBridgeConfig` / `McpStdioBridge` in `mcp_bridge.rs` and add `http3` feature coverage to `Makefile` and `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^mcp-bridge-rs] [^makefile] [^ci-yaml].

## Bounded Read Manifest

- `src/rs/vertebra-http/src/{mcp_bridge,response}.rs` & `tests/it/mcp_discovery_bridge.rs`.
- `Makefile` & `.github/workflows/ci.yml`.

## Contracts & Edge Cases

- Remove `McpBridgeConfig` and `McpStdioBridge` (and unused `std::io` imports) from `src/rs/vertebra-http/src/mcp_bridge.rs`, keeping `McpObservabilityHeaders` and `extract_mcp_observability_headers`; update `tests/it/mcp_discovery_bridge.rs` to test discovery and observability header extraction only.
- Enable `http3` alongside `tls,compression,gcp` in `Makefile` (`lint`, `lint-all`, `rs-test`) and `.github/workflows/ci.yml` Rust clippy/test steps so `--features http3` and `http3_quic` run on every check.

## Test Matrix

- `mcp_discovery_bridge` and `http3_quic` integration suites plus workspace `cargo clippy --all-targets -- -D warnings` with `http3` enabled and disabled.

## Verification

```bash
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,http3,compression
```

## Acceptance Criteria

- [x] Zero unwired `McpStdioBridge` / `McpBridgeConfig` code remains in `vertebra-http`.
- [x] `Makefile` and `.github/workflows/ci.yml` run `clippy` and `cargo test` with `http3` enabled.

[^mcp-bridge-rs]: `src/rs/vertebra-http/src/mcp_bridge.rs::McpStdioBridge` — Dead McpStdioBridge and active extract_mcp_observability_headers.
[^makefile]: `Makefile` — Rust workspace lint and test Make targets.
[^ci-yaml]: `.github/workflows/ci.yml` — Pull request Rust CI workflow.
