---
type: Task
id: "http3-and-http-hygiene:t03"
flow_id: http3-and-http-hygiene
title: Synchronize HTTP Ingress and Protocol Offload OKF Chapters
description: "Update protocol-and-offload-modules.md and ingress-and-routing.md in place to reflect live QUIC HTTP/3 server wiring, Alt-Svc gating, and McpStdioBridge removal."
status: stable
state: completed
commit: f20adf9d
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "http3-and-http-hygiene:t02"
files:
  - .agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md
  - .agents/bundles/knowledge/architecture/http/ingress-and-routing.md
tests: []
read_set:
  - .agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md
  - .agents/bundles/knowledge/architecture/http/ingress-and-routing.md
  - src/rs/vertebra-http/src/server.rs
  - src/rs/vertebra-http/src/http3.rs
  - src/rs/vertebra-http/src/mcp_bridge.rs
estimated_loc: 85
tags:
  - okf
  - http3
  - ingress
  - docs
sources:
  - id: protocol-okf
    resource: .agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md
    title: HTTP protocol and offload modules OKF chapter
  - id: ingress-okf
    resource: .agents/bundles/knowledge/architecture/http/ingress-and-routing.md
    title: HTTP ingress and routing OKF chapter
---

# Task `t03`: Synchronize HTTP Ingress and Protocol Offload OKF Chapters

## Objective & Scope Boundary

Synchronize `protocol-and-offload-modules.md` and `ingress-and-routing.md` (`<= 200` lines each) in present tense per [`../spec.md` §2](../spec.md) [^protocol-okf] [^ingress-okf].

## Bounded Read Manifest

- `.agents/bundles/knowledge/architecture/http/{protocol-and-offload-modules,ingress-and-routing}.md`.
- `src/rs/vertebra-http/src/{server,http3,mcp_bridge}.rs`.

## Contracts & Edge Cases

- Document UDP `quinn::Endpoint` startup in `server::run_with_listener` (`VERTEBRA_HTTP3`), shared `response::handle_buffered` dispatch, active-socket `Alt-Svc` gating, removal of `McpStdioBridge` from `mcp_bridge.rs`, and `--features tls,http3,compression` CI gates.
- Keep both OKF chapters `<= 200` total lines and `<= 40` truth-block lines with refreshed `verified.commit` and valid `sources:` footnotes.

## Test Matrix

- Verified via `make review-check` and `<= 200`-line check.

## Verification

```bash
python3 -c 'import pathlib; [assert_ok := (len(pathlib.Path(p).read_text().splitlines()) <= 200) or (_ for _ in ()).throw(AssertionError(p)) for p in [".agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md", ".agents/bundles/knowledge/architecture/http/ingress-and-routing.md"]]' && make review-check
```

## Acceptance Criteria

- [x] Both HTTP architecture OKF chapters are `<= 200` lines with accurate `sources:` and footnotes.
- [x] `make review-check` exits `0`.

[^protocol-okf]: `.agents/bundles/knowledge/architecture/http/protocol-and-offload-modules.md` — HTTP protocol and offload modules OKF chapter.
[^ingress-okf]: `.agents/bundles/knowledge/architecture/http/ingress-and-routing.md` — HTTP ingress and routing OKF chapter.
