---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch06-stdio-serve-bridge-and-gcp-auth
title: "Chapter 06: Incremental Stdio, CLI Serve/Bridge Discovery, OIDC, and IAP Auth"
description: "Make stdio incremental and session-aware, wire vtb ai mcp serve to discovered apps, upgrade bridge for dual-mode SSE relay and AsyncGcpIdTokenCache OIDC auth, and add IapIdentitySource."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch04-skills-and-mcp-apps-core
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: 7607f4d8
updated_at: "2026-11-13T20:35:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - mcp
  - cli
  - stdio
  - oidc
  - iap
sources:
  - id: stdio-py
    resource: "src/py/vertebra/agent/mcp/stdio.py::run_stdio_server"
    title: Buffered stdio server
  - id: cli-ai
    resource: "src/py/vertebra/cli/ai.py::serve"
    title: vtb ai mcp serve and bridge commands
  - id: bridge-py
    resource: "src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge"
    title: POST-only stdio-to-HTTP bridge
  - id: identity-py
    resource: "src/py/vertebra/agent/core/identity.py::IdentitySource"
    title: Trusted proxy IdentitySource protocol
  - id: iap-docs
    resource: "https://cloud.google.com/iap/docs/signed-headers-howto"
    title: Google Cloud IAP signed JWT header verification contract
---

# Chapter 06: Incremental Stdio, CLI Serve/Bridge Discovery, OIDC, and IAP Auth

<!-- truth: start -->
- `run_stdio_server` dispatches stdin lines incrementally and concurrently with `notifications/cancelled`, legacy `initialize` sessions, and stdout notification/progress relay; `vtb ai mcp serve` discovers the app's `MCPCore` (or delegates Litestar apps to `litestar_mcp`) and fails loud when absent ([#114](https://github.com/cofin/vertebra/issues/114)) [^stdio-py] [^cli-ai].
- `run_stdio_http_bridge` forwards both `2026-07-28` and legacy handshakes without injecting `2026-07-28` into `initialize`, relays legacy `GET` SSE frames with `Last-Event-ID` reconnect (`max 5`), sends `DELETE` at EOF, and authenticates via `StaticTokenSource` or `OidcTokenSource` (`AsyncGcpIdTokenCache` + single `401` refresh retry) ([#114](https://github.com/cofin/vertebra/issues/114)) [^bridge-py] [^cli-ai].
- `IapIdentitySource(audience)` verifies ES256 `x-goog-iap-jwt-assertion` tokens against Google IAP JWKs (`aud`, `iss == 'https://cloud.google.com/iap'`, `exp`), never trusting `x-goog-authenticated-user-email` [^identity-py] [^iap-docs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`run_stdio_server` buffers all stdin before answering and `vtb ai mcp serve` constructs an empty server while ignoring `--name`; `run_stdio_http_bridge` stamps `MCP-Protocol-Version: 2026-07-28` onto every frame, lacks `GET` SSE relay and EOF `DELETE`, and supports only static bearer tokens ([#114](https://github.com/cofin/vertebra/issues/114)) [^stdio-py] [^cli-ai] [^bridge-py]. On the inbound proxy boundary, `IapIdentitySource` is needed to verify signed `x-goog-iap-jwt-assertion` headers [^identity-py] [^iap-docs]. Notably, `ch06` depends on `ch04` rather than `ch05` so native stdio, bridge, OIDC, and IAP work never block on external `litestar-mcp` PyPI publication.

## 2. Requirements, Concrete Contracts & Architecture

- **`R6a` (`t01`)**: Make `run_stdio_server(service: MCPCore, ...)` incremental and concurrent per line with write-locked stdout, `notifications/cancelled` cancellation, `max_message_bytes` `-32600` framing, legacy `initialize` session relay + EOF session cleanup; add `resolve_app_mcp_core(target)` and `is_litestar_mcp_app(target)` in `cli/ai.py`, updating `vtb ai mcp serve [--app APP]` (removing `--name`) to serve the discovered app or raise `click.ClickException` (exit `1`).
- **`R6b` (`t02`)**: Allow `protocol_version: str | None = MCP_PROTOCOL_VERSION` in `client.py::mcp_request_headers`; add `BridgeResponse`, `BridgeTransport`, `UrllibBridgeTransport`, `BridgeTokenSource`, `StaticTokenSource`, and `OidcTokenSource(audience, *, cache: AsyncGcpIdTokenCache | None = None)` in `bridge.py`; relay legacy `GET` SSE notifications (`Last-Event-ID`, `5` retries, stop on `405`), send `DELETE` at EOF, refresh/invalidate once on `401` (`-32001` on second `401`), and add mutually exclusive `--audience` vs `--token` on `vtb ai mcp bridge`.
- **`R6c` (`t03`)**: Implement `IapIdentitySource(audience, *, key_url=IAP_PUBLIC_KEY_URL, key_cache_seconds=3600.0, jwks=None)` in `agent/core/identity.py` (`ValueError` on empty/malformed audience, `MissingDependencyError` for `vertebra-core[iap]`, fail-closed `None` on invalid/expired JWT or plain `x-goog-authenticated-user-email`), and declare the `iap = ["pyjwt[crypto]>=2.9.0"]` optional extra in `pyproject.toml` + `uv.lock` (`uv lock --offline`).

## 3. Documentation & OKF Knowledge Plan

`vtb ai mcp serve`, `vtb ai mcp bridge --audience`, and `IapIdentitySource` are documented in `ch07` (`docs/getting-started/cli.md`, `docs/reference/cli.md`, `docs/usage/agent-protocols.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R6a` | `t01` | `agent/mcp/stdio.py`, `agent/mcp/service.py`, `cli/ai.py` | `pytest src/py/tests/unit/cli/test_mcp_cli.py` |
| `R6b` | `t02` | `agent/mcp/bridge.py`, `agent/mcp/client.py`, `cli/ai.py` | `pytest src/py/tests/unit/cli/test_mcp_cli.py` |
| `R6c` | `t03` | `agent/core/identity.py`, `pyproject.toml`, `uv.lock` | `uv lock --check --offline` + `pytest test_core_and_observability.py` |

## 5. Implementation Plan

- [x] [7607f4d8] [t01-incremental-stdio-and-serve-discovery.md](tasks/t01-incremental-stdio-and-serve-discovery.md) — Make `run_stdio_server` incremental/session-aware and wire `vtb ai mcp serve` app discovery
- [x] [7607f4d8] [t02-bridge-dual-mode-and-oidc-auth.md](tasks/t02-bridge-dual-mode-and-oidc-auth.md) — Add dual-mode `run_stdio_http_bridge`, legacy SSE relay, and `OidcTokenSource` (`--audience`)
- [x] [7607f4d8] [t03-iap-jwt-identity-source.md](tasks/t03-iap-jwt-identity-source.md) — Implement `IapIdentitySource` ES256 signed-header JWT verification and `iap` extra

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 15s`)**: `uv lock --check --offline && uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/cli/ai.py && uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/agent/test_core_and_observability.py -q`.

[^stdio-py]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Buffered stdio server.
[^cli-ai]: `src/py/vertebra/cli/ai.py::serve` — vtb ai mcp serve and bridge commands.
[^bridge-py]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — POST-only stdio-to-HTTP bridge.
[^identity-py]: `src/py/vertebra/agent/core/identity.py::IdentitySource` — Trusted proxy IdentitySource protocol.
[^iap-docs]: `https://cloud.google.com/iap/docs/signed-headers-howto` — Google Cloud IAP signed JWT header verification contract.
