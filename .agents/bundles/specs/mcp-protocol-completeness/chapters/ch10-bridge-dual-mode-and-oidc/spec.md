---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch10-bridge-dual-mode-and-oidc
title: "Chapter 10: Bridge Dual Mode and OIDC"
description: Make vtb ai mcp bridge forward both protocol modes concurrently without injecting 2026-07-28 headers into legacy handshakes, relay legacy GET SSE notifications with reconnection, terminate sessions on EOF, and authenticate with static tokens or refreshed OIDC id_tokens minted for an audience.
status: draft
state: planned
planning_status: empirical_dependent
depends_on_chapters:
  - ch09-stdio-and-serve
replan_trigger: "gcp-embeddings-and-identity lands OIDC id_token minting (#113); confirm AsyncGcpIdTokenCache.get_token(audience, *, force_refresh=False) -> RustDbTaskAwaitable[GcpIdTokenSnapshot], invalidate(audience, failed_generation), and the vertebra.storage re-export before executing t02"
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: bridge
    resource: src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge
    title: Synchronous POST-only bridge
  - id: headers
    resource: src/py/vertebra/agent/mcp/client.py::mcp_request_headers
    title: Header derivation that always sets MCP-Protocol-Version
  - id: cli-bridge
    resource: src/py/vertebra/cli/ai.py::bridge
    title: vtb ai mcp bridge command
  - id: gcp-cache
    resource: .agents/bundles/specs/gcp-embeddings-and-identity/spec.md
    title: AsyncGcpIdTokenCache and GcpIdTokenSnapshot contract from gcp-embeddings-and-identity
tags:
  - mcp
  - bridge
  - oidc
  - cli
---

# Chapter 10: Bridge Dual Mode and OIDC

<!-- truth: start -->
- **Dual mode**: `run_stdio_http_bridge` forwards each stdin frame concurrently through a `BridgeTransport`; legacy handshakes (`initialize`, and `ping` / `notifications/initialized` before negotiation) carry no `MCP-Protocol-Version`; after a legacy `initialize`, frames carry the negotiated version and `Mcp-Session-Id`; without `initialize`, frames carry `2026-07-28` [^bridge] [^headers].
- **Relay**: after a legacy `initialize` returns a session, the bridge opens `GET` with `Accept: text/event-stream`, writes each event `data` line to stdout, reconnects with `Last-Event-ID` up to `5` times (`0.5 s * 2^n` backoff), stops on `405`, and sends `DELETE` with the session at EOF [^bridge].
- **Auth**: `BridgeTokenSource` supplies bearer tokens (`StaticTokenSource`, `OidcTokenSource`); a `401` forces one refresh and retry; a second `401` writes JSON-RPC error `-32001` `"Bridge authentication failed (HTTP 401)"` [^bridge].
- **CLI**: `vtb ai mcp bridge --url URL [--token TOKEN | --audience AUDIENCE] [--timeout S]`; `--token` and `--audience` are mutually exclusive; `--audience` obtains OIDC id_tokens from `AsyncGcpIdTokenCache.get_token(audience)` (`gcp-embeddings-and-identity`), which owns caching and refresh skew; `401` invalidates the snapshot generation before refreshing [^cli-bridge] [^gcp-cache].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`run_stdio_http_bridge` reads stdin synchronously, sends one POST per line, injects `MCP-Protocol-Version: 2026-07-28` into every frame (breaking legacy `initialize` against servers that reject it), never opens a `GET` stream, never terminates sessions, and accepts only a static bearer token or an unrefreshed provider callback ([#114](https://github.com/cofin/vertebra/issues/114)). Cloud Run and IAP-protected MCP servers need OIDC id_tokens for an audience, which `gcp-embeddings-and-identity` adds as `AsyncGcpIdTokenCache` ([#113](https://github.com/cofin/vertebra/issues/113)); at `c8722c76` `AsyncGcpTokenCache` offers only access tokens and self-signed `sign_jwt_for_audience` (service-account keys only).

### 1.2 Upstream Execution Baseline

- Depends on `ch09` (serve side), `ch03`–`ch04` (server protocol behaviour exercised in tests).
- `t02` depends on flow `gcp-embeddings-and-identity` (`replan_trigger`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Bridge | `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge`, `_parse_sse_events` [^bridge] | Remote-owned HTTP behind a `BridgeTransport` port; tests use an in-memory transport. |
| Headers | `src/py/vertebra/agent/mcp/client.py::mcp_request_headers`, `_default_http_request` [^headers] | In-process. |
| CLI | `src/py/vertebra/cli/ai.py::bridge`, `run_mcp_stdio_bridge` [^cli-bridge] | Click. |
| Identity | `AsyncGcpIdTokenCache` contract in `.agents/bundles/specs/gcp-embeddings-and-identity/spec.md` [^gcp-cache] | External boundary (in-memory fake cache in tests). |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`vtb` CLI](../../../../knowledge/domains/framework/cli.md), [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R16.1**: Dual-mode forwarding, legacy relay with reconnection, session termination, concurrent frames.
- **R16.2**: Token sources, `401` refresh-and-retry, OIDC id_tokens, CLI options.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# bridge.py
BRIDGE_ERROR: int = -32001
class BridgeResponse(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    status: int
    headers: dict[str, str]
    body: bytes = b""
    lines: AsyncIterator[bytes] | None = None
class BridgeTransport(Protocol):
    async def __call__(self, method: Literal["POST", "GET", "DELETE"], url: str, headers: Mapping[str, str], body: bytes) -> BridgeResponse: ...
class UrllibBridgeTransport:
    def __init__(self, *, timeout_s: float = 30.0) -> None: ...
    async def __call__(self, method, url, headers, body) -> BridgeResponse: ...
class BridgeTokenSource(Protocol):
    async def token(self, *, force_refresh: bool = False) -> str | None: ...
class StaticTokenSource:
    def __init__(self, token: str | None) -> None: ...
class OidcTokenSource:
    def __init__(self, audience: str, *, cache: AsyncGcpIdTokenCache | None = None) -> None: ...
    async def token(self, *, force_refresh: bool = False) -> str: ...
def run_stdio_http_bridge(*, url: str, token_provider: BridgeTokenSource | None = None, timeout_s: float = 30.0, max_message_bytes: int = 4_194_304, stdin: TextIO | None = None, stdout: TextIO | None = None, transport: BridgeTransport | None = None) -> int: ...

# client.py
def mcp_request_headers(payload: bytes | Mapping[str, Any], *, protocol_version: str | None = MCP_PROTOCOL_VERSION, session_id: str | None = None, bearer_token: str | None = None, trace_context: Any | None = None) -> dict[str, str]: ...

# cli/ai.py
def run_mcp_stdio_bridge(*, url: str, bearer_token: str | None = None, audience: str | None = None, timeout_s: float = 30.0, max_message_bytes: int = 4_194_304, stdin: TextIO | None = None, stdout: TextIO | None = None, transport: BridgeTransport | None = None) -> int: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `vtb ai mcp bridge` -> `run_mcp_stdio_bridge` -> token source (`StaticTokenSource(--token or VERTEBRA_MCP_TOKEN)` / `OidcTokenSource(--audience)`) -> `run_stdio_http_bridge` -> `BridgeTransport` (`UrllibBridgeTransport` by default).
- Bridge frame task -> `mcp_request_headers(protocol_version=...)` -> `POST`; legacy `initialize` success -> relay task (`GET`) -> stdout writer.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Legacy handshake | `initialize` frame | No `MCP-Protocol-Version` header | Legacy servers accept the handshake |
| Server lacks GET | Relay `GET` returns `405` | Relay stops; no stdout output | Optional server push |
| Relay drop | Stream ends | Reconnect with `Last-Event-ID`, max `5` | Resumability |
| Session expired | `POST` returns `404` with session | Body written; session cleared; relay stopped | No zombie session |
| `401` twice | Token rejected after refresh | `-32001` error frame with the request id | Bounded retries |
| Both flags | `--token x --audience y` | `UsageError` exit `2` | Unambiguous auth |
| Token minting fails | `AsyncGcpIdTokenCache.get_token` raises | `-32001` `"Bridge token error: <exc>"` for that request | Errors surface per request |

### 2.5 Acceptance Scenarios

- Given a legacy server, when the host sends `initialize` and then the server publishes a list change, then the bridge writes the `initialize` response and then the notification line.
- Given an expired id_token, when the server returns `401`, then the bridge mints a new token and the retried request succeeds.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| HTTP client | Stdlib `urllib` behind a transport port | Adding an HTTP client dependency for one CLI command. |
| Token minting | Upstream `AsyncGcpIdTokenCache` | Shelling out to `gcloud auth print-identity-token` in the bridge duplicates the cache's gcloud source and its refresh logic. |
| Relay start | After legacy `initialize` only | Opening `GET` for `2026-07-28` servers always returns `405`. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch10]` — bridge usage and `--audience` land in `ch11:t02`; CLI OKF in `ch11:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R16.1 | [t01](tasks/t01-bridge-dual-mode-relay.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_mcp_cli.py` |
| R16.2 | [t02](tasks/t02-bridge-oidc-token-source.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_mcp_cli.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch10:t01 — Forward both protocol modes and relay legacy notifications (tasks/t01-bridge-dual-mode-relay.md)
- [ ] mcp-protocol-completeness:ch10:t02 — Authenticate the bridge with refreshed OIDC id_tokens (tasks/t02-bridge-oidc-token-source.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent/mcp src/py/vertebra/cli/ai.py` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/2` tasks completed; `planning_status: empirical_dependent` for `t02` until id_token minting lands.
- **Downstream Dependents**: `ch11:t02` documents the bridge options.

[^bridge]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — Synchronous POST-only bridge.
[^headers]: `src/py/vertebra/agent/mcp/client.py::mcp_request_headers` — Header derivation that always sets `MCP-Protocol-Version`.
[^cli-bridge]: `src/py/vertebra/cli/ai.py::bridge` — `vtb ai mcp bridge` command.
[^gcp-cache]: `.agents/bundles/specs/gcp-embeddings-and-identity/spec.md` — `AsyncGcpIdTokenCache` and `GcpIdTokenSnapshot` contract from `gcp-embeddings-and-identity`.
