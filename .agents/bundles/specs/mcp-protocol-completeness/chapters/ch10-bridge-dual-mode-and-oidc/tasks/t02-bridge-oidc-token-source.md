---
type: Task
id: "mcp-protocol-completeness:ch10:t02"
chapter_id: ch10-bridge-dual-mode-and-oidc
title: Authenticate the bridge with refreshed OIDC id_tokens
description: Add OidcTokenSource backed by AsyncGcpIdTokenCache from gcp-embeddings-and-identity, make the bridge invalidate, refresh, and retry once on HTTP 401 and emit -32001 on a second 401, and add the mutually exclusive --audience option to vtb ai mcp bridge.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch10:t01"
read_set:
  - src/py/vertebra/agent/mcp/bridge.py
  - src/py/vertebra/cli/ai.py
  - src/py/vertebra/_vertebra.pyi
  - .agents/bundles/specs/gcp-embeddings-and-identity/spec.md
  - src/py/tests/unit/cli/test_mcp_cli.py
files:
  - src/py/vertebra/agent/mcp/bridge.py
  - src/py/vertebra/cli/ai.py
tests:
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 130
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: bridge
    resource: src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge
    title: Bridge from t01
  - id: cli-bridge
    resource: src/py/vertebra/cli/ai.py::bridge
    title: vtb ai mcp bridge command options
  - id: id-token-contract
    resource: .agents/bundles/specs/gcp-embeddings-and-identity/spec.md
    title: AsyncGcpIdTokenCache and GcpIdTokenSnapshot contract from gcp-embeddings-and-identity
tags:
  - oidc
  - bridge
  - cli
---

# Task t02: Authenticate the Bridge with Refreshed OIDC id_tokens

## Objective & Scope Boundary

The bridge reaches Cloud Run and IAP-protected MCP servers using OIDC id_tokens minted from Application Default Credentials [^bridge] [^cli-bridge]. Minting, caching, and refresh skew belong to `AsyncGcpIdTokenCache` from `gcp-embeddings-and-identity` (`get_token(audience, *, force_refresh=False) -> RustDbTaskAwaitable[GcpIdTokenSnapshot]`, `invalidate(audience, failed_generation)`, snapshot fields `id_token`, `audience`, `expires_at_unix_ms`, `generation`) [^id-token-contract]. Out of scope: implementing minting. The chapter `replan_trigger` confirms the landed signature before execution.

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/bridge.py` — `BridgeTokenSource`, `StaticTokenSource`, frame forwarding and relay from `t01`.
- `src/py/vertebra/cli/ai.py` — `bridge` command, `run_mcp_stdio_bridge`.
- `src/py/vertebra/_vertebra.pyi` — `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot` once landed.
- `.agents/bundles/specs/gcp-embeddings-and-identity/spec.md` — Python surface contract.
- `src/py/tests/unit/cli/test_mcp_cli.py`.

## Symbol & Signature Contracts

- `bridge.py::OidcTokenSource(audience: str, *, cache: AsyncGcpIdTokenCache | None = None)`:
  - Empty `audience` raises `ValueError`.
  - `cache` defaults to `AsyncGcpIdTokenCache()` imported at module top from `vertebra.storage`.
  - `async token(self, *, force_refresh: bool = False) -> str`: when `force_refresh` and a previous snapshot exists, calls `cache.invalidate(audience, snapshot.generation)` first; then `snapshot = await cache.get_token(audience, force_refresh=force_refresh)`; stores the snapshot; returns `snapshot.id_token`.
- Bridge `401` handling (frames and relay): with a token provider, `await token_provider.token(force_refresh=True)` and resend once; a second `401` writes `{"jsonrpc": "2.0", "id": <frame id>, "error": {"code": -32001, "message": "Bridge authentication failed (HTTP 401)"}}`; the relay stops after a second `401` with a `sys.stderr` message.
- Token provider exceptions write `{"jsonrpc": "2.0", "id": <frame id>, "error": {"code": -32001, "message": "Bridge token error: <exc>"}}` for the affected request.
- `cli/ai.py`:
  - `bridge(url: str, bearer_token: str | None, audience: str | None, timeout_s: float)` with `@click.option("--audience", default=None, help="Mint OIDC id_tokens for this audience from Application Default Credentials")`; supplying both `--token` and `--audience` raises `click.UsageError("--token and --audience are mutually exclusive")`.
  - `run_mcp_stdio_bridge(*, url, bearer_token=None, audience=None, timeout_s=30.0, max_message_bytes=4_194_304, stdin=None, stdout=None, transport=None) -> int` builds `OidcTokenSource(audience)` when `audience` is set, else `StaticTokenSource(bearer_token or os.environ.get("VERTEBRA_MCP_TOKEN"))`, and passes it as `token_provider`.

## Edge Cases & Error Semantics

- `force_refresh` before any snapshot skips `invalidate`.
- `StaticTokenSource.token(force_refresh=True)` returns the same static token, so a second `401` reports `-32001`.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/cli/test_mcp_cli.py` (new parametrized `test_bridge_oidc_token_source` and `test_vtb_mcp_bridge_audience_option`; the cache is an in-memory fake exposing `get_token` and `invalidate` and returning `GcpIdTokenSnapshot`-shaped objects).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| First token | Fake cache returns snapshot `id_token="t1", generation=1`; `await source.token()` | `"t1"`; cache saw `get_token("https://svc", force_refresh=False)` |
| Forced refresh | Then `await source.token(force_refresh=True)` with next snapshot `t2`, generation `2` | `"t2"`; cache saw `invalidate("https://svc", 1)` before `get_token(..., force_refresh=True)` |
| Empty audience | `OidcTokenSource("", cache=fake)` | `ValueError` |
| 401 retry | Transport returns `401` then `200`; tokens `t1` then `t2` | Second `POST` header `Authorization == "Bearer t2"`; stdout has the `200` body |
| 401 twice | Transport returns `401`, `401` for id `4` | stdout `{"jsonrpc":"2.0","id":4,"error":{"code":-32001,"message":"Bridge authentication failed (HTTP 401)"}}` |
| Token error | Fake cache raises `RuntimeError("no adc")` for id `6` | stdout `{"jsonrpc":"2.0","id":6,"error":{"code":-32001,"message":"Bridge token error: no adc"}}` |
| CLI audience | `CliRunner().invoke(mcp, ["bridge", "--url", "https://x/mcp", "--audience", "https://x"])` with patched `run_mcp_stdio_bridge` | Called with `audience="https://x"`, `bearer_token=None` |
| CLI exclusive | `["bridge", "--url", "u", "--token", "t", "--audience", "a"]` | Exit `2`; output contains `"mutually exclusive"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q -k "oidc or audience"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/bridge.py src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/bridge.py src/py/vertebra/cli/ai.py`.

## Acceptance Criteria

- [ ] R16.2: OIDC token sourcing, `401` invalidate-refresh-retry, token errors, and CLI options match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] `AsyncGcpTokenCache.sign_jwt_for_audience` at `c8722c76` signs self-issued JWTs and requires service-account keys; it is not an OIDC id_token and is not used here.

[^bridge]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — Bridge from t01.
[^cli-bridge]: `src/py/vertebra/cli/ai.py::bridge` — `vtb ai mcp bridge` command options.
[^id-token-contract]: `.agents/bundles/specs/gcp-embeddings-and-identity/spec.md` — `AsyncGcpIdTokenCache` and `GcpIdTokenSnapshot` contract from `gcp-embeddings-and-identity`.
