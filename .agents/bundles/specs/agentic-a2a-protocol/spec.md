---
type: Spec
flow_id: agentic-a2a-protocol
title: "Chapter 3: Agent-to-Agent (A2A) 1.0 Protocol, Streaming, JWS Agent Card Signing, and MCP/A2A Bridges"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T04:45:00Z
description: "Implement the complete A2A 1.0 protocol surface (frozen msgspec models, JSON-RPC + SSE streaming, push notifications, RFC 7515 JWS Agent Card signing, and bidirectional MCP <-> A2A bridges) with full OpenTelemetry gen_ai.* instrumentation."
tags: [feature, agent, a2a, mcp]
parent_prd: vertebra-agentic-harness
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 3: Agent-to-Agent (A2A) 1.0 Protocol, Streaming, JWS Agent Card Signing, and MCP/A2A Bridges

**Flow ID:** `agentic-a2a-protocol`

## Specification

### Parent Context

This chapter is Chapter 3 of [`vertebra-agentic-harness`](../vertebra-agentic-harness/spec.md). It depends on Chapter 1 (`agentic-core-and-observability`) and Tasks `2.1`–`2.2` of Chapter 2 (`agentic-mcp-protocol-and-bridge`). It replaces the pre-1.0, unmounted `src/py/vertebra/framework/a2a/__init__.py` stub (`MCP-04`) with a complete A2A `1.0` implementation (`models.py`, `service.py`, `routes.py`, `signing.py`) and bidirectional `McpToolBridge` / `A2aAgentBridge` (`bridges/{mcp,a2a}.py`). It owns master requirement **AH-R4** and resolves findings `MCP-04`, `MCP-07` (A2A discovery part), `MCP-09` (A2A skills part), and `MCP-10` (A2A conformance part).

### Code Analysis Summary

Inspected in the current repository and `litestar-mcp` (`v0.16.0`):

- **`src/py/vertebra/framework/a2a/__init__.py:25-167` (167 lines):**
  - `AgentCard.to_dict` (`:47-69`) emits pre-1.0 fields (`url`, `authentication.schemes`) and lacks A2A `1.0` `supportedInterfaces` (`protocolBinding: "JSONRPC"`, `protocolVersion: "1.0"`), `defaultInputModes`, `defaultOutputModes`, and `signatures` (`MCP-04`).
  - `A2AServer.handle_jsonrpc` (`:105-167`) mixes legacy `tasks/send` with `message/send`, stores tasks in an unbounded in-memory `self._tasks` dict with zero caller ownership checks, reports `"canceled"` when cancelling nonexistent task IDs (`:154-162`), and has no HTTP, SSE streaming (`message/stream`, `tasks/resubscribe`), push notification (`tasks/pushNotificationConfig/*`), or `agent/authenticatedExtendedCard` support (`MCP-04`).
- **`src/rs/vertebra-http/src/discovery.rs:71-78` (134 lines):** `WellKnownDiscoveryCache::register_agent_card` serves `/.well-known/agent-card.json` with `a2a-version: 1.0` and FNV-1a ETag `304` handling (`MCP-07`).
- **`litestar_mcp/a2a/plugin.py:122-474` & `litestar_mcp/agent/bridges/{mcp,a2a}.py`:** Provide reference card validation (`_validate_card` checking `supportedInterfaces` for a `1.0` `JSONRPC` binding matching the route path), RFC 9110 weak/strong `If-None-Match` ETag comparison, owner-scoped task stores, and bidirectional MCP <-> A2A protocol bridges.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — Frozen `msgspec.Struct(frozen=True, gc=False)` models, `vertebra.utils.serializers` JSON serialization, and zero inline `#` comments.
- [Python Module and Class Layout](../../knowledge/patterns/python/module-and-class-layout.md) — Clean top-down class structure and PEP 695 generics.

### Requirements

| ID | Requirement |
| --- | --- |
| C3-R1 | Implement `src/py/vertebra/agent/a2a/models.py` defining A2A `1.0` frozen `msgspec.Struct` models (`TaskState`, `TaskStatus`, `TextPart`, `FilePart`, `DataPart`, `Part`, `Message`, `Artifact`, `Task`, `TaskStatusUpdateEvent`, `TaskArtifactUpdateEvent`, `PushNotificationAuthenticationInfo`, `PushNotificationConfig`, `TaskPushNotificationConfig`, `AgentInterface`, `AgentCapabilities`, `AgentProvider`, `AgentSkill`, `AgentCardSignature`, and `AgentCard` with `validate_for_path(route_path: str)`). |
| C3-R2 | Implement `src/py/vertebra/agent/a2a/{service,routes,signing}.py` supporting all A2A `1.0` JSON-RPC + SSE methods (`message/send`, `message/stream`, `tasks/get`, `tasks/list`, `tasks/cancel`, `tasks/resubscribe`, `tasks/pushNotificationConfig/set`, `tasks/pushNotificationConfig/get`, `tasks/pushNotificationConfig/list`, `tasks/pushNotificationConfig/delete`, and `agent/authenticatedExtendedCard`), owner-scoped `A2ATaskStore` backed by `ResponseCacheStore`, RFC 7515 JWS compact/detached Agent Card signing and verification (`AgentCardSigner`), `/.well-known/agent-card.json` ETag/304 + Rust `WellKnownDiscoveryCache` registration, and `AgentObservabilityHarness` telemetry. |
| C3-R3 | Implement `src/py/vertebra/agent/bridges/{mcp,a2a}.py` providing `McpToolBridge` (wrapping remote A2A `1.0` agents and their advertised `AgentSkill` entries as callable MCP `Tool` instances with automatic W3C `traceparent` propagation) and `A2aAgentBridge` (projecting an `MCPRegistry`, `MCPService`, or `SkillController` suite into an A2A `1.0` `AgentCard` and `A2AService` executor). |
| C3-R4 | Validate A2A `1.0` models, card validation and JWS signing, unary and SSE streaming methods, owner isolation, push notification webhook delivery, and bidirectional MCP <-> A2A bridges in `src/py/tests/unit/agent/test_a2a_protocol.py`. |

### Target Layout

```text
src/py/vertebra/agent/
  a2a/
    __init__.py                        lazy exports for A2A 1.0 models, A2AConfig, A2AService, A2ATaskStore, A2AHttpHandler, AgentCardSigner
    models.py                          frozen msgspec.Struct A2A 1.0 wire models (Task, Message, Part, Artifact, AgentCard, PushNotificationConfig)
    service.py                         A2ATaskStore, A2AExecutorProtocol, PushNotificationDispatcher, A2AService (all 11 A2A 1.0 RPC methods)
    routes.py                          A2AHttpHandler (JSON-RPC + SSE streaming + /.well-known/agent-card.json ETag/304 & Rust discovery registration)
    signing.py                         AgentCardSigner (HMAC-SHA256 / Ed25519 / ES256 JWS signing and verification for AgentCardSignature)
  bridges/
    __init__.py                        exports McpToolBridge, A2aAgentBridge, a2a_card_to_mcp_tools, mcp_service_to_a2a
    mcp.py                             McpToolBridge: exposes remote or local A2A 1.0 agents/skills as executable MCP Tools
    a2a.py                             A2aAgentBridge: exposes MCP tools, prompts, and SkillControllers as an A2A 1.0 AgentCard + A2AService
src/py/tests/unit/agent/
  test_a2a_protocol.py                 end-to-end A2A 1.0, JWS signing, push notification, and MCP <-> A2A bridge tests
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Cancelling an unknown or already-terminal A2A task silently returns `"canceled"` (the defect in legacy `framework/a2a/__init__.py:154-162`). | `A2ATaskStore.cancel` looks up the task under caller ownership first, raises `A2ATaskNotFoundError` (`-32001`) if missing or owned by another tenant, and raises `A2ATaskNotCancelableError` (`-32002`) if the task is already in a terminal state (`completed`, `failed`, `canceled`, `rejected`). |
| Push notification webhook URLs could target loopback or link-local metadata IP ranges (SSRF). | `PushNotificationDispatcher` validates webhook URLs against allowed schemes (`https://`, or `http://` only when `allow_insecure_webhooks=True` in tests) and rejects private/link-local literals unless explicitly allowlisted. |

## Implementation Plan

### Phase 1: A2A 1.0 Models, Service, Signing, and Bridges
- [ ] 3.1 Full A2A `1.0` data models as `msgspec.Struct(frozen=True, gc=False)` (`agent/a2a/models.py`) ([worksheet](tasks/3.1.md))
- [ ] 3.2 A2A `1.0` JSON-RPC + SSE streaming methods and JWS Agent Card signing (`agent/a2a/{service,routes,signing}.py`) ([worksheet](tasks/3.2.md))
- [ ] 3.3 Bidirectional MCP <-> A2A bridges (`agent/bridges/{mcp,a2a}.py`) ([worksheet](tasks/3.3.md))
- [ ] 3.4 A2A `1.0` end-to-end streaming, push notification, and bridge integration tests (`test_a2a_protocol.py`) ([worksheet](tasks/3.4.md))
- [ ] Chapter gate: `make lint-all && make rs-test && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C3-R1 | 3.1, 3.4 | `uv run pytest src/py/tests/unit/agent/test_a2a_protocol.py -k "models" -q` |
| C3-R2 | 3.2, 3.4 | `uv run pytest src/py/tests/unit/agent/test_a2a_protocol.py -k "service or streaming or signing or push" -q` |
| C3-R3 | 3.3, 3.4 | `uv run pytest src/py/tests/unit/agent/test_a2a_protocol.py -k "bridge" -q` |
| C3-R4 | 3.4 | `uv run pytest src/py/tests/unit/agent/test_a2a_protocol.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| MCP-04 | 3.1, 3.2 |
| MCP-07 (A2A card cache) | 3.2 |
| MCP-09 (A2A skills in APEX/ORDS) | 3.1 |
| MCP-10 (A2A tests) | 3.4 |

## Continuity Snapshot

- Active flow: `agentic-a2a-protocol`, state `planned`. Current task: none. Next step: `/flow:implement agentic-a2a-protocol` after Chapters 1 and 2 (`2.1`–`2.2`) complete.
- Plan identity: plan_revision 2, plan_commit: "". State identity: state_revision: 0.
- Blockers: none.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`.
