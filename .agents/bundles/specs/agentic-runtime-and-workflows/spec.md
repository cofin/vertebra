---
type: Spec
flow_id: agentic-runtime-and-workflows
title: "Chapter 4: AgentRuntime, Hierarchical SpecTree, DynamicWorkflow DAG Engine, Session Stores, Dual-Host Mounting, and TypeScript SDK"
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 1
current_task: null
last_operation: "commit"
operation_targets: [src/py/vertebra/agent/runtime.py, src/py/vertebra/agent/spec.py, src/py/vertebra/agent/workflow.py, src/py/vertebra/agent/sessions.py, src/py/vertebra/agent/controller.py, src/py/vertebra/agent/plugin.py, src/ts/vertebra-client/src/agent/index.ts]
last_verified_checkpoint: "e8edc02"
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T15:33:00Z
description: "Implement the extendable AgentRuntime with ModelProvider and ConfirmationGate, hierarchical SpecTree and DynamicWorkflow DAG engine with LiveState HLC delta broadcasting, pluggable AgentSessionStore backends, unified native App and LitestarPlugin mounting, and the @vertebra/client/agent TypeScript SDK."
tags: [feature, agent, workflow, typescript]
parent_prd: vertebra-agentic-harness
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 4: AgentRuntime, Hierarchical SpecTree, DynamicWorkflow DAG Engine, Session Stores, Dual-Host Mounting, and TypeScript SDK

**Flow ID:** `agentic-runtime-and-workflows`

## Specification

### Parent Context

This chapter is Chapter 4 of [`vertebra-agentic-harness`](../vertebra-agentic-harness/spec.md). It depends on Chapters 1 (`agentic-core-and-observability`), 2 (`agentic-mcp-protocol-and-bridge`), and 3 (`agentic-a2a-protocol`). It delivers the high-level agentic runtime (`Agent`, `AgentGroup`, `AgentRuntime`, `ConfirmationGate`), hierarchical task decomposition (`SpecTree`), concurrent DAG orchestration (`DynamicWorkflow` + `LiveState` HLC binary delta streaming), pluggable session persistence (`AgentSessionStore` + streaming chat controller), unified mounting on both native `vertebra.framework.App` and Litestar (`LitestarPlugin` + `litestar-mcp` interoperability), deletion of the legacy `framework/mcp`, `framework/a2a`, and `extensions/litestar/ecosystem.py` stubs, and the TypeScript `@vertebra/client/agent` SDK subpath. It owns master requirement **AH-R5** and completes findings `MCP-01`, `MCP-04`, `MCP-05`, and `MCP-08`.

### Code Analysis Summary

Inspected in the current repository and `litestar-mcp` (`v0.16.0`):

- **`litestar_mcp/agent/{runtime,spec,workflow,sessions,controller,models,streaming}.py`:**
  - `runtime.py:147-578` implements `AgentRuntime` with `TurnBudget`, owner-scoped `SessionStore`, `CompactionHook`, `AgentGroup` transfer tool synthesis (`transfer_to_agent`), and parallel tool execution, but uses `@dataclass` models instead of `msgspec.Struct(frozen=True, gc=False)` and lacks retry/backoff and human-in-the-loop `ConfirmationGate` pause/resume.
  - `workflow.py:74-296` implements `DynamicWorkflow` and `WorkflowEngine` with topological wave execution via `anyio.create_task_group()`, but lacks hierarchical `SpecTree` task planning, checkpoint serialization, and real-time `LiveState` HLC delta broadcasting.
  - `sessions.py:28-141` implements `MemorySessionStore` and `LitestarStoreSessionStore`, which should be extended in Vertebra to support `Memory`, `ResponseCacheStore`, `SQLSpec`, `Alchemy`, and `ObjectStore` backends.
- **`src/py/vertebra/framework/live/__init__.py:90-250` (404 lines):** Defines `HybridLogicalClock`, `StateDeltaFrame`, `StateOpcode`, `LiveState`, and `LiveStateRoom`, which enable zero-polling binary MessagePack delta broadcasting of `SpecTree` and `DynamicWorkflow` node transitions to connected frontend clients.
- **`src/py/vertebra/extensions/litestar/plugin.py:28-100` (152 lines) & `ecosystem.py:21-93` (93 lines) & `src/py/vertebra/framework/__init__.py:5-22`:** `ecosystem.py` checks `isinstance(state, dict)` (`:85`), which silently no-ops on real Litestar `State` instances (`MCP-05`). `framework/__init__.py` eagerly imports `mcp` and `a2a` (`MCP-08`).
- **`src/ts/vertebra-client/src/index.ts:1-49`, `package.json`, & `tsup.config.ts`:** Exposes `HttpClient`, `SseClient`, `LiveStateClient`, and `TraceContext`, providing the exact building blocks for `@vertebra/client/agent`.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, `vertebra.utils.serializers` JSON/MessagePack codecs, and clean-break domain naming.
- [Client SDK and Framework Adapter Subpaths](../../knowledge/patterns/typescript/client-and-adapter-subpaths.md) — Subpath exports in `src/ts/vertebra-client/package.json` and `tsup.config.ts`.

### Requirements

| ID | Requirement |
| --- | --- |
| C4-R1 | Implement `src/py/vertebra/agent/runtime.py` providing `ModelDelta`, `ModelProvider` protocol, `GoogleGenAIProvider`, `AgentStreamFrame`, `ConfirmationGate` (supporting human-in-the-loop pause/approve/reject for mutating tools), `TurnRequest`, `TurnResponse`, and `AgentRuntime` (multi-turn tool loop, parallel tool execution, exponential backoff retry, `CompactionHook`, `TurnBudget`, `AgentGroup` delegation, and `QueueService(lane="agent")` background turn dispatch) with full `AgentObservabilityHarness` instrumentation. |
| C4-R2 | Implement `src/py/vertebra/agent/{spec,workflow}.py` providing `Agent`, `AgentGroup`, `AgentMessage`, hierarchical `SpecTree` (`SpecNode`, `SpecNodeStatus`, dependency resolution, dynamic subtask expansion, checkpointing), and `DynamicWorkflow` / `WorkflowEngine` (topological wave execution, conditional edges, parallel fan-out/fan-in, checkpoint resume, and live `StateDeltaFrame` broadcasting over `vertebra.framework.live.LiveStateRoom`). |
| C4-R3 | Implement `src/py/vertebra/agent/{sessions,controller}.py` providing `AgentSessionStore` protocol with 5 backends (`MemorySessionStore`, `CacheSessionStore` over `ResponseCacheStore`, `SQLSpecSessionStore`, `AlchemySessionStore`, and `ObjectStoreSessionStore` over `_storage`) and `AgentChatController` serving SSE streaming `POST /agent/chat`, synchronous `POST /agent/turns`, background queued `POST /agent/turns/async` (`202 Accepted` + `Last-Event-ID` replay), confirmation approval `POST /agent/confirmations/{token}`, and owner-scoped `GET /agent/sessions/{session_id}`. |
| C4-R4 | Implement `src/py/vertebra/agent/plugin.py` and update `src/py/vertebra/extensions/litestar/{config,plugin,__init__}.py` and `src/py/vertebra/framework/__init__.py` (deleting `src/py/vertebra/framework/mcp/`, `src/py/vertebra/framework/a2a/`, and `src/py/vertebra/extensions/litestar/ecosystem.py`) so `AgentPlugin` mounts MCP, A2A, and `AgentChatController` + `.well-known` discovery on native `vertebra.framework.App` and `LitestarPlugin` mounts them on Litestar with full `litestar-mcp` route/controller discovery interoperability. |
| C4-R5 | Implement `src/ts/vertebra-client/src/agent/index.ts` (wired into `package.json`, `tsup.config.ts`, and `src/index.ts`) and `src/ts/vertebra-client/tests/agent.test.ts` providing `McpClient`, `A2aClient`, `AgentStreamClient`, and `WorkflowLiveSubscriber` with W3C `TraceContext` propagation and `Last-Event-ID` SSE resumption. |

### Target Layout

```text
src/py/vertebra/
  framework/
    __init__.py                        removes legacy mcp and a2a eager imports
    app.py                             native App router and mounting logic (consolidated in 5.1)
  extensions/litestar/
    plugin.py                          consolidated LitestarConfig and LitestarPlugin mounting AgentPlugin in on_app_init (from 6.3)
  agent/
    __init__.py                        lazy exports for runtime, spec, workflow, sessions, controller, plugin
    runtime.py                         ModelDelta, ModelProvider, GoogleGenAIProvider, ConfirmationGate, AgentStreamFrame, TurnRequest, TurnResponse, AgentRuntime
    spec.py                            Agent, AgentGroup, AgentMessage, SpecNode, SpecNodeStatus, SpecTree
    workflow.py                        WorkflowNode, StepResult, WorkflowContext, WorkflowLiveState, DynamicWorkflow, WorkflowEngine
    sessions.py                        AgentSessionStore, MemorySessionStore, CacheSessionStore, SQLSpecSessionStore, AlchemySessionStore, ObjectStoreSessionStore
    controller.py                      ChatRequest, TurnResult, SessionHistory, ConfirmationRequest, AgentChatController
    plugin.py                          AgentPluginConfig, AgentPlugin (mounts on native vertebra.framework.app.App and Litestar)
src/py/tests/unit/agent/
  test_runtime_and_workflows.py        unit and integration tests for Tasks 4.1–4.4
src/ts/vertebra-client/
  package.json                         adds "./agent" export subpath
  tsup.config.ts                       adds "agent": "src/agent/index.ts" entry (consolidated in 7.6)
  src/
    index.ts                           re-exports ./agent/index
    agent/
      index.ts                         McpClient, A2aClient, AgentStreamClient, WorkflowLiveSubscriber
  tests/
    agent.test.ts                      Vitest suite for @vertebra/client/agent
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Deleting `src/py/vertebra/framework/mcp` and `src/py/vertebra/framework/a2a` breaks existing imports in `extensions/apex` or `extensions/litestar/ecosystem.py`. | Tasks `2.5` and `3.1` already migrated `extensions/apex` to `vertebra.agent`, and Task `4.4` deletes `ecosystem.py` and updates all remaining callers and tests in the same commit, verified by `rg 'framework\.(mcp|a2a)' src/py`. |
| Concurrent `DynamicWorkflow` wave nodes mutating shared `WorkflowContext` state cause race conditions or out-of-order UI deltas. | `WorkflowEngine` updates `WorkflowLiveState` inside a per-run `anyio.Lock`, stamps each field update with `HybridLogicalClock.tick()`, and broadcasts `StateDeltaFrame` packets through `LiveStateRoom`. |

## Implementation Plan

### Phase 1: AgentRuntime, SpecTree, DynamicWorkflow, and Session Stores
- [x] 4.1 Extendable `AgentRuntime` with pluggable model providers, tool execution loop, retry/backoff, and `ConfirmationGate` (`agent/runtime.py`) ([worksheet](tasks/4.1.md))
- [x] 4.2 `SpecTree` hierarchical task planning and `DynamicWorkflow` DAG engine with `LiveState` delta broadcasting (`agent/{spec,workflow}.py`) ([worksheet](tasks/4.2.md))
- [x] 4.3 Pluggable `AgentSessionStore` (5 backends) and streaming chat controller (`agent/{sessions,controller}.py`) ([worksheet](tasks/4.3.md))
- [x] Checkpoint: `make lint-all && uv run pytest src/py/tests/unit/agent/test_runtime_and_workflows.py -k "runtime or workflow or session" -q`

### Phase 2: Dual-Host Mounting and TypeScript `@vertebra/client/agent` SDK
- [x] 4.4 Unified mounting on both native `vertebra.framework.app.App` and Litestar (`agent/plugin.py`, `extensions/litestar/plugin.py`) ([worksheet](tasks/4.4.md))
- [x] 4.5 TypeScript `@vertebra/client/agent` SDK subpath and Vitest suite (`src/ts/vertebra-client/src/agent/index.ts`) ([worksheet](tasks/4.5.md))
- [x] Chapter gate: `make lint-all && make rs-test && make py-test && make ts-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C4-R1 | 4.1, 4.4 | `uv run pytest src/py/tests/unit/agent/test_runtime_and_workflows.py -k "runtime" -q` |
| C4-R2 | 4.2, 4.4 | `uv run pytest src/py/tests/unit/agent/test_runtime_and_workflows.py -k "spectree or workflow" -q` |
| C4-R3 | 4.3, 4.4 | `uv run pytest src/py/tests/unit/agent/test_runtime_and_workflows.py -k "session or controller" -q` |
| C4-R4 | 4.4 | `uv run pytest src/py/tests/unit/agent/test_runtime_and_workflows.py src/py/tests/unit/extensions/test_litestar_plugin.py -q` |
| C4-R5 | 4.5 | `bun run --cwd src/ts/vertebra-client test tests/agent.test.ts` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| MCP-01 (delete legacy `framework/mcp`) | 4.4 |
| MCP-04 (delete legacy `framework/a2a`) | 4.4 |
| MCP-05 (delete broken `ecosystem.py` and wire `LitestarPlugin`) | 4.4 |
| MCP-08 (remove eager `mcp`/`a2a` imports from `framework/__init__.py`) | 4.4 |

## Continuity Snapshot

- Active flow: `agentic-runtime-and-workflows`, state `closed`. All tasks 4.1-4.5 completed and verified.
- Plan identity: plan_revision 2, plan_commit: "". State identity: state_revision 1.
- Blockers: none. Standing by for `feat/queues-and-messaging` (PR 3) to rebase and hand off PR 4.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`, `.agents/bundles/knowledge/patterns/typescript/client-and-adapter-subpaths.md`.
