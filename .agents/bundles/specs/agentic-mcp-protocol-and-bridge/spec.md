---
type: Spec
flow_id: agentic-mcp-protocol-and-bridge
title: "Chapter 2: MCP 2026-07-28 Protocol Surface, Skills Catalog, Async Tasks, Transports, and ORDS/APEX Synthesis"
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 1
current_task: null
last_operation: closed
operation_targets: []
last_verified_checkpoint: "2026-10-22T14:45:00Z"
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T04:45:00Z
description: "Implement the full MCP 2026-07-28 specification, .agents/skills/ progressive disclosure catalog, SkillController/PromptController, QueueService-backed async tasks/*, Streamable HTTP/SSE/stdio transports, vtb mcp bridge, Rust discovery offload, and executable ORDS/26ai Duality View/APEX tool synthesis."
tags: [feature, agent, mcp, skills]
parent_prd: vertebra-agentic-harness
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 2: MCP 2026-07-28 Protocol Surface, Skills Catalog, Async Tasks, Transports, and ORDS/APEX Synthesis

**Flow ID:** `agentic-mcp-protocol-and-bridge`

## Specification

### Parent Context

This chapter is Chapter 2 of [`vertebra-agentic-harness`](../vertebra-agentic-harness/spec.md) and depends on Chapter 1 (`agentic-core-and-observability`). It replaces the partial `src/py/vertebra/framework/mcp/__init__.py` stub, broken `src/py/vertebra/cli/mcp.py` loop, dead `src/rs/vertebra-http/src/mcp_bridge.rs`, and non-executable `src/py/vertebra/extensions/apex/openapi.py` / `gateway.py` tool builders with a spec-conformant MCP `2026-07-28` stack. It owns master requirement **AH-R3** and resolves findings `MCP-01`, `MCP-02`, `MCP-03`, `MCP-07` (MCP part), `MCP-09` (MCP part), and `MCP-10` (MCP part).

### Code Analysis Summary

Inspected in the current repository and `litestar-mcp` (`v0.16.0`):

- **`src/py/vertebra/framework/mcp/__init__.py:52-174` (232 lines):** `McpServer.handle_jsonrpc` hardcodes `"protocolVersion": "2025-03-26"` (`:115`, `MCP-02`), advertises `"prompts": {}` (`:117`) with no `prompts/list` or `prompts/get` handlers, lacks `ping`, `resources/read`, `resources/templates/list`, `resources/subscribe`, `completion/complete`, `logging/setLevel`, `skills/*`, and `tasks/*`, flattens tool return values with `str(out)` (`:151`), and is never mounted on any HTTP or stdio transport (`MCP-01`).
- **`src/py/vertebra/cli/mcp.py:23-113` (113 lines) & `src/rs/vertebra-http/src/mcp_bridge.rs:1-78` (78 lines):** `cli/mcp.py` uses blocking `urllib.request.urlopen`, sends `Accept: application/json, text/event-stream` (`:27`) without parsing SSE frames, ignores `Mcp-Session-Id`, and only checks `params.name` (`:41`, wrong for `resources/read`, `tasks/*`, and `skills/get`). `mcp_bridge.rs` has an unused `endpoint_url` field (`:32`) and no HTTP client (`MCP-03`).
- **`src/rs/vertebra-http/src/discovery.rs:10-134` (134 lines):** `WellKnownDiscoveryCache::register_mcp_manifest` (`:80-87`) is never wired into the HTTP server ingress and hardcodes `DEFAULT_SPA_EXCLUDED_PREFIXES` (`:10`, `MCP-07`).
- **`src/py/vertebra/extensions/apex/openapi.py:67-152` (152 lines) & `gateway.py:106-255` (255 lines):** `ApexOpenAPIGenerator.load_ords_openapi` drops path/query `parameters[]` and `to_mcp_tools` attaches `_noop_tool_handler` (`:109`, `:121`, `:151-152`). `ApexGateway` stores `McpServer` only as a dict container (`:133`, `MCP-09`).
- **`litestar_mcp/mcp/{routes,service,skills,skill_controller,prompt_controller,tasks,stdio,bridge,client}.py`:** Provides the reference `2026-07-28` header validation (`MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` with `=?base64?...?=` sentinel decoding, Origin allowlist), `SkillCatalog` (`skill://` URIs, `SKILL.md` YAML frontmatter, SHA-256 file integrity verification, `resources/directory/read`), `SkillController` / `PromptController`, `MCPTaskStore` (`io.modelcontextprotocol/tasks`), and Streamable HTTP / stdio / bridge transports.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)` models, `vertebra.utils.serializers` JSON encoding, and zero inline `#` comments.
- [Python Module and Class Layout](../../knowledge/patterns/python/module-and-class-layout.md) — Clean top-down class structure and lazy package exports.

### Requirements

| ID | Requirement |
| --- | --- |
| C2-R1 | Implement `src/py/vertebra/agent/mcp/{registry,service,routes}.py` supporting MCP `2026-07-28` (`initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call` with text/image/audio/resource-link content blocks, `structuredContent`, and `isError`, `resources/list`, `resources/read`, `resources/templates/list`, `resources/subscribe`, `resources/unsubscribe`, `prompts/list`, `prompts/get`, `completion/complete`, `logging/setLevel`), `MCP-Protocol-Version` / `Mcp-Method` / `Mcp-Name` header validation, Base64 header sentinel decoding, Origin validation, ETag caching on read/list methods, and full `AgentObservabilityHarness` telemetry. |
| C2-R2 | Implement `src/py/vertebra/agent/mcp/{skills,controllers}.py` providing `.agents/skills/` progressive disclosure (`Skill`, `SkillFile`, `SkillCatalog`, `FilesystemSkillSource`, `SkillSource` protocol, `skill://` URIs, `skills/list`, `skills/get`, `resources/directory/read`, SHA-256 digest verification on read), `PromptController` (`@prompt`), and `SkillController` unifying `@tool`, `@prompt`, shared `guards`, `instructions`, and `to_agent_skill()`. |
| C2-R3 | Implement `src/py/vertebra/agent/mcp/tasks.py` providing durable `io.modelcontextprotocol/tasks` (`tasks/get`, `tasks/result`, `tasks/update`, `tasks/cancel`, `tasks/list`) with owner-scoped `TaskRecord` persistence over `ResponseCacheStore` and background execution on `vertebra.framework.queues.QueueService` (`TaskLane = "agent"`). |
| C2-R4 | Implement `src/py/vertebra/agent/mcp/{stdio,bridge,client}.py` and update `src/py/vertebra/cli/mcp.py` and `src/rs/vertebra-http/src/` (deleting `mcp_bridge.rs` and wiring configurable `WellKnownDiscoveryCache` `.well-known/mcp.json` offload) to support Streamable HTTP, SSE, and newline-delimited stdio transports plus `vtb mcp bridge`. |
| C2-R5 | Implement executable `Tool` and `Resource` synthesizers in `src/py/vertebra/extensions/apex/__init__.py` and `src/py/vertebra/extensions/apex/gateway.py` for ORDS OpenAPI 3.0 endpoints (including path, query, header, and JSON body schemas), Oracle 26ai JSON Relational Duality Views (with ETag optimistic concurrency), and `ApexGateway` PL/SQL / ORDS services, verified by `src/py/tests/unit/agent/test_mcp_conformance.py` and `src/py/tests/unit/agent/test_mcp_enterprise_tools.py`. |

### Target Layout

```text
src/rs/vertebra-http/src/
  lib.rs                               removes pub mod mcp_bridge
  discovery.rs                         configurable SPA exclusion prefixes and dynamic protocol-version header registration, wired into ingress::Pipeline (from 3.7)
src/py/vertebra/
  cli/
    mcp.py                             vtb mcp bridge and vtb mcp serve commands aligned with consolidated cli/ layout (from 7.5)
  extensions/
    apex/
      __init__.py                      ApexOpenAPIGenerator, OrdsMcpSynthesizer building executable Tools, Resources, and SkillController (from 6.6)
    apex/
      __init__.py                      ApexGateway registering executable Tools and A2A skills with ConfirmationGate enforcement (from 6.6)
  agent/mcp/
    __init__.py                        lazy exports for MCPConfig, MCPRegistry, MCPService, SkillCatalog, SkillController, PromptController, MCPTaskStore, MCPClient
    registry.py                        MCPConfig, ToolRegistration, ResourceRegistration, ResourceTemplateRegistration, PromptRegistration, MCPRegistry
    service.py                         MCPService implementing all MCP 2026-07-28 methods, content normalization, and pagination
    routes.py                          MCPHttpHandler (framework-agnostic ASGI + native App + Litestar controller adapter)
    skills.py                          Skill, SkillFile, SkillCatalog, FilesystemSkillSource, SkillIntegrityError
    controllers.py                     PromptController, SkillController, @prompt, @resource
    tasks.py                           TaskRecord, MCPTaskStore, QueueService(lane="agent") task runner bridge
    stdio.py                           run_stdio_server(service_or_app, stdin, stdout)
    bridge.py                          run_stdio_http_bridge(url, headers, token_provider, max_message_bytes)
    client.py                          MCPStreamableHttpClient, encode_mcp_header_value, mcp_request_headers
src/py/tests/unit/agent/
  test_mcp_conformance.py              end-to-end MCP 2026-07-28, skills, tasks, bridge conformance tests
  test_mcp_enterprise_tools.py        executable ORDS/26ai/APEX MCP tool and resource invocation tests
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Non-ASCII tool or resource names in `Mcp-Name` HTTP headers fail HTTP/1.1 Latin-1 header encoding. | `encode_mcp_header_value` encodes non-ASCII or control-character values as `=?base64?<urlsafe_b64>?=` and `decode_mcp_header_value` decodes and validates them against the JSON-RPC body. |
| Symlinked files inside `.agents/skills/` could escape the skill root directory. | `FilesystemSkillSource` resolves every candidate path and rejects symlinks or paths outside the resolved skill root, plus verifies SHA-256 digests on every read. |

## Implementation Plan

### Phase 1: MCP 2026-07-28 Core Surface, Skills, and Tasks
- [x] 2.1 Full MCP `2026-07-28` registry and handlers (`agent/mcp/{registry,service,routes}.py`) ([worksheet](tasks/2.1.md))
- [x] 2.2 Skills catalog, `.agents/skills/` progressive disclosure loader, `SkillController`, and `PromptController` (`agent/mcp/{skills,controllers}.py`) ([worksheet](tasks/2.2.md))
- [x] 2.3 Async MCP `tasks/*` and queue bridge integration (`agent/mcp/tasks.py`) ([worksheet](tasks/2.3.md))
- [x] Checkpoint: `make lint-all && uv run pytest src/py/tests/unit/agent/test_mcp_conformance.py -k "registry or skills or tasks" -q`

### Phase 2: Transports, CLI Bridge, Discovery Offload, and ORDS/APEX Synthesis
- [x] 2.4 Streamable HTTP, SSE, and stdio transports plus `vtb mcp bridge` CLI and Rust `.well-known/mcp.json` discovery offload ([worksheet](tasks/2.4.md))
- [x] 2.5 ORDS/APEX automatic MCP tool/resource synthesis and end-to-end MCP `2026-07-28` conformance tests ([worksheet](tasks/2.5.md))
- [x] Chapter gate: `make lint-all && make rs-test && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C2-R1 | 2.1, 2.5 | `uv run pytest src/py/tests/unit/agent/test_mcp_conformance.py -k "protocol or registry" -q` |
| C2-R2 | 2.2, 2.5 | `uv run pytest src/py/tests/unit/agent/test_mcp_conformance.py -k "skills or controller" -q` |
| C2-R3 | 2.3, 2.5 | `uv run pytest src/py/tests/unit/agent/test_mcp_conformance.py -k "tasks" -q` |
| C2-R4 | 2.4, 2.5 | `cargo test -p vertebra-http --test mcp_discovery_bridge_tests && uv run pytest src/py/tests/unit/cli/test_mcp_cli.py -q` |
| C2-R5 | 2.5 | `uv run pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_mcp_enterprise_tools.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| MCP-01 | 2.1 |
| MCP-02 | 2.1, 2.4 |
| MCP-03 | 2.4 |
| MCP-07 | 2.4 |
| MCP-09 | 2.5 |
| MCP-10 | 2.5 |

## Continuity Snapshot

- Active flow: `agentic-mcp-protocol-and-bridge`, state `planned`. Current task: none. Next step: `/flow:implement agentic-mcp-protocol-and-bridge` after Chapter 1 completes.
- Plan identity: plan_revision 2, plan_commit: "". State identity: state_revision 0.
- Blockers: none. Decisions: Ingress discovery offload wired to ingress::Pipeline from 3.7; CLI mcp commands aligned with cli/ from 7.5; apex/ords tool synthesis consolidated into apex/__init__.py and apex/__init__.py from 6.6 with tests in test_mcp_enterprise_tools.py.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`.
