---
type: Concept
title: "Oracle APEX, ORDS & 26ai Duality View Integration"
description: "The `vertebra.extensions.apex` package (scoped APEX/VPD Oracle sessions, auth handoff, reactive islands, offline HLC sync, federated DataFusion queries, ORDS/26ai Duality View catalogs, and the policy-gated APEX MCP/A2A gateway) and the `@vertebra/client/apex` browser bridge."
status: stable
tags:
  - domains
  - extensions
  - oracle
  - apex
  - ords
  - duality-views
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T05:50:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-extensions-apex-init-py
    resource: src/py/vertebra/extensions/apex/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/apex/__init__.py)"
  - id: src-extensions-apex-session-py
    resource: src/py/vertebra/extensions/apex/session.py
    title: "Python implementation module (src/py/vertebra/extensions/apex/session.py)"
  - id: src-extensions-apex-bridge-py
    resource: src/py/vertebra/extensions/apex/bridge.py
    title: "Python implementation module (src/py/vertebra/extensions/apex/bridge.py)"
  - id: src-extensions-apex-gateway-py
    resource: src/py/vertebra/extensions/apex/gateway.py
    title: "Python implementation module (src/py/vertebra/extensions/apex/gateway.py)"
  - id: src-agent-runtime-py
    resource: src/py/vertebra/agent/runtime.py::ConfirmationGate
    title: "Shared ConfirmationGate implementation imported by APEX gateway"
  - id: src-extensions-apex-openapi-py
    resource: src/py/vertebra/extensions/apex/openapi.py
    title: "Python implementation module (src/py/vertebra/extensions/apex/openapi.py)"
  - id: src-src-apex-index-ts
    resource: src/ts/vertebra-client/src/apex/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/apex/index.ts)"
  - id: src-tests-unit-extensions-test-oracle-apex-py
    resource: src/py/tests/unit/extensions/test_oracle_apex.py
    title: "Python implementation module (src/py/tests/unit/extensions/test_oracle_apex.py)"
---

# Oracle APEX, ORDS & 26ai Duality View Integration

<!-- truth: start -->
- `vertebra.extensions.apex` manages scoped `APEX_SESSION` and VPD PL/SQL attach-detach lifecycles, auth handoff claims, HLC offline outbox sync, ORDS/26ai Duality View catalogs, and policy-gated MCP/A2A tools.
- `@vertebra/client/apex` pairs `ApexRegion` and `ApexOfflineOutbox` in the browser with server-side reactive islands and delta synchronization.
<!-- truth: end -->

[`vertebra.extensions.apex`](../../../../../src/py/vertebra/extensions/apex/__init__.py)[^src-extensions-apex-init-py] connects Oracle APEX applications, ORDS REST modules, and Oracle 26ai JSON Relational Duality Views with Vertebra (`vtb`). Coverage is in [`test_oracle_apex.py`](../../../../../src/py/tests/unit/extensions/test_oracle_apex.py)[^src-tests-unit-extensions-test-oracle-apex-py].

## 1. Scoped Oracle Sessions & Auth Handoff (`session.py`)

| Symbol | Behavior |
| :--- | :--- |
| [`ApexSessionContext`](../../../../../src/py/vertebra/extensions/apex/session.py)[^src-extensions-apex-session-py] | Frozen struct: `app_id`, `page_id`, `session_id`, `app_user`, optional `workspace_id`, `client_info`, `vpd_context`, and `page_items`. Validates page item and VPD identifiers against `^[A-Za-z][A-Za-z0-9_$#]{0,127}$`. |
| [`ApexHandoffClaims`](../../../../../src/py/vertebra/extensions/apex/session.py) & [`ApexSessionAuth`](../../../../../src/py/vertebra/extensions/apex/session.py) | Issues and verifies HMAC-SHA256 signed APEX-to-Vertebra handoff tokens (`issue_handoff_token`, `verify_handoff_token`, `authenticate_headers`), enforcing token expiration (`exp`) and reconstructing an `ApexSessionContext` so users authenticated in Oracle APEX seamlessly authenticate into a Vertebra app. |
| `OracleSessionManager.build_attach_plsql` (static) | Returns one `BEGIN ... END;` block and a bind dict. The block runs `DBMS_SESSION.SET_IDENTIFIER(:app_user)`, optional `DBMS_APPLICATION_INFO.SET_CLIENT_INFO(:client_info)`, `DBMS_SESSION.SET_CONTEXT` for each `vpd_context` entry, `APEX_SESSION.ATTACH`, and one `APEX_UTIL.SET_SESSION_STATE(:item_k_N, :item_v_N)` per page item. |
| `OracleSessionManager.build_detach_plsql` (static) | Returns `BEGIN APEX_SESSION.DETACH; DBMS_SESSION.CLEAR_IDENTIFIER; END;`. |
| [`OracleSessionManager.acquire_scoped`](../../../../../src/py/vertebra/extensions/apex/session.py) | Async context manager over an `OraclePoolLike`. Sets `conn.module`, `conn.action`, `conn.client_identifier`, and `conn.ecid` / `conn.econtext_id` (zero-RTT OTel piggybacking), runs `build_attach_plsql`, yields the connection, and always runs `build_detach_plsql` and clears the attributes on exit. |

## 2. Reactive Islands, Offline HLC Sync & Federated Query (`bridge.py`)

| Symbol | Behavior |
| :--- | :--- |
| [`ApexIslandSpec`](../../../../../src/py/vertebra/extensions/apex/bridge.py)[^src-extensions-apex-bridge-py] & [`ApexIslandRegistry`](../../../../../src/py/vertebra/extensions/apex/bridge.py) | Renders `<vertebra-island>` HTML mounts hydrated with bound APEX page items and session metadata (`render_island_html`), and synchronizes `ApexSessionContext.page_items` bidirectionally with `LiveState` (`sync_context_to_live_state`, `sync_live_state_to_context`). |
| [`ApexOfflineMutation`](../../../../../src/py/vertebra/extensions/apex/bridge.py) & [`ApexOfflineSyncEngine`](../../../../../src/py/vertebra/extensions/apex/bridge.py) | Replays batches of offline APEX mutations (`set_item`, `row_upsert`, `row_delete`, `duality_patch`) ordered by 64-bit `HybridLogicalClock` timestamps (`sync_batch`), detecting ETag mismatches and stale HLC watermarks while persisting accepted mutations inside `OracleSessionManager.acquire_scoped`. |
| [`ApexFederatedSource`](../../../../../src/py/vertebra/extensions/apex/bridge.py) & [`ApexFederatedQuery`](../../../../../src/py/vertebra/extensions/apex/bridge.py) | Registers Oracle, Parquet, Arrow, or Postgres tables in `DataFusionEngine` and compiles federated query plans enriched with `:APP_ID`, `:APP_PAGE_ID`, `:APP_SESSION`, `:APP_USER`, and bound APEX page items. |

## 3. ORDS and 26ai Duality View Catalogs (`openapi.py`)

| Symbol | Behavior |
| :--- | :--- |
| `OrdsEndpointSpec`, `DualityViewSpec` | Frozen structs describing an ORDS operation or an Oracle 26ai JSON Relational Duality View (`view_name`, `root_table`, `etag_checked=True`, `json_schema`). |
| `ApexToolSpec`, `ApexSkillSpec` | Frozen structs for generated MCP tool and A2A skill descriptors. |
| [`ApexOpenAPIGenerator`](../../../../../src/py/vertebra/extensions/apex/openapi.py)[^src-extensions-apex-openapi-py] | `load_ords_openapi(module_name, openapi_doc)` walks `paths[path][method]` and extracts path/query `parameters[]` and `requestBody` into `OrdsEndpointSpec` entries. `register_duality_view(spec)` adds a `DualityViewSpec`. `to_mcp_tools()` and `to_a2a_skills()` emit `ApexToolSpec` and `ApexSkillSpec` lists. |

## 4. Policy-Gated APEX & ORDS Agent Gateway (`gateway.py`)

| Symbol | Behavior |
| :--- | :--- |
| [`ApexPageToolSpec`](../../../../../src/py/vertebra/extensions/apex/gateway.py)[^src-extensions-apex-gateway-py] | Describes an Oracle APEX page process or region action (`app_id`, `page_id`, `process_name`, `package_procedure`, `page_items`) and converts it into an `ApexToolSpec` and `ApexSkillSpec`. |
| [`ApexAccessPolicy`](../../../../../src/py/vertebra/extensions/apex/gateway.py) & [`ApexPolicyEvaluator`](../../../../../src/py/vertebra/extensions/apex/gateway.py) | Evaluates `authorization_schemes`, `allowed_targets` (deny-by-default), and `max_mutation_amount`, returning `ApexPolicyDecision(allowed, requires_confirmation, reason)`. |
| [`ConfirmationGate`](../../../../../src/py/vertebra/agent/runtime.py)[^src-agent-runtime-py] | Single-use payload-bound confirmation token gate imported directly from `vertebra.agent.runtime` (`approve`, `is_approved`, `consume`). |
| [`ApexGateway`](../../../../../src/py/vertebra/extensions/apex/gateway.py) | Registers APEX page tools (`register_page_tool`) and skills (`register_skill`), publishes `ApexAgentCard`, and executes PL/SQL page processes (`execute_plsql_process`) inside `OracleSessionManager.acquire_scoped` or ORDS REST endpoints (`execute_ords_rest`) with policy and `ConfirmationGate` enforcement. |

## 5. TypeScript Browser Bridge (`@vertebra/client/apex`)

[`src/ts/vertebra-client/src/apex/index.ts`](../../../../../src/ts/vertebra-client/src/apex/index.ts)[^src-src-apex-index-ts] (re-exported from [`src/ts/vertebra-client/src/index.ts`](../../../../../src/ts/vertebra-client/src/index.ts)):

- `ApexRegion(options)` wraps `window.apex.item(name).getValue()` / `setValue(...)` with an in-memory `boundItems` fallback and an embedded `ApexOfflineOutbox`, and triggers APEX Dynamic Actions via `window.apex.event.trigger`.
- `ApexRegion.parseIslands(html | attrs)` parses `<vertebra-island>` tags and `data-component` / `data-region-id` / `data-live-channel` / `data-bound-items` / `data-props` attributes into `IslandDescriptor` objects.
- `ApexOfflineOutbox` queues offline `set_item`, `row_upsert`, `row_delete`, and `duality_patch` mutations with monotonic HLC timestamps, deduplicating repeated edits to the same page item and draining ordered batches on reconnect.

[^src-extensions-apex-init-py]: `src/py/vertebra/extensions/apex/__init__.py` — Python implementation module (src/py/vertebra/extensions/apex/__init__.py).
[^src-extensions-apex-session-py]: `src/py/vertebra/extensions/apex/session.py` — Python implementation module (src/py/vertebra/extensions/apex/session.py).
[^src-extensions-apex-bridge-py]: `src/py/vertebra/extensions/apex/bridge.py` — Python implementation module (src/py/vertebra/extensions/apex/bridge.py).
[^src-extensions-apex-gateway-py]: `src/py/vertebra/extensions/apex/gateway.py` — Python implementation module (src/py/vertebra/extensions/apex/gateway.py).
[^src-agent-runtime-py]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Shared `ConfirmationGate` implementation imported by APEX gateway.
[^src-extensions-apex-openapi-py]: `src/py/vertebra/extensions/apex/openapi.py` — Python implementation module (src/py/vertebra/extensions/apex/openapi.py).
[^src-src-apex-index-ts]: `src/ts/vertebra-client/src/apex/index.ts` — TypeScript client module (src/ts/vertebra-client/src/apex/index.ts).
[^src-tests-unit-extensions-test-oracle-apex-py]: `src/py/tests/unit/extensions/test_oracle_apex.py` — Python implementation module (src/py/tests/unit/extensions/test_oracle_apex.py).
