---
type: Guide
title: "Oracle APEX, ORDS & E-Business Suite Gateway"
description: "The `vertebra.extensions.apex` package (scoped APEX/EBS Oracle sessions, ORDS and Duality View catalogs for MCP and A2A, ApexLang install scripts, and the policy-gated EBS gateway) and the `@vertebra/client/apex` browser bridge."
status: stable
tags: [domains, extensions, oracle, apex, ebs]
sources:
  - resource: src/py/vertebra/extensions/apex/__init__.py
  - resource: src/py/vertebra/extensions/apex/session.py
  - resource: src/py/vertebra/extensions/apex/openapi.py
  - resource: src/py/vertebra/extensions/apex/apexlang.py
  - resource: src/py/vertebra/extensions/apex/ebs_gateway.py
  - resource: src/ts/vertebra-client/src/apex/index.ts
  - resource: src/py/tests/unit/extensions/test_oracle_apex_and_ebs_gateway.py
---

# Oracle APEX, ORDS & E-Business Suite Gateway

[`vertebra.extensions.apex`](../../../../../src/py/vertebra/extensions/apex/__init__.py) builds Oracle session setup PL/SQL, turns ORDS and Duality View metadata into MCP tools and A2A skills, and gates agent calls into Oracle E-Business Suite (EBS). It depends on [`vertebra.framework.mcp`](../../../../../src/py/vertebra/framework/mcp/__init__.py) (`McpServer`, `McpTool`) and [`vertebra.framework.a2a`](../../../../../src/py/vertebra/framework/a2a/__init__.py) (`AgentCard`, `AgentSkill`). The package has no callers outside itself. Coverage is in [`test_oracle_apex_and_ebs_gateway.py`](../../../../../src/py/tests/unit/extensions/test_oracle_apex_and_ebs_gateway.py).

## 1. Scoped Sessions (`session.py`)

| Symbol | Responsibility |
|---|---|
| `OracleConnectionLike` / `OraclePoolLike` | Protocols for an async `python-oracledb` connection (`module`, `action`, `ecid`, `client_identifier`, `execute`) and pool (`acquire()` as an async context manager). |
| [`ApexSessionContext`](../../../../../src/py/vertebra/extensions/apex/session.py) | Frozen struct: `app_id`, `page_id`, `session_id`, `app_user`, optional `workspace_id`, `ebs_user_id`, `ebs_resp_id`, `ebs_resp_appl_id`, `ebs_org_id`, and `page_items`. |
| `OracleSessionManager.build_attach_plsql` (static) | Returns one `BEGIN ... END;` block and a bind dict. The block runs `DBMS_SESSION.SET_IDENTIFIER(:app_user)`, then `APPS.FND_GLOBAL.APPS_INITIALIZE` (only when user, resp, and resp-appl ids are all set), `MO_GLOBAL.SET_POLICY_CONTEXT('S', :ebs_org_id)` when an org is set, `APEX_SESSION.ATTACH`, and one `APEX_UTIL.SET_SESSION_STATE(:item_k_N, :item_v_N)` per page item. Item names and values are always bound. |
| `OracleSessionManager.build_detach_plsql` (static) | `APEX_SESSION.DETACH; DBMS_SESSION.CLEAR_IDENTIFIER;`. |
| `OracleSessionManager.acquire_scoped` (async context manager) | Acquires a pooled connection and sets `module` (48 chars), `action` (64), `client_identifier` (64), and `ecid` (`trace_id[:span_id]`, 64). It then runs the attach block, yields the connection, and always runs the detach block and clears the attributes in `finally`. |

Doing setup in one PL/SQL block costs one round trip. The `finally` detach keeps APEX, EBS, and VPD state from leaking to the next borrower of the pooled connection.

## 2. ORDS & Duality View Catalog (`openapi.py`)

| Symbol | Responsibility |
|---|---|
| `OrdsEndpointSpec` | `module_name`, `uri_template`, `method`, `operation_id`, `description`, `parameters`. |
| `DualityViewSpec` | `view_name`, `root_table`, `etag_checked`, `json_schema`. |
| [`ApexOpenAPIGenerator`](../../../../../src/py/vertebra/extensions/apex/openapi.py) | `register_endpoint`, `register_duality_view`. `load_ords_openapi(module, doc)` walks `paths` → method → operation, takes `operationId` or synthesizes `{module}_{method}_{path}`, uses `summary`/`description` as the description, and stores `requestBody` as `parameters`. `to_mcp_tools()` returns `McpTool` entries (Duality Views are named `duality_<view>`). `to_a2a_skills()` returns `AgentSkill` entries tagged `oracle`/`ords` or `oracle`/`26ai`/`duality-view`. |

Every generated `McpTool` uses `_noop_tool_handler`, which echoes its kwargs. The catalog describes tools and does not call ORDS. `base_ords_url` is stored but unused.

## 3. ApexLang Install Scripts (`apexlang.py`)

[`ApexLangRunner(workspace_name="VERTEBRA_WS")`](../../../../../src/py/vertebra/extensions/apex/apexlang.py)`.compile_spec(app_id, source_path, spec_source)` returns an `ApexExportArtifact(app_id, source_path, output_sql)`. The SQL is an install header only: it runs `APEX_APPLICATION_INSTALL.SET_WORKSPACE` (single quotes escaped) and `SET_APPLICATION_ID(int(app_id))`, followed by a comment with the source file name and line count. The `.apexlang` content is not parsed.

## 4. EBS Gateway (`ebs_gateway.py`)

| Symbol | Responsibility |
|---|---|
| `EbsResponsibilityMapping` | Maps `principal_email` to EBS user, responsibility, application, and org ids, plus `max_approval_amount` and an `allowed_packages` allow-list. |
| `EbsPolicyEvaluator(confirmation_threshold=10000.0)` | `evaluate(mapping, package_or_service, *, is_mutating, amount)` returns an `EbsPolicyDecision(allowed, requires_confirmation, reason)`. The call is denied when the allow-list is non-empty and does not contain the name (case-insensitive), or when `amount` exceeds a positive `max_approval_amount`. Confirmation is required when the call mutates or `amount >= confirmation_threshold`. |
| [`ConfirmationGate`](../../../../../src/py/vertebra/extensions/apex/ebs_gateway.py) | In-memory set of approved tokens (`approve`, `is_approved`). Tokens are never consumed or expired, so an approved token can be reused. |
| `EbsGateway(agent_name, endpoint_url, *, policy_evaluator, confirmation_gate, isg_transport)` | `register_skill(skill, tool_spec)` adds to both the A2A skill list and an internal `McpServer`. `build_agent_card()` returns an `AgentCard`. `build_mcp_tools()` returns the server's tool map. |
| `EbsGateway.execute_plsql_package(pool, mapping, context, package_procedure, binds, ...)` | Evaluates policy on the package name (text before the first `.`). Raises `PermissionError` on deny. Returns `{"status": "confirmation_required", ...}` when confirmation is needed and the token is not approved. Otherwise it merges the mapping's EBS ids into the context, renders `BEGIN pkg.proc(k => :k, ...); END;`, executes it inside `OracleSessionManager.acquire_scoped`, and returns `{"status": "executed", ...}`. |
| `EbsGateway.execute_isg_rest(mapping, service_alias, payload, *, bearer_token, ...)` | Same policy and confirmation flow. Builds `Authorization` and `X-EBS-Resp-Id` / `X-EBS-Resp-Appl-Id` / `X-EBS-Org-Id` headers and awaits `isg_transport(service_alias, payload, headers)`. Without a transport it returns `{"status": "dispatched", ...}`, including the headers. |

`package_procedure` and bind keys are interpolated into the PL/SQL text. Only bind values are parameterized, so procedure names must come from trusted configuration or a validated allow-list.

## 5. Browser Bridge (`@vertebra/client/apex`)

[`src/ts/vertebra-client/src/apex/index.ts`](../../../../../src/ts/vertebra-client/src/apex/index.ts) is built as the `./apex` package export and re-exported from the package index.

- `ApexRegion` holds `regionId`, `appId`, `pageId`, `sessionId`, and an optional injected `apex` global (`ApexGlobalNamespace`).
- `ApexRegion.parseIslands(html | attrs)` finds `<vertebra-island>` tags and returns `IslandDescriptor { tagName, componentName (data-component or data-island), props (JSON data-props) }`. Malformed `data-props` becomes `{}`.
- `ApexRegion.dispatchDynamicAction(...)` prefers `apex.event.trigger` and falls back to a bubbling `CustomEvent` on the given root.
- Instance methods: `readPageItem`, `writePageItem`, `triggerRegionEvent`.
