---
type: Task
id: critical-correctness-fixes:t02
title: WebMCP per-tool opt-in and MCP confirmation enforcement
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/agent/mcp/webmcp.py
  - src/py/vertebra/agent/mcp/controllers.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/runtime.py
  - src/py/vertebra/agent/plugin.py
  - docs/usage/a2a-and-workflows.md
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/extensions/test_vite_and_docs_portal.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
plan_revision: 2
plan_commit: 24e2931
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:00:00Z
commit: null
tags: [bugfix, security, mcp, webmcp]
---

# t02 — WebMCP per-tool opt-in and MCP confirmation enforcement

## Objective

Browser WebMCP manifests list only tools that opt in with `@mcp_tool(webmcp=True)`, and `MCPService` refuses to run a tool marked `requires_confirmation=True` until the caller presents a ticket approved for that tool, caller, and arguments. `ConfirmationGate.approve` and `reject` change no state when the ticket is unknown or owned by another caller.

## Context

- **Strategy**: `regression_tdd`. Both defects reproduce at `142cc49`.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest` skips a tool only when `cfg.include_tagged_only and not meta.get("webmcp", True)`. `WebMCPConfig.include_tagged_only` defaults to `False`, and `src/py/vertebra/agent/mcp/controllers.py::mcp_tool` defaults `webmcp=True`, so every tool is listed, including `Tool` objects without `_mcp_tool_meta` (the GCP database tools from `db/mcp_tools.py::register_gcp_database_tools`).
  - `src/py/vertebra/agent/mcp/service.py::MCPService._handle_tools_call` never reads `target_tool.requires_confirmation`.
  - `src/py/vertebra/agent/runtime.py::ConfirmationGate.approve` adds the token to `_approved_tokens` before it checks that the ticket exists and that the owner matches. It returns `True` for an unknown token and `False` for another owner, but in both cases the token is already recorded, and `is_approved` then returns `True` for the ticket's real owner. `ConfirmationGate.reject` likewise discards the token before its owner check, so another caller can revoke an approval.
  - `src/py/vertebra/agent/plugin.py::AgentPlugin.__init__` builds `MCPService` before `AgentRuntime`, so the two use different gates and `/agent/confirmations/{token}` cannot approve MCP tickets.
- **Controller tools**: `SkillController.get_tools` wraps `@mcp_tool` methods as `Tool(bound_method, ...)`. A bound method exposes the function's `_mcp_tool_meta`, so tagged controller methods stay visible to the manifest. Guarded `@tool` attributes (`_guarded_fn`) carry no `_mcp_tool_meta` and are never browser-exposed.
- **Opt-dict tools**: tools registered from a route `opt["mcp_tool"]` entry (`src/py/vertebra/agent/mcp/registry.py::MCPRegistry.register_route_handler` `opt_tool` branch, `src/py/vertebra/agent/plugin.py` `opt["mcp_tool"]` handling) carry no `_mcp_tool_meta`, so they have no WebMCP exposure path until `mcp-webmcp-core` adds one (recorded in spec R4 and Non-Goals).
- **Contract chosen** (spec §2.2): the token is read from `params["_meta"]["vertebra/confirmationToken"]`. The service runs the tool only when `ticket.status == "approved"`, `ticket.owner_id is None or ticket.owner_id == <caller owner>`, `ticket.tool_name == <tool>`, and `ticket.arguments == <call arguments>`. Otherwise it issues a new ticket (arguments copied) and raises `JSONRPCErrorException(JSONRPCError(code=CONFIRMATION_REQUIRED, message=..., data={"confirmationToken": <new token>, "tool": <name>}))` with `CONFIRMATION_REQUIRED = -32003`. Tokens stay reusable for identical calls after approval (single-use tokens are out of scope, `mcp-webmcp-core`).
- **Gate consumers checked**: `AgentRuntime` (`is_approved` with existing tickets), `AgentChatController.decide_confirmation` (maps `False` to `SessionNotFoundError`, which is now also the unknown-token result), and `db/mcp_tools.py` (approves existing tickets). The APEX gateway (`extensions/apex/gateway.py`) has its own gate class and is unaffected. No test approves an unknown token.
- **Existing test that changes**: `src/py/tests/unit/extensions/test_vite_and_docs_portal.py::test_configurable_docs_portal_and_route_annotated_webmcp` asserts both untagged tools appear in the manifest. It is updated to tag `archive_workspace` and assert only that tool is listed.
- **Canonical test modules**: `src/py/tests/unit/extensions/test_vite_and_docs_portal.py` (WebMCP manifest tests), `src/py/tests/unit/agent/test_mcp_conformance.py` (MCPService tests).
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.
- **Knowledge**: [mcp-and-a2a-protocol-harness](../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md).

## Steps

### 1. Write failing tests (RED)

1. `src/py/tests/unit/extensions/test_vite_and_docs_portal.py`:
   - Add `from vertebra.agent.core.schema import Tool` to the first-party imports.
   - In `test_configurable_docs_portal_and_route_annotated_webmcp`, change the controller decorator to `@mcp_tool(name="archive_workspace", description="Archive a workspace by ID", webmcp=True)`, leave `create_ticket` untagged, and replace the final four manifest assertions with:

   ```python
   webmcp_json = render_webmcp_manifest_json(registry, WebMCPConfig(mcp_endpoint="/mcp"))
   webmcp_data = decode_json(webmcp_json)
   assert webmcp_data["mcpEndpoint"] == "/mcp"
   assert [t["name"] for t in webmcp_data["tools"]] == ["archive_workspace"]
   manifest_obj = build_webmcp_manifest(registry)
   assert [t.name for t in manifest_obj.tools] == ["archive_workspace"]
   ```

   - Append:

   ```python
   def test_webmcp_manifest_lists_only_tagged_tools() -> None:
       """build_webmcp_manifest lists @mcp_tool(webmcp=True) tools and omits untagged tools."""

       @mcp_tool(name="browser_search", description="Search from the browser", webmcp=True)
       def browser_search(query: str) -> list[str]:
           return [query]

       @mcp_tool(name="server_only", description="Server-side only tool")
       def server_only(query: str) -> list[str]:
           return [query]

       def bigquery_query(sql: str) -> dict[str, str]:
           return {"sql": sql}

       registry = MCPRegistry()
       registry.register_route_handler(browser_search)
       registry.register_route_handler(server_only)
       registry.register_tool(Tool(bigquery_query, name="bigquery_query", description="Run a governed query"))

       manifest = build_webmcp_manifest(registry, WebMCPConfig())
       assert [t.name for t in manifest.tools] == ["browser_search"]
   ```

2. `src/py/tests/unit/agent/test_mcp_conformance.py`:
   - Add imports (top level, grouped):

   ```python
   from vertebra.agent.controller import ConfirmationDecisionRequest
   from vertebra.agent.core.jsonrpc import JSONRPCErrorException
   from vertebra.agent.mcp.service import CONFIRMATION_REQUIRED, CONFIRMATION_TOKEN_META_KEY
   from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
   from vertebra.agent.runtime import ConfirmationGate
   ```

   - Append:

   ```python
   @pytest.mark.anyio
   async def test_mcp_tool_call_requires_approved_confirmation_token() -> None:
       """A confirmation-gated tool runs only with an approved ticket issued for that tool."""
       gate = ConfirmationGate()
       service = MCPService(confirmation_gate=gate)
       calls: list[str] = []

       @tool(name="drop_workspace", description="Drop a workspace", requires_confirmation=True)
       def drop_workspace(workspace_id: str) -> str:
           calls.append(workspace_id)
           return f"dropped:{workspace_id}"

       @tool(name="archive_workspace", description="Archive a workspace", requires_confirmation=True)
       def archive_workspace(workspace_id: str) -> str:
           calls.append(f"archive:{workspace_id}")
           return f"archived:{workspace_id}"

       service.registry.register_tool(drop_workspace)
       service.registry.register_tool(archive_workspace)

       with pytest.raises(JSONRPCErrorException) as first:
           await service.handle_tool_call({"name": "drop_workspace", "arguments": {"workspace_id": "w1"}})
       assert first.value.error.code == CONFIRMATION_REQUIRED
       assert first.value.error.data["tool"] == "drop_workspace"
       token = first.value.error.data["confirmationToken"]
       assert calls == []

       with pytest.raises(JSONRPCErrorException) as unapproved:
           await service.handle_tool_call(
               {"name": "drop_workspace", "arguments": {"workspace_id": "w1"}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
           )
       assert unapproved.value.error.code == CONFIRMATION_REQUIRED
       assert calls == []

       assert gate.approve(token) is True
       with pytest.raises(JSONRPCErrorException) as wrong_tool:
           await service.handle_tool_call(
               {"name": "archive_workspace", "arguments": {"workspace_id": "w1"}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
           )
       assert wrong_tool.value.error.code == CONFIRMATION_REQUIRED
       assert calls == []

       with pytest.raises(JSONRPCErrorException) as wrong_arguments:
           await service.handle_tool_call(
               {"name": "drop_workspace", "arguments": {"workspace_id": "w2"}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
           )
       assert wrong_arguments.value.error.code == CONFIRMATION_REQUIRED
       assert calls == []

       result = await service.handle_tool_call(
           {"name": "drop_workspace", "arguments": {"workspace_id": "w1"}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
       )
       assert result["content"][0]["text"] == "dropped:w1"
       assert calls == ["w1"]


   @pytest.mark.anyio
   async def test_mcp_confirmation_rejects_approval_by_another_owner() -> None:
       """A failed approval by another owner changes no gate state and leaves the call blocked."""
       gate = ConfirmationGate()
       service = MCPService(confirmation_gate=gate)
       calls: list[str] = []

       @tool(name="drop_workspace", description="Drop a workspace", requires_confirmation=True)
       def drop_workspace(workspace_id: str) -> str:
           calls.append(workspace_id)
           return f"dropped:{workspace_id}"

       service.registry.register_tool(drop_workspace)
       alice = AgentContext(identity=CallerIdentity(user_id="alice"))
       call = {"name": "drop_workspace", "arguments": {"workspace_id": "w1"}}

       with pytest.raises(JSONRPCErrorException) as first:
           await service.handle_tool_call(call, context=alice)
       token = first.value.error.data["confirmationToken"]
       assert gate.get_ticket(token).owner_id == "default:alice"

       assert gate.approve(token, owner_id="default:mallory") is False
       assert gate.get_ticket(token).status == "pending"
       assert gate.is_approved(token, owner_id="default:alice") is False

       with pytest.raises(JSONRPCErrorException) as blocked:
           await service.handle_tool_call({**call, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}, context=alice)
       assert blocked.value.error.code == CONFIRMATION_REQUIRED
       assert calls == []


   def test_confirmation_gate_rejects_unknown_tokens() -> None:
       """Approving or rejecting an unknown token fails without recording the token."""
       gate = ConfirmationGate()
       assert gate.approve("unknown-token") is False
       assert gate.is_approved("unknown-token") is False
       assert gate.reject("unknown-token") is False
       assert gate.get_ticket("unknown-token") is None


   @pytest.mark.anyio
   async def test_agent_plugin_mcp_confirmation_uses_runtime_gate() -> None:
       """AgentPlugin approvals through the chat controller unlock MCP confirmation tickets."""
       plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="confirm-mcp")))
       assert plugin.mcp_service is not None

       @tool(name="purge_cache", description="Purge the cache", requires_confirmation=True)
       def purge_cache() -> str:
           return "purged"

       plugin.mcp_service.registry.register_tool(purge_cache)
       with pytest.raises(JSONRPCErrorException) as exc_info:
           await plugin.mcp_service.handle_tool_call({"name": "purge_cache", "arguments": {}})
       token = exc_info.value.error.data["confirmationToken"]

       decision = await plugin.chat_controller.decide_confirmation(token, ConfirmationDecisionRequest(decision="approve"))
       assert decision == {"status": "ok", "decision": "approve", "token": token}

       result = await plugin.mcp_service.handle_tool_call(
           {"name": "purge_cache", "arguments": {}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
       )
       assert result["content"][0]["text"] == "purged"
   ```

   `AgentContext` and `CallerIdentity` are already imported from `vertebra.agent.core.context` in this module. `CallerIdentity(user_id="alice").owner_key()` is `"default:alice"`.

3. Run RED:

   ```bash
   uv run pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py::test_webmcp_manifest_lists_only_tagged_tools src/py/tests/unit/extensions/test_vite_and_docs_portal.py::test_configurable_docs_portal_and_route_annotated_webmcp src/py/tests/unit/agent/test_mcp_conformance.py -q
   ```

   Expected: `test_mcp_conformance.py` fails at collection with `ImportError: cannot import name 'CONFIRMATION_REQUIRED'`; both WebMCP tests fail on the tool-name assertions (the manifest lists untagged tools). Before the import exists, `test_confirmation_gate_rejects_unknown_tokens` would fail on `assert gate.approve("unknown-token") is False`, and the other-owner test on `assert gate.is_approved(token, owner_id="default:alice") is False`.

### 2. Implement (GREEN)

1. `src/py/vertebra/agent/mcp/controllers.py::mcp_tool`: change the parameter to `webmcp: bool = False`. Extend the docstring with an `Args:` section documenting `webmcp` as "List the tool in the browser WebMCP manifest. Defaults to `False`; browser exposure is opt-in per tool."
2. `src/py/vertebra/agent/mcp/webmcp.py`:
   - Delete the `include_tagged_only` field from `WebMCPConfig`.
   - In `build_webmcp_manifest`, replace the metadata lookup and skip condition with:

   ```python
   meta = getattr(t.fn, "_mcp_tool_meta", None) or {}
   if meta.get("webmcp") is not True:
       continue
   ```

   - Update the docstring to: `"""Build a WebMCPManifest listing only tools decorated with ``@mcp_tool(webmcp=True)``.\n\n    Tools without that tag, including plain ``Tool`` objects, are never browser-exposed.\n    """`
3. `src/py/vertebra/agent/mcp/service.py`:
   - Add `from vertebra.agent.runtime import ConfirmationGate` to the first-party imports.
   - Add module constants after `logger = ...`:

   ```python
   CONFIRMATION_REQUIRED: int = -32003
   CONFIRMATION_TOKEN_META_KEY: str = "vertebra/confirmationToken"
   ```

   - Extend `__all__` to `["CONFIRMATION_REQUIRED", "CONFIRMATION_TOKEN_META_KEY", "MCPService"]`.
   - `MCPService.__init__`: add a final parameter `confirmation_gate: ConfirmationGate | None = None` and set `self.confirmation_gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()` next to the other attribute assignments.
   - `_handle_tools_call`: after the `target_tool is None` check, compute `meta = params.get("_meta", {})` and `call_id = str(params.get("callId", "call_0"))` (move the existing `call_id` line up), then insert:

   ```python
   if target_tool.requires_confirmation:
       self._require_confirmation(tool_name, call_id, arguments, meta, context)
   ```

   before the `io.modelcontextprotocol/task` branch, so both the task path and the direct path are gated.
   - Add the private method directly after `_caller_owner`:

   ```python
   def _require_confirmation(
       self,
       tool_name: str,
       call_id: str,
       arguments: dict[str, Any],
       meta: Any,
       context: Any,
   ) -> None:
       """Raise unless the call carries a ticket approved for this tool, caller, and arguments.

       The token is read from ``_meta["vertebra/confirmationToken"]``. It must name an existing
       ticket with ``status == "approved"``, issued for ``tool_name`` with identical
       ``arguments``, and owned by the caller (or by no owner).

       Raises:
           JSONRPCErrorException: With code ``CONFIRMATION_REQUIRED`` and a newly issued ticket
               token in ``data`` when the token is missing, unknown, unapproved, owned by another
               caller, or issued for another tool or other arguments.
       """
       owner_id = self._caller_owner(context)
       token = meta.get(CONFIRMATION_TOKEN_META_KEY) if isinstance(meta, Mapping) else None
       ticket = self.confirmation_gate.get_ticket(token) if isinstance(token, str) else None
       if (
           ticket is not None
           and ticket.status == "approved"
           and (ticket.owner_id is None or ticket.owner_id == owner_id)
           and ticket.tool_name == tool_name
           and ticket.arguments == arguments
       ):
           return
       issued = self.confirmation_gate.request_confirmation(
           tool_name,
           call_id,
           dict(arguments),
           owner_id=owner_id,
           reason=f"Tool '{tool_name}' requires confirmation.",
       )
       raise JSONRPCErrorException(
           JSONRPCError(
               code=CONFIRMATION_REQUIRED,
               message=f"Tool '{tool_name}' requires confirmation",
               data={"confirmationToken": issued.token, "tool": tool_name},
           )
       )
   ```

4. `src/py/vertebra/agent/runtime.py::ConfirmationGate`:
   - Replace `approve` so state changes only after both checks pass:

   ```python
   def approve(self, token: str, *, owner_id: str | None = None) -> bool:
       """Approve a pending confirmation ticket.

       Returns:
           True when the ticket exists and ``owner_id`` may decide it; False otherwise, with no
           change to the gate.
       """
       ticket = self._tickets.get(token)
       if ticket is None:
           return False
       if owner_id is not None and ticket.owner_id is not None and ticket.owner_id != owner_id:
           return False
       self._approved_tokens.add(token)
       self._tickets[token] = msgspec.structs.replace(ticket, status="approved")
       return True
   ```

   - Replace `reject` the same way: look up the ticket, return `False` (no mutation) when it is missing or owned by another caller, then `self._approved_tokens.discard(token)` and store `msgspec.structs.replace(ticket, status="rejected", reason=reason or ticket.reason)`; return `True`.
   - `msgspec` is already imported in `runtime.py`; `msgspec.structs.replace` keeps every other ticket field, replacing the hand-written `ConfirmationTicket(...)` copies.
5. `src/py/vertebra/agent/plugin.py::AgentPlugin.__init__`: move `self.runtime = self.config.runtime or AgentRuntime(telemetry=recorder)` above `self.mcp_registry = MCPRegistry()` and pass `confirmation_gate=self.runtime.confirmation_gate` to `MCPService(...)`. Leave the A2A service and chat controller construction unchanged.
6. `src/py/vertebra/agent/mcp/__init__.py`: no change (the constants are imported from `vertebra.agent.mcp.service`).
7. Docs `docs/usage/a2a-and-workflows.md`:
   - In the `## Browser WebMCP ...` section, append to the Browser WebMCP bullet: `` Server manifests (`build_webmcp_manifest`, `/.well-known/webmcp.json`) list only tools decorated with `@mcp_tool(webmcp=True)`; untagged tools are never browser-exposed. ``
   - In `## Agent Runtime, Confirmation Gates & Sessions`, append the paragraph: `` MCP `tools/call` enforces `requires_confirmation`: an unconfirmed call returns JSON-RPC error `-32003` with `data.confirmationToken`. Approve the token (for example through `/agent/confirmations/{token}` when using `AgentPlugin`) and repeat the same call, with the same arguments, setting `_meta["vertebra/confirmationToken"]` to that token. ``
8. Knowledge `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`, in the `McpService` table row, append: `` `tools/call` enforces `Tool.requires_confirmation` through a `ConfirmationGate` (`_meta["vertebra/confirmationToken"]`, error `-32003`); approval binds to the tool, caller, and arguments, and `AgentPlugin` shares the runtime gate. WebMCP manifests list only `@mcp_tool(webmcp=True)` tools; `opt["mcp_tool"]` tools have no WebMCP path. ``
9. Changelog `docs/changelog/index.md`, `## 0.3.0`:
   - `### Changed`: `` - WebMCP exposure is opt-in per tool: `mcp_tool(webmcp=...)` defaults to `False`, `build_webmcp_manifest` lists only tagged tools, and `WebMCPConfig.include_tagged_only` is removed. ``
   - `### Fixed`: `` - `MCPService` enforces `requires_confirmation` on `tools/call` through `ConfirmationGate` (`_meta["vertebra/confirmationToken"]`, JSON-RPC error `-32003`), binding approval to the tool, caller, and arguments; `ConfirmationGate.approve` / `reject` no longer record unknown or other-owner tokens; `AgentPlugin` shares the runtime gate with its MCP service. ``

### 3. Refactor & verify

1. Run the GREEN commands in `## Verification`; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/agent/mcp/webmcp.py src/py/vertebra/agent/mcp/controllers.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/runtime.py src/py/vertebra/agent/plugin.py src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/agent/test_mcp_conformance.py docs/usage/a2a-and-workflows.md .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md docs/changelog/index.md
   git commit -S -m "fix(agent): make WebMCP exposure opt-in and enforce MCP tool confirmation"
   ```

## Verification

- **RED**: the Step 1.3 command exits `1`/`2` with the collection error and assertion failures described.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_runtime_and_workflows.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q
  uv run ruff check src/py/vertebra/agent/mcp/webmcp.py src/py/vertebra/agent/mcp/controllers.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/runtime.py src/py/vertebra/agent/plugin.py src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/agent/test_mcp_conformance.py
  uv run ruff format --check src/py/vertebra/agent/mcp/webmcp.py src/py/vertebra/agent/mcp/controllers.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/runtime.py src/py/vertebra/agent/plugin.py src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/agent/test_mcp_conformance.py
  uv run mypy src/py/vertebra/agent/mcp/webmcp.py src/py/vertebra/agent/mcp/controllers.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/runtime.py src/py/vertebra/agent/plugin.py
  rg -n "include_tagged_only" src docs
  ```

  The first four commands exit `0`. The `rg` command prints nothing and exits `1` (at planning time its only hits are `src/py/vertebra/agent/mcp/webmcp.py::WebMCPConfig` and `::build_webmcp_manifest`, both listed in `files:`).
- **Phase gate**: Phase 1 checkpoint in `spec.md`.

## Acceptance Criteria

- [ ] R4: Only `@mcp_tool(webmcp=True)` tools appear in the manifest; `mcp_tool` defaults `webmcp=False`; `include_tagged_only` is gone from `src` and `docs`; plain `Tool` objects never appear.
- [ ] R5: Confirmation-gated tools raise `-32003` unless the ticket is approved for the same tool, caller, and arguments; the tool body does not run; the matching call runs once; `AgentPlugin` approvals unlock MCP tickets.
- [ ] R5: `ConfirmationGate.approve` / `reject` return `False` and change no state for unknown or other-owner tokens.
- [ ] Docs, knowledge chapter, and changelog updated.
- [ ] Targeted tests, ruff, ruff format, and mypy pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning: `ConfirmationGate.approve` records the token before its ticket and owner checks; the gate is fixed at the root, and the service also checks ticket status, owner, tool, and arguments explicitly.
