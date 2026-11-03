---
type: Task
id: critical-correctness-fixes:t03
title: Mcp-Name binding and nested-params rejection
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/core/jsonrpc.py
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_core_and_observability.py
plan_revision: 2
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
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:00:00Z
commit: null
tags: [bugfix, security, mcp, jsonrpc]
---

# t03 — `Mcp-Name` binding and nested-params rejection

## Objective

An `Mcp-Name` header always binds to the body field it names, `Mcp-Method`/`Mcp-Name` headers are rejected on JSON-RPC batch bodies, and the JSON-RPC router never unpacks a nested `params` object into a handler's `params` argument.

## Context

- **Strategy**: `regression_tdd`. The bypasses reproduce at `142cc49`.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/agent/mcp/routes.py::MCPHttpHandler.handle_request` compares the header only when `target_val is not None`. A body without `params.name` skips the check.
  - The same method takes `frame_method` from `classify_jsonrpc_frame(body)`, which returns `(2, None, None)` for a JSON array. With `frame_method is None`, both header checks are skipped, and the batch runs every call in it.
  - `src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter._invoke_handler` first calls `handler(**params, **context_kwargs)`. For a body `{"params": {"params": {"name": "calc_add", ...}}}`, that call becomes `handler(params={"name": "calc_add", ...})`; `MCPService._handle_tools_call` is `async def`, so no `TypeError` is raised and `calc_add` runs. For a sync handler, a `TypeError` raised inside the handler body triggers the `except TypeError` retry chain, which calls the handler a second time with `params=<outer dict>`.
  - Combined: header `Mcp-Name: other` plus a nested body or a batch body runs `calc_add` with no header mismatch.
- **Handler inventory** (grep of `JSONRPCRouter()` / `.router.register(` in `src/py/vertebra`): the only routers are `agent/mcp/service.py::MCPService._register_handlers` and `agent/a2a/service.py::A2AService.__init__`; every registered handler declares a `params` parameter, so none relies on the `TypeError` fallback. Test-only handlers in `test_core_and_observability.py` (`lambda: "pong"`, `lambda message: message`, `failing_handler()`) take keyword arguments or none. The new dict rule keeps both shapes working: handlers with a `params` parameter get `params=<dict>`, others get the dict unpacked as keyword arguments.
- **Out of scope**: HTTP status codes for header errors stay 200 with a JSON-RPC error, and JSON-RPC batches without MCP headers are still accepted (`mcp-webmcp-core` changes status codes to 400 and removes batch support).
- **Canonical test modules**: `src/py/tests/unit/agent/test_mcp_conformance.py` (`MCPHttpHandler` tests), `src/py/tests/unit/agent/test_core_and_observability.py` (`JSONRPCRouter` tests).
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.

## Steps

### 1. Write failing tests (RED)

1. `src/py/tests/unit/agent/test_mcp_conformance.py` (imports `HEADER_MISMATCH`, `MCPHttpHandler`, `MCPService`, `tool`, `decode_json` already exist). Append:

   ```python
   _CALC_ADD_CALL = {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "calc_add", "arguments": {"a": 1, "b": 2}}}


   @pytest.mark.anyio
   @pytest.mark.parametrize(
       "payload",
       [
           {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"params": {"name": "calc_add", "arguments": {"a": 1, "b": 2}}}},
           {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"arguments": {"a": 1, "b": 2}}},
           [_CALC_ADD_CALL],
       ],
       ids=["nested-params", "missing-name", "batch-body"],
   )
   async def test_mcp_http_handler_rejects_mcp_name_without_body_name(payload: object) -> None:
       """An Mcp-Name header with no matching body name, or on a batch body, is rejected before the tool runs."""
       service = MCPService()
       calls: list[tuple[int, int]] = []

       @tool(name="calc_add", description="Add two integers")
       def add(a: int, b: int) -> int:
           calls.append((a, b))
           return a + b

       service.registry.register_tool(add)
       handler = MCPHttpHandler(service)
       status, _, raw = await handler.handle_request(
           "POST",
           "/mcp",
           {"Mcp-Method": "tools/call", "Mcp-Name": "other"},
           encode_json_bytes(payload),
       )
       assert status == 200
       decoded = decode_json(raw)
       assert decoded["error"]["code"] == HEADER_MISMATCH
       assert calls == []
   ```

   Add `encode_json_bytes` to the existing `from vertebra.utils.serializers import decode_json` line.

2. `src/py/tests/unit/agent/test_core_and_observability.py` (imports `JSONRPCRouter` and `JSONRPCRequest` already). Append:

   ```python
   @pytest.mark.anyio
   async def test_jsonrpc_router_passes_dict_params_without_unwrapping() -> None:
       """Handlers declaring ``params`` receive the request params dict as-is, nested keys included."""
       router = JSONRPCRouter()
       received: list[object] = []

       async def capture(params: object = None, context: object = None) -> str:
           received.append(params)
           return "ok"

       router.register("capture", capture)
       nested = {"params": {"name": "calc_add"}}
       response = await router.dispatch(JSONRPCRequest(method="capture", id=1, params=nested))
       assert response == {"jsonrpc": "2.0", "id": 1, "result": "ok"}
       assert received == [nested]


   @pytest.mark.anyio
   async def test_jsonrpc_router_does_not_retry_handler_type_errors() -> None:
       """A TypeError raised inside a sync params handler is reported once, with the params unchanged."""
       router = JSONRPCRouter()
       attempts: list[object] = []

       def broken(params: object = None, context: object = None) -> str:
           attempts.append(params)
           raise TypeError("handler bug")

       router.register("broken", broken)
       response = await router.dispatch(JSONRPCRequest(method="broken", id=2, params={"params": {"x": 1}}))
       assert response is not None
       assert response["error"]["code"] == INTERNAL_ERROR
       assert attempts == [{"params": {"x": 1}}]
   ```

   `INTERNAL_ERROR`, `JSONRPCRouter`, and `JSONRPCRequest` are already imported from `vertebra.agent.core.jsonrpc` in this module. `broken` stays a plain `def` so its `TypeError` is raised inside the router's call, which is what triggers the retry chain at HEAD.

3. Run RED:

   ```bash
   uv run pytest src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_http_handler_rejects_mcp_name_without_body_name src/py/tests/unit/agent/test_core_and_observability.py::test_jsonrpc_router_passes_dict_params_without_unwrapping src/py/tests/unit/agent/test_core_and_observability.py::test_jsonrpc_router_does_not_retry_handler_type_errors -q
   ```

   Expected at HEAD:
   - `nested-params`: the nested dict is unwrapped and `calc_add` runs, so the test fails with `KeyError: 'error'` (the response holds a `result`).
   - `missing-name`: `handler(arguments=...)` raises `TypeError`, the fallback calls `_handle_tools_call(params=...)`, which returns `INVALID_PARAMS` (`-32602`, "Missing required 'name'"), so the test fails on `assert decoded["error"]["code"] == HEADER_MISMATCH`.
   - `batch-body`: the header checks are skipped and the batch runs `calc_add`; the decoded response is a list, so the test fails with `TypeError: list indices must be integers or slices, not str`.
   - The unwrapping test fails because `received == [{"name": "calc_add"}]`.
   - The retry test fails because `attempts == [{"x": 1}, {"params": {"x": 1}}]`.

### 2. Implement (GREEN)

1. `src/py/vertebra/agent/mcp/routes.py::MCPHttpHandler.handle_request`:
   - Change `_, frame_method, frame_id = classify_jsonrpc_frame(body)` to `frame_kind, frame_method, frame_id = classify_jsonrpc_frame(body)`.
   - Read both headers right after classification (`mcp_method_header = lower_headers.get("mcp-method")`, `mcp_name_header = lower_headers.get("mcp-name")`; remove the later duplicate lookups) and insert before the existing `Mcp-Method` comparison:

   ```python
   if frame_kind == 2 and (mcp_method_header is not None or mcp_name_header is not None):
       err = JSONRPCError(
           code=HEADER_MISMATCH,
           message="Mcp-Method and Mcp-Name headers cannot be used with JSON-RPC batch requests",
       )
       err_body = encode_json_bytes({"jsonrpc": "2.0", "id": None, "error": {"code": err.code, "message": err.message}})
       return 200, {"Content-Type": "application/json"}, err_body
   ```

   (`2` is the batch frame kind returned by both the native `classify_jsonrpc_frame` and the module's Python fallback.)
   - In the `mcp_name_header` block, replace `params = body_dict.get("params") or {}` with:

   ```python
   raw_params = body_dict.get("params") if isinstance(body_dict, dict) else None
   params = raw_params if isinstance(raw_params, dict) else {}
   ```

   - Replace `if target_val is not None and str(target_val) != decoded_name:` with `if target_val is None or str(target_val) != decoded_name:`.
   - Change the error message to `f"Header Mcp-Name '{decoded_name}' does not match body parameter {field_name!r}"` so a missing field reports the field name.
2. `src/py/vertebra/agent/core/jsonrpc.py::JSONRPCRouter._invoke_handler`: replace the whole `elif isinstance(params, dict):` branch (including the `try/except TypeError` fallback chain) with:

   ```python
   elif isinstance(params, dict):
       if "params" in sig.parameters:
           result = handler(params=params, **kwargs)
       else:
           call_kwargs = dict(params)
           call_kwargs.update(kwargs)
           result = handler(**call_kwargs)
   ```

   Update the method docstring to: `"""Invoke a handler with params and optional context injection.\n\n        A dict ``params`` is passed as ``params=<dict>`` to handlers that declare a ``params``\n        parameter and unpacked as keyword arguments otherwise. Nested ``params`` keys are never\n        unwrapped.\n        """`
3. Knowledge `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`, in the `McpService` table row, append: `` `MCPHttpHandler` rejects an `Mcp-Name` header when the body field it names is missing or different, rejects `Mcp-Method`/`Mcp-Name` headers on JSON-RPC batch bodies, and `JSONRPCRouter` passes dict params to `params` handlers without unwrapping nested keys. ``
4. Changelog `docs/changelog/index.md`, `## 0.3.0` → `### Fixed`, append: `` - `MCPHttpHandler.handle_request` rejects an `Mcp-Name` header when the body name is missing and rejects MCP routing headers on JSON-RPC batch bodies; `JSONRPCRouter` no longer unwraps nested `params` objects or retries handlers that raise `TypeError`. ``

### 3. Refactor & verify

1. Run the GREEN commands; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/core/jsonrpc.py src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_core_and_observability.py .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md docs/changelog/index.md
   git commit -S -m "fix(agent): bind Mcp-Name to the body name and stop unwrapping nested params"
   ```

## Verification

- **RED**: the Step 1.3 command exits `1` with the failures described.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/agent -q
  uv run ruff check src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/core/jsonrpc.py src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_core_and_observability.py
  uv run ruff format --check src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/core/jsonrpc.py src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_core_and_observability.py
  uv run mypy src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/core/jsonrpc.py
  ```

  All exit `0`. `src/py/tests/unit/agent` includes the A2A tests, which exercise `A2AService` handlers through the changed router branch.
- **Phase gate**: Phase 1 checkpoint in `spec.md`.

## Acceptance Criteria

- [ ] R6: A present `Mcp-Name` header with a missing or different body field returns `HEADER_MISMATCH` and the tool does not run.
- [ ] R6: `Mcp-Method` or `Mcp-Name` headers on a JSON array body return `HEADER_MISMATCH` and no call in the batch runs.
- [ ] R6: Handlers with a `params` parameter receive the dict unchanged; nested `params` keys are never unwrapped; handler `TypeError`s are not retried.
- [ ] A2A and MCP test suites stay green.
- [ ] Knowledge chapter and changelog updated; targeted lint/type checks pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning: all production `JSONRPCRouter` handlers declare `params`, so removing the `TypeError` fallback chain changes no production call shape.
