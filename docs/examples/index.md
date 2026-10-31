# Examples

Every code listing on this page is embedded directly from a runnable Python module under `docs/examples/` and executed continuously in the test suite (`pytest src/py/tests/unit/docs/test_examples.py`).

## Quickstart Application

Create an `App` with typed path parameters and `msgspec.Struct` responses, verified with `extract_path_params` and `handler_id`:

```python title="docs/examples/quickstart_app.py"
--8<-- "docs/examples/quickstart_app.py:example"
```

## Modular Routing & DTO Validation

Mount a modular `Router` with prefix and tags onto an `App` and validate request/response DTOs:

```python title="docs/examples/routing_and_dtos.py"
--8<-- "docs/examples/routing_and_dtos.py:example"
```

## Background Job Queues

Enqueue, lease, and acknowledge background jobs with `MemoryQueueBackend`:

```python title="docs/examples/background_queues.py"
--8<-- "docs/examples/background_queues.py:example"
```

## Reactive LiveState & Inertia Responses

Dispatch actions on a server-owned `LiveState` component and construct an `InertiaResponse` page payload:

```python title="docs/examples/live_state_and_ssr.py"
--8<-- "docs/examples/live_state_and_ssr.py:example"
```

## Transactional Object Storage & SQLite

Coordinate staged `ObjectStore` uploads with database transactions and automatic rollback cleanup via `UploadTracker`:

```python title="docs/examples/storage_and_db.py"
--8<-- "docs/examples/storage_and_db.py:example"
```

## Agent MCP Tools

Register typed Python tools with `@tool` and dispatch JSON-RPC 2.0 `tools/list` and `tools/call` requests through `JsonRpcRouter` and `execute_tool_calls`:

```python title="docs/examples/agent_mcp_tools.py"
--8<-- "docs/examples/agent_mcp_tools.py:example"
```
