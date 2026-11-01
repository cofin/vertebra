# Examples

Every example on this page is embedded directly from a runnable Python file in `docs/examples/` and tested in CI (`pytest src/py/tests/unit/docs/test_examples.py`).

## Quickstart Application

Create an `App` with typed path parameters and `msgspec.Struct` responses:

```python title="docs/examples/quickstart_app.py"
--8<-- "docs/examples/quickstart_app.py:example"
```

## Controllers, Routers & Dependency Injection

Group endpoints on a `Controller` with `Provide` dependencies, mount a `Router` onto an `App`, and validate `msgspec.Struct` request and response models:

```python title="docs/examples/routing_and_dtos.py"
--8<-- "docs/examples/routing_and_dtos.py:example"
```

## Background Job Queues

Enqueue, lease, and acknowledge background jobs with `MemoryQueueBackend`:

```python title="docs/examples/background_queues.py"
--8<-- "docs/examples/background_queues.py:example"
```

## Reactive LiveState & Inertia Responses

Run actions on a `LiveState` component and build an `InertiaResponse` page payload:

```python title="docs/examples/live_state_and_ssr.py"
--8<-- "docs/examples/live_state_and_ssr.py:example"
```

## Transactional Object Storage & SQLite

Save model file attachments and execute SQLite transactions using `ObjectStore` and Advanced Alchemy's `ObjectStoreBackend`:

```python title="docs/examples/storage_and_db.py"
--8<-- "docs/examples/storage_and_db.py:example"
```

## Agent MCP Tools

Register typed Python tools with `@tool` and call them over JSON-RPC 2.0 `tools/list` and `tools/call`:

```python title="docs/examples/agent_mcp_tools.py"
--8<-- "docs/examples/agent_mcp_tools.py:example"
```

## Shared-Memory Vector Embeddings & Top-k Search

Embed queries and documents into a shared-memory `VectorBatchView`, apply Matryoshka `.truncate()`, encode binary database wire payloads, and run off-GIL top-$k$ cosine search:

```python title="docs/examples/embeddings_app.py"
--8<-- "docs/examples/embeddings_app.py:example"
```
