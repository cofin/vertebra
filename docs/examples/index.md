# Examples

Every snippet below comes from a runnable Python file in `docs/examples/` and is tested in CI.

## Quickstart App

Define an app with path parameters and `msgspec.Struct` responses:

```python title="docs/examples/quickstart_app.py"
--8<-- "docs/examples/quickstart_app.py:example"
```

## Controllers, Routers & Dependency Injection

Group routes on a controller with injected dependencies and typed request models:

```python title="docs/examples/routing_and_dtos.py"
--8<-- "docs/examples/routing_and_dtos.py:example"
```

## Background Queues

Enqueue, lease, and acknowledge jobs with an in-memory queue backend:

```python title="docs/examples/background_queues.py"
--8<-- "docs/examples/background_queues.py:example"
```

## Live State & Inertia

Run actions on a live state component and build an Inertia response:

```python title="docs/examples/live_state_and_ssr.py"
--8<-- "docs/examples/live_state_and_ssr.py:example"
```

## Object Storage & SQLite

Save model file attachments and run SQLite transactions with object storage:

```python title="docs/examples/storage_and_db.py"
--8<-- "docs/examples/storage_and_db.py:example"
```

## MCP Agent Tools

Register Python tools and call them over MCP:

```python title="docs/examples/agent_mcp_tools.py"
--8<-- "docs/examples/agent_mcp_tools.py:example"
```

## Vector Embeddings & Search

Embed text into shared memory, truncate vectors, and run cosine similarity search:

```python title="docs/examples/embeddings_app.py"
--8<-- "docs/examples/embeddings_app.py:example"
```
