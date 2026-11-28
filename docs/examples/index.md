# Examples

Run these examples from the repository root after installing Vertebra. Start
the server in one terminal and send requests from a second terminal.

## Quickstart App

Start the app and request the health endpoint. It returns a JSON response with
the service name and status.

```bash
vtb run docs.examples.quickstart_app:app
```

```bash
curl http://localhost:8000/health
```

```python title="docs/examples/quickstart_app.py"
--8<-- "docs/examples/quickstart_app.py:example"
```

## Controllers, Routers & Guards

Start the inventory API with a credential for the local example. The request
with the bearer token returns item 42. Omitting the header produces HTTP 403.

```bash
VERTEBRA_INVENTORY_TOKEN=local-inventory vtb run docs.examples.routing_and_dtos:app
```

```bash
curl -H 'Authorization: Bearer local-inventory' \
  http://localhost:8000/api/v1/items/42
```

```python title="docs/examples/routing_and_dtos.py"
--8<-- "docs/examples/routing_and_dtos.py:example"
```

## Background Queues

Run a local report worker. The script displays the queued job and the resulting
total. Its in-memory queue belongs to this process.

```bash
python -m docs.examples.background_queues
```

```python title="docs/examples/background_queues.py"
--8<-- "docs/examples/background_queues.py:example"
```

## Live State

Serve a counter and open [localhost:8000/live](http://localhost:8000/live).
The page receives an initial value and three updates through server-sent events.

```bash
vtb run docs.examples.live_state_and_ssr:app
```

```python title="docs/examples/live_state_and_ssr.py"
--8<-- "docs/examples/live_state_and_ssr.py:example"
```

## Object Storage & SQLite

Save an attachment, record its key in a SQLite transaction, and display the
retrieved JSON.

```bash
python -m docs.examples.storage_and_db
```

```python title="docs/examples/storage_and_db.py"
--8<-- "docs/examples/storage_and_db.py:example"
```

## MCP Agent Tools

Expose a stock lookup tool. An MCP client can connect to
`http://localhost:8000/mcp` and discover or call the tool. To inspect tool
discovery from the terminal, use the protocol request below.

```bash
vtb run docs.examples.agent_mcp_tools:app
```

```bash
curl http://localhost:8000/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

```python title="docs/examples/agent_mcp_tools.py"
--8<-- "docs/examples/agent_mcp_tools.py:example"
```

## Vector Embeddings & Search

Start the search API and submit some documents. It ranks the documents using a
deterministic local embedder.

```bash
vtb run docs.examples.embeddings_app:app
```

```bash
curl -X POST http://localhost:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"inventory","documents":["inventory levels","weather report"]}'
```

```python title="docs/examples/embeddings_app.py"
--8<-- "docs/examples/embeddings_app.py:example"
```


## WebAssembly in the Browser

Start the app and open [localhost:8000](http://localhost:8000). Enter two integers
and select **Add**. The browser loads the module and executes its `add` export.
The default inputs produce `19 + 23 = 42`. Addition wraps at 32 bits, as defined
by the module's `i32.add` instruction.

```bash
vtb run docs.examples.wasm_in_browser:app
```

```python title="docs/examples/wasm_in_browser.py"
--8<-- "docs/examples/wasm_in_browser.py:example"
```

The embedded bytes implement this WebAssembly text format (WAT) module.

```wat
(module
  (func (export "add") (param i32 i32) (result i32)
    local.get 0
    local.get 1
    i32.add))
```

See [the TypeScript SDK guide](../usage/typescript-sdk.md#webassembly) for loading
the same module with `WASMClient`.
