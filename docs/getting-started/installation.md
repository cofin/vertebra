# Installation

Vertebra supports **Python 3.12 through 3.15**, including free-threaded (**GIL-disabled**) `3.14t` and `3.15t` builds across Linux and macOS.

## Install the Python Package

Install `vertebra-core` into your project environment:

=== "uv"
    ```bash
    uv add vertebra-core
    ```
=== "pip"
    ```bash
    pip install vertebra-core
    ```
=== "pdm"
    ```bash
    pdm add vertebra-core
    ```
=== "poetry"
    ```bash
    poetry add vertebra-core
    ```

Verify the installation and `vtb` CLI entrypoint:

```bash
vtb --help
```

## Optional Extras

Vertebra keeps its base dependency footprint minimal (`msgspec` and `rich-click`) and exposes optional extras in `pyproject.toml`:

| Extra | Install Command | Included Capabilities |
| --- | --- | --- |
| `sqlspec` | `uv add "vertebra-core[sqlspec]"` | First-class `sqlspec` (`>=0.69.0`) driver adapters, configs, and PyArrow integration |
| `alchemy` | `uv add "vertebra-core[alchemy]"` | `advanced-alchemy` (1.12+), `sqlalchemy` (2.0.36+), and `alembic` (1.14+) |
| `arrow` | `uv add "vertebra-core[arrow]"` | Native PyArrow IPC streaming and Parquet datasets |
| `pydantic` | `uv add "vertebra-core[pydantic]"` | Zero-copy `pydantic-core` (`validate_json` / `to_json`) schema integration |
| `attrs` | `uv add "vertebra-core[attrs]"` | Zero-copy `attrs` (`@attrs.define`) schema integration |
| `polars` | `uv add "vertebra-core[polars]"` | Zero-copy Polars `DataFrame` / `LazyFrame` Arrow PyCapsule integration |
| `litestar` | `uv add "vertebra-core[litestar]"` | Core Litestar route compiler, fast-path bypass, channels, and response cache store |
| `starlette` | `uv add "vertebra-core[starlette]"` | First-class Starlette route compiler, 24-slot `AsgiScope` fastpath, and middleware offload |
| `fastapi` | `uv add "vertebra-core[fastapi]"` | First-class FastAPI `CompiledDependantPlan` DI solver and zero-copy `pydantic-core` fastpath |
| `flask` | `uv add "vertebra-core[flask]"` | First-class Flask route compiler, PEP 3118 `WsgiInput` fastpath, and ecosystem bridges |
| `django` | `uv add "vertebra-core[django]"` | First-class Django route compiler, `vertebra.extensions.django.db.*` backends, and ecosystem bridges |
| `queues` | `uv add "vertebra-core[queues]"` | `litestar-queues` integration (`vertebra.extensions.litestar.queues`) with SHM backends and SQLSpec queues |
| `security` | `uv add "vertebra-core[security]"` | `litestar-security` integration with native JWT and mTLS identity mechanisms |
| `autowire` | `uv add "vertebra-core[autowire]"` | `litestar-autowire` automatic controller discovery and bridge |
| `email` | `uv add "vertebra-core[email]"` | `litestar-email` transactional email with native AWS SigV4 SES backend |
| `mcp` | `uv add "vertebra-core[mcp]"` | `litestar-mcp` tools, resources, and agent cards |
| `vite` | `uv add "vertebra-core[vite]"` | `litestar-vite` bundling and client integration |
| `asyncapi` | `uv add "vertebra-core[asyncapi]"` | `litestar-asyncapi` AsyncAPI 3.0 specification and channel integration |
| `htmx` | `uv add "vertebra-core[htmx]"` | `litestar-htmx` server-driven fragment and response helpers |
| `saq` | `uv add "vertebra-core[saq]"` | `litestar-saq` background worker and queue integration |
| `embeddings` | `uv add "vertebra-core[embeddings]"` | Local ONNX / `fastembed` embedding models via `1x` RAM `EmbeddingBroker` |
| `bigquery` | `uv add "vertebra-core[bigquery]"` | Standalone native Rust `vertebra-bigquery` (`vertebra._bigquery`) driver wheel |
| `bundler` | `uv add "vertebra-core[bundler]"` | Standalone native Rust `vertebra-bundler` (`vertebra._bundler`) Rolldown/Oxc & QuickJS SSR wheel |
| `spanner` | `uv add "vertebra-core[spanner]"` | Standalone native Rust `vertebra-spanner` (`vertebra._spanner`) driver wheel |
| `storage` | `uv add "vertebra-core[storage]"` | Standalone native Rust `vertebra-storage` (`vertebra._storage`) multicloud object storage wheel |
| `rloop` | `uv add "vertebra-core[rloop]"` | Optional `rloop` asyncio event loop integration on POSIX hosts |
| `all` | `uv add "vertebra-core[all]"` | Full ecosystem extras suite |

Development, lint, type-checking, test, and benchmark dependencies are managed via PEP 735 `[dependency-groups]` (`uv sync --group dev`).

## Free-Threaded Python (`3.14t` / `3.15t`)

All native Rust extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`, `vertebra._bigquery`, `vertebra._spanner`) run without the GIL and support multi-core execution inside a single OS process under `--topology embedded`:

```bash
uv python install 3.14t
uv venv --python 3.14t
uv sync
```

## TypeScript Client SDK (Native Wheel Distribution)

Vertebra ships the complete `@vertebra/client` TypeScript SDK directly inside the Python wheel (`vertebra-core`), completely eliminating external NPM registry dependencies:

- **Browser Applications**: Import directly from the running Vertebra server via native browser ESM without Node.js or `node_modules`:
  ```html
  <script type="module" src="/__vertebra/client/index.js"></script>
  ```
- **Vite & Full-Stack Projects**: Manage ESM dependencies using `[tool.vtb.js]` in `pyproject.toml` and vendor or link the client bundle into your workspace:
  ```bash
  vtb js install
  ```
- **Existing NPM/Vite Workspaces (Optional)**: If integrating with an existing external Node.js or Bun build pipeline, the compiled client distribution can also be linked into your project.
