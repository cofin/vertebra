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

Prebuilt `vertebra-core` wheels ship a relocatable `vertebra/bin/vertebra-server` binary across Linux (`linux-x86_64`, `linux-aarch64`) and macOS (`arm64`, `x86_64`), and Windows wheels ship `vertebra/bin/vertebra-server.exe` (`win_amd64`). On POSIX hosts, the packaged binary requires a shared `libpython` runtime (`ENABLE_SHARED=1`, provided by `uv python install` and standard shared CPython builds) or an explicit `VERTEBRA_SERVER_BINARY` override.

## Optional Extras

Vertebra keeps its base dependency footprint minimal (`anyio`, `msgspec`, `rich`, and `rich-click`) and exposes optional extras in `pyproject.toml`:

| Extra | Package Specifier | Included Capabilities |
| --- | --- | --- |
| `sqlspec` | `"vertebra-core[sqlspec]"` | First-class `sqlspec` (`>=0.74.1`) driver adapters, configs, and PyArrow integration |
| `alchemy` | `"vertebra-core[alchemy]"` | `advanced-alchemy` (`>=1.13.0b3`), `sqlalchemy` (`>=2.1.0`), and `alembic` (`>=1.18.0`) |
| `arrow` | `"vertebra-core[arrow]"` | Optional `pyarrow` (`>=18.0.0`) materialization (`to_arrow_table()`) and Parquet dataset staging |
| `pydantic` | `"vertebra-core[pydantic]"` | Zero-copy `pydantic-core` (`validate_json` / `to_json`) schema integration |
| `attrs` | `"vertebra-core[attrs]"` | Zero-copy `attrs` (`@attrs.define`) schema integration |
| `polars` | `"vertebra-core[polars]"` | Zero-copy Polars `DataFrame` / `LazyFrame` Arrow integration |
| `litestar` | `"vertebra-core[litestar]"` | Core Litestar route compiler, fast-path bypass, channels, and response cache store |
| `starlette` | `"vertebra-core[starlette]"` | First-class Starlette route compiler, 24-slot `AsgiScope` fastpath, and middleware offload |
| `fastapi` | `"vertebra-core[fastapi]"` | First-class FastAPI `CompiledDependantPlan` DI solver and zero-copy `pydantic-core` fastpath |
| `flask` | `"vertebra-core[flask]"` | First-class Flask route compiler, PEP 3118 `WsgiInput` fastpath, and ecosystem bridges |
| `django` | `"vertebra-core[django]"` | First-class Django route compiler, `vertebra.extensions.django.db.*` database backends, `ArrowQuerySet`, `CacheBackend`, `DjangoStorage`, `EmailBackend`, `ChannelLayer`, and `ORMBridge` |
| `queues` | `"vertebra-core[queues]"` | `litestar-queues` integration (`vertebra.extensions.litestar.queues`) with SHM backends and SQLSpec queues |
| `security` | `"vertebra-core[security]"` | `litestar-security` integration with native JWT and mTLS identity mechanisms |
| `autowire` | `"vertebra-core[autowire]"` | `litestar-autowire` automatic controller discovery and bridge |
| `email` | `"vertebra-core[email]"` | `litestar-email` transactional email with native AWS SigV4 SES backend |
| `mcp` | `"vertebra-core[mcp]"` | `litestar-mcp` tools, resources, and agent cards |
| `vite` | `"vertebra-core[vite]"` | `litestar-vite` bundling and client integration |
| `asyncapi` | `"vertebra-core[asyncapi]"` | `litestar-asyncapi` AsyncAPI 3.0 specification and channel integration |
| `htmx` | `"vertebra-core[htmx]"` | `litestar-htmx` server-driven fragment and response helpers |
| `embeddings` | `"vertebra-core[embeddings]"` | Local ONNX / `fastembed` embedding models via `1x` RAM `EmbeddingBroker` |
| `iap` | `"vertebra-core[iap]"` | Google Cloud Identity-Aware Proxy / OIDC JWT signature verification (`pyjwt[crypto]`) |
| `opentelemetry` | `"vertebra-core[opentelemetry]"` | Optional `opentelemetry-api` (`>=1.28.0`) event-loop lag and active-task metric instruments |
| `bigquery` | `"vertebra-core[bigquery]"` | Standalone native Rust `vertebra-bigquery` (`vertebra._bigquery`) driver wheel |
| `bundler` | `"vertebra-core[bundler]"` | Standalone native Rust `vertebra-bundler` (`vertebra._bundler`) Rolldown/Oxc & QuickJS SSR wheel |
| `duckdb` | `"vertebra-core[duckdb]"` | Standalone native Rust `vertebra-duckdb` (`vertebra._duckdb`) DuckDB 2.0 & Arrow wheel |
| `mssql` | `"vertebra-core[mssql]"` | Standalone native Rust `vertebra-mssql` (`vertebra._mssql`) SQL Server TDS 7.4 wheel |
| `mysql` | `"vertebra-core[mysql]"` | Standalone native Rust `vertebra-mysql` (`vertebra._mysql`) MySQL/MariaDB binary protocol wheel |
| `oracle` | `"vertebra-core[oracle]"` | Standalone native Rust `vertebra-oracle` (`vertebra._oracle`) Oracle 26ai Thin TNS/TTC wheel |
| `postgres` | `"vertebra-core[postgres]"` | Standalone native Rust `vertebra-postgres` (`vertebra._postgres`) pure-Rust PostgreSQL wheel |
| `redis` | `"vertebra-core[redis]"` | Standalone native Rust `vertebra-redis` (`vertebra._redis`) Redis 7.2+/8.0 RESP3 client wheel |
| `spanner` | `"vertebra-core[spanner]"` | Standalone native Rust `vertebra-spanner` (`vertebra._spanner`) driver wheel |
| `sqlite` | `"vertebra-core[sqlite]"` | Standalone native Rust `vertebra-sqlite` (`vertebra._sqlite`) wheel with bundled SQLite 3.45+ |
| `storage` | `"vertebra-core[storage]"` | Standalone native Rust `vertebra-storage` (`vertebra._storage`) multicloud object storage wheel |
| `valkey` | `"vertebra-core[valkey]"` | Standalone native Rust `vertebra-valkey` (`vertebra._valkey`) Valkey 8.0+/9.0 RESP3 client wheel |
| `uvloop` | `"vertebra-core[uvloop]"` | Optional `uvloop` asyncio event loop integration on POSIX hosts |
| `winloop` | `"vertebra-core[winloop]"` | Optional `winloop` asyncio event loop integration on Windows |
| `rloop` | `"vertebra-core[rloop]"` | Optional `rloop` asyncio event loop integration on POSIX hosts |
| `zuvloop` | `"vertebra-core[zuvloop]"` | Optional Zig-backed `zuvloop` asyncio event loop integration on Python 3.14+ |

Native database drivers produce `VTBARROW` Apache Arrow IPC buffers directly in shared memory (`ArrowCursor.buffer_view()`) and export Arrow C Schema and C Stream PyCapsules without requiring any Python Arrow dependency. Install `[arrow]` for `pyarrow.Table` materialization and Parquet staging, or `[polars]` independently.

Install any extra with your preferred package manager (shown with `[postgres]`):

=== "uv"

    ```bash
    uv add "vertebra-core[postgres]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[postgres]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[postgres]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[postgres]"
    ```

Development, lint, type-checking, test, and benchmark dependencies are managed via PEP 735 `[dependency-groups]` (`uv sync --group dev`).

## Free-Threaded Python (`3.14t` / `3.15t`)

All 13 native Rust extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`, `vertebra._bigquery`, `vertebra._duckdb`, `vertebra._mssql`, `vertebra._mysql`, `vertebra._oracle`, `vertebra._postgres`, `vertebra._redis`, `vertebra._spanner`, `vertebra._sqlite`, `vertebra._valkey`) run without the GIL and support multi-core execution inside a single OS process under `--topology embedded`:

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
