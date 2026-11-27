# Installation

Vertebra supports Python 3.12 through 3.15, including free-threaded builds, on Linux, macOS, and Windows.

## Install the Python Package

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

Verify the `vtb` CLI is available:

```bash
vtb --help
```

Prebuilt wheels include the `vertebra-server` binary. On Linux and macOS, the packaged server requires a shared `libpython` build (provided by `uv python install` and most system Python builds) unless you point `VERTEBRA_SERVER_BINARY` to a custom binary.

## Optional Extras

The base package keeps dependencies small (`anyio`, `msgspec`, `rich`, `rich-click`). Add extras for the drivers, frameworks, or tools you use.

### Databases & Caches

| Extra | What it adds |
| --- | --- |
| `postgres` | PostgreSQL driver |
| `mysql` | MySQL and MariaDB driver |
| `sqlite` | SQLite driver (with bundled SQLite) |
| `oracle` | Oracle driver (no Instant Client required) |
| `mssql` | SQL Server driver |
| `duckdb` | DuckDB driver |
| `bigquery` | Google BigQuery driver |
| `spanner` | Google Cloud Spanner driver |
| `redis` | Redis RESP3 driver |
| `valkey` | Valkey RESP3 driver |

### Storage & Frontend

| Extra | What it adds |
| --- | --- |
| `storage` | Cloud and local object storage (S3, GCS, Azure, R2, OCI, HTTP, local, memory) |
| `bundler` | Embedded JavaScript/TypeScript bundler and QuickJS SSR |

### Web Frameworks & Data Tools

| Extra | What it adds |
| --- | --- |
| `litestar` | Litestar plugin, route compiler, and middleware offload |
| `starlette` | Starlette route compiler and middleware offload |
| `fastapi` | FastAPI dependency plan compiler and fast validation path |
| `flask` | Flask route compiler and WSGI bridge |
| `django` | Django route compiler, database backends, cache, storage, email, and channels |
| `sqlspec` | SQLSpec adapters, stores, and Arrow integration |
| `alchemy` | SQLAlchemy, Advanced Alchemy, and Alembic integration |
| `arrow` | PyArrow table materialization and Parquet helpers |
| `polars` | Polars DataFrame and LazyFrame integration |
| `pydantic` | Pydantic schema integration |
| `attrs` | `attrs` schema integration |

### Litestar Ecosystem

| Extra | What it adds |
| --- | --- |
| `queues` | `litestar-queues` integration |
| `security` | `litestar-security` integration with JWT and mTLS helpers |
| `autowire` | `litestar-autowire` controller discovery |
| `email` | `litestar-email` integration with AWS SES signing |
| `mcp` | `litestar-mcp` tools, resources, and agent cards |
| `vite` | `litestar-vite` bundling and client integration |
| `asyncapi` | `litestar-asyncapi` specification and channel support |
| `htmx` | `litestar-htmx` fragment and header helpers |

### AI, Auth, Telemetry & Event Loops

| Extra | What it adds |
| --- | --- |
| `embeddings` | Local ONNX and EmbeddingGemma models |
| `iap` | Google Cloud IAP and OIDC JWT verification |
| `opentelemetry` | OpenTelemetry event-loop and task metrics |
| `uvloop` | `uvloop` event loop on Linux and macOS |
| `winloop` | `winloop` event loop on Windows |
| `rloop` | `rloop` event loop on Linux and macOS |
| `zuvloop` | `zuvloop` event loop on Python 3.14+ |

Database drivers export Arrow C Stream PyCapsules without requiring `pyarrow`. Install `[arrow]` only when you need `pyarrow.Table` objects or Parquet helpers, or `[polars]` on its own.

Install any extra with your package manager (shown with `postgres`):

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

Development, lint, test, and benchmark dependencies use PEP 735 dependency groups (`uv sync --group dev`).

## Free-Threaded Python

All native extensions release the GIL and support free-threaded Python. Under `--topology embedded`, Vertebra runs Python worker threads in parallel inside one process:

```bash
uv python install 3.14t
uv venv --python 3.14t
uv sync
```

## TypeScript Client SDK

Vertebra bundles `@vertebra/client` inside the Python wheel, so you do not need npm to get started:

- **Direct browser imports**: Load modules straight from a running Vertebra server:
  ```html
  <script type="module" src="/__vertebra/client/index.js"></script>
  ```
- **Type checking & local bundling**: Manage JS packages in `[tool.vtb.js]` inside `pyproject.toml` and link client types into your workspace:
  ```bash
  vtb js install
  ```
- **Existing Node or Bun setups**: You can also link the bundled client into an existing Vite or npm workspace.
