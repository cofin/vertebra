# CLI Reference

```python
from vertebra.cli import cli
```

Command-line interface (`vtb`) for running Vertebra applications, hot-reload development, frontend asset and TypeScript client generation, standalone PyApp binary builds, MCP stdio-to-HTTP bridges, and background task queue workers. See the [CLI Guide](../getting-started/cli.md) for interactive usage examples.

## Global Usage

```bash
vtb [OPTIONS] COMMAND [ARGS]...
```

Vertebra: High-performance polyglot application backbone.

Python-first architecture with Rust IPC primitives.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--app` | `text` | - | Application import path in MODULE:ATTR format |
| `--app-dir` | `directory` | - | Directory added to sys.path for application discovery |
| `--version` | `flag` | `False` | Show the version and exit. |

## Application Lifecycle & Packaging

#### `vtb run`

Run the Vertebra server.

```bash
vtb run [OPTIONS] [APP]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `app` | `text` | No | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--host` | `text` | `"127.0.0.1"` | Host to bind to |
| `--port` | `integer` | `8000` | Port to bind to |
| `--workers` | `integer` | - | Number of worker processes |
| `--threads` | `integer` | - | Number of threads per process |
| `--topology` | `choice(auto | embedded | shm)` | - | Runtime topology (auto, embedded, or shm) |
| `--jit`, `--no-jit` | `flag` | `False` | Enable or disable CPython experimental JIT (PYTHON_JIT) |
| `--config` | `path` | - | Path to application config |
| `--log-level` | `text` | `"info"` | Log level |
| `--drain-timeout` | `integer` | `5000` | Drain timeout in milliseconds |
| `--shutdown-timeout` | `integer` | `30000` | Shutdown timeout in milliseconds |
| `--asgi` | `flag` | `False` | Run in ASGI mode (disable native pipeline) |
| `--interface` | `choice(auto | asgi | rsgi | wsgi)` | `"auto"` | Gateway protocol interface (auto, asgi, rsgi, or wsgi) |
| `--loop` | `choice(auto | uvloop | winloop | rloop | asyncio)` | `"auto"` | Event loop backend (auto, uvloop, winloop, rloop, or asyncio) |
| `--tls-cert` | `path` | - | Path to PEM certificate for HTTPS |
| `--tls-key` | `path` | - | Path to PEM private key for HTTPS |
| `--worker-only` | `flag` | `False` | Run background worker lanes with health probes only |
| `--oneshot-job` | `text` | - | Execute a single registered job inside the app lifespan and exit |
| `--with-workers`, `--no-workers` | `flag` | `False` | Co-locate background WorkerRunner lanes inside the server process |
| `--queues` | `text` | - | Comma-separated queue lane names for co-located workers (e.g. default,embeddings) |
| `--lane-concurrency` | `text` | - | Comma-separated lane=limit assignments (e.g. default=4,embeddings=1) |
| `--no-banner` | `flag` | `False` | Disable startup banner |

#### `vtb dev`

Run Vertebra in development mode.

Single worker, auto-reload enabled, frontend HMR active.

APP is the application import string (e.g., myapp:app).

```bash
vtb dev [OPTIONS] [APP]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `app` | `text` | No | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--host` | `text` | `"127.0.0.1"` | Bind host |
| `--port` | `integer` | `8000` | Bind port |
| `--log-level` | `text` | `"info"` | Log level |
| `--no-reload` | `flag` | `False` | Disable file watching and auto-reload |
| `--asgi` | `flag` | `False` | Use standard ASGI instead of native C ABI offload |
| `--interface` | `choice(auto | asgi | rsgi | wsgi)` | `"auto"` | Gateway protocol interface (auto, asgi, rsgi, or wsgi) |
| `--loop` | `choice(auto | uvloop | winloop | rloop | asyncio)` | `"auto"` | Event loop backend (auto, uvloop, winloop, rloop, or asyncio) |
| `--no-banner` | `flag` | `False` | Suppress startup banner |
| `--no-tui` | `flag` | `False` | Disable interactive TUI dashboard |
| `--verbose` | `flag` | `False` | Enable verbose tracebacks with local variables |
| `--with-workers`, `--no-workers` | `flag` | `False` | Co-locate background WorkerRunner lanes inside the dev server process |
| `--queues` | `text` | - | Comma-separated queue lane names for co-located workers (e.g. default,embeddings) |
| `--lane-concurrency` | `text` | - | Comma-separated lane=limit assignments (e.g. default=4,embeddings=1) |

#### `vtb init`

Initialize a new Vertebra project.

```bash
vtb init [OPTIONS] <NAME>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `name` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--template` | `text` | `"default"` | Project template: default, web |

#### `vtb build`

Build Vertebra application assets, standalone distributions, or onefile binaries.

```bash
vtb build [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--target` | `text` | `"frontend"` | Build target: frontend, backend, all, or platform triple (e.g. linux-x86_64, darwin-aarch64) |
| `--profile` | `text` | `"release"` | Build profile: debug, release |
| `--standalone` | `flag` | `False` | Assemble a relocatable standalone distribution directory |
| `--onefile` | `flag` | `False` | Package application into a self-extracting single-file executable |
| `--server-binary` | `file` | - | Compiled vertebra-server executable to bundle under bin/ in the staged distribution |
| `--entrypoint` | `text` | `"app:app"` | Application entrypoint module:attribute (default: app:app) |
| `--python-version` | `text` | `"3.12"` | Target Python runtime version (3.12, 3.13, 3.14, 3.14t, 3.15, 3.15t) |
| `--output` | `path` | - | Output directory (for --standalone) or binary path (for --onefile) |
| `--project-dir` | `directory` | - | Project directory containing pyproject.toml and any frontend workspace |
| `--include-agent-skills` | `flag` | `False` | Stage .agents/skills into the standalone bundle (application repositories only) |

## JavaScript & Frontend Tooling

#### `vtb js add`

Add ESM dependencies to ``[tool.vtb.js]`` in pyproject.toml.

PACKAGES are package names, optionally with versions (e.g., ``react@18``).

```bash
vtb js add [OPTIONS] <PACKAGES>...
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `packages` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--dev`, `-D` | `flag` | `False` | Add as dev dependency. |

#### `vtb js remove`

Remove ESM dependencies from ``[tool.vtb.js]`` in pyproject.toml.

PACKAGES are package names to remove.

```bash
vtb js remove <PACKAGES>...
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `packages` | `text` | Yes | - |

#### `vtb js list`

List ESM dependencies from ``[tool.vtb.js]`` in pyproject.toml.

```bash
vtb js list
```

#### `vtb js install`

Sync the wheel-bundled @vertebra/client SDK and generate import map.

```bash
vtb js install
```

#### `vtb js build`

Build frontend assets for production.

```bash
vtb js build
```

#### `vtb js generate-types`

Generate TypeScript types from Python route handlers.

```bash
vtb js generate-types [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--output`, `-o` | `path` | - | Output directory. |

#### `vtb js exec`

Execute a command with npx/bunx.

CMD is the command and arguments to run.

```bash
vtb js exec <CMD>...
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `cmd` | `text` | Yes | - |

## Agent MCP & Background Workers

#### `vtb ai models list`

Display the built-in embedding model catalog and local cache / SHM status.

```bash
vtb ai models list [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--provider` | `choice(vertex | gemini | gemma | fastembed | static)` | - | Filter models by provider |
| `--json` | `flag` | `False` | Emit catalog as JSON |
| `--cache-dir` | `path` | - | Local model cache directory |

#### `vtb ai models pull`

Pull model artifacts from an unauthenticated mirror or cloud bucket into the local cache.

```bash
vtb ai models pull [OPTIONS] <MODEL>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `model` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--quant` | `choice(int8 | q4 | bf16 | fp32)` | - | Target model quantization |
| `--cache-dir` | `path` | - | Local model cache directory |
| `--source` | `text` | - | Override source mirror or bucket URI |

#### `vtb ai models stage`

Stage a verified model bundle and deterministic manifest.json to a cloud bucket.

```bash
vtb ai models stage [OPTIONS] <MODEL>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `model` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--to` | `text` | - | **Required.** Destination cloud or local store URI (gs://, s3://, file://) |
| `--quant` | `choice(int8 | q4 | bf16 | fp32)` | - | Target model quantization |
| `--cache-dir` | `path` | - | Local model cache directory |
| `--source` | `text` | - | Optional source mirror URI when not yet pulled |

#### `vtb ai models warm`

Fetch a staged model bundle, verify SHA-256 digests, and optionally warm /dev/shm.

```bash
vtb ai models warm [OPTIONS] <MODEL>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `model` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--from` | `text` | - | Staged cloud or local store URI (defaults to VERTEBRA_MODEL_SOURCE) |
| `--shm` | `flag` | `False` | Map verified model artifacts into /dev/shm (1x RAM across workers) |
| `--cache-dir` | `path` | - | Local model cache directory |

#### `vtb ai mcp bridge`

Bridge local newline-delimited JSON-RPC 2.0 stdio to a remote HTTP/SSE MCP endpoint.

```bash
vtb ai mcp bridge [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--url` | `text` | - | **Required.** Remote MCP HTTP/SSE endpoint URL |
| `--token` | `text` | - | Bearer / OAuth2 / OIDC access token |
| `--timeout` | `float` | `30.0` | Per-request HTTP timeout in seconds |

#### `vtb ai mcp serve`

Run an in-process MCP service over standard input/output (stdio).

```bash
vtb ai mcp serve [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--name` | `text` | `"vertebra-mcp"` | Server name |

#### `vtb workers run`

Run admission-controlled background worker lanes or a single --oneshot job for APP.

```bash
vtb workers run [OPTIONS] [APP]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `app` | `text` | No | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--queues` | `text` | `"default"` | Comma-separated queue lane names (e.g. default,etl) |
| `--concurrency` | `text` | `"10"` | Comma-separated per-lane concurrency limits (e.g. 10,2) |
| `--drain-timeout` | `float` | `30.0` | Graceful drain timeout in seconds |
| `--oneshot` | `text` | - | Execute a single registered background job and exit |
| `--payload` | `text` | - | Optional JSON payload passed to --oneshot job |
