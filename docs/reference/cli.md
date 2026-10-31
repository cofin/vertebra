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
| `--version` | `flag` | `False` | Show the version and exit. |

## Application Lifecycle & Packaging

#### `vtb run`

Run the Vertebra server.

```bash
vtb run [OPTIONS] <APP>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `app` | `text` | Yes | - |

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
| `--tls-cert` | `path` | - | Path to PEM certificate for HTTPS |
| `--tls-key` | `path` | - | Path to PEM private key for HTTPS |
| `--worker-only` | `flag` | `False` | Run background worker lanes with health probes only |
| `--oneshot-job` | `text` | - | Execute a single registered job inside the app lifespan and exit |
| `--no-banner` | `flag` | `False` | Disable startup banner |

#### `vtb dev`

Run Vertebra in development mode.

Single worker, auto-reload enabled, frontend HMR active.

APP is the application import string (e.g., myapp:app).

```bash
vtb dev [OPTIONS] <APP>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `app` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--host` | `text` | `"127.0.0.1"` | Bind host |
| `--port` | `integer` | `8000` | Bind port |
| `--log-level` | `text` | `"info"` | Log level |
| `--no-reload` | `flag` | `False` | Disable file watching and auto-reload |
| `--asgi` | `flag` | `False` | Use standard ASGI instead of native C ABI offload |
| `--interface` | `choice(auto | asgi | rsgi | wsgi)` | `"auto"` | Gateway protocol interface (auto, asgi, rsgi, or wsgi) |
| `--no-banner` | `flag` | `False` | Suppress startup banner |
| `--no-tui` | `flag` | `False` | Disable interactive TUI dashboard |
| `--verbose` | `flag` | `False` | Enable verbose tracebacks with local variables |

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
| `--pgo` | `flag` | `False` | Enable Profile-Guided Optimization metadata during packaging |
| `--profdata` | `file` | - | Merged LLVM .profdata file for Rust -Cprofile-use packaging metadata |
| `--server-binary` | `file` | - | Compiled vertebra-server executable to bundle under bin/ in the staged distribution |
| `--entrypoint` | `text` | `"app:app"` | Application entrypoint module:attribute (default: app:app) |
| `--python-version` | `text` | `"3.12"` | Target Python runtime version (3.12, 3.13, 3.14, 3.14t, 3.15, 3.15t) |
| `--output` | `path` | - | Output directory (for --standalone) or binary path (for --onefile) |
| `--project-dir` | `directory` | - | Project directory containing pyproject.toml and any frontend workspace |

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

Resolve JS dependencies and generate import map.

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

#### `vtb mcp bridge`

Bridge local newline-delimited JSON-RPC 2.0 stdio to a remote HTTP/SSE MCP endpoint.

```bash
vtb mcp bridge [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--url` | `text` | - | **Required.** Remote MCP HTTP/SSE endpoint URL |
| `--token` | `text` | - | Bearer / OAuth2 / OIDC access token |
| `--timeout` | `float` | `30.0` | Per-request HTTP timeout in seconds |

#### `vtb mcp serve`

Run an in-process MCP service over standard input/output (stdio).

```bash
vtb mcp serve [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--name` | `text` | `"vertebra-mcp"` | Server name |

#### `vtb workers run`

Run admission-controlled background worker lanes for APP.

```bash
vtb workers run [OPTIONS] <APP>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `app` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--queues` | `text` | `"default"` | Comma-separated queue lane names (e.g. default,etl) |
| `--concurrency` | `text` | `"10"` | Comma-separated per-lane concurrency limits (e.g. 10,2) |
| `--drain-timeout` | `float` | `30.0` | Graceful drain timeout in seconds |
