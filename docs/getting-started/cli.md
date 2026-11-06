# CLI Guide

The `vtb` command-line interface manages the full lifecycle of a Vertebra project: scaffolding, hot-reload development, production execution, frontend asset bundling, TypeScript client generation, MCP stdio bridges, background queue workers, and standalone binary packaging.

## Running CLI Commands

=== "Direct CLI (Activated Environment)"
    ```bash
    vtb --help
    ```
=== "uvx (Ad-hoc Execution)"
    ```bash
    uvx vertebra-core --help
    ```
=== "uv (Project Runner)"
    ```bash
    uv run vtb --help
    ```

## Common Workflows

### Scaffold a New Project

Create a new project directory with `vtb init`:

```bash
vtb init my-service
```

### Development & Production Servers

Run in development mode with automatic Python reload and frontend HMR:

```bash
vtb dev app:app --port 8000
```

Run in production mode with multi-worker scale-out (`--workers 4` automatically selects `shm` on standard GIL Python and `embedded` on free-threaded `3.14t`/`3.15t`; pass `--topology embedded` or `--topology shm` to override explicitly) and TLS certificates:

```bash
vtb run app:app \
  --host 0.0.0.0 \
  --port 8443 \
  --workers 4 \
  --tls-cert certs/server.pem \
  --tls-key certs/server.key
```

### Frontend & TypeScript Codegen (`vtb js`)

Manage frontend dependencies, bundle assets through `vertebra-bundler` (Rolldown/Oxc), and generate end-to-end TypeScript route and DTO types:

```bash
vtb js install
vtb js add zod
vtb js build
vtb js generate-types
```

The `@vertebra/client` TypeScript SDK is supplied directly with the Python package distribution and linked automatically without requiring an external NPM install.

### Background Queue Workers (`vtb workers`)

Run dedicated background task queue and cron workers:

```bash
vtb workers run app:app --concurrency 16 --queues cpu,io,agent
```

### MCP Stdio-to-HTTP Bridge (`vtb ai mcp`)

Expose a remote or local Vertebra MCP HTTP endpoint over standard input/output for desktop AI hosts:

```bash
vtb ai mcp bridge --url http://127.0.0.1:8000/mcp --timeout 30
```

### Standalone Binary Packaging (`vtb build`)

Package your Python application, native extensions, and static assets into a relocatable standalone directory or self-contained single-file executable:

```bash
vtb build --standalone --onefile
```

See the complete generated flag reference in the [CLI Reference](../reference/cli.md).
