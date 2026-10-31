# CLI Guide

The `vtb` command-line interface manages the full lifecycle of a Vertebra project: scaffolding, hot-reload development, production execution, frontend asset bundling, TypeScript client generation, MCP stdio bridges, background queue workers, and standalone binary packaging.

## Common Workflows

### Scaffold a New Project

Create a new project directory with `vtb init`:

```bash
uv run vtb init my-service
```

### Development & Production Servers

Run in development mode with automatic Python reload and frontend HMR:

```bash
uv run vtb dev app:app --port 8000
```

Run in production mode with explicit topology, worker count, and TLS certificates:

```bash
uv run vtb run app:app \
  --host 0.0.0.0 \
  --port 8443 \
  --topology shm \
  --workers 4 \
  --tls-cert certs/server.pem \
  --tls-key certs/server.key
```

### Frontend & TypeScript Codegen (`vtb js`)

Manage frontend dependencies, bundle assets through `vertebra-bundler` (Rolldown/Oxc), and generate end-to-end TypeScript route and DTO types:

```bash
uv run vtb js install
uv run vtb js add zod @vertebra/client
uv run vtb js build
uv run vtb js generate-types
```

### Background Queue Workers (`vtb workers`)

Run dedicated background task queue and cron workers:

```bash
uv run vtb workers run app:app --concurrency 16 --lanes cpu,io,agent
```

### MCP Stdio-to-HTTP Bridge (`vtb mcp`)

Expose a remote or local Vertebra MCP HTTP endpoint over standard input/output for desktop AI hosts:

```bash
uv run vtb mcp bridge --url http://127.0.0.1:8000/mcp --timeout 30
```

### Standalone Binary Packaging (`vtb build`)

Package your Python application, native extensions, and static assets into a wheel or self-contained PyApp executable:

```bash
uv run vtb build --standalone --onefile
```

See the complete generated flag reference in the [CLI Reference](../reference/cli.md).
