# CLI Guide

The `vtb` CLI handles project setup, development servers, production runs, frontend builds, type generation, background workers, MCP bridges, and standalone packaging.

## Running `vtb`

=== "Activated environment"
    ```bash
    vtb --help
    ```
=== "uvx"
    ```bash
    uvx vertebra-core --help
    ```
=== "uv run"
    ```bash
    uv run vtb --help
    ```

## Common Workflows

### Scaffold a Project

```bash
vtb init my-service
```

### Development & Production Servers

Run with Python reload and frontend hot reload:

```bash
vtb dev app:app --port 8000
```

Run in production with multiple workers and TLS (use `--topology embedded` or `--topology shm` to override automatic mode selection):

```bash
vtb run app:app \
  --host 0.0.0.0 \
  --port 8443 \
  --workers 4 \
  --tls-cert certs/server.pem \
  --tls-key certs/server.key
```

### Frontend & Type Generation

Install JS dependencies, bundle assets, and generate TypeScript types from your Python routes:

```bash
vtb js install
vtb js add zod
vtb js build
vtb js generate-types
```

`@vertebra/client` ships inside the Python wheel and links automatically without npm.

### Background Workers

Run standalone queue and cron workers:

```bash
vtb workers run app:app --concurrency 16 --queues cpu,io,agent
```

Or run workers with HTTP health probes (`/healthz`, `/readyz`, `/startupz`) and no HTTP app ingress:

```bash
vtb run app:app --worker-only --host 0.0.0.0 --port 8080
```

### MCP Server & Bridge

Serve your app's MCP tools over stdio, or bridge a local stdio client to a remote HTTPS/IAP MCP server:

```bash
vtb ai mcp serve --app app:app
vtb ai mcp bridge \
  --url https://agent.example.com/mcp \
  --audience https://agent.example.com \
  --timeout 30
```

### Standalone Binaries

Package your app, extensions, and static assets into a standalone directory or single-file executable (add `--include-agent-skills` to bundle `.agents/skills`):

```bash
vtb build --standalone --onefile
```

See the full flag list in the [CLI Reference](../reference/cli.md).
