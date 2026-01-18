# PRD-7: MCP Integration - Session Recovery Guide

**Created**: 2026-01-17
**Last Updated**: 2026-01-18
**Workspace**: `/home/cody/code/c/vertebra/docs/prd/PRD-7-mcp-integration/`

---

## Quick Start

```bash
# Read main PRD
cat /home/cody/code/c/vertebra/docs/prd/PRD-7-mcp-integration.md

# Review task list
cat /home/cody/code/c/vertebra/docs/prd/PRD-7-mcp-integration/tasks.md
```

---

## What is PRD-7?

MCP Integration adds a Bun-based MCP server that routes tool/resource calls to Python handlers via low-overhead IPC. Goals:
- Type-safe MCP schema generation from Python annotations
- Fast tool invocation (p95 < 5ms overhead target)
- Multi-transport support (stdio, SSE, Streamable HTTP)
- Cross-platform support (Linux/macOS/Windows)

---

## Architecture (High Level)

```
MCP Client
   │ JSON-RPC
Bun MCP Server
   │ IPC transport (UDS / named pipe)
Python Tool/Resource Handlers
```

---

## Current Status

- Planning complete; implementation not started.
- Dependencies: PRD-1 (IPC), PRD-2 (Python bindings/type bridge), PRD-3 (Bun server layer).

---

## Key Decisions

- **IPC transport**: UDS on Unix, named pipes (or TCP fallback) on Windows.
- **Schema generation**: from Python annotations + `Annotated` metadata.
- **Spec version**: lock per release; update only at decision gate.

---

## Next Steps (Phase 1)

1. Implement MCP JSON-RPC core in Bun.
2. Add stdio transport and baseline conformance tests.
3. Define MCP server module layout in `src/ts/src/mcp/`.
