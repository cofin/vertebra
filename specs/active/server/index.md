# Server & Runtime - Consolidated Specification

**Status**: Active
**Priority**: P0
**Date**: 2026-01-19
**Target**: Vertebra 1.0 Release

---

## Overview

This directory contains the **consolidated specification** for Server & Runtime work in Vertebra 1.0. It combines MCP Protocol implementation, Pool Health & Lifecycle Management, and Unified Observability.

**Key Principle**: All tool invocation routes through shared-memory IPC (<5ms P99 target).

---

## Documents

### [PRD: Server & Runtime](./prd.md)
**Word Count**: 7,200+ words

Comprehensive product requirements document covering:
- MCP Protocol Implementation (stdio, SSE, WebSocket transports)
- Python `@mcp_tool` decorator and schema generation
- Pool Health Checks and Lifecycle Hooks
- Unified Observability (`/__metrics__` endpoint)

**Sections**:
1. Executive Summary
2. Background & Context
3. Architecture
4. Implementation Phases (1-7)
5. Testing Strategy
6. Success Metrics

---

### [Tasks Breakdown](./tasks.md)
**Total Tasks**: 80+
**Duration**: 11 weeks

Detailed task breakdown for each phase:
- **Phase 1**: MCP Foundation (Week 1-2)
- **Phase 2**: Python Integration (Week 3-4)
- **Phase 3**: Advanced MCP Features (Week 5-6)
- **Phase 4**: Pool Health Checks (Week 7-8)
- **Phase 5**: Pool Lifecycle Hooks (Week 8-9)
- **Phase 6**: Unified Observability (Week 9-10)
- **Phase 7**: Production Hardening (Week 10-11)

---

### [Research Plan](./research/plan.md)
**Word Count**: 2,000+ words

Comprehensive research covering:
1. MCP Protocol Specification
2. JSON-RPC 2.0 Implementation
3. Pool Health Check Patterns
4. Prometheus Metrics Format
5. Performance Considerations

---

### [Recovery Guide](./recovery.md)

Session resumption guide covering:
- Quick context and current state
- Key files and locations
- Architecture principles
- Implementation order
- Common commands
- Success metrics

**Quick Resume**:
```bash
cd /home/cody/code/c/vertebra
cat specs/active/server/recovery.md  # Read this file
cat specs/active/server/prd.md       # Read full PRD
cat specs/active/server/tasks.md     # Check task breakdown
```

---

## Quick Start

### 1. Read the PRD
```bash
cat specs/active/server/prd.md
```

### 2. Choose Your Phase

**Starting fresh?** -> Phase 1 (MCP Foundation) - builds protocol layer

**Working on Python?** -> Phase 2 (Python Integration) - @mcp_tool decorator

**Working on pools?** -> Phase 4 (Health Checks) - independent of MCP phases

### 3. Check Tasks
```bash
cat specs/active/server/tasks.md
```

### 4. Create Branch
```bash
git checkout -b feat/server-phase-N
```

### 5. Start Implementation
Follow task groups in `tasks.md`, run tests after each group.

---

## Architecture Summary

### Data Flow
```
AI Assistants (Claude/Gemini)
    |
    | JSON-RPC 2.0 (stdio/SSE/WS)
    v
Bun MCP Server (TypeScript)
    |
    | Shared-Memory IPC
    v
Python Workers (@mcp_tool functions)
    |
    v
Connection Pools (with Health Checks)
    |-> PostgreSQL
    |-> MySQL
    |-> Redis
    |-> Storage
```

### Key Principles

1. **Zero-Socket Tool Invocation**: MCP tools execute via shared memory IPC
2. **Type-Safe Schemas**: Python type hints -> JSON Schema via Type Bridge IR
3. **Health-Checked Pools**: Periodic validation (30s), idle reaping (5min)
4. **Single Metrics Endpoint**: `/__metrics__` aggregates all statistics
5. **Multi-Transport Support**: stdio (Claude Desktop), SSE (web), WebSocket (bidirectional)

---

## File Paths

### TypeScript Implementation

**MCP Server**:
```
src/ts/src/mcp/
|-- server.ts              # Core MCP server (enhance)
|-- transports/
|   |-- stdio.ts           # stdio transport (NEW)
|   |-- sse.ts             # SSE transport (NEW)
|   |-- websocket.ts       # WebSocket transport (NEW)
|-- ipc-bridge.ts          # Python IPC bridge (NEW)
|-- schema-registry.ts     # Tool/resource schemas (NEW)
|-- metrics.ts             # MCP metrics (NEW)
```

**Broker Enhancements**:
```
src/ts/src/broker/
|-- base-pool.ts           # Health checks + hooks (enhance)
|-- serve.ts               # Pool health implementations (enhance)
|-- metrics.ts             # Broker metrics (NEW)
```

**Server Observability**:
```
src/ts/src/server/
|-- metrics.ts             # Metrics endpoint (NEW)
|-- prometheus.ts          # Prometheus formatter (NEW)
```

### Python Implementation

**MCP Module**:
```
src/py/vertebra/mcp/
|-- __init__.py            # Public API
|-- decorators.py          # @mcp_tool decorator
|-- registry.py            # Procedure registry
|-- schema.py              # JSON Schema generator
|-- handler.py             # IPC message handler
|-- resources.py           # @mcp_resource decorator
```

### Tests

**TypeScript**:
```
src/ts/tests/integration/
|-- mcp.test.ts            # MCP integration tests (NEW)
|-- pool-health.test.ts    # Health check tests (NEW)
|-- metrics.test.ts        # Metrics tests (NEW)

src/ts/tests/unit/
|-- lifecycle-hooks.test.ts # Hook tests (NEW)
```

**Python**:
```
src/py/tests/
|-- test_mcp_integration.py # MCP integration tests (NEW)
```

---

## Success Metrics

### Functional
- [ ] Claude Desktop can discover and call Vertebra tools
- [ ] All three transports (stdio, SSE, WebSocket) work
- [ ] Python `@mcp_tool` decorator generates correct schemas
- [ ] Health checks detect and remove bad connections within 30s
- [ ] `/__metrics__` endpoint returns aggregated statistics

### Quality
- [ ] 0 TypeScript type errors
- [ ] 0 Python mypy errors (strict mode)
- [ ] Test coverage > 85%

### Performance
- [ ] MCP Tool Invocation (P99): < 5ms
- [ ] Pool Acquisition (P99): < 2ms
- [ ] Metrics Endpoint (P99): < 50ms
- [ ] Throughput: > 1000 tool calls/sec

---

## Related Specifications

- **Database & ORM**: `/home/cody/code/c/vertebra/specs/active/database/` - SQLSpec, SQLAlchemy, Advanced Alchemy
- **Archived Specs**: `/home/cody/code/c/vertebra/specs/archive/` - Previous PRDs consolidated into this spec

---

## Common Commands

### Development
```bash
# Install dependencies
make install

# Format code
make lint

# Type checking
make mypy

# Run all tests
make test-all
```

### Testing
```bash
# Python tests
make test

# TypeScript tests
make js-test

# Integration tests (requires Docker)
uv run pytest src/py/tests -m integration --integration
```

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Active Specification
