# Vertebra PRD Index

This directory contains Product Requirement Documents (PRDs) for the Vertebra project.

## Active PRDs

### [PRD-0: Foundation and Repository Cleanup](./PRD-0-foundation.md)
**Status:** Draft
**Estimated Effort:** 3-5 days

Complete removal of DMA legacy code and establishment of multi-language build infrastructure (Python + Rust + TypeScript/Bun).

**Deliverables:**
- Remove all DMA references from codebase
- Configure maturin for Rust+Python integration
- Create Rust workspace (vertebra-core, vertebra-py, vertebra-js)
- Create TypeScript package with Bun
- Update Makefile with cross-language targets
- Create CLAUDE.md and docs/arch.md

**Verification:**
```bash
make clean && make install
uv run python -c "import vertebra; print(vertebra.__version__)"
cd src/rs && cargo build && cargo test
cd src/js && bun install && bun test
grep -r "dma" --include="*.py" --include="*.toml" src/  # Should return empty
```

## Planned PRDs

### PRD-1: Zero-Copy IPC Foundation
Core IPC primitives, protocol design, shared memory management.

### PRD-2: Procedure System
tRPC-like decorators, router implementation, procedure registry.

### PRD-3: Type Bridge
Python AST → Rust types, TypeScript type generation, validation.

### PRD-4: ASGI Server
Rust+Bun HTTP server, Litestar integration, worker management.

### PRD-5: Bun Database Drivers
PostgreSQL, MySQL, Redis drivers using Bun's fast I/O.

### PRD-6: WebSocket Support
Bi-directional type-safe communication, subscriptions.

### PRD-7: Distributed Tracing
OpenTelemetry integration, trace correlation across languages.

### PRD-8: Code Generation CLI
`vertebra generate` command, watch mode, plugins.

### PRD-9: Browser SDK
Optimized client bundle, React hooks, tree-shaking.

### PRD-10: Schema Versioning
API versioning, backward compatibility, migration tools.

### PRD-11: ORM Integration
SQLAlchemy, Tortoise ORM, Prisma integration patterns.

## PRD Template

Use this structure for new PRDs:

```markdown
# PRD-N: Title

**Status:** Draft | In Progress | Complete
**Author:** Name
**Created:** YYYY-MM-DD
**Last Updated:** YYYY-MM-DD

## Executive Summary
Brief overview and success criteria.

## Background
Context and motivation.

## Goals and Non-Goals
What's in scope and what's not.

## User Stories
As a [user], I want [goal] so that [benefit].

## Technical Approach
High-level design decisions.

## Detailed Specification
Component-by-component breakdown.

## Implementation Phases
Ordered tasks with dependencies.

## Verification and Testing
Acceptance criteria and test strategy.

## Risks and Mitigations
Potential issues and contingencies.

## Success Metrics
How we measure completion.

## Research and References
External sources and prior art.
```

## PRD Workflow

1. **Draft:** Initial proposal, seeking feedback
2. **In Progress:** Actively being implemented
3. **Complete:** All verification passed, merged to main

## Contributing

When creating a new PRD:

1. Copy the template above
2. Fill in all required sections
3. Add to this index with brief summary
4. Submit for review before implementation
5. Update status as work progresses

## Questions?

See [CLAUDE.md](../../CLAUDE.md) for project conventions and [docs/arch.md](../arch.md) for architectural context.
