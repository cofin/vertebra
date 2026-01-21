# Database & ORM Integration - Consolidated Specification

**Status**: Active - Critical Path to 1.0
**Priority**: P0
**Date**: 2026-01-19
**Target**: Vertebra 1.0 Release

---

## Overview

This directory contains the **consolidated specification** for all Database & ORM work in Vertebra 1.0. It combines SQLSpec stabilization, SQLAlchemy integration, Advanced Alchemy support, serialization infrastructure, and production-grade connection pooling.

**Key Principle**: All database access routes through Vertebra's shared-memory broker (no sockets).

---

## Documents

### 📋 [PRD: Database & ORM Integration](./prd.md)
**Word Count**: 7,842 words

Comprehensive product requirements document covering:
- SQLSpec stabilization (API rename, Arrow serialization, storage)
- SQLAlchemy DBAPI 2.0 + dialects (Postgres, MySQL, SQLite)
- Advanced Alchemy integration (Repository/Service patterns)
- Serialization module (30+ types for IPC and HTTP)
- Litestar plugin overhaul (separate plugins per ORM)
- Pool health checks and lifecycle hooks

**Sections**:
1. Architecture Overview
2. Phase 1: SQLSpec Stabilization
3. Phase 2: Serialization Module
4. Phase 3: SQLAlchemy Integration
5. Phase 4: Pool Health Checks
6. Phase 5: Advanced Alchemy Integration
7. Phase 6: Litestar Plugin Overhaul
8. Testing Strategy
9. Implementation Roadmap
10. Success Metrics

---

### ✅ [Tasks Breakdown](./tasks.md)
**Total Tasks**: 120+
**Duration**: 4-5 weeks

Detailed task breakdown for each phase:
- **Phase 1**: SQLSpec Stabilization (Week 1)
  - Task Group 1.1: API Rename (8 tasks)
  - Task Group 1.2: Arrow Serialization (7 tasks)
  - Task Group 1.3: Storage Integration (5 tasks)

- **Phase 2**: Serialization Module (Week 1-2)
  - Task Group 2.1: Type Support Matrix (7 tasks)
  - Task Group 2.2: MessagePack Codec (5 tasks)
  - Task Group 2.3: Litestar Integration (4 tasks)

- **Phase 3**: SQLAlchemy Integration (Week 2-3)
  - Task Group 3.1: DBAPI Module (6 tasks)
  - Task Group 3.2: Postgres Dialect (5 tasks)
  - Task Group 3.3: MySQL and SQLite Dialects (3 tasks)
  - Task Group 3.4: Transaction Support (3 tasks)

- **Phase 4**: Pool Health Checks (Week 3)
  - Task Group 4.1: Health Check Implementation (7 tasks)
  - Task Group 4.2: Lifecycle Hooks (3 tasks)
  - Task Group 4.3: Idle Connection Management (3 tasks)

- **Phase 5**: Advanced Alchemy Integration (Week 4)
  - Task Group 5.1: Base Config Classes (3 tasks)
  - Task Group 5.2: Dialect-Specific Configs (3 tasks)
  - Task Group 5.3: Storage Backend (4 tasks)
  - Task Group 5.4: Integration Tests (3 tasks)

- **Phase 6**: Litestar Plugin Overhaul (Week 4)
  - Task Group 6.1: Plugin Infrastructure (2 tasks)
  - Task Group 6.2: SQLSpec Plugin (2 tasks)
  - Task Group 6.3: SQLAlchemy Plugin (2 tasks)
  - Task Group 6.4: Advanced Alchemy Plugin (2 tasks)

---

### 🔬 [Research Plan](./research/plan.md)
**Word Count**: 3,440 words

Comprehensive research covering:
1. **Existing Codebase Analysis**
   - SQLSpec current implementation (~85% complete)
   - Broker architecture (Bun pools)
   - Shared memory IPC (Rust C ABI)

2. **Best Practices Research**
   - SQLAlchemy DBAPI implementation
   - SQLAlchemy dialect patterns
   - Arrow serialization strategies
   - Storage backend patterns
   - Litestar plugin architecture

3. **Serialization Type Research**
   - Litestar type encoders (existing)
   - MessagePack type hooks (msgspec)
   - Database-specific types (Postgres arrays, JSONB, MySQL spatial)

4. **Performance Considerations**
   - IPC overhead benchmarks (< 10ms target)
   - Arrow threshold analysis (1000 rows OR 1MB)

5. **Mypyc Compatibility**
   - Potential issues (no dynamic attributes)
   - Safe patterns (explicit delegation)
   - Testing strategy (test both variants)

6. **Implementation Priorities**
   - Critical path (Phases 1-6)
   - Risk mitigation strategies

---

### 🔄 [Recovery Guide](./recovery.md)

Session resumption guide covering:
- Quick context and current state
- Key files and locations
- Architecture principles
- Implementation order (where to start)
- Common commands
- Success metrics
- Dependencies and risks

**Quick Resume**:
```bash
cd /home/cody/code/c/vertebra
cat specs/active/database/recovery.md  # Read this file
cat specs/active/database/prd.md      # Read full PRD
cat specs/active/database/tasks.md    # Check task breakdown
```

---

## Quick Start

### 1. Read the PRD
```bash
cat specs/active/database/prd.md
```

### 2. Choose Your Phase

**Starting fresh?** → Phase 2 (Serialization) - builds foundation

**Continuing SQLSpec work?** → Phase 1 (SQLSpec Stabilization) - completes existing work

**Working on pools?** → Phase 4 (Health Checks) - independent of other phases

### 3. Check Tasks
```bash
cat specs/active/database/tasks.md
```

### 4. Create Branch
```bash
git checkout -b feat/database-phase-N
```

### 5. Start Implementation
Follow task groups in `tasks.md`, run tests after each group.

---

## Architecture Summary

### Data Flow
```
Python Application (Worker Process)
    ↓
ORM Layer (SQLSpec / SQLAlchemy / Advanced Alchemy)
    ↓
Vertebra Extension (Config + Proxy Driver)
    ↓
C ABI Shared Memory (Ring Buffers)
    ↓
Bun Broker Process
    ├─→ SQL Pools (Postgres, MySQL, SQLite)
    ├─→ Redis Pool
    └─→ Storage Pool (fsspec-compatible)
```

### Key Principles

1. **Zero-Socket Data Plane**: No TCP/UDS/HTTP for queries
2. **Central Authority**: Bun broker owns all connection pools
3. **Fairness**: Scheduler distributes connections across workers
4. **Serialization Strategy**:
   - Small results (< 1000 rows, < 1MB): MessagePack
   - Large results (≥ 1000 rows or ≥ 1MB): Arrow IPC
5. **Naming Convention**: User-facing APIs use `Vertebra*` prefix

---

## File Paths

### Python Implementation

**SQLSpec** (existing):
```
src/py/vertebra/extensions/sqlspec/
├── _config.py          # Base classes (RENAME: Broker* → Vertebra*)
├── _driver.py          # Driver implementations
├── postgres/           # Postgres-specific
├── mysql/              # MySQL-specific
└── sqlite/             # SQLite-specific
```

**Serialization** (new):
```
src/py/vertebra/extensions/serialization/
├── __init__.py
├── _types.py           # Type definitions
├── _encoders.py        # Encoder hooks
├── _decoders.py        # Decoder hooks
├── msgpack.py          # VertebraCodec
├── json.py             # JSON codec
└── litestar.py         # Litestar integration
```

**SQLAlchemy** (new):
```
src/py/vertebra/extensions/sqlalchemy/
├── __init__.py
├── _dbapi.py           # PEP 249 DBAPI
├── _base.py            # Base dialect classes
├── postgres/           # Postgres dialect
├── mysql/              # MySQL dialect
└── sqlite/             # SQLite dialect
```

**Advanced Alchemy** (new):
```
src/py/vertebra/extensions/advanced_alchemy/
├── __init__.py
├── _config.py          # Base config classes
├── _engine.py          # Engine factory
├── _storage.py         # VertebraStorageBackend
├── postgres/           # Postgres configs
├── mysql/              # MySQL configs
└── sqlite/             # SQLite configs
```

**Litestar** (overhaul):
```
src/py/vertebra/extensions/litestar/
├── __init__.py
├── _base.py            # Base plugin infrastructure
├── _serialization.py   # Auto-registration
├── sqlspec.py          # VertebraSQLSpecPlugin
├── sqlalchemy.py       # VertebraSQLAlchemyPlugin
└── advanced_alchemy.py # VertebraAdvancedAlchemyPlugin
```

### TypeScript Broker

```
src/ts/src/broker/
├── serve.ts            # Main broker loop, pool implementations
├── base-pool.ts        # Abstract pool adapter
├── health.ts           # Health checkers (NEW)
├── scheduler.ts        # Fairness scheduler
└── client.ts           # BrokerClient types
```

### Tests

**Python**:
```
src/py/tests/
├── test_sqlspec_*.py               # SQLSpec tests
├── test_sqlalchemy_*.py            # SQLAlchemy tests (NEW)
├── test_advanced_alchemy.py        # AA tests (NEW)
├── test_serialization_*.py         # Serialization tests (NEW)
└── test_litestar_plugin.py         # Plugin tests (NEW)
```

**TypeScript**:
```
src/ts/tests/unit/
├── pool-health.test.ts             # Health check tests (NEW)
└── lifecycle-hooks.test.ts         # Hook tests (NEW)
```

---

## Success Metrics

### Functional
- [ ] `VertebraPostgresAsyncConfig` works with SQLSpec
- [ ] `create_engine("postgresql+vertebra://main")` works
- [ ] `create_async_engine("postgresql+vertebra://main")` works
- [ ] Advanced Alchemy Repository/Service patterns work
- [ ] Pool health checks run and report metrics
- [ ] All three Litestar plugins work

### Quality
- [ ] 0 mypy errors in strict mode
- [ ] Test coverage > 85%
- [ ] All modern type syntax

### Performance
- [ ] Throughput: > 10k req/sec (small queries)
- [ ] Latency: P99 < 10ms (IPC + query)
- [ ] Arrow transport: < 5ms overhead for 10k rows

---

## Dependencies

**Core** (always installed):
- `nanoarrow >= 0.5` - Arrow C Data Interface (~1MB)
- `msgspec >= 0.18` - Fast serialization (~500KB)

**Optional** (user choice):
- `sqlspec >= 0.1` - For SQLSpec extension
- `sqlalchemy >= 2.0` - For SQLAlchemy extension
- `advanced-alchemy >= 0.1` - For AA extension
- `pyarrow >= 14.0` - For Arrow compute (optional, 100MB+)
- `litestar >= 2.0` - For Litestar integration

---

## Related Specifications

- **PRD-5**: Resource Management - Pool fairness scheduler
- **Remaining Work**: Consolidated 1.0 tasks
- **Shared-Memory Broker**: Broker architecture spec

---

## Status Tracking

### Current Phase
- 🔧 **Phase 1**: SQLSpec Stabilization (API rename, Arrow, storage)

### Upcoming Phases
- 📋 **Phase 2**: Serialization Module (30+ types)
- 📋 **Phase 3**: SQLAlchemy Integration (DBAPI + dialects)
- 📋 **Phase 4**: Pool Health Checks (parallel with Phase 3)
- 📋 **Phase 5**: Advanced Alchemy Integration
- 📋 **Phase 6**: Litestar Plugin Overhaul

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

### Debugging
```bash
# Check shared memory
ls -lh /dev/shm/ | grep vertebra

# Clean stale shared memory
rm -rf /dev/shm/vertebra-*

# Monitor broker
cd src/ts && bun run src/broker/index.ts
```

---

## Contact

For questions or clarifications, refer to:
- **PRD**: Detailed requirements and architecture
- **Tasks**: Step-by-step implementation guide
- **Research**: Background and best practices
- **Recovery**: Session resumption guide

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Active Specification
**Word Count (Total)**: 11,000+ words across all documents
