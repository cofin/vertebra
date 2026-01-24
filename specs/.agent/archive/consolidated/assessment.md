# Vertebra Requirements Consolidation Assessment

## Executive Summary

After comprehensive review of all specs in `specs/active/` and `docs/prd/`, this document identifies:
- **Superseded/Erroneous specs** to archive
- **Completed PRDs** to move to historical reference
- **Remaining work** to consolidate into focused PRDs
- **Optional dependencies** strategy

---

## 1. SPECS TO ARCHIVE (Superseded or Off-Track)

### 1.1 `driver-benchmark/` - ARCHIVE
**Reason**: Fundamentally misaligned with architecture
- Assumes direct driver comparison (asyncpg vs Vertebra)
- But Vertebra doesn't expose a direct `vertebra.connect()` API - it uses broker-backed adapters
- The "Driver Bet" hypothesis is invalid - Vertebra's value is shared pooling, not raw driver speed
- The `vertebra_runner.py` is a non-functional stub with speculative APIs
- **Action**: Archive entire directory

### 1.2 `bun-broker-pool-manager/` - ARCHIVE
**Reason**: Already implemented differently
- PRD describes a PoolManager with async queues and health checks
- Actual implementation delegates to FairScheduler in broker
- The `BasePoolAdapter` pattern is already implemented in `src/ts/src/broker/base-pool.ts`
- Remaining work (idle tracking, health checks) belongs in production-readiness, not separate spec
- **Action**: Archive; merge remaining tasks into production-readiness

### 1.3 `eventfd-async-transport/` - ARCHIVE
**Reason**: Already implemented
- Commit f354dbd (2026-01-19) implemented the async ring buffer with eventfd
- All Phase 1-3 tasks complete per agent review
- Phase 4 migration (23 call sites) is operational cleanup, not new architecture
- **Action**: Archive as "completed"; remaining migration is tech debt cleanup

### 1.4 `sqlalchemy-integration/` - MERGE INTO orm-extensions
**Reason**: Duplicates orm-extensions spec
- Both specs cover SQLAlchemy dialect support
- orm-extensions is more comprehensive (includes Advanced Alchemy, serialization)
- sqlalchemy-integration has conflicting URL scheme decisions
- **Action**: Archive; consolidate into orm-extensions

### 1.5 `build-distribution-experience/` - KEEP (Active)
**Reason**: Required for 1.0 distribution experience
- Comprehensive spec for single-file distribution
- 0% implemented; all 7 phases pending
- **Action**: Keep as active Phase 5 work

---

## 2. COMPLETED PRDs TO ARCHIVE (Historical Reference)

### 2.1 `docs/prd/PRD-0-foundation.md` - ARCHIVE
**Status**: 100% complete
- Build system configured, workspace created, documentation complete
- No remaining work
- **Action**: Move to `docs/prd/archive/`

### 2.2 `docs/prd/PRD-1-rust-ipc.md` and `/PRD-1-rust-ipc/` - ARCHIVE
**Status**: 100% complete
- Shared memory, SPSC/MPMC channels, buffer pools, sync primitives all implemented
- All benchmarks passing
- **Action**: Move to `docs/prd/archive/`

### 2.3 `docs/prd/PRD-2-python-bindings.md` and `/PRD-2-python-bindings/` - PARTIAL
**Status**: ~90% complete
- @procedure decorator, type IR, extractor, serializer, TypeScript generator done
- Only WebSocket integration pending (depends on server work)
- **Action**: Move to `docs/prd/archive/`; WebSocket is server concern

### 2.4 `docs/prd/PRD-3-bun-server.md` - PARTIAL ARCHIVE
**Status**: ~85% complete
- HTTP/1.1, HTTP/2, WebSocket, ASGI bridge, worker manager all done
- Remaining: HTTP/3 (Bun pending), unified metrics aggregation
- **Action**: Move core to archive; remaining items in production-readiness

### 2.5 `docs/prd/PRD-4-bun-drivers.md` and `/PRD-4-bun-drivers/` - ARCHIVE
**Status**: Architecture superseded
- Original socket-based driver architecture replaced by shared-memory broker
- SQLSpec integration handles database access now
- **Action**: Move to `docs/prd/archive/`; SQLSpec work in orm-extensions

---

## 3. ACTIVE SPECS TO KEEP AND CONSOLIDATE

### 3.1 `production-readiness/` - KEEP (Primary Consolidation Target)
**Status**: ~60% complete
- Phase 1 (Server): 100% done
- Phase 2 (Broker): 100% done
- Phase 3 (Drivers): 85% done (SQLSpec complete, AA/Storage pending)
- Phase 4 (MCP): 15% done
- Phase 5 (Distribution): 0% (defer)
- Phase 6 (Polish): 0%

**Remaining Work**:
1. Pool health checks and lifecycle hooks
2. MCP protocol implementation
3. Code quality (mypy errors, type audit)
4. Documentation and examples

### 3.2 `orm-extensions/` - KEEP (Merge sqlalchemy-integration)
**Status**: ~40% complete
- SQLSpec broker adapters implemented
- Critical: Rename `Broker*` to `Vertebra*` in public APIs
- SQLAlchemy dialects not started
- Advanced Alchemy not started
- Serialization module not started

**Remaining Work**:
1. API rename (Broker → Vertebra prefix)
2. SQLAlchemy DBAPI + dialects
3. Advanced Alchemy configs
4. Serialization codec
5. Litestar plugin overhaul

### 3.3 `codebase-consolidation/` - KEEP (75% Complete)
**Status**: Phases 1-3 done, Phase 4 pending
- Filesystem wrappers deleted
- Metrics merged
- BasePoolAdapter created
- ChannelHandle added to vertebra-js

**Remaining Work**:
1. Rename TypeScript worker files
2. Update CLAUDE.md
3. Add architecture documentation

### 3.4 `test-deadlock-fix/` - KEEP (Implementation Ready)
**Status**: 0% implemented, fully specified
- Root cause analysis complete
- Solution designed (unique SHM names, pytest-timeout)
- 10 tasks in 3 phases ready to execute

**Remaining Work**:
1. Add pytest-timeout config
2. Create test helpers module
3. Update test files with unique names

### 3.5 `shared-memory-broker/` - ARCHIVED
**Status**: 100% complete (implementation done)
- Defines authoritative architecture
- All phases implemented as of f354dbd
- Moved to `specs/archive/` as reference

---

## 4. OPTIONAL DEPENDENCIES STRATEGY

Based on user's assessment and architecture review:

### 4.1 Required Core Dependencies
```toml
[project.dependencies]
# Minimal core - lightweight dependencies only
click = ">=8.0"        # CLI (~200KB)
msgspec = ">=0.18"     # Fast serialization (~500KB)
anyio = ">=4.0"        # Async compatibility (~100KB)
nanoarrow = ">=0.5"    # Arrow C Data Interface (~1MB)
```

### 4.2 Optional Dependency Groups
```toml
[project.optional-dependencies]
# SQLSpec integration (opt-in)
sqlspec = ["sqlspec>=0.1"]

# Full PyArrow for power users who want compute
data = ["pyarrow>=14.0"]

# Development
dev = ["pytest", "pytest-asyncio", "mypy", "ruff"]

# All optional features
all = ["vertebra[sqlspec]"]
```

### 4.3 Rationale

**nanoarrow (CORE DEPENDENCY)**
- ~1MB installed size (same class as msgspec)
- Enables zero-copy Arrow C Data Interface for large result sets
- Vertebra-py exports `__arrow_c_stream__` - consumers use their preferred library
- Small enough to include by default; big enough to matter for performance

**pyarrow (OPTIONAL - `data` extra)**
- ~100MB installed size
- Overkill for simple API servers
- Only needed for data engineering use cases who want Arrow compute
- Users can consume Arrow results via Polars/DuckDB/pandas without pyarrow

**sqlspec (OPTIONAL - `sqlspec` extra)**
- Database integration is opt-in
- Many users just want HTTP server, not SQL
- Keep core lightweight

### 4.4 Implementation Pattern
```python
# In vertebra/_internal/dependencies.py
def require_sqlspec():
    """Raise helpful error if sqlspec not installed."""
    try:
        import sqlspec
        return sqlspec
    except ImportError:
        raise MissingDependencyError(
            "sqlspec",
            "pip install vertebra[sqlspec]"
        )
```

---

## 5. CONSOLIDATED WORK BREAKDOWN

### Phase 1: Immediate (Test & Quality)
1. **Test deadlock fix** - Unblock CI/development
2. **Codebase consolidation Phase 4** - Documentation updates
3. **API rename** - `Broker*` → `Vertebra*`

### Phase 2: Core Features (1-2 weeks)
1. **ORM Extensions** - SQLAlchemy dialects, serialization
2. **Pool health checks** - From production-readiness Phase 2
3. **MCP Protocol** - Basic tool support

### Phase 3: Polish (2-4 weeks)
1. **Type audit** - Fix 120 mypy errors
2. **Documentation** - Getting started, API docs
3. **Examples** - 2-3 reference applications

### Phase 4: Distribution (Post-MVP)
1. **Single-file build** - build-distribution-experience
2. **CI/CD pipeline** - Cross-platform wheels

---

## 6. RECOMMENDED ARCHIVE STRUCTURE

```
docs/prd/
├── archive/                    # Completed/superseded PRDs
│   ├── PRD-0-foundation.md
│   ├── PRD-1-rust-ipc.md
│   ├── PRD-1-rust-ipc/
│   ├── PRD-2-python-bindings.md
│   ├── PRD-2-python-bindings/
│   ├── PRD-3-bun-server.md
│   ├── PRD-4-bun-drivers.md
│   └── PRD-4-bun-drivers/
├── PRD-5-resource-management.md  # Keep - has remaining work
├── PRD-6-integration.md          # Keep - not started
├── PRD-7-mcp-integration.md      # Keep - in progress
├── PRD-8-production-readiness.md # Keep - main tracking doc
└── README.md

specs/
├── archive/                    # Superseded/completed specs + PRDs
│   ├── shared-memory-broker/   # Completed - architecture reference
│   ├── eventfd-async-transport/ # Completed
│   ├── driver-benchmark/       # Misaligned with architecture
│   ├── bun-broker-pool-manager/ # Merged into production-readiness
│   ├── sqlalchemy-integration/  # Merged into orm-extensions
│   └── PRD-0 through PRD-4     # Completed PRDs
├── active/
│   ├── consolidated/           # Master requirements
│   ├── test-deadlock-fix/      # P0 - Immediate priority
│   ├── codebase-consolidation/ # Code quality
│   ├── orm-extensions/         # Database integration
│   ├── production-readiness/   # Primary tracking
│   └── build-distribution-experience/ # Distribution
└── index.md
```
