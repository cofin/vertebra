# Recovery Guide: Database & ORM Integration

**PRD**: `/home/cody/code/c/vertebra/specs/active/database/prd.md`
**Tasks**: `/home/cody/code/c/vertebra/specs/active/database/tasks.md`
**Research**: `/home/cody/code/c/vertebra/specs/active/database/research/plan.md`

---

## Quick Context

This is a **consolidated PRD** combining all Database & ORM work for Vertebra 1.0. It covers:

1. **SQLSpec Stabilization** (~85% done, needs rename + Arrow + storage)
2. **SQLAlchemy DBAPI 2.0 + Dialects** (0% done)
3. **Advanced Alchemy Integration** (0% done)
4. **Serialization Module** (0% done, ~30 types)
5. **Litestar Plugin Overhaul** (basic exists, needs separation)
6. **Pool Health Checks** (needs implementation)

**Total Duration**: 4-5 weeks
**Total Tasks**: 120+
**Critical Path**: Phase 1 → Phase 2 → Phase 3 → Phase 5 → Phase 6

---

## Current State

### Completed
- ✅ Shared-memory broker operational (Bun process)
- ✅ Ring buffer IPC primitives (Rust C ABI)
- ✅ Python CFFI bindings to shared memory
- ✅ Basic SQLSpec integration (~85%)
- ✅ Connection pool implementations (Postgres, MySQL, SQLite, Redis)
- ✅ Fairness scheduler

### In Progress
- 🔧 SQLSpec API rename (Broker* → Vertebra*)
- 🔧 Arrow serialization
- 🔧 Storage integration

### Not Started
- 📋 SQLAlchemy DBAPI + dialects
- 📋 Advanced Alchemy integration
- 📋 Serialization module (30+ types)
- 📋 Litestar plugin overhaul
- 📋 Pool health checks
- 📋 Integration tests

---

## Key Files

### Python Implementation

**SQLSpec** (existing, needs rename):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/*.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/*.py`

**Serialization** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/serialization/` (NEW)

**SQLAlchemy** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlalchemy/` (NEW)

**Advanced Alchemy** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/advanced_alchemy/` (NEW)

**Litestar** (overhaul):
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/litestar/` (NEW structure)

### TypeScript Broker

**Pools**:
- `/home/cody/code/c/vertebra/src/ts/src/broker/serve.ts` - Main broker loop
- `/home/cody/code/c/vertebra/src/ts/src/broker/base-pool.ts` - Abstract pool adapter
- `/home/cody/code/c/vertebra/src/ts/src/broker/health.ts` (NEW)

### Tests

**Python**:
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlspec_*.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_sqlalchemy_*.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_advanced_alchemy.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_serialization_*.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_litestar_plugin.py` (NEW)

**TypeScript**:
- `/home/cody/code/c/vertebra/src/ts/tests/unit/pool-health.test.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/tests/unit/lifecycle-hooks.test.ts` (NEW)

---

## Architecture Principles

### 1. Zero-Socket Data Plane
All database access routes through shared-memory broker. No TCP, UDS, or HTTP for queries.

### 2. Central Authority
Bun broker owns all connection pools. Python workers request connections via ring buffers.

### 3. Fairness
Scheduler distributes connections fairly across workers to prevent starvation.

### 4. Serialization Strategy
- **Small results** (< 1000 rows, < 1MB): MessagePack
- **Large results** (≥ 1000 rows or ≥ 1MB): Arrow IPC (zero-copy)

### 5. Naming Convention
All user-facing config classes use `Vertebra*` prefix (not `Broker*`):
- `VertebraPostgresAsyncConfig` ✅
- `BrokerPostgresAsyncConfig` ❌

---

## Implementation Order

### Phase 1: SQLSpec Stabilization (Week 1)

**Tasks**:
1. API Rename (20 class renames)
2. Arrow Serialization (threshold detection + nanoarrow integration)
3. Storage Integration (`VertebraObjectStore`)
4. Integration Tests

**Start Here**:
```bash
cd /home/cody/code/c/vertebra
git checkout -b feat/database-sqlspec-stabilization

# Task 1.1.1: Rename _config.py classes
code src/py/vertebra/extensions/sqlspec/_config.py
```

**Acceptance**:
- All public APIs use `Vertebra*` prefix
- Query with 10k rows uses Arrow IPC
- Storage operations route through broker
- Integration tests pass

---

### Phase 2: Serialization Module (Week 1-2)

**Tasks**:
1. Type Support Matrix (30+ types)
2. MessagePack Codec (`VertebraCodec`)
3. JSON Codec
4. Litestar Integration

**Start Here**:
```bash
mkdir -p src/py/vertebra/extensions/serialization
touch src/py/vertebra/extensions/serialization/__init__.py
touch src/py/vertebra/extensions/serialization/_types.py
touch src/py/vertebra/extensions/serialization/_encoders.py
touch src/py/vertebra/extensions/serialization/_decoders.py
```

**Priority Types** (P0):
- `datetime`, `date`, `Decimal`, `UUID`, `Enum`
- `bytes`, `Pydantic BaseModel`, `msgspec.Struct`
- `Arrow Table`, `FileObject`

**Acceptance**:
- `VertebraCodec.encode()` handles all P0 types
- Round-trip tests pass
- Litestar type encoders work

---

### Phase 3: SQLAlchemy Integration (Week 2-3)

**Tasks**:
1. DBAPI Module (PEP 249 compliance)
2. Postgres Dialect
3. MySQL Dialect
4. SQLite Dialect
5. Transaction Support
6. Integration Tests

**Start Here**:
```bash
mkdir -p src/py/vertebra/extensions/sqlalchemy/postgres
mkdir -p src/py/vertebra/extensions/sqlalchemy/mysql
mkdir -p src/py/vertebra/extensions/sqlalchemy/sqlite

touch src/py/vertebra/extensions/sqlalchemy/__init__.py
touch src/py/vertebra/extensions/sqlalchemy/_dbapi.py
touch src/py/vertebra/extensions/sqlalchemy/_base.py
```

**DBAPI Structure**:
```python
# _dbapi.py
class Connection:
    def cursor(self) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...

class Cursor:
    def execute(self, query: str, params=None): ...
    def fetchone(self) -> tuple | None: ...
    def fetchall(self) -> list[tuple]: ...
```

**Acceptance**:
- `create_engine("postgresql+vertebra://main")` works
- `create_async_engine("postgresql+vertebra://main")` works
- Transactions work correctly
- Integration tests pass

---

### Phase 4: Pool Health Checks (Week 3)

**Tasks**:
1. Health Check Loop (30s interval)
2. Lifecycle Hooks (`onAcquire`, `onRelease`, `onError`, `onTimeout`)
3. Idle Connection Management (5min timeout)
4. Metrics Enhancement

**Start Here**:
```bash
touch src/ts/src/broker/health.ts
```

**Health Checker Interface**:
```typescript
export interface HealthChecker {
  check(connection: any): Promise<boolean>;
}

export class PostgresHealthChecker implements HealthChecker {
  async check(connection: any): Promise<boolean> {
    try {
      await connection.execute("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }
}
```

**Acceptance**:
- Health checks run periodically
- Unhealthy connections removed
- Idle connections reaped
- Metrics exposed

---

### Phase 5: Advanced Alchemy Integration (Week 4)

**Tasks**:
1. Base Config Classes
2. Dialect-Specific Configs
3. Storage Backend (`VertebraStorageBackend`)
4. Integration Tests

**Start Here**:
```bash
mkdir -p src/py/vertebra/extensions/advanced_alchemy/postgres
mkdir -p src/py/vertebra/extensions/advanced_alchemy/mysql
mkdir -p src/py/vertebra/extensions/advanced_alchemy/sqlite

touch src/py/vertebra/extensions/advanced_alchemy/__init__.py
touch src/py/vertebra/extensions/advanced_alchemy/_config.py
touch src/py/vertebra/extensions/advanced_alchemy/_storage.py
```

**Config Pattern**:
```python
from advanced_alchemy.config import AsyncAlchemyConfig
from sqlalchemy.ext.asyncio import create_async_engine

class VertebraAsyncAlchemyConfig(AsyncAlchemyConfig):
    def __init__(self, pool: str, **kwargs):
        self.pool = pool
        engine = create_async_engine(f"postgresql+vertebra://{pool}")
        super().__init__(engine=engine, **kwargs)
```

**Acceptance**:
- Repository pattern works
- Service pattern works
- File Object Storage routes through broker
- Alembic migrations work

---

### Phase 6: Litestar Plugin Overhaul (Week 4)

**Tasks**:
1. Plugin Infrastructure
2. SQLSpec Plugin
3. SQLAlchemy Plugin
4. Advanced Alchemy Plugin
5. Serialization Auto-Registration

**Start Here**:
```bash
mkdir -p src/py/vertebra/extensions/litestar

touch src/py/vertebra/extensions/litestar/__init__.py
touch src/py/vertebra/extensions/litestar/_base.py
touch src/py/vertebra/extensions/litestar/_serialization.py
touch src/py/vertebra/extensions/litestar/sqlspec.py
touch src/py/vertebra/extensions/litestar/sqlalchemy.py
touch src/py/vertebra/extensions/litestar/advanced_alchemy.py
```

**Plugin Pattern**:
```python
from litestar.plugins.sqlspec import SQLSpecPlugin

class VertebraSQLSpecPlugin(SQLSpecPlugin):
    def on_app_init(self, app):
        # Auto-register type encoders/decoders
        from ._serialization import register_vertebra_types
        register_vertebra_types(app)

        super().on_app_init(app)
```

**Acceptance**:
- All three plugins work with Litestar
- DI provides sessions/connections correctly
- Type encoders auto-registered

---

## Testing Strategy

### Unit Tests

Run after each task group:
```bash
# Python tests
make test

# TypeScript tests
make js-test

# Single test
uv run pytest src/py/tests/test_sqlspec_config.py::test_rename -v
cd src/ts && bun test src/broker/health.test.ts
```

### Integration Tests (Container-Based)

Run after each phase:
```bash
# Requires Docker/Podman
uv run pytest src/py/tests -m integration --integration

# Specific database
uv run pytest src/py/tests/test_sqlspec_integration.py -m postgres --integration
```

### Performance Benchmarks

Run after phase completion:
```bash
# Benchmark suite
uv run python benchmarks/bench_sqlspec.py
uv run python benchmarks/bench_sqlalchemy.py

# Target metrics:
# - Throughput: > 10k queries/sec
# - Latency: P99 < 10ms
# - Arrow transport: < 5ms overhead for 10k rows
```

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
make js-type-check

# Run all tests
make test-all
```

### Database Testing

```bash
# Start test databases (Docker Compose)
docker-compose -f tests/docker-compose.yml up -d

# Run integration tests
uv run pytest src/py/tests -m integration --integration

# Stop test databases
docker-compose -f tests/docker-compose.yml down
```

### Debugging IPC

```bash
# Check shared memory
ls -lh /dev/shm/ | grep vertebra

# Clean stale shared memory
rm -rf /dev/shm/vertebra-*

# Monitor broker
cd src/ts && bun run src/broker/index.ts

# Tail broker logs
tail -f /var/log/vertebra/broker.log
```

---

## Success Metrics

### Functional Requirements
- [ ] `VertebraPostgresAsyncConfig` works with SQLSpec
- [ ] `create_engine("postgresql+vertebra://main")` works
- [ ] `create_async_engine("postgresql+vertebra://main")` works
- [ ] Advanced Alchemy Repository/Service patterns work
- [ ] Pool health checks run and report metrics
- [ ] All three Litestar plugins work

### Quality Requirements
- [ ] 0 mypy errors in strict mode
- [ ] Test coverage > 85%
- [ ] All modern type syntax (no `Optional[T]`)

### Performance Requirements
- [ ] Throughput: > 10k req/sec (small queries)
- [ ] Latency: P99 < 10ms (IPC + query)
- [ ] Arrow transport: < 5ms overhead for 10k rows
- [ ] Memory: < 50MB per idle worker

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

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| SQLAlchemy dialect complexity | Start with simple DBAPI, iterate |
| Arrow serialization overhead | Benchmark early, adjust threshold |
| mypyc incompatibility | Test both variants in CI |
| Storage backend complexity | Start with local filesystem, iterate |
| Transaction support complexity | Implement minimal viable first |

---

## Related Specifications

- **PRD-5**: `/home/cody/code/c/vertebra/specs/archive/consolidated/PRD-5-resource-management.md` - Pool fairness
- **Remaining Work**: `/home/cody/code/c/vertebra/specs/archive/consolidated/prd-remaining-work.md` - 1.0 tasks
- **Broker Spec**: `/home/cody/code/c/vertebra/specs/archive/shared-memory-broker/` - Broker architecture

---

## Next Steps

1. **Choose Phase**: Start with Phase 1 (SQLSpec) if stabilizing existing work, or Phase 2 (Serialization) if building foundation
2. **Create Branch**: `git checkout -b feat/database-phase-N`
3. **Read PRD Section**: Review relevant phase in PRD
4. **Check Tasks**: Use tasks.md for detailed steps
5. **Write Tests First**: TDD for all new code
6. **Iterate**: Complete task groups, run tests, commit

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation

**Quick Resume Command**:
```bash
cd /home/cody/code/c/vertebra
cat specs/active/database/recovery.md  # Read this file
cat specs/active/database/prd.md      # Read full PRD
cat specs/active/database/tasks.md    # Check task breakdown
```
