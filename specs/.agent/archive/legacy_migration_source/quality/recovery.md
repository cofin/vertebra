# Recovery Guide: Infrastructure & Quality Consolidation

**PRD**: `/home/cody/code/c/vertebra/specs/active/quality/prd.md`
**Tasks**: `/home/cody/code/c/vertebra/specs/active/quality/tasks.md`
**Research**: `/home/cody/code/c/vertebra/specs/active/quality/research/plan.md`

---

## Quick Context

This is a **consolidated PRD** combining all Infrastructure & Quality work for Vertebra 1.0. It covers:

1. **Test Deadlock Fix** (P0) - Unblock development by fixing hanging tests
2. **Codebase Consolidation Phase 4** (P1) - File renames and documentation
3. **API Rename** (P0) - `Broker*` to `Vertebra*` prefix
4. **Type Audit** (P1) - Fix 120+ mypy errors

**Total Duration**: 2-3 weeks
**Total Tasks**: 45+
**Critical Path**: Phase 1 -> Phase 2 -> Phase 3 -> Phase 4

---

## Current State

### Completed
- Test helpers module created (`src/py/tests/helpers/`)
- pytest-timeout configured
- Basic cleanup fixtures in conftest.py

### In Progress
- Test file updates (using unique names and cleanup)

### Not Started
- TypeScript file renames
- CLAUDE.md updates
- API rename (Broker* -> Vertebra*)
- Type audit (mypy strict mode)

---

## Key Files

### Test Infrastructure
- `/home/cody/code/c/vertebra/pyproject.toml` - pytest-timeout config
- `/home/cody/code/c/vertebra/src/py/tests/conftest.py` - Shared fixtures
- `/home/cody/code/c/vertebra/src/py/tests/helpers/__init__.py` - Test helpers
- `/home/cody/code/c/vertebra/src/py/tests/helpers/shm.py` - SHM helpers

### Test Files to Update
- `/home/cody/code/c/vertebra/src/py/tests/test_broker_client.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_broker_pool.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_broker_multi_client.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_asgi_worker.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_ipc_channel.py`
- `/home/cody/code/c/vertebra/src/py/tests/test_ipc_integration.py`

### SQLSpec Files (API Rename)
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_config.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/postgres/`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/mysql/`
- `/home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/sqlite/`

### TypeScript Files (Consolidation)
- `/home/cody/code/c/vertebra/src/ts/src/server/worker-pool.ts` -> `worker-connections.ts`
- `/home/cody/code/c/vertebra/src/ts/src/server/worker-manager.ts` -> `process-manager.ts`
- `/home/cody/code/c/vertebra/src/ts/src/server/index.ts`

---

## Implementation Order

### Phase 1: Test Deadlock Fix (START HERE if tests hang)

**Immediate Actions**:
```bash
# 1. Clean stale shared memory
rm -rf /dev/shm/test-* /dev/shm/*vertebra*

# 2. Verify cleanup
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l  # Should be 0

# 3. Run single test to verify
uv run pytest src/py/tests/test_broker_client.py -v --timeout=30

# 4. If still hanging, check for hardcoded names
grep -n "vertebra-broker-test" src/py/tests/
```

**Pattern for Updating Tests**:
```python
from tests.helpers.shm import unique_shm_name, tracked_shm_cleanup

@pytest.mark.anyio
async def test_example() -> None:
    name = unique_shm_name("vertebra-broker-test")

    with tracked_shm_cleanup() as created_names:
        # Create shared memory with unique name
        ring = ShmRing.create(ShmRingConfig(
            name=f"/{name}.control",
            slot_size=4096,
            slots=64
        ))
        created_names.append(f"/{name}.control")

        # Test code here
        # Cleanup happens automatically
```

**Verification**:
```bash
# Run full test suite
time make test  # Should complete in < 60 seconds

# Run 5 times consecutively
for i in {1..5}; do make test || exit 1; done

# Check for stale SHM
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l  # Should be 0
```

---

### Phase 2: Codebase Consolidation (after tests pass)

**File Renames**:
```bash
cd /home/cody/code/c/vertebra

# Rename files
git mv src/ts/src/server/worker-pool.ts src/ts/src/server/worker-connections.ts
git mv src/ts/src/server/worker-manager.ts src/ts/src/server/process-manager.ts

# Update imports in index.ts
# Edit src/ts/src/server/index.ts

# Verify
make js-test
make js-type-check
```

**CLAUDE.md Update Checklist**:
- [ ] Update Directory Layout with new file names
- [ ] Document storage module purpose
- [ ] Document resources/broker.py purpose
- [ ] Add naming conventions section

---

### Phase 3: API Rename (after consolidation)

**Rename Strategy**:
```bash
# Find all occurrences
grep -r "BrokerSyncConfigBase" src/py/vertebra/extensions/sqlspec/
grep -r "BrokerAsyncConfigBase" src/py/vertebra/extensions/sqlspec/
grep -r "BrokerProxySyncDriver" src/py/vertebra/extensions/sqlspec/
grep -r "BrokerProxyAsyncDriver" src/py/vertebra/extensions/sqlspec/

# Rename base classes first
# Then dialect classes
# Then update tests
# Then verify
```

**Verification**:
```bash
# No Broker* in public APIs
grep -r "BrokerPostgres\|BrokerMysql\|BrokerSqlite" src/py/vertebra/extensions/sqlspec/
# Should return nothing

# BrokerClient should remain
grep -r "BrokerClient" src/py/vertebra/resources/broker.py
# Should return BrokerClient (internal API)

# Tests pass
make test
```

---

### Phase 4: Type Audit (after API rename)

**Module-by-Module Approach**:
```bash
# Start with core modules
mypy --strict src/py/vertebra/_internal/shm.py
mypy --strict src/py/vertebra/_internal/serialization.py

# Then resources
mypy --strict src/py/vertebra/resources/

# Then extensions
mypy --strict src/py/vertebra/extensions/sqlspec/

# Then server
mypy --strict src/py/vertebra/server/
```

**Type Syntax Modernization**:
```python
# Before (Python 3.9 syntax)
from typing import Optional, Union, List, Dict

def foo(x: Optional[str]) -> Union[int, str]:
    items: List[str] = []
    mapping: Dict[str, int] = {}

# After (Python 3.12+ syntax)
def foo(x: str | None) -> int | str:
    items: list[str] = []
    mapping: dict[str, int] = {}
```

---

## Common Commands

### Testing
```bash
# Full test suite
make test

# Single test
uv run pytest src/py/tests/test_broker_client.py -v

# With timeout
uv run pytest src/py/tests -v --timeout=30

# Parallel execution
uv run pytest src/py/tests -n 4 --timeout=30
```

### Type Checking
```bash
# Python
mypy --strict src/py/vertebra/

# TypeScript
make js-type-check
```

### Cleanup
```bash
# Clean stale shared memory
rm -rf /dev/shm/test-* /dev/shm/*vertebra*

# Verify
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l
```

### Linting
```bash
# Format all
make lint

# Full lint + type checks
make lint-all
```

---

## Success Metrics

### Phase 1 (Test Deadlock Fix)
- [ ] `make test` completes in < 60 seconds
- [ ] All 44+ tests pass
- [ ] No stale `/dev/shm/` files after run
- [ ] 5 consecutive runs succeed
- [ ] Parallel execution works

### Phase 2 (Codebase Consolidation)
- [ ] TypeScript files renamed
- [ ] `make js-test` passes
- [ ] `make js-type-check` passes
- [ ] CLAUDE.md updated

### Phase 3 (API Rename)
- [ ] All `Broker*` -> `Vertebra*` in public APIs
- [ ] `BrokerClient` unchanged in `resources/broker.py`
- [ ] All tests pass
- [ ] No mypy errors introduced

### Phase 4 (Type Audit)
- [ ] 0 mypy errors in strict mode
- [ ] No `Optional[T]` remaining
- [ ] No `Union[A, B]` remaining
- [ ] TypeScript strict mode enabled
- [ ] < 10 justified `any` types

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Test fixes break existing tests | Incremental changes, verify after each file |
| API rename breaks user code | Pre-1.0, no users yet; document in migration guide |
| Type audit uncovers runtime bugs | Fix bugs discovered; consider it a quality win |
| TypeScript strict mode too restrictive | Document justified `any` usage with comments |

---

## Related Specifications

- **Database**: `/home/cody/code/c/vertebra/specs/active/database/` - ORM integration
- **Server**: `/home/cody/code/c/vertebra/specs/active/server/` - Server readiness

### Archived References
- **Test Deadlock Fix (original)**: `/home/cody/code/c/vertebra/specs/archive/test-deadlock-fix/`
- **Codebase Consolidation (original)**: `/home/cody/code/c/vertebra/specs/archive/codebase-consolidation/`

---

## Next Steps

1. **Check Current State**: Are tests hanging? Start with Phase 1
2. **Read Tasks**: Use `tasks.md` for detailed steps
3. **Create Branch**: `git checkout -b feat/quality-phase-N`
4. **Implement**: Follow task groups, run tests after each
5. **Verify**: Run verification commands at phase end

---

**Quick Resume Command**:
```bash
cd /home/cody/code/c/vertebra
cat specs/active/quality/recovery.md  # Read this file
cat specs/active/quality/prd.md       # Read full PRD
cat specs/active/quality/tasks.md     # Check task breakdown
```

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation
