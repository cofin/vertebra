# Infrastructure & Quality Consolidation - Consolidated Specification

**Status**: Active - Critical Path to 1.0
**Priority**: P0
**Date**: 2026-01-19
**Target**: Vertebra 1.0 Release

---

## Overview

This directory contains the **consolidated specification** for all Infrastructure & Quality work in Vertebra 1.0. It combines test fixes, codebase consolidation, API renaming, and type auditing.

**Key Principle**: All quality work must complete before 1.0 release to ensure stable, maintainable codebase.

---

## Documents

### [PRD: Infrastructure & Quality Consolidation](./prd.md)
**Word Count**: 5,000+ words

Comprehensive product requirements document covering:
- Test Deadlock Fix (P0) - Unblock development
- Codebase Consolidation Phase 4 (P1) - File renames and documentation
- API Rename (P0) - `Broker*` to `Vertebra*` prefix
- Type Audit (P1) - Fix mypy errors and modernize syntax

**Sections**:
1. Executive Summary
2. Problem Statement
3. Acceptance Criteria
4. Technical Approach
5. Implementation Phases
6. Testing Strategy
7. Risk Assessment
8. Migration Guide

### Related Specs
- `specs/active/test-consolidation-and-db-fixtures/` tracks the test taxonomy reorg and pytest-databases removal, and should be treated as complementary to Phase 1 test stability work here.

---

### [Tasks Breakdown](./tasks.md)
**Total Tasks**: 45+
**Duration**: 2-3 weeks

Detailed task breakdown for each phase:
- **Phase 1**: Test Deadlock Fix (2-3 days)
  - Immediate unblock tasks
  - Test helpers module
  - Test file updates
  - Verification

- **Phase 2**: Codebase Consolidation Phase 4 (1-2 days)
  - TypeScript file renames
  - CLAUDE.md updates
  - Module docstrings

- **Phase 3**: API Rename (2-3 days)
  - Base class renames
  - Dialect-specific renames
  - Test updates

- **Phase 4**: Type Audit (4-6 days)
  - Python module audits
  - Type syntax modernization
  - TypeScript strict mode

---

### [Research Plan](./research/plan.md)
**Word Count**: 1,500+ words

Research covering:
1. **Test Deadlock Analysis**
   - Root cause identification
   - Shared memory cleanup patterns
   - pytest-timeout integration

2. **API Naming Patterns**
   - Public vs internal API conventions
   - SQLSpec naming patterns

3. **Type Modernization**
   - Python 3.12+ syntax
   - TypeScript strict mode

---

### [Recovery Guide](./recovery.md)

Session resumption guide covering:
- Quick context and current state
- Key files and locations
- Implementation order
- Common commands
- Success metrics

**Quick Resume**:
```bash
cd /home/cody/code/c/vertebra
cat specs/active/quality/recovery.md  # Read this file
cat specs/active/quality/prd.md       # Read full PRD
cat specs/active/quality/tasks.md     # Check task breakdown
```

---

## Quick Start

### 1. Read the PRD
```bash
cat specs/active/quality/prd.md
```

### 2. Choose Your Phase

**Test suite hanging?** -> Phase 1 (Test Deadlock Fix) - critical unblock

**Continuing consolidation?** -> Phase 2 (Codebase Consolidation) - file renames

**Working on APIs?** -> Phase 3 (API Rename) - public API stabilization

**Type safety?** -> Phase 4 (Type Audit) - mypy fixes

### 3. Check Tasks
```bash
cat specs/active/quality/tasks.md
```

### 4. Create Branch
```bash
git checkout -b feat/quality-phase-N
```

### 5. Start Implementation
Follow task groups in `tasks.md`, run tests after each group.

---

## Success Metrics

### Phase 1 (Test Deadlock Fix)
- [ ] `make test` completes within 60 seconds
- [ ] All 44+ tests pass
- [ ] No stale `/dev/shm/` files after test run
- [ ] 5 consecutive test runs succeed

### Phase 2 (Codebase Consolidation)
- [ ] TypeScript files renamed consistently
- [ ] CLAUDE.md reflects actual structure
- [ ] Module docstrings added

### Phase 3 (API Rename)
- [ ] All public SQLSpec APIs use `Vertebra*` prefix
- [ ] Internal `BrokerClient` unchanged
- [ ] All tests pass with new names

### Phase 4 (Type Audit)
- [ ] 0 mypy errors in strict mode
- [ ] All modern Python 3.12+ syntax
- [ ] TypeScript strict mode enabled

---

## File Paths

### Python Tests
```
src/py/tests/
  ├── conftest.py              # Shared fixtures
  ├── helpers/                 # Test helpers (NEW)
  │   ├── __init__.py
  │   └── shm.py              # Shared memory helpers
  ├── test_broker_client.py    # Broker tests
  ├── test_broker_pool.py
  ├── test_broker_multi_client.py
  ├── test_asgi_worker.py      # ASGI tests
  ├── test_ipc_channel.py      # IPC tests
  └── test_ipc_integration.py
```

### SQLSpec (API Rename)
```
src/py/vertebra/extensions/sqlspec/
  ├── _config.py               # Base classes (RENAME)
  ├── _driver.py               # Driver classes (RENAME)
  ├── postgres/                # Postgres dialect (RENAME)
  ├── mysql/                   # MySQL dialect (RENAME)
  └── sqlite/                  # SQLite dialect (RENAME)
```

### TypeScript (Consolidation)
```
src/ts/src/server/
  ├── index.ts                 # Main entry point
  ├── worker-connections.ts    # IPC connection pool (RENAMED)
  ├── process-manager.ts       # Process lifecycle (RENAMED)
  └── config.ts                # Configuration
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

# Run all tests
make test-all
```

### Testing
```bash
# Python tests
make test

# TypeScript tests
make js-test

# Single Python test
uv run pytest src/py/tests/test_broker_client.py -v

# Verify no stale shared memory
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l
```

### Cleanup
```bash
# Clean stale shared memory
rm -rf /dev/shm/test-* /dev/shm/*vertebra*
```

---

## Related Specifications

- **Database**: `/home/cody/code/c/vertebra/specs/active/database/` - ORM integration
- **Server**: `/home/cody/code/c/vertebra/specs/active/server/` - Server readiness
- **Distribution**: `/home/cody/code/c/vertebra/specs/active/distribution/` - Packaging

### Archived References
- **Test Deadlock Fix (original)**: `/home/cody/code/c/vertebra/specs/archive/test-deadlock-fix/`
- **Codebase Consolidation (original)**: `/home/cody/code/c/vertebra/specs/archive/codebase-consolidation/`

---

## Contact

For questions or clarifications, refer to:
- **PRD**: Detailed requirements and architecture
- **Tasks**: Step-by-step implementation guide
- **Research**: Background and analysis
- **Recovery**: Session resumption guide

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Active Specification
