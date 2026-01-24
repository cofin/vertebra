# Documentation & Adoption - Session Recovery Guide

**PRD**: `/home/cody/code/c/vertebra/specs/active/docs/prd.md`
**Tasks**: `/home/cody/code/c/vertebra/specs/active/docs/tasks.md`
**Status**: Active
**Last Updated**: 2026-01-19

---

## Quick Context

This PRD consolidates all **Documentation & Adoption** work for Vertebra 1.0, covering:
- Integration test suite (90%+ coverage)
- Performance benchmarks vs Granian/Uvicorn/Hypercorn
- 6 example applications
- Migration guides (litestar-vite, ASGI servers)
- Complete API documentation (Python/TypeScript/Rust)
- Developer guides (Getting Started, Type Bridge, Performance, Deployment)

**Success Criteria**: Vertebra 1.0 is fully documented, tested, and ready for developer adoption.

---

## Current State

### Completed
- ✅ Core architecture complete (PRD-0 through PRD-5)
- ✅ Basic functionality working
- ✅ Some architecture documentation exists (`docs/architecture/`)

### In Progress
- 🔧 Integration test infrastructure (pytest-databases configured)
- 🔧 Basic test patterns established

### Not Started
- ❌ Cross-language integration tests
- ❌ Performance benchmarks
- ❌ Example applications
- ❌ Migration guides
- ❌ Complete API documentation
- ❌ Developer guides

---

## Project Structure

```
/home/cody/code/c/vertebra/
├── specs/active/docs/          # THIS PRD
│   ├── prd.md                  # Main specification (9,847 words)
│   ├── tasks.md                # Task breakdown (170+ tasks)
│   ├── recovery.md             # THIS FILE
│   └── research/
│       └── plan.md             # Research notes
├── src/
│   ├── py/
│   │   ├── vertebra/           # Python package
│   │   └── tests/
│   │       ├── integration/    # Integration tests (TO CREATE)
│   │       └── helpers/        # Test utilities
│   ├── ts/
│   │   ├── src/                # TypeScript source
│   │   └── tests/
│   │       ├── integration/    # TS integration tests (TO CREATE)
│   │       └── unit/
│   └── rs/                     # Rust workspace
├── benchmarks/                 # Performance benchmarks (TO CREATE)
│   ├── apps/
│   ├── scripts/
│   └── results/
├── examples/                   # Example applications (TO CREATE)
│   ├── todo-api/
│   ├── blog-platform/
│   ├── ai-tools/
│   ├── realtime-chat/
│   ├── data-pipeline/
│   └── grpc-service/
├── docs/
│   ├── api/                    # API reference (TO ENHANCE)
│   ├── guides/                 # Developer guides (TO CREATE)
│   │   ├── getting-started.md
│   │   ├── type-bridge.md
│   │   ├── performance.md
│   │   └── deployment.md
│   ├── migration/              # Migration guides (TO CREATE)
│   │   ├── from-litestar-vite.md
│   │   └── from-asgi-servers.md
│   └── conf.py                 # Sphinx config (TO CREATE)
└── .github/workflows/
    ├── integration-tests.yml   # CI for integration tests (TO CREATE)
    ├── docs.yml                # CI for documentation (TO CREATE)
    └── benchmarks.yml          # CI for benchmarks (TO CREATE)
```

---

## How to Resume Work

### Option 1: Start from Beginning (Recommended for new contributors)

```bash
# 1. Read the PRD
cat /home/cody/code/c/vertebra/specs/active/docs/prd.md

# 2. Review task breakdown
cat /home/cody/code/c/vertebra/specs/active/docs/tasks.md

# 3. Begin Phase 1: Test Infrastructure
# Start with Task 1.1: Configure pytest-databases
cd /home/cody/code/c/vertebra
cat pyproject.toml | grep pytest-databases  # Already configured

# 4. Create first integration test
mkdir -p src/py/tests/integration
touch src/py/tests/integration/conftest.py
```

### Option 2: Jump to Specific Phase

**Phase 1: Test Infrastructure (Weeks 1-2)**
```bash
# Read Phase 1 in PRD
sed -n '/Phase 1: Test Infrastructure/,/Phase 2/p' specs/active/docs/prd.md

# Check what's configured
cat pyproject.toml | grep -A5 "\[tool.pytest"
ls -la src/py/tests/
```

**Phase 2: Integration Tests (Weeks 3-5)**
```bash
# Read integration test section
sed -n '/### 1. Integration Test Suite/,/### 2. Performance Benchmarks/p' specs/active/docs/prd.md

# Start with Python ↔ TypeScript tests
mkdir -p src/py/tests/integration
```

**Phase 3: Performance Benchmarks (Weeks 6-7)**
```bash
# Read benchmarks section
sed -n '/### 2. Performance Benchmarks/,/### 3. Example Applications/p' specs/active/docs/prd.md

# Create benchmark structure
mkdir -p benchmarks/{apps,scripts,results}
```

**Phase 4: Example Applications (Weeks 8-11)**
```bash
# Read examples section
sed -n '/### 3. Example Applications/,/### 4. Migration Guides/p' specs/active/docs/prd.md

# Start with Todo API
mkdir -p examples/todo-api/{backend,frontend}
```

**Phase 5: Migration Guides (Weeks 12-13)**
```bash
# Read migration section
sed -n '/### 4. Migration Guides/,/### 5. API Documentation/p' specs/active/docs/prd.md

# Create migration docs
mkdir -p docs/migration
```

**Phase 6: API Documentation (Weeks 14-15)**
```bash
# Read API docs section
sed -n '/### 5. API Documentation/,/### 6. Developer Guides/p' specs/active/docs/prd.md

# Set up Sphinx
pip install sphinx sphinx-autobuild sphinx-rtd-theme
```

**Phase 7: Developer Guides (Weeks 16-17)**
```bash
# Read guides section
sed -n '/### 6. Developer Guides/,/## Timeline and Phases/p' specs/active/docs/prd.md

# Create guide structure
mkdir -p docs/guides
```

### Option 3: Focus on Specific Deliverable

**Integration Tests Only**
```bash
# Read full integration test specification
sed -n '/### 1. Integration Test Suite/,/^### 2\./p' specs/active/docs/prd.md > /tmp/integration-tests.md
cat /tmp/integration-tests.md

# Review task checklist
grep -A100 "## Phase 2: Integration Tests" specs/active/docs/tasks.md
```

**Benchmarks Only**
```bash
# Read benchmark specification
sed -n '/### 2. Performance Benchmarks/,/^### 3\./p' specs/active/docs/prd.md > /tmp/benchmarks.md
cat /tmp/benchmarks.md
```

**Examples Only**
```bash
# Read examples specification
sed -n '/### 3. Example Applications/,/^### 4\./p' specs/active/docs/prd.md > /tmp/examples.md
cat /tmp/examples.md
```

---

## Key Files to Reference

### PRD and Planning
- **Main PRD**: `/home/cody/code/c/vertebra/specs/active/docs/prd.md`
- **Task Breakdown**: `/home/cody/code/c/vertebra/specs/active/docs/tasks.md`
- **Research Notes**: `/home/cody/code/c/vertebra/specs/active/docs/research/plan.md`

### Source PRDs (Already Consolidated)
- **PRD-6 Integration**: `/home/cody/code/c/vertebra/specs/archive/consolidated/PRD-6-integration.md`
- **Remaining Work**: `/home/cody/code/c/vertebra/specs/archive/consolidated/prd-remaining-work.md`

### Existing Documentation
- **Architecture**: `/home/cody/code/c/vertebra/docs/architecture/`
- **CLAUDE.md**: `/home/cody/code/c/vertebra/CLAUDE.md`
- **README.md**: `/home/cody/code/c/vertebra/README.md`

### Test Infrastructure
- **conftest.py**: `/home/cody/code/c/vertebra/src/py/tests/conftest.py`
- **Test helpers**: `/home/cody/code/c/vertebra/src/py/tests/helpers/`
- **pyproject.toml**: `/home/cody/code/c/vertebra/pyproject.toml`

---

## Dependencies and Prerequisites

### Already Complete (Can Assume Working)
- ✅ PRD-0: Project setup and tooling
- ✅ PRD-1: Rust server implementation
- ✅ PRD-2: Python (Litestar) integration
- ✅ PRD-3: TypeScript integration
- ✅ PRD-4: Bun-backed drivers
- ✅ PRD-5: Resource management

### Required Tools (Should Be Installed)
- Python 3.12+
- Bun (JavaScript runtime)
- Rust toolchain (via rustup)
- uv (Python package manager)
- Docker or Podman (for integration tests)

### Python Dependencies (Already in pyproject.toml)
```toml
[dependency-groups.test]
pytest>=7.4.4
pytest-cov>=5.0.0
pytest-databases>=0.10.0  # For container management
pytest-timeout>=2.3.1
pytest-xdist>=3.6.1
```

### New Dependencies Needed
```bash
# For WebSocket testing
uv add --group test playwright
uv run playwright install chromium

# For documentation
uv add --group docs sphinx sphinx-autobuild sphinx-rtd-theme

# For TypeScript docs
cd src/ts
bun add -D typedoc typedoc-plugin-markdown
```

---

## Common Commands

### Testing
```bash
# Run all tests
make test

# Run integration tests
uv run pytest src/py/tests/integration/ -m integration --integration -v

# Run specific integration test
uv run pytest src/py/tests/integration/test_api_contract.py -v

# Run with coverage
uv run pytest src/py/tests/integration/ --cov=vertebra --cov-report=html
```

### Benchmarking
```bash
# Run JSON benchmark
./benchmarks/scripts/bench_json.sh vertebra

# Compare all servers
for server in vertebra granian uvicorn hypercorn; do
  ./benchmarks/scripts/bench_json.sh $server
done

# Analyze results
uv run python benchmarks/scripts/analyze.py
```

### Documentation
```bash
# Build Python docs
cd docs && sphinx-build -b html . _build/html

# Live rebuild
cd docs && sphinx-autobuild . _build/html --open-browser

# Build TypeScript docs
cd src/ts && bun run docs

# Check for broken links
uv run python scripts/check_docs.py
```

### Examples
```bash
# Run Todo API example
cd examples/todo-api
docker-compose up

# Run Blog Platform example
cd examples/blog-platform
docker-compose up

# Run AI Tools Server
cd examples/ai-tools
uv run vertebra mcp --transport stdio
```

---

## Success Metrics (How to Verify Progress)

### Integration Testing
```bash
# Check coverage
uv run pytest src/py/tests/integration/ --cov=vertebra --cov-report=term-missing

# Should show:
# - 90%+ coverage for integration points
# - Tests complete in <5 minutes
# - All tests passing
```

### Benchmarks
```bash
# Verify performance targets
cat benchmarks/results/vertebra_json.txt | grep "Requests/sec"
# Should show: >50,000 RPS

cat benchmarks/results/vertebra_json.txt | grep "99.000%"
# Should show: p99 latency <50ms
```

### Examples
```bash
# Verify examples build
for example in examples/*/; do
  cd "$example"
  docker-compose build
  cd -
done

# Should complete without errors
```

### Documentation
```bash
# Check API coverage
uv run python scripts/check_docs.py

# Should report:
# - 100% public APIs documented
# - 0 broken links
```

---

## Next Actions (Immediate Steps)

### If Starting Fresh
1. Read main PRD (`specs/active/docs/prd.md`)
2. Review task breakdown (`specs/active/docs/tasks.md`)
3. Begin Phase 1, Task 1.1: Configure pytest-databases
4. Create first integration test fixture

### If Resuming Work
1. Check latest commit: `git log -1`
2. Check what phase was in progress
3. Review task checklist for that phase
4. Continue from last completed task

### If Blocked
1. Review dependencies section in PRD
2. Check if prerequisite PRDs are complete
3. Verify all tools installed
4. Consult troubleshooting sections in guides

---

## Questions to Ask When Resuming

1. **What phase am I in?** (Check tasks.md for progress markers)
2. **What was I working on?** (Check git log and uncommitted changes)
3. **What's the next task?** (Refer to tasks.md checklist)
4. **Do I have all dependencies?** (Check prerequisites section)
5. **What's the acceptance criteria?** (Each phase has clear criteria in PRD)

---

## Emergency Contacts / Resources

### Documentation
- Main PRD: `specs/active/docs/prd.md`
- Tasks: `specs/active/docs/tasks.md`
- Research: `specs/active/docs/research/plan.md`

### Source Material
- PRD-6: `specs/archive/consolidated/PRD-6-integration.md`
- Remaining Work: `specs/archive/consolidated/prd-remaining-work.md`

### Testing References
- pytest-databases: https://pytest-databases.readthedocs.io/
- Playwright: https://playwright.dev/docs/api/class-websocket
- Testcontainers: https://testcontainers.com/guides/getting-started-with-testcontainers-for-python/

### Benchmarking References
- wrk2: https://github.com/giltene/wrk2
- Granian benchmarks: https://github.com/emmett-framework/granian

### Documentation References
- Sphinx: https://www.sphinx-doc.org/
- TypeDoc: https://typedoc.org/
- rustdoc: https://doc.rust-lang.org/rustdoc/

---

## Timeline Reference

**Total Duration**: 18 weeks

| Phase | Weeks | Deliverable |
|-------|-------|-------------|
| Phase 1 | 1-2 | Test infrastructure ready |
| Phase 2 | 3-5 | 90%+ integration coverage |
| Phase 3 | 6-7 | Benchmarks prove superiority |
| Phase 4 | 8-11 | 6 example apps complete |
| Phase 5 | 12-13 | Migration guides validated |
| Phase 6 | 14-15 | 100% API documentation |
| Phase 7 | 16-17 | Developer guides tested |
| Phase 8 | 18 | Release ready |

**Current Week**: To be determined by reviewing git log and task completion

---

**Last Updated**: 2026-01-19
**Maintained By**: Implementation team
**Status**: Active - Ready for implementation
