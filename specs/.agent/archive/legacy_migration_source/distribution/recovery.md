# Recovery Guide: Distribution & Packaging

**PRD**: `/home/cody/code/c/vertebra/specs/active/distribution/prd.md`
**Tasks**: `/home/cody/code/c/vertebra/specs/active/distribution/tasks.md`
**Research**: `/home/cody/code/c/vertebra/specs/active/distribution/research/plan.md`

---

## Quick Context

This is a **consolidated PRD** for all Distribution & Packaging work for Vertebra 1.0. It covers:

1. **Rust Supervisor** (GIL-free process management)
2. **Bun Engine Compilation** (Bun.build() for standalone executables)
3. **Project Scaffolding** (`vertebra init` command)
4. **Unified Build Command** (`vertebra build --one-file`)
5. **pyembed Integration** (embedded Python runtime)
6. **CI/CD Pipeline** (cross-platform wheels, PyPI publishing)
7. **npm Package** (optional TypeScript integration)

**Total Duration**: 4-6 weeks
**Total Tasks**: 42+
**Critical Path**: Phase 1 -> Phase 2 -> Phase 4 -> Phase 5 -> Phase 6

---

## Current State

### Completed
- Core IPC primitives (Rust C ABI)
- Bun broker operational
- Basic CLI structure exists
- Server implementation works

### In Progress
- Production readiness improvements (parallel work)

### Not Started
- Rust supervisor
- Bun.build() integration
- Project scaffolding
- Unified build command
- pyembed integration
- Cross-platform wheels
- npm package

---

## Key Files

### Python Implementation

**CLI** (existing, needs extension):
- `/home/cody/code/c/vertebra/src/py/vertebra/cli.py`

**Build Module** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/build/` (NEW)
- `/home/cody/code/c/vertebra/src/py/vertebra/cli/init.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/vertebra/cli/build.py` (NEW)

**Templates** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/templates/` (NEW)

**Supervisor** (update existing):
- `/home/cody/code/c/vertebra/src/py/vertebra/supervisor.py`

### Rust Implementation

**Supervisor** (new module):
- `/home/cody/code/c/vertebra/src/rs/vertebra-py/src/supervisor.rs` (NEW)
- `/home/cody/code/c/vertebra/src/rs/vertebra-py/src/embedded.rs` (NEW)
- `/home/cody/code/c/vertebra/src/rs/vertebra-py/Cargo.toml` (update)

### TypeScript Build Tools

**Build Scripts** (new):
- `/home/cody/code/c/vertebra/tools/build-engine.ts` (NEW)
- `/home/cody/code/c/vertebra/tools/build-cli.ts` (NEW)
- `/home/cody/code/c/vertebra/tools/bundle-standalone.py` (NEW)

### CI/CD

**Workflows**:
- `/home/cody/code/c/vertebra/.github/workflows/release.yaml` (NEW)

---

## Architecture Principles

### 1. Zero-Build Installation
`pip install vertebra` delivers complete framework. No additional tools required.

### 2. Split Packages
- `vertebra` (main package, ~5MB)
- `vertebra-engine` (platform-specific Bun binary, ~80MB)

### 3. Build vs Runtime Config
- **Build config**: `pyproject.toml` `[tool.vertebra]` section
- **Runtime config**: Python code (`app.py`)

### 4. GIL-Free Supervision
Rust supervisor handles process management without GIL contention. Supports uvloop/rloop.

### 5. Native API Integration
Use pyembed and Bun.build() directly (not PyApp patching).

### 6. Offline Builds
All builds work without network access after dependencies cached.

---

## Implementation Order

### Phase 1: Rust Supervisor (Week 1)

**Tasks**:
1. Create supervisor module in Rust
2. Implement process spawning with GIL release
3. Implement Unix signal handling
4. Implement Windows process management
5. Update Python supervisor wrapper
6. Test with uvloop/rloop

**Start Here**:
```bash
cd /home/cody/code/c/vertebra
git checkout -b feat/distribution-rust-supervisor

# Create supervisor module
touch src/rs/vertebra-py/src/supervisor.rs

# Update Cargo.toml with dependencies
code src/rs/vertebra-py/Cargo.toml
```

**Acceptance**:
- Supervisor spawns Bun engine and workers
- GIL released during blocking operations
- uvloop/rloop compatibility verified
- Works on Linux, macOS, Windows

---

### Phase 2: Bun Compilation (Week 1-2)

**Tasks**:
1. Create build-engine.ts script
2. Implement target mapping
3. Add bytecode/minification options
4. Create CLI wrapper
5. Add to package scripts

**Start Here**:
```bash
mkdir -p tools
touch tools/build-engine.ts
touch tools/build-cli.ts
```

**Acceptance**:
- All platform targets build successfully
- Baseline variants work on older CPUs
- Binary size ~80MB per platform

---

### Phase 3: Project Scaffolding (Week 2)

**Tasks**:
1. Create template directory
2. Create pyproject.toml template
3. Create Python package templates
4. Create TypeScript templates (optional)
5. Implement init command
6. Register CLI command
7. Add tests

**Start Here**:
```bash
mkdir -p src/py/vertebra/templates/default
mkdir -p src/py/vertebra/cli
touch src/py/vertebra/cli/__init__.py
touch src/py/vertebra/cli/init.py
```

**Acceptance**:
- `vertebra init myproject` creates working project
- All package managers supported
- TypeScript integration works

---

### Phase 4: Build Command (Week 2-3)

**Tasks**:
1. Create builder module structure
2. Implement BuildConfig
3. Implement type generation
4. Implement engine compilation
5. Implement wheel building
6. Implement one-file build
7. Create build CLI command
8. Add tests

**Start Here**:
```bash
mkdir -p src/py/vertebra/build
touch src/py/vertebra/build/__init__.py
touch src/py/vertebra/build/config.py
touch src/py/vertebra/build/builder.py
touch src/py/vertebra/cli/build.py
```

**Acceptance**:
- `vertebra build` succeeds
- `vertebra build --one-file` produces working binary
- Cross-platform targets work

---

### Phase 5: pyembed Integration (Week 3-4)

**Tasks**:
1. Add pyembed dependency
2. Implement embedded Python runner
3. Create standalone binary builder
4. Implement resource layout
5. Test offline operation

**Start Here**:
```bash
# Update Cargo.toml
code src/rs/vertebra-py/Cargo.toml

# Create embedded module
touch src/rs/vertebra-py/src/embedded.rs
touch tools/bundle-standalone.py
```

**Acceptance**:
- Standalone binary includes Python runtime
- No external Python required
- Offline operation verified
- Size under 150MB

---

### Phase 6: CI/CD Pipeline (Week 4)

**Tasks**:
1. Create wheel build workflow
2. Add cross-platform matrix
3. Implement offline testing
4. Configure PyPI publishing
5. Add npm publishing (optional)
6. Add code signing

**Start Here**:
```bash
touch .github/workflows/release.yaml
```

**Acceptance**:
- All platform wheels build in CI
- PyPI publishing works on tag
- macOS binaries signed

---

## Testing Strategy

### Unit Tests

Run after each task group:
```bash
# Python tests
make test

# Rust tests
make rs-test

# TypeScript tests
make js-test

# Single test
uv run pytest src/py/tests/test_build.py -v
```

### Integration Tests

Run after each phase:
```bash
# Test scaffolding
vertebra init testproject --no-interactive
cd testproject && uv run vertebra serve --help

# Test build
vertebra build --one-file --output dist/myapp
./dist/myapp --version
```

### Offline Verification

```bash
# Test in network-isolated environment
unshare --net -- ./dist/myapp --version
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
make lint-all

# Run all tests
make test-all
```

### Building

```bash
# Build engine for current platform
cd tools && bun run build-engine.ts

# Build wheel
python -m build

# Build standalone binary
vertebra build --one-file --output dist/myapp
```

### Debugging

```bash
# Check Rust compilation
cd src/rs && cargo build --workspace

# Check Bun build
cd src/ts && bun build --compile src/server/index.ts

# Test supervisor
uv run python -c "from vertebra._vertebra import Supervisor; print(Supervisor)"
```

---

## Success Metrics

### Functional Requirements
- [ ] `pip install vertebra` works without additional tools
- [ ] `vertebra init myproject` creates working project
- [ ] `vertebra build --one-file` produces standalone binary
- [ ] Standalone binary runs offline
- [ ] uvloop/rloop compatibility

### Quality Requirements
- [ ] 0 mypy errors in strict mode
- [ ] Test coverage > 85%
- [ ] All platforms supported

### Performance Requirements
- [ ] `pip install vertebra` < 30s
- [ ] `vertebra init` < 5s
- [ ] `vertebra build --one-file` < 60s
- [ ] Standalone binary size < 150MB

---

## Dependencies

**Core** (always installed):
- `click >= 8.0` - CLI framework
- `jinja2 >= 3.0` - Template engine

**Build** (optional):
- `pyembed >= 0.26` - Python embedding
- `cibuildwheel >= 2.0` - Wheel building (CI only)

**External Tools**:
- Bun (for engine compilation)
- Rust (for supervisor compilation)

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| pyembed API changes | Pin version, monitor releases |
| Bun.build() API changes | Pin Bun version in CI |
| Large wheel size | Split into vertebra + vertebra-engine |
| Cross-compilation failures | CI-only cross-compilation |
| uvloop compatibility | GIL-free Rust supervisor |
| Windows orphan processes | Job objects for process management |

---

## Related Specifications

- **Production Readiness**: `/home/cody/code/c/vertebra/specs/active/production-readiness/`
- **Database**: `/home/cody/code/c/vertebra/specs/active/database/`
- **Archived Spec**: `/home/cody/code/c/vertebra/specs/archive/build-distribution-experience/`

---

## Next Steps

1. **Choose Phase**: Start with Phase 1 (Rust Supervisor)
2. **Create Branch**: `git checkout -b feat/distribution-phase-1`
3. **Read PRD Section**: Review Phase 1 in PRD
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
cat specs/active/distribution/recovery.md  # Read this file
cat specs/active/distribution/prd.md      # Read full PRD
cat specs/active/distribution/tasks.md    # Check task breakdown
```
