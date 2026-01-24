# Distribution & Packaging - Consolidated Specification

**Status**: Active - Critical Path to 1.0
**Priority**: P1
**Date**: 2026-01-19
**Target**: Vertebra 1.0 Release

---

## Overview

This directory contains the **consolidated specification** for all Distribution & Packaging work in Vertebra 1.0. It combines scaffolding, build tooling, single-file distribution, and cross-platform wheel publishing.

**Key Principle**: Zero-build installation via `pip install vertebra`, with optional single-file distribution via `vertebra build --one-file`.

---

## Documents

### [PRD: Distribution & Packaging](./prd.md)
**Word Count**: 7,000+ words

Comprehensive product requirements document covering:
- Zero-build installation (pip install delivers complete framework)
- Project scaffolding (`vertebra init`)
- Native Python embedding (pyembed crate)
- Bun compilation (`Bun.build({ compile: true })`)
- Rust supervisor (GIL-free process management)
- Unified build command (`vertebra build --one-file`)
- Cross-platform wheels (Linux/macOS/Windows)
- Optimization profiles (modern/baseline/native)

**Sections**:
1. Executive Summary
2. Problem Statement
3. Goals and Non-Goals
4. User Stories
5. Technical Specification
6. Implementation Phases
7. Testing Strategy
8. API Reference
9. Consensus Review Findings
10. Success Metrics

---

### [Tasks Breakdown](./tasks.md)
**Total Tasks**: 42+
**Duration**: 4-6 weeks

Detailed task breakdown for each phase:
- **Phase 1**: Rust Supervisor (Week 1)
  - Task Group 1.1: Supervisor Implementation (7 tasks)

- **Phase 2**: Bun Compilation (Week 1-2)
  - Task Group 2.1: Build Script (5 tasks)

- **Phase 3**: Project Scaffolding (Week 2)
  - Task Group 3.1: Templates (7 tasks)

- **Phase 4**: Build Command (Week 2-3)
  - Task Group 4.1: Builder Module (8 tasks)

- **Phase 5**: pyembed Integration (Week 3-4)
  - Task Group 5.1: Embedded Python (5 tasks)

- **Phase 6**: CI/CD Pipeline (Week 4)
  - Task Group 6.1: Release Workflow (6 tasks)

- **Phase 7**: npm Package (Week 5, Optional)
  - Task Group 7.1: TypeScript Package (4 tasks)

---

### [Research Plan](./research/plan.md)
**Word Count**: 2,000+ words

Comprehensive research covering:
1. **Existing Codebase Analysis**
   - Current server implementation
   - CLI structure
   - Build tooling

2. **Best Practices Research**
   - pyembed/PyOxidizer patterns
   - Bun.build() compilation
   - python-build-standalone integration
   - cibuildwheel patterns

3. **Platform Considerations**
   - Cross-platform wheel building
   - Code signing (macOS/Windows)
   - glibc compatibility

4. **Performance Considerations**
   - Binary size targets
   - Startup time optimization

---

### [Recovery Guide](./recovery.md)

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
cat specs/active/distribution/recovery.md  # Read this file
cat specs/active/distribution/prd.md      # Read full PRD
cat specs/active/distribution/tasks.md    # Check task breakdown
```

---

## Quick Start

### 1. Read the PRD
```bash
cat specs/active/distribution/prd.md
```

### 2. Choose Your Phase

**Starting fresh?** -> Phase 1 (Rust Supervisor) - builds foundation

**Working on scaffolding?** -> Phase 3 (Project Scaffolding) - independent

**Working on builds?** -> Phase 4 (Build Command) - depends on Phase 1-2

### 3. Check Tasks
```bash
cat specs/active/distribution/tasks.md
```

### 4. Create Branch
```bash
git checkout -b feat/distribution-phase-N
```

### 5. Start Implementation
Follow task groups in `tasks.md`, run tests after each group.

---

## Architecture Summary

### Distribution Flow
```
Developer Machine
    |
    v
vertebra init myproject  --> Project scaffolding
    |
    v
vertebra serve app:app   --> Development server
    |
    v
vertebra build --one-file --> Standalone binary
    |
    v
Production Deployment (single binary, ~80-150MB)
```

### Key Principles

1. **Zero-Build Installation**: `pip install vertebra` delivers complete framework
2. **Split Packages**: `vertebra` (main) + `vertebra-engine` (platform-specific)
3. **Offline Builds**: All builds work without network access
4. **Native APIs**: Use pyembed and Bun.build() (not external tool patching)
5. **GIL-Free Supervision**: Rust supervisor for custom event loop support

---

## File Paths

### Python Implementation

**CLI**:
```
src/py/vertebra/cli/
├── __init__.py         # CLI entry point
├── init.py             # vertebra init command (NEW)
├── build.py            # vertebra build command (NEW)
└── serve.py            # vertebra serve command (existing)
```

**Build Module**:
```
src/py/vertebra/build/
├── __init__.py
├── config.py           # BuildConfig, BuildTarget
├── builder.py          # Builder class
└── standalone.py       # python-build-standalone integration
```

**Templates**:
```
src/py/vertebra/templates/
├── default/
│   ├── pyproject.toml.j2
│   ├── src/py/{{name}}/__init__.py.j2
│   └── src/py/{{name}}/app.py.j2
└── typescript/
    ├── package.json.j2
    └── tsconfig.json.j2
```

### Rust Implementation

**Supervisor**:
```
src/rs/vertebra-py/
├── Cargo.toml          # Add signal-hook, windows crates
├── src/
│   ├── lib.rs          # Module exports
│   ├── supervisor.rs   # Rust supervisor (NEW)
│   └── embedded.rs     # pyembed integration (NEW)
```

### TypeScript Build Tools

**Build Scripts**:
```
tools/
├── build-engine.ts     # Bun.build() wrapper (NEW)
├── build-cli.ts        # CLI for build script (NEW)
└── bundle-standalone.py # Standalone bundler (NEW)
```

### CI/CD

**Workflows**:
```
.github/workflows/
├── ci.yaml             # Existing CI
└── release.yaml        # Release workflow (NEW)
```

---

## Success Metrics

### Functional
- [ ] `pip install vertebra` works without additional tools
- [ ] `vertebra init myproject` creates working project
- [ ] `vertebra build --one-file` produces standalone binary
- [ ] Standalone binary runs offline

### Quality
- [ ] 0 mypy errors in strict mode
- [ ] Test coverage > 85%
- [ ] All platforms supported (Linux/macOS/Windows)

### Performance
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

## Related Specifications

- **Production Readiness**: `/home/cody/code/c/vertebra/specs/active/production-readiness/` - Server, CLI, streaming
- **Database**: `/home/cody/code/c/vertebra/specs/active/database/` - SQLSpec, SQLAlchemy integration
- **Archived**: `/home/cody/code/c/vertebra/specs/archive/build-distribution-experience/` - Original consolidated spec

---

## Status Tracking

### Current Phase
- Phase 1: Rust Supervisor (starting)

### Upcoming Phases
- Phase 2: Bun Compilation
- Phase 3: Project Scaffolding
- Phase 4: Build Command
- Phase 5: pyembed Integration
- Phase 6: CI/CD Pipeline
- Phase 7: npm Package (Optional)

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

### Testing
```bash
# Python tests
make test

# TypeScript tests
make js-test

# Single test
uv run pytest src/py/tests/test_build.py -v
```

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Active Specification
