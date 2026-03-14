# Flow Spec: Scaffold Base 0.1.0
## Goal
Establish the foundational project structure, toolchains, and basic configurations for Vertebra 0.1.0.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Toolchain Init
- [ ] Task 0: Open GitHub issue for "Project Scaffolding and Toolchain Setup".
- [ ] Task 1: Initialize root `pyproject.toml` targeting Python 3.12+. Set project name to `vertebra`, version to `0.1.0`.
- [ ] Task 2: Configure `uv` workspace members and initial dependencies (`msgspec`, `anyio`).
- [ ] Task 3: Create `src/rs/Cargo.toml` workspace with members: `vertebra-core`, `vertebra-http`, `vertebra-py`.
- [ ] Task 4: Initialize the 3 Rust crates as empty libraries.

### Phase 2: Build Pipeline
- [ ] Task 5: Create a new `Makefile` with `install`, `rs-build`, `py-build`, `test-all`, `lint-all` targets.
- [ ] Task 6: Add minimal `__init__.py` in `src/py/vertebra` to establish the package.
