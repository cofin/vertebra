# Flow Spec: Clean Slate Wipe
## Goal
Archive the current POC codebase to a scratch directory to prepare for the 0.1.0 "Ghost Framework" rebuild.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Archive Current Code
- [ ] Task 0: Open GitHub issue for "Project Clean Slate Archival".
- [ ] Task 1: Create `scratch/` at the root.
- [ ] Task 2: Move `src/rs/` to `scratch/src-rs-archive/`.
- [ ] Task 3: Move `src/py/` to `scratch/src-py-archive/`.
- [ ] Task 4: Move `tools/` and `benchmarks/` to `scratch/`.
- [ ] Task 5: Move `Makefile`, `pyproject.toml`, and `uv.lock` to `scratch/`.
- [ ] Task 6: Ensure `.agent/`, `.beads/`, `.github/`, and `.gitignore` are PRESERVED.
