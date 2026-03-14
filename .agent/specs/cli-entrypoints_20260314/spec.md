# Flow Spec: CLI Entrypoints & Banners
## Goal
Implement the user-facing CLI and branding logic.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: CLI Implementation
- [ ] Task 0: Open GitHub issue for "Vertebra CLI Entrypoints and Commands".
- [ ] Task 1: Implement `src/py/vertebra/cli/__init__.py`. Add `main()` entrypoint.
- [ ] Task 2: Implement `src/py/vertebra/cli/run.py` using `argparse`. Commands: `run`, `build`, `init`.
- [ ] Task 3: Add `src/py/vertebra/cli/config.py` for CLI-level configuration loading.

### Phase 2: Branding
- [ ] Task 4: Implement `src/py/vertebra/cli/banner/renderer.py` for ASCII art rendering.
- [ ] Task 5: Add startup logging hooks to display banner on server boot.
