# Recovery Guide for CLI Polish

## Quick Resume

**Last Phase:** Planning Complete
**Current Status:** PRD and tasks created, ready for implementation
**PRD Location:** `specs/active/cli-polish/`

## Intelligence Context

**Complexity:** Medium-Complex (13 tasks across 6 phases)
**Focus:** Rich styling for all CLI commands (no Textual TUI)

**Stack:**
- Python 3.12+ with rich-click
- Rust IPC via PyO3 (vertebra._vertebra)
- Bun TypeScript server
- Make-based build system

**Commands:**
- Test: `make test` or `uv run pytest src/py/tests`
- Lint: `make lint` or `make lint-all`
- Build: `make install` or `maturin develop`
- Run CLI: `uv run vertebra --help`

**Documentation:**
- System: None (no Sphinx/MkDocs configured)
- Location: `README.md`, `CLAUDE.md`
- Skills: N/A

**Similar implementations:**
- `/home/cody/code/g/dma/accelerator/src/py/dma/cli/config.py`
- `/home/cody/code/g/dma/accelerator/src/py/dma/cli/errors.py`
- `/home/cody/code/g/dma/accelerator/src/py/dma/cli/commands/manage.py`

**Patterns being followed:**
- Centralized theme definitions (dataclass with color constants)
- Error handling with CLIError class and hints
- Reusable UI components (section_header, key_value, etc.)
- Progressive enhancement (Rich for output, Textual for TUI)

## State Summary

### Completed
- [x] Phase 0: Project discovery and analysis
- [x] Phase 1: Complexity assessment (Complex - 17 tasks)
- [x] Phase 2: Pattern recognition from accelerator project
- [x] Phase 3: Workspace creation (`specs/active/cli-polish/`)
- [x] Phase 4: Analysis using sequential thinking
- [x] Phase 5: Research documentation (2276 words)
- [x] Phase 6: PRD creation (3614 words)
- [x] Phase 7: Task breakdown (17 tasks)
- [x] Phase 8: Recovery guide creation
- [x] Phase 9: Git verification (no source code modified)

### Pending
- [ ] Implementation of all 17 tasks
- [ ] Testing across terminal types
- [ ] Documentation updates

## Workspace Contents

```
specs/active/cli-polish/
├── prd.md                    # Main PRD (3614 words)
├── tasks.md                  # Implementation tasks (17 tasks)
├── recovery.md               # This file
├── research/
│   ├── analysis.md           # Technical analysis
│   └── plan.md               # Research documentation (2276 words)
└── tmp/                      # Temporary working files
```

## Key Files to Modify

### New Files (to be created)
1. `src/py/vertebra/cli/theme.py` - Color and icon definitions
2. `src/py/vertebra/cli/config.py` - rich-click configuration
3. `src/py/vertebra/cli/errors.py` - Error handling classes
4. `src/py/vertebra/cli/components.py` - Reusable UI components
5. `src/py/vertebra/cli/tui/__init__.py` - TUI package init
6. `src/py/vertebra/cli/tui/app.py` - Textual application
7. `src/py/vertebra/cli/tui/widgets.py` - Custom widgets

### Files to Modify
1. `src/py/vertebra/cli/__init__.py` - Import config
2. `src/py/vertebra/cli/serve.py` - Add --tui, use components
3. `src/py/vertebra/cli/build.py` - Add progress bar
4. `src/py/vertebra/cli/init.py` - Styled prompts
5. `src/py/vertebra/cli/banner/styles.py` - Use theme
6. `src/py/vertebra/cli/banner/renderer.py` - Use components
7. `pyproject.toml` - Add dependencies

### Test Files (to be created)
1. `src/py/tests/unit/cli/test_theme.py`
2. `src/py/tests/unit/cli/test_errors.py`
3. `src/py/tests/unit/cli/test_components.py`
4. `src/py/tests/integration/cli/test_help_output.py`

## Dependencies to Add

**pyproject.toml changes:**
```toml
dependencies = [
    # ... existing
    "rich>=13.0.0",  # Ensure explicit rich dependency
]

[project.optional-dependencies]
tui = ["textual>=1.0.0"]  # Optional TUI support
```

## Brand Colors Reference

```python
COLORS = {
    "primary": "#6366f1",      # Indigo (brand)
    "accent": "#d4af37",       # Gold (highlights)
    "success": "#4ade80",      # Green
    "warning": "#facc15",      # Yellow
    "error": "#f87171",        # Red
    "info": "#60a5fa",         # Blue
    "text": "#f1f5f9",         # Light text
    "muted": "#94a3b8",        # Gray text
    "dim": "#64748b",          # Dimmed text
}
```

## Quick Test Commands

After implementation, verify with:

```bash
# Test help styling
uv run vertebra --help
uv run vertebra serve --help

# Test error handling
uv run vertebra serve nonexistent:app

# Test unit tests
uv run pytest src/py/tests/unit/cli/ -v

# Test integration
uv run pytest src/py/tests/integration/cli/ -v
```

## Next Steps

Run `/implement cli-polish` to begin implementation.

The implementation should follow the task order specified in `tasks.md`:
1. Start with theme.py (foundation)
2. Then config.py (enables styled help immediately)
3. Then errors.py and components.py
4. Then update existing files
5. Finally add TUI support

## Decision Log

| Decision | Rationale |
|----------|-----------|
| Hybrid Rich + Textual | Rich for static output (fast, always works), Textual for TUI (optional, feature-rich) |
| Textual as optional | Keep core CLI lightweight, TUI is opt-in |
| Preserve banner system | Existing banner is well-designed, enhance rather than replace |
| Indigo as primary | Aligns with Litestar/Starlite brand identity |
| 17 tasks | Complex feature requires granular breakdown for tracking |

## Troubleshooting

**If help output doesn't show colors:**
- Check that `config.py` is imported in `__init__.py`
- Verify `USE_RICH_MARKUP = True` is set
- Check terminal supports colors (`echo $COLORTERM`)

**If TUI doesn't start:**
- Verify Textual is installed: `uv pip install textual`
- Check for import errors in `tui/__init__.py`
- Ensure stdout is a TTY

**If tests fail:**
- Run `make lint` to check for syntax errors
- Verify import paths are correct
- Check for circular imports between theme/config/components
