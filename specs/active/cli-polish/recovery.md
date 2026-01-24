# Recovery Guide for CLI Polish

## Quick Resume

**Last Phase:** Implementation Complete (12/14 tasks)
**Current Status:** Core implementation done, documentation remaining
**PRD Location:** `specs/active/cli-polish/`

## Intelligence Context

**Complexity:** Medium-Complex (14 tasks across 6 phases)
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
- Global exception handling for professional error display

## State Summary

### Completed
- [x] Phase 0: Project discovery and analysis
- [x] Phase 1: Complexity assessment
- [x] Phase 2: Pattern recognition from accelerator project
- [x] Phase 3: PRD and task breakdown
- [x] **Implementation Phase 1**: Theme module (theme.py)
- [x] **Implementation Phase 2**: Config module (config.py)
- [x] **Implementation Phase 3**: Errors module (errors.py)
- [x] **Implementation Phase 4**: Components module (components.py)
- [x] **Implementation Phase 5**: Banner integration (deleted styles.py, updated renderer.py)
- [x] **Implementation Phase 6**: Command updates (serve.py, build.py, init.py)
- [x] **Implementation Phase 7**: Unit tests (45 tests, all passing)
- [x] **Implementation Phase 8**: vtb alias added

### Pending
- [ ] Documentation updates (README.md, CLAUDE.md)
- [ ] Manual testing across terminal types

## Workspace Contents

```
specs/active/cli-polish/
├── prd.md                    # Main PRD (3614 words)
├── tasks.md                  # Implementation tasks (14 tasks)
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

### Files to Modify
1. `src/py/vertebra/cli/__init__.py` - Import config, add global exception handler
2. `src/py/vertebra/cli/serve.py` - Use components, styled errors
3. `src/py/vertebra/cli/build.py` - Add progress bar
4. `src/py/vertebra/cli/init.py` - Styled prompts
5. `src/py/vertebra/cli/banner/styles.py` - Use theme
6. `src/py/vertebra/cli/banner/renderer.py` - Use components
7. `pyproject.toml` - Add rich dependency

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
```

Note: Textual TUI support deferred to future phase.

## Brand Colors Reference (from banner/styles.py - DO NOT CHANGE)

```python
# Existing colors in banner/styles.py - these are the source of truth
COLORS_RGB = {
    "slate": (30, 58, 95),      # #1e3a5f - primary deep blue
    "gold": (212, 175, 55),     # #d4af37 - accent gold
    "success": (74, 222, 128),  # #4ade80 - green
    "warning": (250, 204, 21),  # #facc15 - yellow
    "error": (248, 113, 113),   # #f87171 - red
    "muted": (148, 163, 184),   # #94a3b8 - gray
    "text": (241, 245, 249),    # #f1f5f9 - light text
    "dim": (100, 116, 139),     # #64748b - dim text
}

# theme.py will convert these to hex strings for rich-click
COLORS = {
    "slate": "#1e3a5f",        # Primary (use for commands, panels)
    "gold": "#d4af37",         # Accent (use sparingly)
    "success": "#4ade80",      # Green
    "warning": "#facc15",      # Yellow
    "error": "#f87171",        # Red
    "muted": "#94a3b8",        # Gray
    "text": "#f1f5f9",         # Light text
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
4. Then update existing files (banner, commands)
5. Testing and documentation

## Decision Log

| Decision | Rationale |
|----------|-----------|
| Rich-only (no Textual) | Focus on static styling first; TUI deferred to future phase if needed |
| Preserve banner system | Existing banner is well-designed, enhance rather than replace |
| Indigo as primary | Aligns with Litestar/Starlite brand identity |
| Global exception handler | Ensures all crashes display professionally (Gemini 3 Pro recommendation) |
| 14 tasks | Focused scope after removing TUI requirements |

## Troubleshooting

**If help output doesn't show colors:**
- Check that `config.py` is imported in `__init__.py`
- Verify `USE_RICH_MARKUP = True` is set
- Check terminal supports colors (`echo $COLORTERM`)

**If tests fail:**
- Run `make lint` to check for syntax errors
- Verify import paths are correct
- Check for circular imports between theme/config/components
