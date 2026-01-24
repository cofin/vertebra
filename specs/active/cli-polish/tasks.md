# CLI Polish Implementation Tasks

## Overview

**Complexity:** Medium-Complex (14 tasks across 6 phases)
**PRD Location:** `specs/active/cli-polish/prd.md`
**Research Location:** `specs/active/cli-polish/research/`

**Focus:** Rich styling for all CLI commands (no Textual TUI for now)

---

## Phase 1: Foundation (Theme & Configuration)

### Task 1: Create Theme Module ✅
- [x] Create `src/py/vertebra/cli/theme.py`
- [x] Define colors, chars, terminal caps (moved from banner/styles.py)
- [x] Add `COLORS` dict with hex color strings (converted from RGB tuples)
- [x] Add unit test `tests/unit/cli/test_theme.py`
- [x] Delete `banner/styles.py` (moved to theme.py)
- [x] Update `banner/renderer.py` to import from theme.py

**Files:**
- NEW: `src/py/vertebra/cli/theme.py`
- NEW: `src/py/tests/unit/cli/test_theme.py`
- DELETED: `src/py/vertebra/cli/banner/styles.py`
- MODIFIED: `src/py/vertebra/cli/banner/renderer.py`

### Task 2: Create rich-click Configuration ✅
- [x] Create `src/py/vertebra/cli/config.py`
- [x] Import theme colors
- [x] Configure `USE_RICH_MARKUP = True`
- [x] Configure `USE_MARKDOWN = True`
- [x] Set all STYLE_* options with theme colors
- [x] Define `COMMAND_GROUPS` for Server, Build, Project
- [x] Set WIDTH and MAX_WIDTH constraints
- [x] Update `__init__.py` to import config module

**Files:**
- NEW: `src/py/vertebra/cli/config.py`
- MODIFY: `src/py/vertebra/cli/__init__.py`

---

## Phase 2: Error Handling

### Task 3: Create Error Handling Module ✅
- [x] Create `src/py/vertebra/cli/errors.py`
- [x] Define exit code constants (SUCCESS, USER_ERROR, CONFIG_ERROR, INTERNAL_ERROR)
- [x] Create `CLIError` class extending `click.ClickException`
- [x] Implement `CLIError.show()` with Rich Panel
- [x] Create `format_error()` function for exception mapping
- [x] Create `print_cli_error()` utility function
- [x] Create `raise_cli_error()` helper
- [x] Add hint mapping for common exceptions
- [x] Add unit test `tests/unit/cli/test_errors.py`

**Files:**
- NEW: `src/py/vertebra/cli/errors.py`
- NEW: `src/py/tests/unit/cli/test_errors.py`

---

## Phase 3: UI Components

### Task 4: Create Component Library ✅
- [x] Create `src/py/vertebra/cli/components.py`
- [x] Create `console` singleton (Rich Console instance)
- [x] Implement `section_header(title, style)` function
- [x] Implement `key_value(key, value, key_width)` function
- [x] Implement `status_indicator(status)` function
- [x] Implement `progress_bar(task_name, total, transient)` function
- [x] Implement `status_spinner(message)` context manager
- [x] Implement `styled_table(title, show_header)` function
- [x] Add unit test `tests/unit/cli/test_components.py`

**Files:**
- NEW: `src/py/vertebra/cli/components.py`
- NEW: `src/py/tests/unit/cli/test_components.py`

---

## Phase 4: Banner Enhancement

### Task 5: Integrate Theme with Banner ✅
- [x] theme.py is now the source of truth for colors
- [x] banner/styles.py deleted (moved to theme.py)
- [x] banner/renderer.py imports from theme.py
- [x] Existing banner tests pass

**Files:**
- DELETED: `src/py/vertebra/cli/banner/styles.py`
- MODIFIED: `src/py/vertebra/cli/banner/renderer.py`

### Task 6: Enhance Banner Renderer (SKIP - Already Complete) ✅
- [x] Banner renderer already has its own rendering methods
- [x] Responsive layouts already work
- [x] Logo system already complete with responsive variants

**NOTE:** The banner renderer is already well-designed. No modifications needed.

---

## Phase 5: Command Updates

### Task 7: Update Serve Command ✅
- [x] Import error handling from errors.py
- [x] Replace `click.secho()` with styled error handling (CLIError)
- [x] Use vertebra.serializers instead of json module

**Files:**
- MODIFY: `src/py/vertebra/cli/serve.py`

### Task 8: Update Build Command ✅
- [x] Import components from components.py
- [x] Replace `click.echo()` with styled output (console.print)
- [x] Use section_header for build configuration
- [x] Use error handling for failures (CLIError)

**Files:**
- MODIFY: `src/py/vertebra/cli/build.py`

### Task 9: Update Init Command ✅
- [x] Import components from components.py
- [x] Replace rich_click.secho() with console.print()
- [x] Use section_header for "Next Steps"
- [x] Style the final success output

**Files:**
- MODIFY: `src/py/vertebra/cli/init.py`

---

## Phase 6: Dependencies, Testing & Documentation

### Task 10: Update Dependencies ✅
- [x] `rich` added to core dependencies in pyproject.toml (by user)
- [x] `vtb` alias added to pyproject.toml

**Files:**
- MODIFY: `pyproject.toml`

### Task 11: Unit Testing ✅
- [x] Create `tests/unit/cli/test_theme.py` (21 tests)
- [x] Create `tests/unit/cli/test_errors.py` (11 tests)
- [x] Create `tests/unit/cli/test_components.py` (13 tests)
- [x] All 45 tests pass

**Files:**
- NEW: `src/py/tests/unit/cli/test_theme.py`
- NEW: `src/py/tests/unit/cli/test_errors.py`
- NEW: `src/py/tests/unit/cli/test_components.py`

### Task 12: Update Documentation
- [ ] Update README.md with CLI screenshots (placeholder)
- [ ] Update CLAUDE.md with new CLI structure
- [ ] Add CLI section describing theme system

**Files:**
- MODIFY: `README.md`
- MODIFY: `CLAUDE.md`

### Task 13: Manual Testing & Polish
- [ ] Test in various terminals
- [ ] Fix any visual issues discovered
- [ ] Verify startup time is acceptable

### Task 14: Add `vtb` Command Alias ✅
- [x] Add `vtb` entry point in pyproject.toml `[project.scripts]`
- [x] Both `vertebra` and `vtb` invoke the same CLI
- [x] Verified both commands work identically

**Files:**
- MODIFY: `pyproject.toml`

---

## Task Summary

| Phase | Tasks | Status |
|-------|-------|--------|
| 1. Foundation | 2 tasks | ✅ Complete |
| 2. Error Handling | 1 task | ✅ Complete |
| 3. UI Components | 1 task | ✅ Complete |
| 4. Banner Enhancement | 2 tasks | ✅ Complete |
| 5. Command Updates | 3 tasks | ✅ Complete |
| 6. Testing & Docs | 5 tasks | 3/5 Complete |

**Total: 14 tasks (12 complete, 2 remaining)**

---

## Remaining Work

1. **Documentation updates** (Task 12) - Update README.md and CLAUDE.md
2. **Manual testing** (Task 13) - Test in various terminals

---

## Future Enhancement (Deferred)

If Rich styling proves insufficient for interactive monitoring, consider adding:
- **Textual TUI Dashboard** - Live server monitoring with `--tui` flag
- **Rich Live Display** - Auto-updating server status without full TUI
- **Log Streaming** - Real-time log viewer in terminal

These can be added in a follow-up PRD if needed.
