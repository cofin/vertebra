# CLI Polish Implementation Tasks

## Overview

**Complexity:** Medium-Complex (14 tasks across 6 phases)
**PRD Location:** `specs/active/cli-polish/prd.md`
**Research Location:** `specs/active/cli-polish/research/`

**Focus:** Rich styling for all CLI commands (no Textual TUI for now)

---

## Phase 1: Foundation (Theme & Configuration)

### Task 1: Create Theme Module
- [ ] Create `src/py/vertebra/cli/theme.py`
- [ ] Define `Colors` dataclass with brand colors (PRIMARY, ACCENT, etc.)
- [ ] Define semantic colors (SUCCESS, WARNING, ERROR, INFO)
- [ ] Define neutral colors (TEXT, MUTED, DIM, SURFACE)
- [ ] Define `Icons` dataclass with Unicode characters
- [ ] Define `IconsASCII` dataclass for fallbacks
- [ ] Add `get_icons()` function for capability-based selection
- [ ] Add unit test `tests/unit/cli/test_theme.py`

**Files:**
- NEW: `src/py/vertebra/cli/theme.py`
- NEW: `src/py/tests/unit/cli/test_theme.py`

### Task 2: Create rich-click Configuration
- [ ] Create `src/py/vertebra/cli/config.py`
- [ ] Import theme colors
- [ ] Configure `USE_RICH_MARKUP = True`
- [ ] Configure `USE_MARKDOWN = True`
- [ ] Set all STYLE_* options with theme colors
- [ ] Define `COMMAND_GROUPS` for Server, Build, Project
- [ ] Set WIDTH and MAX_WIDTH constraints
- [ ] Update `__init__.py` to import config module

**Files:**
- NEW: `src/py/vertebra/cli/config.py`
- MODIFY: `src/py/vertebra/cli/__init__.py`

---

## Phase 2: Error Handling

### Task 3: Create Error Handling Module
- [ ] Create `src/py/vertebra/cli/errors.py`
- [ ] Define exit code constants (SUCCESS, USER_ERROR, CONFIG_ERROR, INTERNAL_ERROR)
- [ ] Create `CLIError` class extending `click.ClickException`
- [ ] Implement `CLIError.show()` with Rich Panel
- [ ] Create `format_error()` function for exception mapping
- [ ] Create `print_cli_error()` utility function
- [ ] Create `raise_cli_error()` helper
- [ ] Add hint mapping for common exceptions
- [ ] Create `global_exception_handler()` decorator for CLI entry point
- [ ] Add unit test `tests/unit/cli/test_errors.py`

The global exception handler ensures ALL unhandled exceptions display professionally,
not just those explicitly caught. This wraps the main CLI group.

**Files:**
- NEW: `src/py/vertebra/cli/errors.py`
- NEW: `src/py/tests/unit/cli/test_errors.py`
- MODIFY: `src/py/vertebra/cli/__init__.py` (apply global handler)

---

## Phase 3: UI Components

### Task 4: Create Component Library
- [ ] Create `src/py/vertebra/cli/components.py`
- [ ] Create `console` singleton (Rich Console instance)
- [ ] Implement `section_header(title, style)` function
- [ ] Implement `key_value(key, value, key_width)` function
- [ ] Implement `status_indicator(status)` function
- [ ] Implement `progress_bar(task_name, total, transient)` function
- [ ] Implement `status_spinner(message)` context manager
- [ ] Implement `styled_table(title, show_header)` function
- [ ] Add unit test `tests/unit/cli/test_components.py`

**Files:**
- NEW: `src/py/vertebra/cli/components.py`
- NEW: `src/py/tests/unit/cli/test_components.py`

---

## Phase 4: Banner Enhancement

### Task 5: Integrate Theme with Banner
- [ ] Update `banner/styles.py` to import from theme.py
- [ ] Remove duplicate color definitions
- [ ] Update `apply_color()` to use theme COLORS
- [ ] Update `get_status_color()` to use theme COLORS
- [ ] Update `get_chars()` to use theme ICONS
- [ ] Verify existing tests pass

**Files:**
- MODIFY: `src/py/vertebra/cli/banner/styles.py`

### Task 6: Enhance Banner Renderer
- [ ] Import components from new module
- [ ] Update `_render_section_header()` to use `section_header()`
- [ ] Update `_render_kv()` to use `key_value()`
- [ ] Update `_status_indicator()` to use `status_indicator()`
- [ ] Add gradient/color effect to logo (if truecolor supported)
- [ ] Update `_render_component_status()` to use new components
- [ ] Verify responsive layouts still work

**Files:**
- MODIFY: `src/py/vertebra/cli/banner/renderer.py`

---

## Phase 5: Command Updates

### Task 7: Update Serve Command
- [ ] Import error handling from errors.py
- [ ] Import components from components.py
- [ ] Replace `click.secho()` with styled error handling
- [ ] Use `status_spinner()` for startup operations
- [ ] Update help text with rich markup

**Files:**
- MODIFY: `src/py/vertebra/cli/serve.py`

### Task 8: Update Build Command
- [ ] Import components from components.py
- [ ] Replace `click.echo()` with styled output
- [ ] Add progress bar for build steps
- [ ] Use error handling for failures
- [ ] Update help text with rich markup

**Files:**
- MODIFY: `src/py/vertebra/cli/build.py`

### Task 9: Update Init Command
- [ ] Import components from components.py
- [ ] Replace basic prompts with styled prompts
- [ ] Add progress bar for project setup steps
- [ ] Use error handling for failures
- [ ] Update help text with rich markup
- [ ] Style the final success output

**Files:**
- MODIFY: `src/py/vertebra/cli/init.py`

---

## Phase 6: Dependencies, Testing & Documentation

### Task 10: Update Dependencies
- [ ] Add `rich>=13.0.0` to core dependencies in pyproject.toml
- [ ] Ensure `rich-click>=1.7.0` version constraint
- [ ] Run `uv sync` to verify dependency resolution
- [ ] Run tests to verify no regressions

**Files:**
- MODIFY: `pyproject.toml`

### Task 11: Integration Testing
- [ ] Create `tests/integration/cli/test_help_output.py`
- [ ] Test command groups appear in help
- [ ] Test error panels display correctly
- [ ] Test progress bars work for build command

**Files:**
- NEW: `src/py/tests/integration/cli/test_help_output.py`

### Task 12: Update Documentation
- [ ] Update README.md with CLI screenshots (placeholder)
- [ ] Update CLAUDE.md with new CLI structure
- [ ] Add CLI section describing theme system

**Files:**
- MODIFY: `README.md`
- MODIFY: `CLAUDE.md`

### Task 13: Manual Testing & Polish
- [ ] Test in iTerm2 (macOS)
- [ ] Test in Terminal.app (macOS)
- [ ] Test in VS Code terminal
- [ ] Test via SSH session
- [ ] Test in CI/CD (non-TTY)
- [ ] Fix any visual issues discovered
- [ ] Verify startup time is acceptable (<50ms increase)

**Files:**
- Various fixes as needed

### Task 14: Add `vtb` Command Alias
- [ ] Add `vtb` entry point in pyproject.toml `[project.scripts]`
- [ ] Both `vertebra` and `vtb` should invoke the same CLI
- [ ] Test both commands work identically
- [ ] Update help text to mention both command names

This allows testing the shorter `vtb` name while keeping `vertebra` available.
The plan is to eventually standardize on `vtb` if it feels right.

**Files:**
- MODIFY: `pyproject.toml`

```toml
[project.scripts]
vertebra = "vertebra.cli:cli"
vtb = "vertebra.cli:cli"
```

---

## Task Summary

| Phase | Tasks | Status |
|-------|-------|--------|
| 1. Foundation | 2 tasks | Not Started |
| 2. Error Handling | 1 task | Not Started |
| 3. UI Components | 1 task | Not Started |
| 4. Banner Enhancement | 2 tasks | Not Started |
| 5. Command Updates | 3 tasks | Not Started |
| 6. Testing & Docs | 5 tasks | Not Started |

**Total: 14 tasks**

---

## Execution Order

Recommended order for implementation:

1. **Task 1** - Theme (foundation for all styling)
2. **Task 2** - Config (enables styled help immediately)
3. **Task 3** - Errors (needed before command updates)
4. **Task 4** - Components (needed for command updates)
5. **Task 10** - Dependencies (ensure all deps available)
6. **Task 5** - Banner/styles (integrate theme)
7. **Task 6** - Banner/renderer (use components)
8. **Task 7** - Serve command
9. **Task 8** - Build command
10. **Task 9** - Init command
11. **Task 11** - Integration tests
12. **Task 12** - Documentation
13. **Task 13** - Manual testing & polish

---

## Future Enhancement (Deferred)

If Rich styling proves insufficient for interactive monitoring, consider adding:
- **Textual TUI Dashboard** - Live server monitoring with `--tui` flag
- **Rich Live Display** - Auto-updating server status without full TUI
- **Log Streaming** - Real-time log viewer in terminal

These can be added in a follow-up PRD if needed.
