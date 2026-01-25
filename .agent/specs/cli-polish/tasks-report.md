# Task Checklist Report: cli-polish

- Generated: 2026-01-24
- Source: `specs/active/cli-polish/tasks.md`
- Total tasks: 65
- Done: 59
- Pending: 6

## Checklist

# CLI Polish Implementation Tasks

## Overview




## Phase 1: Foundation (Theme & Configuration)

### Task 1: Create Theme Module ✅
- [x] Create `src/py/vertebra/cli/theme.py`
- [x] Define colors, chars, terminal caps (moved from banner/styles.py)
- [x] Add `COLORS` dict with hex color strings (converted from RGB tuples)
- [x] Add unit test `tests/unit/cli/test_theme.py`
- [x] Delete `banner/styles.py` (moved to theme.py)
- [x] Update `banner/renderer.py` to import from theme.py


### Task 2: Create rich-click Configuration ✅
- [x] Create `src/py/vertebra/cli/config.py`
- [x] Import theme colors
- [x] Configure `USE_RICH_MARKUP = True`
- [x] Configure `USE_MARKDOWN = True`
- [x] Set all STYLE_* options with theme colors
- [x] Define `COMMAND_GROUPS` for Server, Build, Project
- [x] Set WIDTH and MAX_WIDTH constraints
- [x] Update `__init__.py` to import config module



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



## Phase 4: Banner Enhancement

### Task 5: Integrate Theme with Banner ✅
- [x] theme.py is now the source of truth for colors
- [x] banner/styles.py deleted (moved to theme.py)
- [x] banner/renderer.py imports from theme.py
- [x] Existing banner tests pass


### Task 6: Enhance Banner Renderer (SKIP - Already Complete) ✅
- [x] Banner renderer already has its own rendering methods
- [x] Responsive layouts already work
- [x] Logo system already complete with responsive variants



## Phase 5: Command Updates

### Task 7: Update Serve Command ✅
- [x] Import error handling from errors.py
- [x] Replace `click.secho()` with styled error handling (CLIError)
- [x] Use vertebra.serializers instead of json module


### Task 8: Update Build Command ✅
- [x] Import components from components.py
- [x] Replace `click.echo()` with styled output (console.print)
- [x] Use section_header for build configuration
- [x] Use error handling for failures (CLIError)


### Task 9: Update Init Command ✅
- [x] Import components from components.py
- [x] Replace rich_click.secho() with console.print()
- [x] Use section_header for "Next Steps"
- [x] Style the final success output



## Phase 6: Dependencies, Testing & Documentation

### Task 10: Update Dependencies ✅
- [x] `rich` added to core dependencies in pyproject.toml (by user)
- [x] `vtb` alias added to pyproject.toml


### Task 11: Unit Testing ✅
- [x] Create `tests/unit/cli/test_theme.py` (21 tests)
- [x] Create `tests/unit/cli/test_errors.py` (11 tests)
- [x] Create `tests/unit/cli/test_components.py` (13 tests)
- [x] All 45 tests pass


### Task 12: Update Documentation
- [ ] Update README.md with CLI screenshots (placeholder)
- [ ] Update CLAUDE.md with new CLI structure
- [ ] Add CLI section describing theme system


### Task 13: Manual Testing & Polish
- [ ] Test in various terminals
- [ ] Fix any visual issues discovered
- [ ] Verify startup time is acceptable

### Task 14: Add `vtb` Command Alias ✅
- [x] Add `vtb` entry point in pyproject.toml `[project.scripts]`
- [x] Both `vertebra` and `vtb` invoke the same CLI
- [x] Verified both commands work identically



## Task Summary




## Remaining Work



## Future Enhancement (Deferred)
