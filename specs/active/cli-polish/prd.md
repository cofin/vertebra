# PRD: CLI Polish and TUI Dashboard

## Intelligence Context

**Complexity Assessment:** Complex (12+ checkpoints)
- Multiple interacting systems (CLI output + TUI dashboard)
- New dependency integration (Textual for TUI)
- Architectural changes (centralized theme, error handling system)
- 15+ files to create or modify
- Cross-cutting concerns (styling affects all commands)

**Similar Features Referenced:**
1. `accelerator/src/py/dma/cli/config.py` - rich-click configuration patterns
2. `accelerator/src/py/dma/cli/errors.py` - CLIError class with hints
3. `accelerator/src/py/dma/cli/commands/manage.py` - Styled tables, panels, status
4. `vertebra/cli/banner/` - Existing responsive banner system

**Patterns to Follow:**
- Centralized theme definitions (accelerator's config.py)
- Error handling with hints and exit codes (accelerator's errors.py)
- Reusable UI components (tables, panels, rules)
- Progressive enhancement (Rich for output, Textual for TUI)

**Detected Stack:**
- Python 3.12+ with `rich-click` (already installed)
- Rust IPC via PyO3 bindings
- Bun TypeScript server
- Make-based build system

**Build Commands:**
- Test: `make test` / `uv run pytest src/py/tests`
- Lint: `make lint` / `make lint-all`
- Build: `make install` / `maturin develop`

## Problem Statement

### User Problem

Vertebra's current CLI lacks visual polish and professional styling. While the underlying technology is sophisticated (shared-memory IPC, Rust primitives, Bun engine), the CLI presentation doesn't reflect this quality. Users encounter:

1. **Plain help output** - No colors, no command organization, no branding
2. **Basic error messages** - Text-only errors without hints or context
3. **Static server output** - Banner displays once, no live updates
4. **Missing progress indicators** - Long operations show no feedback
5. **Inconsistent styling** - Different commands use different output patterns; the banner has nice styling but other commands are plain
6. **Banner isolation** - The banner's good styling patterns (TTY detection, colors, responsive layouts) are confined to startup; they don't benefit other commands

### Business Value

A polished CLI delivers:

1. **First Impressions Matter** - CLI is the first touchpoint; professional appearance builds confidence in the framework's quality
2. **Developer Experience** - Better feedback during development reduces friction and increases adoption
3. **Differentiation** - Unique TUI dashboard distinguishes Vertebra from standard ASGI servers
4. **Productivity** - Live monitoring during development catches issues faster
5. **Brand Recognition** - Consistent styling reinforces Vertebra as a cohesive product

### Success Criteria

1. **Help output uses branded colors** - All commands display with Vertebra color palette
2. **Commands organized into groups** - Logical categorization (Server, Build, Project)
3. **Error messages include hints** - Actionable guidance for common issues
4. **Progress bars for long operations** - Build and init show visual progress
5. **Serve banner uses shared theme** - Banner styling comes from same theme as all other commands
6. **Startup time unchanged** - Styling adds <50ms to startup
7. **Terminal compatibility** - Works on major terminals, fallbacks for basic ones

## Acceptance Criteria

### AC1: Theme System
- [ ] Centralized `theme.py` defines all colors, icons, and styles
- [ ] Colors follow Litestar/Vertebra brand identity (indigo primary, gold accent)
- [ ] Semantic color names (success, error, warning, info, muted)
- [ ] Unicode icons with ASCII fallbacks
- [ ] Theme is importable: `from vertebra.cli.theme import COLORS, ICONS`

### AC2: rich-click Configuration
- [ ] All styling options configured in `config.py`
- [ ] Command groups defined: Server, Build, Project
- [ ] Markdown support enabled (`USE_MARKDOWN = True`)
- [ ] Rich markup enabled (`USE_RICH_MARKUP = True`)
- [ ] Width constraints set (120 chars max)
- [ ] Styled option/argument/command colors

### AC3: Error Handling
- [ ] `CLIError` class with message, hint, and exit_code
- [ ] Styled error panel with colored border
- [ ] Hint text shown below main error
- [ ] Exit codes: 0=success, 1=user error, 2=config error, 3=internal error
- [ ] Error type to hint mapping for common exceptions

### AC4: UI Components
- [ ] `section_header(title)` - Styled rule with title
- [ ] `key_value(key, value)` - Formatted key-value pair
- [ ] `status_indicator(status)` - Colored circle based on status
- [ ] `progress_bar(task, total)` - Rich progress bar wrapper
- [ ] `status_spinner(message)` - Context manager for async operations
- [ ] `styled_table(...)` - Pre-configured table creator

### AC5: Command Updates
- [ ] `serve` command uses new error handling and styled output
- [ ] `build` command shows progress bar during build steps
- [ ] `init` command uses styled prompts and progress
- [ ] All commands use consistent output styling from shared components

### AC6: Banner Enhancement
- [ ] Banner integrates with theme system
- [ ] Logo displays with gradient/color effect (if terminal supports)
- [ ] Component status uses new status_indicator component
- [ ] Responsive layouts work at all breakpoints

### AC7: Dependencies
- [ ] `rich>=13.0.0` added to core dependencies (explicit, not just via rich-click)
- [ ] `rich-click>=1.7.0` version constraint verified

### AC8: Testing
- [ ] Unit tests for theme module
- [ ] Unit tests for error handling
- [ ] Unit tests for UI components
- [ ] Integration test for help output styling
- [ ] Manual testing on: iTerm2, Terminal.app, VS Code, Windows Terminal

### AC9: Documentation
- [ ] README updated with CLI screenshots
- [ ] CLAUDE.md updated with new CLI structure

## Technical Approach

### Architecture Overview

```
src/py/vertebra/cli/
├── __init__.py          # Entry point (import config for side effects)
├── theme.py             # NEW: Colors, icons, styles
├── config.py            # NEW: rich-click configuration
├── errors.py            # NEW: CLIError and error handling
├── components.py        # NEW: Reusable UI components
├── serve.py             # MODIFIED: Use error handling and components
├── build.py             # MODIFIED: Add progress bar
├── init.py              # MODIFIED: Add styled prompts
└── banner/
    ├── __init__.py
    ├── styles.py        # MODIFIED: Import from theme.py
    ├── logos.py         # MODIFIED: Add color gradient
    ├── renderer.py      # MODIFIED: Use new components
    ├── hooks.py
    └── collector.py
```

### Pattern Reference: Theme Module

Following the accelerator pattern with Vertebra branding:

```python
# theme.py
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Colors:
    """Vertebra CLI color palette."""
    # Brand
    PRIMARY = "#6366f1"       # Indigo
    PRIMARY_LIGHT = "#818cf8"
    PRIMARY_DARK = "#4f46e5"
    ACCENT = "#d4af37"        # Gold

    # Semantic
    SUCCESS = "#4ade80"
    WARNING = "#facc15"
    ERROR = "#f87171"
    INFO = "#60a5fa"

    # Neutral
    TEXT = "#f1f5f9"
    MUTED = "#94a3b8"
    DIM = "#64748b"
    SURFACE = "#1e293b"


COLORS = Colors()
```

### Pattern Reference: rich-click Configuration

```python
# config.py
import rich_click as click

# Enable rich features
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True

# Style definitions
click.rich_click.STYLE_COMMAND = f"bold {COLORS.PRIMARY}"
click.rich_click.STYLE_OPTION = f"bold {COLORS.SUCCESS}"
click.rich_click.STYLE_ARGUMENT = "bold cyan"
click.rich_click.STYLE_METAVAR = f"{COLORS.ACCENT}"
click.rich_click.STYLE_ERRORS_PANEL_BORDER = COLORS.ERROR
click.rich_click.STYLE_OPTIONS_PANEL_BORDER = f"dim {COLORS.PRIMARY}"

# Command groups
click.rich_click.COMMAND_GROUPS = {
    "vertebra": [
        {"name": "Server", "commands": ["serve", "dev"]},
        {"name": "Build", "commands": ["build", "bundle"]},
        {"name": "Project", "commands": ["init", "config"]},
    ]
}

# Layout
click.rich_click.WIDTH = 120
click.rich_click.MAX_WIDTH = 120
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
```

### Pattern Reference: Error Handling

```python
# errors.py
import click
from rich.console import Console
from rich.panel import Panel

class CLIError(click.ClickException):
    def __init__(self, message: str, *, hint: str | None = None, exit_code: int = 1):
        super().__init__(message)
        self.hint = hint
        self.exit_code = exit_code

    def show(self, file=None):
        from vertebra.cli.theme import COLORS
        content = f"[bold]{self.message}[/bold]"
        if self.hint:
            content += f"\n\n[dim]Hint: {self.hint}[/dim]"

        console = Console(stderr=True)
        console.print(Panel(
            content,
            title="Error",
            title_align="left",
            border_style=COLORS.ERROR,
            padding=(0, 1)
        ))
```

### Pattern Reference: UI Components

```python
# components.py
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.panel import Panel
from contextlib import contextmanager

console = Console()

def section_header(title: str) -> None:
    """Render a styled section header."""
    from vertebra.cli.theme import COLORS
    console.print(Rule(title, style=COLORS.PRIMARY, align="left"))

def status_indicator(status: str) -> str:
    """Return colored status indicator."""
    from vertebra.cli.theme import COLORS, ICONS
    color = {
        "ready": COLORS.SUCCESS,
        "starting": COLORS.WARNING,
        "error": COLORS.ERROR,
    }.get(status.lower(), COLORS.MUTED)
    return f"[{color}]{ICONS.STATUS_READY}[/{color}]"

@contextmanager
def status_spinner(message: str):
    """Context manager for spinner during operations."""
    with console.status(f"[{COLORS.WARNING}]{message}[/]", spinner="dots"):
        yield
```

### Database Changes

None required.

### API Endpoints

None required.

### Frontend Changes

None required (CLI-only feature).

### Documentation Updates

1. **README.md**: Add CLI screenshot showing styled help output
2. **CLAUDE.md**: Update CLI section with new structure
3. **docs/cli.md**: Create CLI usage guide with TUI documentation

## Testing Strategy

### Unit Tests

```python
# tests/unit/cli/test_theme.py
def test_colors_are_valid_hex():
    from vertebra.cli.theme import COLORS
    for name in dir(COLORS):
        if not name.startswith('_'):
            color = getattr(COLORS, name)
            assert color.startswith('#'), f"{name} should be hex color"
            assert len(color) == 7, f"{name} should be #RRGGBB format"

# tests/unit/cli/test_errors.py
def test_cli_error_with_hint():
    from vertebra.cli.errors import CLIError
    error = CLIError("Test error", hint="Try this instead")
    assert error.hint == "Try this instead"
    assert error.exit_code == 1

# tests/unit/cli/test_components.py
def test_status_indicator_colors():
    from vertebra.cli.components import status_indicator
    assert "success" in status_indicator("ready").lower() or "#4ade80" in status_indicator("ready")
```

### Integration Tests

```python
# tests/integration/cli/test_help_output.py
def test_help_shows_command_groups(cli_runner):
    result = cli_runner.invoke(main, ["--help"])
    assert "Server" in result.output
    assert "Build" in result.output
    assert "Project" in result.output

def test_serve_help_shows_tui_option(cli_runner):
    result = cli_runner.invoke(main, ["serve", "--help"])
    assert "--tui" in result.output
```

### Manual Testing Checklist

- [ ] iTerm2 (macOS) - Full truecolor support
- [ ] Terminal.app (macOS) - 256 color support
- [ ] VS Code integrated terminal - Various color support
- [ ] Windows Terminal - Full truecolor support
- [ ] CMD.exe - Basic color support
- [ ] SSH session - Verify TERM detection
- [ ] CI/CD (non-TTY) - Verify plain output

## Implementation Notes

### Pattern Deviations

1. **Textual as optional**: Unlike accelerator which doesn't use TUI, we're adding Textual as an optional feature. This requires careful import guards.

2. **Hybrid approach**: We use Rich for static output and Textual for live dashboard, rather than forcing one or the other.

3. **Banner patterns as foundation**: The existing banner system's well-architected patterns (TTY detection, responsive layouts, color management) are extracted into the shared theme/components modules. These become the foundation for ALL CLI output, not just the startup banner. The banner renderer is then refactored to consume these shared components, ensuring consistency across every command.

### Dependencies

**Core (pyproject.toml dependencies):**
```toml
dependencies = [
    "rich>=13.0.0",
    "rich-click>=1.7.0",
    # ... existing deps
]
```

Note: `rich-click` already pulls in `rich`, but we explicitly declare `rich>=13.0.0` to ensure we have the version with all features we need (Live, better Progress, etc.).

### Migration Considerations

1. **No breaking changes**: All existing commands continue to work
2. **Progressive enhancement**: New features are additive
3. **Graceful degradation**: Non-TTY environments get plain output

### Performance Considerations

1. **Theme caching**: Colors computed once at import, reused throughout
2. **Minimal startup cost**: rich-click configuration is cheap (~10ms)
3. **Lazy component creation**: Progress bars and spinners created on demand
4. **TTY detection cached**: Terminal capability checks done once

### Security Considerations

1. **No credential display**: Passwords/tokens masked in output
2. **Input validation**: All user input sanitized in prompts
3. **Error messages**: Don't leak sensitive paths or internal details

## File Structure Summary

### New Files (4)

1. `src/py/vertebra/cli/theme.py` - Color and icon definitions
2. `src/py/vertebra/cli/config.py` - rich-click configuration
3. `src/py/vertebra/cli/errors.py` - Error handling classes
4. `src/py/vertebra/cli/components.py` - Reusable UI components

### Modified Files (8)

1. `src/py/vertebra/cli/__init__.py` - Import config, update groups
2. `src/py/vertebra/cli/serve.py` - Use error handling and components
3. `src/py/vertebra/cli/build.py` - Add progress bar
4. `src/py/vertebra/cli/init.py` - Styled prompts
5. `src/py/vertebra/cli/banner/styles.py` - Use theme
6. `src/py/vertebra/cli/banner/renderer.py` - Use components
7. `pyproject.toml` - Dependencies
8. `README.md` - CLI documentation

### Test Files (4)

1. `src/py/tests/unit/cli/test_theme.py`
2. `src/py/tests/unit/cli/test_errors.py`
3. `src/py/tests/unit/cli/test_components.py`
4. `src/py/tests/integration/cli/test_help_output.py`

## Detailed Component Specifications

### Theme Module Specification

The theme module serves as the single source of truth for all visual styling across the CLI. This centralization enables easy updates to the entire application's appearance from one location.

**Color System Architecture:**

The color system follows a three-tier hierarchy:
1. **Brand Colors** - Primary and accent colors defining Vertebra's visual identity
2. **Semantic Colors** - Functional colors for success, warning, error, info states
3. **Neutral Colors** - Text, surface, border colors for general UI elements

Each color includes variants (light, dark) for use in different contexts such as hover states, disabled states, or emphasis levels.

**Icon System Architecture:**

Icons are divided into categories:
1. **Status Icons** - Indicate component health (ready, starting, warning, error)
2. **Navigation Icons** - Arrows, chevrons for menu and hierarchical display
3. **Progress Icons** - Spinner frames, progress bar characters
4. **Structural Icons** - Box-drawing characters for layouts and tables

The icon system provides automatic fallback to ASCII equivalents when Unicode is not supported, detected via the `LANG` environment variable and terminal capabilities.

### Error Handling Specification

The error handling system provides a consistent, helpful experience when users encounter issues. Following the pattern from accelerator, errors are categorized by severity and include contextual hints.

**Exit Code Mapping:**
- `EXIT_SUCCESS = 0` - Operation completed successfully
- `EXIT_USER_ERROR = 1` - User input or validation errors (recoverable by user)
- `EXIT_CONFIG_ERROR = 2` - Configuration or setup errors (requires system changes)
- `EXIT_INTERNAL_ERROR = 3` - Unexpected internal errors (report as bug)

**Error to Hint Mapping:**

The system maps common exception types to helpful hints:
- `FileNotFoundError` → "Check that the file path exists and you have read permissions."
- `ConnectionError` → "Verify the server is running and the port is correct."
- `PermissionError` → "Run with elevated permissions or check file ownership."
- `ValueError` → "Check the input format matches the expected type."

**Error Panel Styling:**

Errors are displayed in a Rich Panel with:
- Red border (using `COLORS.ERROR`)
- Left-aligned "Error" title
- Bold message text
- Italic hint text (if provided)
- Padding for readability

### Component Library Specification

The component library provides reusable UI elements ensuring consistency across all commands.

**section_header(title: str, style: str | None = None) -> None:**

Renders a styled rule with left-aligned title. Used to separate logical sections of output.

Example output: `Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`

**key_value(key: str, value: str, key_width: int = 15) -> str:**

Formats a key-value pair with consistent alignment and optional coloring based on value type.

Example output: `  ▸ Host         127.0.0.1`

**status_indicator(status: str) -> str:**

Returns a colored status indicator character. Maps status strings to colors:
- "ready", "connected", "healthy" → Green circle
- "starting", "degraded", "warning" → Yellow circle
- "error", "failed", "disconnected" → Red circle
- Other → Gray circle

**progress_bar(task_name: str, total: int, transient: bool = True) -> Progress:**

Creates a pre-configured Rich Progress bar with Vertebra styling. Includes:
- Task description
- Bar with custom colors
- Percentage complete
- Time elapsed/remaining

**status_spinner(message: str) -> ContextManager:**

Context manager that displays a spinner during async operations. Automatically handles terminal cleanup.

**styled_table(title: str | None = None, show_header: bool = True) -> Table:**

Creates a pre-configured Rich Table with Vertebra styling:
- Colored borders (using `COLORS.PRIMARY`)
- Padded cells
- Optional title
- Consistent width handling

## Appendix: Visual Examples

### Current Help Output
```
Usage: vertebra [OPTIONS] COMMAND [ARGS]...

  Vertebra: High-performance polyglot application backbone.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  build  Build Vertebra application artifacts.
  init   Initialize a new Vertebra project.
  serve  Run the Bun-backed ASGI server.
```

### Target Help Output
```
╭─ Vertebra ─────────────────────────────────────────────────────────────────╮
│ High-performance polyglot application backbone                             │
│                                                                            │
│ Python-first architecture with Rust IPC and Bun server layer.              │
│ tRPC-like developer experience with shared-memory performance.             │
╰────────────────────────────────────────────────────────────────────────────╯

Usage: vertebra [OPTIONS] COMMAND [ARGS]...

╭─ Server Commands ──────────────────────────────────────────────────────────╮
│ serve   Run the Bun-backed ASGI server                                     │
│ dev     Start development server with hot reload                           │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Build Commands ───────────────────────────────────────────────────────────╮
│ build   Build production artifacts                                         │
│ bundle  Create deployment bundle                                           │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Project Commands ─────────────────────────────────────────────────────────╮
│ init    Initialize a new Vertebra project                                  │
│ config  View or modify configuration                                       │
╰────────────────────────────────────────────────────────────────────────────╯

Options:
  --version  Show version and exit
  --help     Show this message and exit
```

### Target Error Output
```
╭─ Error ────────────────────────────────────────────────────────────────────╮
│ Cannot find application module 'myapp:app'                                 │
│                                                                            │
│ Hint: Ensure the module path is correct and the app object exists.         │
│       Example: vertebra serve mymodule.main:app                            │
╰────────────────────────────────────────────────────────────────────────────╯
```

### Target Build Progress
```
Building ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 67% 0:00:23
  ▸ Compiling Rust bindings...
```

### Target Status Display
```
Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Listening      http://127.0.0.1:8000
  ▸ Workers        4 processes
  ▸ Log Level      info

Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ● Bun Engine ·················· ready (v1.0.0)
  ● Rust IPC ···················· ready (4 channels)
  ● Broker ······················ ready (pg: 10, redis: 5)
```

## Appendix: Implementation Recipes

### Recipe: Adding a New Command with Styling

When adding a new command to Vertebra CLI, follow this pattern:

```python
# Example: vertebra status command
import click
from vertebra.cli.components import console, section_header, key_value, status_spinner
from vertebra.cli.errors import CLIError

@click.command()
@click.option("--verbose", "-v", is_flag=True, help="Show detailed information")
def status(verbose: bool) -> None:
    """Show current Vertebra status."""
    try:
        section_header("Vertebra Status")
        console.print()

        with status_spinner("Checking components..."):
            # Gather status information
            components = check_components()

        for name, state in components.items():
            console.print(key_value(name, state))

        if verbose:
            console.print()
            section_header("Detailed Metrics")
            # Additional verbose output

    except ConnectionError as e:
        raise CLIError(
            f"Cannot connect to server: {e}",
            hint="Ensure the server is running with 'vertebra serve'",
            exit_code=2
        )
```

### Recipe: Adding a Progress Bar to Long Operations

For operations with known step counts:

```python
from vertebra.cli.components import progress_bar

def build_project(steps: list[str]) -> None:
    with progress_bar("Building project", len(steps)) as progress:
        for step in steps:
            progress.update(description=step)
            # Perform build step
            do_build_step(step)
            progress.advance(1)
```

### Recipe: Styled Error with Context

When raising errors with rich context:

```python
from vertebra.cli.errors import CLIError, format_error_context

try:
    process_config(config_path)
except ValueError as e:
    context = format_error_context(
        file=config_path,
        line=e.line if hasattr(e, 'line') else None,
        message=str(e)
    )
    raise CLIError(
        f"Invalid configuration: {e}",
        hint=f"Check syntax at {context}",
        exit_code=2
    )
```

## Appendix: Terminal Compatibility Matrix

| Terminal | Truecolor | 256-Color | Unicode | Notes |
|----------|-----------|-----------|---------|-------|
| iTerm2 (macOS) | Yes | Yes | Yes | Full support |
| Terminal.app (macOS) | No | Yes | Yes | Fallback to 256 |
| Windows Terminal | Yes | Yes | Yes | Full support |
| CMD.exe | No | Limited | Limited | Basic colors only |
| VS Code Terminal | Varies | Yes | Yes | Depends on settings |
| GNOME Terminal | Yes | Yes | Yes | Full support |
| Alacritty | Yes | Yes | Yes | Full support |
| tmux | Yes* | Yes | Yes | *Requires config |
| SSH sessions | Varies | Usually | Usually | Depends on client |
| CI/CD (non-TTY) | N/A | N/A | N/A | JSON output mode |

The CLI automatically detects capabilities via:
- `COLORTERM` environment variable (truecolor/24bit)
- `TERM` environment variable (xterm-256color, etc.)
- `LANG` environment variable (UTF-8 detection)
- `sys.stdout.isatty()` (TTY detection)

## Appendix: Research References

### Primary Sources

1. **Rich Library Documentation** - https://rich.readthedocs.io/en/latest/
   - Console API for output handling
   - Progress bar customization
   - Table and Panel styling

2. **Textual Framework** - https://textual.textualize.io/
   - Widget development guide
   - CSS-like styling (TCSS)
   - Event handling patterns

3. **rich-click Repository** - https://github.com/ewels/rich-click
   - Configuration options
   - Command grouping
   - Style customization

### Industry Research

4. **10 Best Python TUI Libraries for 2025** - Medium/Towards Data Engineering
   - Comparison of Rich vs Textual vs Curses
   - Performance benchmarks
   - Use case recommendations

5. **7 Things Learned Building a Modern TUI Framework** - Textualize Blog
   - Resize handling best practices
   - Performance optimization
   - Cross-platform considerations

6. **Python Textual Tutorial** - Real Python (March 2025)
   - Widget composition patterns
   - Reactive data binding
   - Application architecture

### Related Implementations

7. **DMA Accelerator CLI** - `/home/cody/code/g/dma/accelerator/src/py/dma/cli/`
   - Production-quality rich-click configuration
   - Error handling with hints
   - Status display patterns

8. **Litestar CLI** - In project's venv
   - Command structure patterns
   - Integration with web framework
