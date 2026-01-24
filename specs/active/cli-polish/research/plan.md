# CLI Polish Research Plan

## Executive Summary

This document outlines the comprehensive research and technical planning for overhauling Vertebra's CLI interface to achieve a polished, professional, and visually impressive terminal experience. The goal is to transform the current basic CLI into a head-turning TUI that showcases the power and elegance of the Vertebra application backbone while maintaining excellent usability and performance.

## 1. Current State Analysis

### 1.1 Existing CLI Structure

The current Vertebra CLI is located at `src/py/vertebra/cli/` and consists of:

**Entry Point (`__init__.py`):**
- Basic rich-click configuration (3 lines only)
- Simple command group with `serve`, `build`, `init` commands
- No command grouping or organization
- No markdown support enabled

**Serve Command (`serve.py`):**
- Functional but minimal styling
- Banner rendering system exists but underutilized
- Basic `click.secho()` for error messages
- No live updates during server operation

**Build Command (`build.py`):**
- Plain text output with `click.echo()`
- Simple success/failure messages
- No progress indicators

**Init Command (`init.py`):**
- Uses `rich_click.secho()` for colored output
- Basic template rendering
- No interactive prompts with styling

**Banner System (`banner/`):**
- Well-architected with renderer, styles, logos, hooks, collector
- Supports TTY detection and responsive layouts
- Unicode block art logo
- Component status display
- BUT: Static output only, no live updates

### 1.2 Gap Analysis

| Feature | Current State | Target State | Gap |
|---------|---------------|--------------|-----|
| Color Theme | Basic RGB | Branded palette | Major |
| Help Styling | Minimal | Full markdown | Major |
| Command Groups | None | Organized categories | Major |
| Progress Bars | None | Rich progress | Major |
| Error Panels | Basic text | Styled panels w/hints | Major |
| Live Updates | None | Textual TUI | Major |
| Status Tables | Basic | Formatted with styles | Moderate |
| Interactive Prompts | None | Rich prompts | Moderate |

## 2. Reference Implementation Analysis

### 2.1 DMA Accelerator CLI Architecture

The DMA Accelerator project demonstrates professional CLI design with:

**Configuration (`cli/config.py`):**
```python
# Google Cloud branding colors
ERROR_COLOR = "#EA4335"
WARNING_COLOR = "#FBBC04"
SUCCESS_COLOR = "#34A853"
INFO_COLOR = "#4285F4"

# Comprehensive rich-click styling
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.STYLE_COMMAND = "bold #4285F4"
click.rich_click.STYLE_OPTION = "bold #34A853"
click.rich_click.STYLE_SWITCH = "bold #34A853"
click.rich_click.STYLE_ARGUMENT = "bold cyan"
click.rich_click.STYLE_METAVAR = "#FBBC04"
click.rich_click.STYLE_USAGE = "bold"
click.rich_click.STYLE_USAGE_COMMAND = "bold #4285F4"
click.rich_click.STYLE_HELPTEXT = "dim"
click.rich_click.STYLE_ERRORS_SUGGESTION = "italic #FBBC04"
click.rich_click.STYLE_REQUIRED_SHORT = "#EA4335"
click.rich_click.STYLE_REQUIRED_LONG = "dim #EA4335"
click.rich_click.STYLE_ERRORS_PANEL_BORDER = "#EA4335"
click.rich_click.STYLE_OPTIONS_PANEL_BORDER = "dim #4285F4"
click.rich_click.STYLE_COMMANDS_PANEL_BORDER = "dim #4285F4"
click.rich_click.WIDTH = 120
click.rich_click.MAX_WIDTH = 120
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.SHOW_METAVARS_COLUMN = True
click.rich_click.APPEND_METAVARS_HELP = False
click.rich_click.COMMAND_GROUPS = {
    "dma": [
        {"name": "Database Assessment", "commands": ["collection", "server"]},
        {"name": "Administration", "commands": ["manage", "version"]},
    ]
}
```

**Error Handling (`cli/errors.py`):**
- `CLIError` class extending `click.ClickException`
- Automatic hint generation based on exception type
- Styled error panels with borders
- Appropriate exit codes (0=success, 1=user, 2=config, 3=internal)

**Status Display Patterns:**
- `console.status()` with spinners for async operations
- `Table` with formatted rows for configuration display
- `Panel` for grouped information
- Left-aligned `Rule` for section headers
- Color-coded values based on type (boolean, path, URL, etc.)

### 2.2 Key Design Patterns

**Centralized Theme Management:**
The accelerator uses a centralized approach where all colors are defined once and referenced throughout. This enables:
- Consistent branding across all commands
- Easy theme modifications
- Clear semantic meaning (success=green, error=red)

**Progressive Enhancement:**
- Basic output works everywhere
- Enhanced styling when terminal supports it
- Fallbacks for non-TTY environments

**Component Reusability:**
- `left_aligned_rule()` helper for section headers
- `_format_config_value()` for smart value display
- Consistent table creation patterns

## 3. Rich and Textual Library Research

### 3.1 Rich Library Capabilities

**Core Features:**
- Text styling (colors, bold, italic, underline)
- Tables with borders, alignment, padding
- Panels with titles and borders
- Progress bars (single and multi-task)
- Spinners for async operations
- Syntax highlighting
- Markdown rendering
- Console protocol for custom renderables

**Best Practices (2024-2025):**
1. **Centralized Themes:** Define all colors in a single module
2. **Semantic Styles:** Name styles by purpose, not appearance
3. **Responsive Layouts:** Use `Console.width` for adaptive designs
4. **Performance:** Use `Live` for updates, not repeated prints
5. **Accessibility:** Provide text alternatives, test color-blind modes

**Production Considerations:**
- Rich automatically detects terminal capabilities
- Fallback to basic output in non-TTY environments
- Thread-safe console operations
- Memory-efficient for large outputs

### 3.2 Textual Library Capabilities

**Framework Features:**
- Full application framework (not just rendering)
- CSS-like styling with TCSS
- Widget-based architecture
- Event-driven programming
- Async-first design
- Hot-reload during development
- Built on top of Rich

**Key Widgets:**
- `DataTable`: Sortable, scrollable data display
- `Static`: Simple text/Rich renderable display
- `Footer`: Keybinding hints
- `Header`: Application title bar
- `ProgressBar`: Visual progress indication
- `Log`: Scrollable log viewer
- `Input`: Text input field
- Custom widgets via `Widget` class

**Performance Benchmarks (PyCon 2024):**
- 40% reduction in dev time for dashboard apps
- 12ms render times achievable
- 85% latency reduction vs web dashboards
- 30% lower CPU than web-based alternatives

**Best Practices:**
1. **Throttle Resize Events:** Prevent CPU spikes on window resize
2. **Use Workers:** For async data fetching
3. **Reactive Bindings:** Let Textual manage UI updates
4. **Truecolor Fallbacks:** Handle 256-color terminals gracefully

### 3.3 rich-click Configuration

**Available Style Options:**
```python
# Command and argument styling
STYLE_COMMAND = "bold cyan"
STYLE_ARGUMENT = "bold cyan"
STYLE_OPTION = "bold green"
STYLE_SWITCH = "bold green"
STYLE_METAVAR = "bold yellow"
STYLE_METAVAR_APPEND = "dim yellow"

# Usage and help text
STYLE_USAGE = "yellow"
STYLE_USAGE_COMMAND = "bold"
STYLE_HELPTEXT = ""
STYLE_HELPTEXT_FIRST_LINE = ""
STYLE_OPTION_HELP = ""

# Headers and footers
STYLE_HEADER_TEXT = ""
STYLE_FOOTER_TEXT = ""
STYLE_EPILOG_TEXT = ""

# Environment variables
STYLE_OPTION_ENVVAR = "dim yellow"

# Errors and requirements
STYLE_DEPRECATED = "red"
STYLE_REQUIRED_SHORT = "red"
STYLE_REQUIRED_LONG = "dim red"
STYLE_ERRORS_SUGGESTION = "italic"
STYLE_ABORTED = "red"

# Panel styling
STYLE_OPTIONS_PANEL_BORDER = "dim"
STYLE_COMMANDS_PANEL_BORDER = "dim"
STYLE_ERRORS_PANEL_BORDER = "red"

# Table styling
STYLE_OPTIONS_TABLE_SHOW_LINES = False
STYLE_OPTIONS_TABLE_LEADING = 0
STYLE_OPTIONS_TABLE_PAD_EDGE = False
STYLE_OPTIONS_TABLE_PADDING = (0, 1)
STYLE_OPTIONS_TABLE_BOX = ""

# Layout options
WIDTH = 100  # Max help width
MAX_WIDTH = None  # Max for any content
SHOW_ARGUMENTS = True
SHOW_METAVARS_COLUMN = True
APPEND_METAVARS_HELP = True
GROUP_ARGUMENTS_OPTIONS = False
USE_MARKDOWN = False
USE_RICH_MARKUP = False
```

**Command Groups:**
```python
COMMAND_GROUPS = {
    "cli_name": [
        {"name": "Group Name", "commands": ["cmd1", "cmd2"]},
        {"name": "Other Group", "commands": ["cmd3", "cmd4"]},
    ]
}
```

## 4. Proposed Design System

### 4.1 Vertebra Brand Colors

Based on the existing banner styles and Litestar alignment:

```python
# Primary palette
COLORS = {
    # Brand colors (Litestar/Starlite aligned)
    "primary": "#6366f1",      # Indigo - primary brand
    "primary_light": "#818cf8", # Lighter indigo
    "primary_dark": "#4f46e5",  # Darker indigo

    # Accent (from existing banner)
    "accent": "#d4af37",        # Gold - highlights, badges
    "accent_light": "#e5c04e",  # Lighter gold

    # Semantic colors
    "success": "#4ade80",       # Green - ready, connected
    "success_dark": "#22c55e",
    "warning": "#facc15",       # Yellow - starting, degraded
    "warning_dark": "#eab308",
    "error": "#f87171",         # Red - failed, disconnected
    "error_dark": "#ef4444",
    "info": "#60a5fa",          # Blue - informational
    "info_dark": "#3b82f6",

    # Neutral colors
    "text": "#f1f5f9",          # Primary text
    "text_secondary": "#cbd5e1", # Secondary text
    "muted": "#94a3b8",         # Muted text
    "dim": "#64748b",           # Dimmed text
    "surface": "#1e293b",       # Background surfaces
    "surface_dark": "#0f172a",  # Darker surfaces
    "border": "#334155",        # Borders
}
```

### 4.2 Typography and Icons

**Unicode Characters:**
```python
ICONS = {
    # Status indicators
    "ready": "●",       # U+25CF filled circle
    "starting": "○",    # U+25CB empty circle
    "warning": "◐",     # U+25D0 circle with left half black
    "error": "◉",       # U+25C9 fisheye

    # Arrows and pointers
    "arrow_right": "▸",  # U+25B8 right-pointing triangle
    "arrow_down": "▾",   # U+25BE down-pointing triangle
    "arrow_up": "▴",     # U+25B4 up-pointing triangle
    "chevron": "›",      # U+203A right angle quote

    # Progress
    "spinner": ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
    "progress_done": "█",
    "progress_todo": "░",

    # Boxes and dividers
    "box_h": "─",        # U+2500 horizontal line
    "box_h_heavy": "━",  # U+2501 heavy horizontal
    "box_v": "│",        # U+2502 vertical line
    "corner_tl": "┌",    # U+250C top-left corner
    "corner_tr": "┐",    # U+2510 top-right corner
    "corner_bl": "└",    # U+2514 bottom-left corner
    "corner_br": "┘",    # U+2518 bottom-right corner
    "dots": "·",         # U+00B7 middle dot

    # Checks and marks
    "check": "✓",        # U+2713 check mark
    "cross": "✗",        # U+2717 ballot x
    "bullet": "•",       # U+2022 bullet
}
```

**ASCII Fallbacks:**
```python
ICONS_ASCII = {
    "ready": "*",
    "starting": "o",
    "warning": "!",
    "error": "x",
    "arrow_right": ">",
    "arrow_down": "v",
    "arrow_up": "^",
    "chevron": ">",
    "box_h": "-",
    "box_h_heavy": "=",
    "box_v": "|",
    "dots": ".",
    "check": "+",
    "cross": "x",
    "bullet": "*",
}
```

### 4.3 TUI Dashboard Layout

```
┌────────────────────────────────────────────────────────────────────────────┐
│ ▐█▌ VERTEBRA v0.1.0                          http://127.0.0.1:8000  [DEV] │
├────────────────────────────────────────────────────────────────────────────┤
│ Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│   ● Bun Engine ·················· ready (v1.0.0)                          │
│   ● Rust IPC ···················· ready (4 channels, P99: 0.3ms)         │
│   ● Broker ······················ ready (pg: 10, redis: 5, sqlite: 2)    │
├────────────────────────────────────────────────────────────────────────────┤
│ Workers (4) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│ ┌──────┬──────────┬───────────┬──────────┬──────────────────────────────┐ │
│ │  PID │  Status  │  Requests │  Memory  │  CPU                         │ │
│ ├──────┼──────────┼───────────┼──────────┼──────────────────────────────┤ │
│ │ 1234 │ ● ready  │    1,234  │  45.2 MB │  ████░░░░░░ 38%              │ │
│ │ 1235 │ ● ready  │    1,189  │  43.8 MB │  ███░░░░░░░ 28%              │ │
│ │ 1236 │ ● ready  │    1,156  │  44.1 MB │  █████░░░░░ 52%              │ │
│ │ 1237 │ ○ start  │        0  │  12.3 MB │  ░░░░░░░░░░  0%              │ │
│ └──────┴──────────┴───────────┴──────────┴──────────────────────────────┘ │
├────────────────────────────────────────────────────────────────────────────┤
│ Metrics ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│   Requests: 523/s      P50: 1.2ms      P99: 4.3ms      Errors: 0.02%     │
├────────────────────────────────────────────────────────────────────────────┤
│ [Q]uit  [R]estart Workers  [+/-] Workers  [L]og Level  [H]elp            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Help Output Design

```
╭─ Vertebra ─────────────────────────────────────────────────────────────────╮
│ High-performance polyglot application backbone                             │
│                                                                            │
│ Vertebra combines Python's productivity with Rust's performance through    │
│ shared-memory IPC, delivering tRPC-like developer experience.              │
╰────────────────────────────────────────────────────────────────────────────╯

Usage: vertebra [OPTIONS] COMMAND [ARGS]...

╭─ Server Commands ──────────────────────────────────────────────────────────╮
│ serve   Run the Bun-backed ASGI server with live TUI dashboard            │
│ dev     Start development server with hot reload                           │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Build Commands ───────────────────────────────────────────────────────────╮
│ build   Build production artifacts (standalone executables, containers)    │
│ bundle  Create deployment bundle with optimized assets                     │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Project Commands ─────────────────────────────────────────────────────────╮
│ init    Initialize a new Vertebra project                                  │
│ config  View or modify project configuration                               │
╰────────────────────────────────────────────────────────────────────────────╯

Options:
  --version  Show version and exit
  --help     Show this message and exit
```

## 5. Implementation Strategy

### 5.1 Phased Approach

**Phase 1: Foundation (Theme + Config)**
- Create `theme.py` with color and icon definitions
- Create `config.py` with rich-click configuration
- Update `__init__.py` to import configuration
- Result: Immediately improved help output

**Phase 2: Components (Error + Progress)**
- Create `errors.py` with CLIError class
- Create `components.py` with reusable UI components
- Update commands to use new components
- Result: Consistent, styled output

**Phase 3: Banner Enhancement**
- Integrate theme with existing banner system
- Improve responsive layouts
- Add gradient/color effects to logo
- Result: Polished startup experience

**Phase 4: TUI Dashboard**
- Create `tui/` package with Textual application
- Implement widgets (status, workers, metrics)
- Add `--tui` flag to serve command
- Result: Live server monitoring

**Phase 5: Polish and Testing**
- Add interactive prompts to init
- Add progress bars to build
- Test across terminal types
- Result: Complete, polished experience

### 5.2 Dependency Strategy

**Core Dependencies (Always Required):**
```toml
dependencies = [
    "rich>=13.0.0",
    "rich-click>=1.7.0",
]
```

**Optional TUI Dependencies:**
```toml
[project.optional-dependencies]
tui = ["textual>=1.0.0"]
```

This allows:
- Basic CLI works without Textual
- TUI features available when Textual installed
- Graceful degradation with helpful messages

### 5.3 Backward Compatibility

**Preserve Existing Behavior:**
- All current commands continue to work
- Banner system remains functional
- Non-TTY environments produce clean output

**Add New Capabilities:**
- `--tui` flag enables dashboard mode
- `--no-color` respects user preference
- JSON output for machine parsing

## 6. Testing Strategy

### 6.1 Visual Testing

- Screenshot comparisons across terminals
- Color rendering verification
- Unicode fallback testing
- Responsive layout breakpoints

### 6.2 Functional Testing

- Command execution with new styling
- Error handling paths
- Progress indicator updates
- TUI widget interactions

### 6.3 Performance Testing

- Startup time benchmarks
- Memory usage in TUI mode
- Update frequency limits
- Large output handling

## 7. Documentation Updates

### 7.1 User Documentation

- Update README with new CLI screenshots
- Add TUI mode documentation
- Document color/theme customization
- Add troubleshooting for terminal issues

### 7.2 Developer Documentation

- Theme system API reference
- Component usage examples
- TUI widget development guide
- Contribution guidelines for UI

## 8. Success Criteria

1. **Visual Appeal:** Help output looks professional and branded
2. **Consistency:** All commands share common styling patterns
3. **Performance:** No noticeable startup delay from styling
4. **Accessibility:** Works in basic terminals with fallbacks
5. **Maintainability:** Centralized theme makes changes easy
6. **User Experience:** TUI dashboard provides genuine value during development

## 9. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Terminal compatibility | Users see broken output | Comprehensive capability detection, ASCII fallbacks |
| Performance overhead | Slow startup | Lazy imports, minimal runtime cost |
| Textual learning curve | Implementation delays | Start with Rich-only, add Textual incrementally |
| Over-engineering | Complex codebase | Focus on reusable patterns, not one-off styling |
| Color blindness | Accessibility issues | Use icons alongside colors, test with simulators |

## 10. References

### Research Sources

- [Rich Python Library Documentation](https://rich.readthedocs.io/en/latest/)
- [Textual TUI Framework](https://textual.textualize.io/)
- [rich-click GitHub](https://github.com/ewels/rich-click)
- [10 Best Python TUI Libraries for 2025](https://medium.com/towards-data-engineering/10-best-python-text-user-interface-tui-libraries-for-2025-79f83b6ea16e)
- [Real Python: Python Textual](https://realpython.com/python-textual/)
- [7 Things Learned Building a Modern TUI Framework](https://www.textualize.io/blog/7-things-ive-learned-building-a-modern-tui-framework/)
- [ArjanCodes: Rich Python Library for CLI Design](https://arjancodes.com/blog/rich-python-library-for-interactive-cli-tools/)

### Similar Implementations

- DMA Accelerator CLI (`/home/cody/code/g/dma/accelerator/src/py/dma/cli/`)
- Litestar CLI (in vertebra's venv)
- Advanced Alchemy CLI
