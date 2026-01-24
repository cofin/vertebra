# CLI Polish Analysis

## Analysis Method: Multi-Step Planning with Sequential Thinking

### Phase 1: Current State Assessment

**Vertebra CLI Structure:**
- Entry point: `src/py/vertebra/cli/__init__.py`
- Commands: `serve`, `build`, `init`
- Banner system: `src/py/vertebra/cli/banner/` (renderer, styles, logos, hooks, collector)
- Dependencies: `rich-click` (basic configuration only)

**Current Issues Identified:**
1. **Minimal rich-click configuration** - Only 3 lines of configuration vs 45+ in accelerator
2. **No command grouping** - Commands appear as flat list without organization
3. **No color theming** - No defined color palette or brand identity
4. **Plain help output** - No styled descriptions, no markdown support
5. **Basic error handling** - Using `click.secho()` instead of rich panels
6. **No progress indicators** - Build and init commands lack visual feedback
7. **No live TUI** - Serve mode shows static banner, no live updates

### Phase 2: Target State (Accelerator Reference)

**What Accelerator Does Well:**
1. **Rich theming** - Google Cloud branding colors (`#4285F4`, `#34A853`, `#FBBC04`, `#EA4335`)
2. **Command groups** - Organized by function (Assessment, Administration)
3. **Styled panels** - Borders, headers, and content all styled
4. **Formatted tables** - Proper spacing, alignment, color coding
5. **Progress spinners** - `console.status()` with dots spinner
6. **Error handling** - `CLIError` class with hints and exit codes
7. **Left-aligned rules** - Section headers with styled rules
8. **Configuration display** - Smart formatting based on value types

### Phase 3: Enhancement Opportunities

**Beyond Accelerator - TUI Runtime Mode:**
1. **Textual dashboard** - Live server status during `vertebra serve`
2. **Real-time metrics** - Request rate, latency, memory usage
3. **Worker status** - Show Python worker health
4. **Log viewer** - Scrollable, filterable log output
5. **Interactive controls** - Restart workers, change log level

### Phase 4: Technical Requirements

**Dependencies to Add:**
- `rich>=13.0.0` (explicit, not just via rich-click)
- `textual>=1.0.0` (for TUI runtime)

**Components to Create:**
1. **Theme System** - Centralized color and style definitions
2. **CLI Configuration** - rich-click styling module
3. **Progress Components** - Reusable spinners, progress bars
4. **Error Handling** - CLIError with hints, exit codes
5. **TUI Dashboard** - Textual-based live server view
6. **Widget Library** - Reusable status indicators, tables

### Phase 5: Complexity Assessment

**Complexity Level: COMPLEX**
- Multiple interacting systems (CLI + TUI)
- New dependency integration (Textual)
- Architectural changes (theme system, error handling)
- 10+ files to create/modify
- Estimated tasks: 12+ checkpoints

## Key Patterns from Accelerator

### Color Palette
```python
ERROR_COLOR = "#EA4335"   # Google Red
WARNING_COLOR = "#FBBC04" # Google Yellow
SUCCESS_COLOR = "#34A853" # Google Green
INFO_COLOR = "#4285F4"    # Google Blue
```

### Rich-Click Configuration
```python
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.STYLE_COMMAND = "bold #4285F4"
click.rich_click.STYLE_OPTION = "bold #34A853"
click.rich_click.WIDTH = 120
click.rich_click.COMMAND_GROUPS = {...}
```

### Error Handling Pattern
```python
class CLIError(click.ClickException):
    def __init__(self, message, hint=None, exit_code=1):
        self.hint = hint
        self.exit_code = exit_code

    def show(self):
        panel = Panel(content, title="Error", border_style=ERROR_STYLE)
        error_console.print(panel)
```

### Status Display Pattern
```python
with console.status("[yellow]Checking...", spinner="dots"):
    result = await check_something()

table = Table(show_header=False, box=None, padding=(0, 2))
table.add_row("Setting", f"[cyan]{value}[/cyan]")
console.print(Panel(table, title="[bold]Settings[/bold]"))
```

## Proposed Vertebra Design System

### Brand Colors (Litestar-aligned)
```python
COLORS = {
    "primary": "#6366f1",    # Indigo - primary brand
    "accent": "#d4af37",     # Gold - accent (from existing)
    "success": "#4ade80",    # Green
    "warning": "#facc15",    # Yellow
    "error": "#f87171",      # Red
    "info": "#60a5fa",       # Blue
    "muted": "#94a3b8",      # Gray
    "text": "#f1f5f9",       # Light text
    "dim": "#64748b",        # Dim text
    "surface": "#1e293b",    # Dark surface
}
```

### TUI Architecture

```
┌─────────────────────────────────────────────────────────┐
│ VERTEBRA v0.1.0                    [http://127.0.0.1:8000] │
├─────────────────────────────────────────────────────────┤
│ ● Bun Engine      ready   1.0.0                          │
│ ● Rust IPC        ready   4 channels                     │
│ ● Broker          ready   pg: 10, redis: 5               │
├─────────────────────────────────────────────────────────┤
│ Workers                                                  │
│ ┌─────┬─────────┬─────────┬─────────┐                   │
│ │ PID │ Status  │ Requests│ Memory  │                   │
│ ├─────┼─────────┼─────────┼─────────┤                   │
│ │ 1234│ ● ready │ 1,234   │ 45.2 MB │                   │
│ │ 1235│ ● ready │ 1,189   │ 43.8 MB │                   │
│ └─────┴─────────┴─────────┴─────────┘                   │
├─────────────────────────────────────────────────────────┤
│ Request Rate: 523/s   P99 Latency: 2.3ms                 │
├─────────────────────────────────────────────────────────┤
│ [Q]uit  [R]estart Workers  [L]og Level  [H]elp          │
└─────────────────────────────────────────────────────────┘
```

## Files to Create/Modify

### New Files
1. `src/py/vertebra/cli/theme.py` - Centralized theme definitions
2. `src/py/vertebra/cli/config.py` - rich-click configuration
3. `src/py/vertebra/cli/errors.py` - CLIError and error handling
4. `src/py/vertebra/cli/components.py` - Reusable UI components
5. `src/py/vertebra/cli/tui/__init__.py` - TUI package
6. `src/py/vertebra/cli/tui/app.py` - Main Textual application
7. `src/py/vertebra/cli/tui/widgets.py` - Custom TUI widgets
8. `src/py/vertebra/cli/tui/screens.py` - TUI screens

### Modified Files
1. `src/py/vertebra/cli/__init__.py` - Import config, add command groups
2. `src/py/vertebra/cli/serve.py` - Add TUI mode, improve error handling
3. `src/py/vertebra/cli/build.py` - Add progress bar, improve output
4. `src/py/vertebra/cli/init.py` - Add styled prompts, progress
5. `src/py/vertebra/cli/banner/styles.py` - Integrate with theme
6. `src/py/vertebra/cli/banner/renderer.py` - Use new components
7. `pyproject.toml` - Add dependencies

## Decision: Textual vs Pure Rich

**Recommendation: Hybrid Approach**

1. **CLI Mode (default)** - Pure Rich for static output
   - Fast startup
   - No extra dependencies in basic usage
   - Compatible with CI/CD pipelines
   - Works in all terminals

2. **TUI Mode (--tui flag)** - Textual for live dashboard
   - `vertebra serve --tui` activates dashboard
   - Requires TTY terminal
   - Optional dependency (textual in extras)
   - Full interactivity

This allows users to choose their experience level while keeping the core CLI fast and lightweight.
