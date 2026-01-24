# Research: Server Startup Banner System

## Executive Summary

This research document explores the design and implementation of a modern, extensible startup banner system for Vertebra. The banner will display server status, component health, broker pools, and runtime information in a visually appealing format that adapts to terminal capabilities and screen size.

---

## 1. Industry Analysis: Modern CLI Banner Patterns

### 1.1 Sanic Framework MOTD

Sanic implements a sophisticated Message of the Day (MOTD) system with the following characteristics:

**Architecture:**
- Abstract `MOTD` base class with `display()` method
- `MOTDBasic` for non-TTY terminals (plain text logging)
- `MOTDTTY` for enhanced terminal display with box drawing

**Key Techniques:**
- Terminal width detection with 108-character fallback
- Dynamic value width calculation: `terminal_width - 108 + 36`
- Box-drawing characters: `┌`, `├`, `└`, `│`, `─`
- Logo integration with line-by-line retrieval
- Text wrapping for long values using `textwrap.wrap()`
- Centering calculation for headers

**Rendering Flow:**
1. Detect TTY capabilities via `sys.stdout.isatty()`
2. Calculate dimensions based on data content
3. Render header with version and serve location
4. Render data rows with key-value alignment
5. Integrate logo on left side
6. Complete with bottom border

### 1.2 Litestar/SQLSpec Design Language

Based on the SQLSpec documentation site and Litestar branding:

**Color Palette:**
- Primary: Deep slate blue (#1e3a5f to #0f172a range)
- Accent: Gold/Amber (#d4af37 to #f59e0b range)
- Success indicators: Green variants
- Warning indicators: Yellow/Amber
- Error indicators: Red variants

**Design Principles:**
- Clean, minimal aesthetic
- High contrast for readability
- Professional appearance suitable for enterprise
- Consistent with documentation branding

### 1.3 Comparison with Other Frameworks

| Framework | Logo Style | Status Display | Extensibility | TTY Detection |
|-----------|------------|----------------|---------------|---------------|
| Sanic | ASCII art | Table format | Limited | Yes |
| FastAPI | None | Simple text | None | No |
| Django | None | Simple text | None | No |
| Rails | ASCII banner | Detailed info | None | Partial |
| Next.js | Unicode art | Compact | None | Yes |

**Key Insight:** Most frameworks lack user extensibility for banner content. This represents an opportunity for Vertebra to differentiate.

---

## 2. Unicode Block Art Design

### 2.1 Character Selection

For the Vertebra logo, we will use Unicode block elements that render consistently across terminals:

**Full Block Characters:**
- `█` (U+2588) - Full block
- `▀` (U+2580) - Upper half block
- `▄` (U+2584) - Lower half block
- `▌` (U+258C) - Left half block
- `▐` (U+2590) - Right half block

**Box Drawing Characters:**
- `─` (U+2500) - Light horizontal
- `━` (U+2501) - Heavy horizontal
- `│` (U+2502) - Light vertical
- `┃` (U+2503) - Heavy vertical

**Status Indicators:**
- `●` (U+25CF) - Filled circle (status dot)
- `○` (U+25CB) - Empty circle
- `▸` (U+25B8) - Right-pointing triangle
- `◆` (U+25C6) - Diamond

### 2.2 Logo Variants

**Full Logo (Width >= 100):**
```
██╗   ██╗███████╗██████╗ ████████╗███████╗██████╗ ██████╗  █╗
██║   ██║██╔════╝██╔══██╗╚══██╔══╝██╔════╝██╔══██╗██╔══██╗██║
██║   ██║█████╗  ██████╔╝   ██║   █████╗  ██████╔╝██████╔╝██║
╚██╗ ██╔╝██╔══╝  ██╔══██╗   ██║   ██╔══╝  ██╔══██╗██╔══██╗██║
 ╚████╔╝ ███████╗██║  ██║   ██║   ███████╗██████╔╝██║  ██║██║
  ╚═══╝  ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═════╝ ╚═╝  ╚═╝╚═╝
```

**Compact Logo (Width 60-99):**
```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█
```

**Minimal Logo (Width < 60):**
```
▐█▌
```
Or simply: `V` with styling

### 2.3 Responsive Design Strategy

The banner adapts based on terminal width detection:

```
Terminal Width Detection:
┌──────────────────────────────────────────────────────────┐
│  shutil.get_terminal_size() → (columns, lines)          │
│                                                          │
│  Breakpoints:                                            │
│  ├── >= 100 cols: Full logo + all panes side-by-side    │
│  ├── 60-99 cols:  Compact logo + stacked panes          │
│  └── < 60 cols:   Minimal "V" + essential info only     │
└──────────────────────────────────────────────────────────┘
```

---

## 3. Architecture Design

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     BANNER SYSTEM ARCHITECTURE                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Python     │    │    Bun       │    │    Rust      │       │
│  │   CLI        │◄───│   Server     │◄───│    IPC       │       │
│  │              │    │              │    │              │       │
│  │ BannerRender │    │ StatusEndpt  │    │ HealthCheck  │       │
│  │ StatusCollect│    │ MetricsAPI   │    │              │       │
│  │ HookRegistry │    │              │    │              │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                   │                   │                │
│         │                   │                   │                │
│         ▼                   ▼                   ▼                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    TERMINAL OUTPUT                       │    │
│  │                                                          │    │
│  │  ┌─────────┐  ┌───────────────┐  ┌────────────────┐     │    │
│  │  │  Logo   │  │  Server Info  │  │  Components    │     │    │
│  │  │  Pane   │  │  Pane         │  │  Pane          │     │    │
│  │  └─────────┘  └───────────────┘  └────────────────┘     │    │
│  │                                                          │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │              User Extensions Pane               │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow

```
Startup Sequence:
═══════════════════════════════════════════════════════════════════

1. User runs: vertebra serve app:app
                    │
                    ▼
2. Python CLI starts
   ├── Parse arguments (host, port, workers)
   ├── Load config file if specified
   └── Initialize supervisor
                    │
                    ▼
3. Supervisor forks Bun engine
   ├── Bun starts HTTP server
   ├── Bun initializes broker
   └── Bun registers status endpoint
                    │
                    ▼
4. Banner renders (initial)
   ├── Static config displayed immediately
   ├── Components show "starting..."
   └── URL displayed
                    │
                    ▼
5. Status collector polls Bun (async)
   ├── GET /internal/status
   ├── Parse response
   └── Update banner (if TTY)
                    │
                    ▼
6. Server running
   └── Banner complete, logs begin
```

### 3.3 Module Structure

```
src/py/vertebra/cli/banner/
├── __init__.py          # Public exports: render_banner, banner_section
├── renderer.py          # BannerRenderer class
│   ├── TTYRenderer      # Box drawing, colors, panes
│   └── BasicRenderer    # Plain text for non-TTY
├── collector.py         # StatusCollector class
│   └── fetch_status()   # HTTP call to Bun /internal/status
├── styles.py            # Color definitions, Unicode chars
│   ├── COLORS           # Litestar color palette
│   └── CHARS            # Unicode characters
├── hooks.py             # Extension registry
│   ├── registry         # Dict of registered sections
│   └── banner_section() # Decorator for user extensions
└── logos.py             # Logo variants
    ├── LOGO_FULL        # Full Unicode block art
    ├── LOGO_COMPACT     # Abbreviated version
    └── LOGO_MINIMAL     # Single character
```

---

## 4. Pane Layout Design

### 4.1 Layout Without Full Borders

Per user requirements, we avoid full content borders that don't resize well. Instead, we use:
- Section headers with underlines
- Indented content
- Subtle separators between sections

**Example Layout (Width >= 100):**
```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█   v0.1.0

Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Listening    http://127.0.0.1:8000
  ▸ Workers      4 processes
  ▸ Log Level    info

Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ● Bun Engine ···················· ready (1.1.0)
  ● Rust IPC ······················ ready (4 channels)
  ● Broker ························ ready (SQL: 10, Redis: 5)

```

**Example Layout (Width 60-99):**
```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█   v0.1.0
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█

Server
  http://127.0.0.1:8000 | 4 workers | info

Components
  ● Bun ····· ready   ● IPC ····· ready
  ● Broker ·· ready

```

**Example Layout (Width < 60):**
```
▐█▌ VERTEBRA v0.1.0
http://127.0.0.1:8000
4 workers | ● ready
```

### 4.2 Dev Mode Display

When running `vertebra dev app:app`:

```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█   v0.1.0 [DEV]

Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Listening    http://127.0.0.1:8000
  ▸ Workers      1 process (auto-reload)
  ▸ Log Level    debug

Development ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Hot Reload   enabled (watching src/)
  ▸ Debug UI     http://127.0.0.1:8000/__debug__
  ▸ Reload       http://127.0.0.1:8000/__reload__

Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ● Bun Engine ···················· ready
  ● Rust IPC ······················ ready
  ● Broker ························ ready

```

---

## 5. Color Implementation

### 5.1 ANSI Color Codes

Click supports color output via `click.style()`. We'll define a palette:

```python
# Litestar-inspired color scheme
COLORS = {
    # Primary colors (256-color mode)
    "slate": (30, 58, 95),      # #1e3a5f - deep blue
    "gold": (212, 175, 55),     # #d4af37 - accent gold

    # Status colors
    "success": (74, 222, 128),  # #4ade80 - green
    "warning": (250, 204, 21),  # #facc15 - yellow
    "error": (248, 113, 113),   # #f87171 - red
    "muted": (148, 163, 184),   # #94a3b8 - gray

    # Text colors
    "text": (241, 245, 249),    # #f1f5f9 - light text
    "dim": (100, 116, 139),     # #64748b - dim text
}
```

### 5.2 Fallback Strategy

For terminals without 256-color support:

```python
COLORS_BASIC = {
    "slate": "blue",
    "gold": "yellow",
    "success": "green",
    "warning": "yellow",
    "error": "red",
    "muted": "white",
    "text": "white",
    "dim": "white",
}
```

Detection via `TERM` environment variable and `COLORTERM`.

---

## 6. User Extension System

### 6.1 Hook API Design

Users can register custom banner sections:

```python
from vertebra import banner_section

@banner_section("API Status")
def api_status() -> dict[str, str]:
    """Return key-value pairs for display in banner."""
    return {
        "Version": "v2.1.0",
        "Endpoints": "42 active",
        "Rate Limit": "1000 req/min",
    }

@banner_section("Database", order=10)
def db_status() -> dict[str, str]:
    return {
        "PostgreSQL": "connected (pool: 10)",
        "Redis": "connected (pool: 5)",
    }
```

### 6.2 Registry Implementation

```python
from collections.abc import Callable
from dataclasses import dataclass, field

@dataclass
class BannerSection:
    name: str
    callback: Callable[[], dict[str, str]]
    order: int = 100

class HookRegistry:
    _sections: list[BannerSection] = field(default_factory=list)

    def register(
        self,
        name: str,
        callback: Callable[[], dict[str, str]],
        order: int = 100,
    ) -> None:
        self._sections.append(BannerSection(name, callback, order))
        self._sections.sort(key=lambda s: s.order)

    def collect(self) -> list[tuple[str, dict[str, str]]]:
        results = []
        for section in self._sections:
            try:
                data = section.callback()
                results.append((section.name, data))
            except Exception:
                results.append((section.name, {"Error": "Failed to collect"}))
        return results

# Global registry
_registry = HookRegistry()

def banner_section(name: str, *, order: int = 100):
    """Decorator to register a banner section."""
    def decorator(func: Callable[[], dict[str, str]]):
        _registry.register(name, func, order)
        return func
    return decorator
```

### 6.3 Integration Points

The extension system integrates at two points:

1. **Import time**: Decorators register sections when module is imported
2. **Render time**: `BannerRenderer.render()` calls `_registry.collect()`

For user sections to appear, the user's app module must be imported before banner renders. This is automatic since `vertebra serve app:app` imports the app module first.

---

## 7. Status Protocol

### 7.1 Bun Status Endpoint

Add to `src/ts/src/server/status.ts`:

```typescript
interface ComponentStatus {
  status: "ready" | "starting" | "degraded" | "error";
  version?: string;
  uptime_ms?: number;
  details?: Record<string, unknown>;
}

interface ServerStatus {
  bun: ComponentStatus;
  broker: ComponentStatus & {
    clients?: number;
    pools?: Record<string, number>;
  };
  ipc: ComponentStatus & {
    channels?: number;
    latency_p99_us?: number;
  };
}

export function createStatusHandler(): RequestHandler {
  return async (req) => {
    const status: ServerStatus = {
      bun: {
        status: "ready",
        version: Bun.version,
        uptime_ms: performance.now(),
      },
      broker: {
        status: broker.isReady() ? "ready" : "starting",
        clients: broker.clientCount(),
        pools: broker.poolStats(),
      },
      ipc: {
        status: "ready",
        channels: workerPool.channelCount(),
      },
    };
    return new Response(JSON.stringify(status), {
      headers: { "Content-Type": "application/json" },
    });
  };
}
```

### 7.2 Python Status Collector

```python
import httpx
from dataclasses import dataclass
from typing import Literal

StatusLevel = Literal["ready", "starting", "degraded", "error"]

@dataclass
class ComponentStatus:
    status: StatusLevel
    version: str | None = None
    uptime_ms: int | None = None
    details: dict | None = None

@dataclass
class ServerStatus:
    bun: ComponentStatus
    broker: ComponentStatus
    ipc: ComponentStatus

class StatusCollector:
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url
        self._client = httpx.Client(timeout=1.0)

    def fetch(self) -> ServerStatus | None:
        try:
            resp = self._client.get(f"{self.base_url}/internal/status")
            data = resp.json()
            return ServerStatus(
                bun=ComponentStatus(**data["bun"]),
                broker=ComponentStatus(**data["broker"]),
                ipc=ComponentStatus(**data["ipc"]),
            )
        except Exception:
            return None
```

---

## 8. TTY vs Non-TTY Behavior

### 8.1 Detection Strategy

```python
import os
import sys

def is_tty() -> bool:
    """Check if stdout is a TTY."""
    return sys.stdout.isatty()

def supports_color() -> bool:
    """Check if terminal supports color."""
    if not is_tty():
        return False
    term = os.environ.get("TERM", "")
    colorterm = os.environ.get("COLORTERM", "")
    return (
        "color" in term.lower() or
        term in ("xterm", "xterm-256color", "screen", "screen-256color") or
        colorterm in ("truecolor", "24bit")
    )

def supports_unicode() -> bool:
    """Check if terminal supports Unicode."""
    if not is_tty():
        return False
    lang = os.environ.get("LANG", "")
    return "UTF-8" in lang.upper() or "UTF8" in lang.upper()
```

### 8.2 Output Modes

**TTY Mode:**
- Full Unicode block art logo
- Colors via ANSI escape codes
- Box-drawing characters
- Status indicators with symbols
- Responsive to terminal width

**Non-TTY Mode (JSON Logging):**
```json
{
  "event": "server.startup",
  "version": "0.1.0",
  "host": "127.0.0.1",
  "port": 8000,
  "workers": 4,
  "components": {
    "bun": "ready",
    "broker": "ready",
    "ipc": "ready"
  }
}
```

This ensures banner information is still captured in log files and CI environments.

---

## 9. Performance Considerations

### 9.1 Startup Time Impact

The banner system should not delay server startup. Strategy:

1. **Immediate render**: Display static config immediately (no IPC needed)
2. **Async status**: Fetch component status after server starts
3. **Optional update**: If TTY, update banner with status; if not, log separately

### 9.2 Status Polling

For long-running servers, status can optionally be refreshed:

```python
# Optional: Update banner periodically (dev mode only)
async def status_watcher(renderer: BannerRenderer):
    while True:
        await asyncio.sleep(5.0)
        status = await collector.fetch_async()
        renderer.update_status(status)
```

This is disabled in production mode to avoid overhead.

---

## 10. Testing Strategy

### 10.1 Unit Tests

```python
# test_renderer.py
def test_tty_detection_false_when_piped(monkeypatch):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    renderer = BannerRenderer()
    assert isinstance(renderer._impl, BasicRenderer)

def test_responsive_breakpoints():
    assert get_logo_variant(120) == "full"
    assert get_logo_variant(80) == "compact"
    assert get_logo_variant(40) == "minimal"

def test_color_fallback():
    colors = get_color_mode("dumb")
    assert colors["gold"] == "yellow"  # Fallback to basic
```

### 10.2 Integration Tests

```python
# test_banner_integration.py
def test_banner_renders_without_bun():
    """Banner should work even if Bun hasn't started yet."""
    renderer = BannerRenderer(mode="dev")
    output = renderer.render(status=None)
    assert "starting" in output.lower()

def test_user_extension_appears():
    @banner_section("Test")
    def test_section():
        return {"Key": "Value"}

    renderer = BannerRenderer()
    output = renderer.render()
    assert "Test" in output
    assert "Key" in output
```

---

## 11. Comparison: What Makes This Different

| Feature | Sanic | FastAPI | Vertebra (Proposed) |
|---------|-------|---------|---------------------|
| Logo | ASCII art | None | Unicode block art |
| Color scheme | Basic | None | Litestar branded |
| Component status | None | None | Real-time health |
| User extensions | None | None | `@banner_section` hook |
| TTY detection | Yes | No | Yes |
| Responsive | Partial | N/A | Full breakpoints |
| Dev/Prod modes | No | No | Yes |
| Non-TTY fallback | Text | N/A | JSON logging |

---

## 12. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Unicode rendering issues | Medium | Low | Fallback to ASCII mode |
| Color display inconsistency | Medium | Low | Test on multiple terminals |
| IPC status delay | Low | Medium | Show "starting" immediately |
| Extension callback errors | Medium | Low | Catch and display error |
| Large logo breaks layout | Low | High | Strict width checking |

---

## 13. Open Questions for User

1. **Logo Design**: Should we commission custom Unicode block art, or adapt an existing pattern?

2. **Status Refresh**: Should dev mode periodically refresh component status in the terminal?

3. **Color Depth**: Should we support 24-bit true color for exact Litestar palette matching, or stick to 256-color for compatibility?

4. **Extension Async**: Should `@banner_section` callbacks support async functions?

5. **Logging Integration**: Should banner use Python logging or direct stdout?

---

## 14. Conclusion

The Server Startup Banner System will significantly improve Vertebra's developer experience by providing:

1. **Visual Identity**: Unicode block art logo with Litestar color scheme
2. **Real-Time Status**: Component health displayed at startup
3. **Extensibility**: User hooks for custom information
4. **Adaptability**: Responsive design for any terminal size
5. **Mode Awareness**: Different displays for dev vs production

The implementation follows established patterns from Sanic's MOTD system while adding modern features like user extensibility and cross-language status collection.

---

*Word count: 2847 words*
