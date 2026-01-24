# PRD: Server Startup Banner System

## Intelligence Context

### Complexity Assessment

- **Classification**: Medium-High
- **Estimated Phases**: 5
- **Risk Level**: Low (additive feature, no breaking changes)
- **Breaking Changes**: None

### Similar Features Referenced

1. `src/py/vertebra/cli/serve.py` - Current basic banner implementation (replace)
2. `src/ts/src/server/logging.ts` - Bun-side logging patterns (extend)
3. `src/ts/src/server/metrics.ts` - Health/metrics collection patterns (reference)
4. `src/ts/src/broker/server.ts` - Broker status tracking (integrate)
5. Sanic `application/motd.py` - Industry pattern for MOTD systems (inspiration)

### Patterns to Follow

- **CLI Output**: Use `click.style()` and `click.echo()` for terminal output
- **TTY Detection**: Use `sys.stdout.isatty()` for mode selection
- **Status Protocol**: MessagePack over IPC for cross-language data
- **Extension Pattern**: Decorator-based registration similar to Flask/Litestar plugins

### Detected Stack and Commands

- **Languages**: Python 3.12+, TypeScript (Bun)
- **Test Commands**: `make test`, `make js-test`
- **Lint Command**: `make lint-all`
- **Build Command**: `make install`
- **Dependencies**: `click` (already installed), `httpx` (already installed)

---

## Problem Statement

### User Problem Being Solved

When developers start a Vertebra server, they see a minimal ASCII banner with basic configuration. This lacks:

1. **Visual identity**: No distinctive branding that identifies Vertebra
2. **Component health**: No indication if Bun, IPC, or Broker are ready
3. **Runtime information**: No pool sizes, channel counts, or latency metrics
4. **Extensibility**: Users cannot add their own application info to the banner
5. **Mode differentiation**: No distinction between development and production modes
6. **Adaptability**: Banner doesn't adapt to terminal width or capabilities

Modern frameworks like Sanic, Rails, and Next.js provide rich startup displays that inform developers about their environment. Vertebra should match or exceed this experience.

### Business Value

1. **Developer Experience**: First impression of quality and professionalism
2. **Debugging Aid**: Component status visible immediately at startup
3. **Brand Recognition**: Consistent Litestar ecosystem visual identity
4. **Feature Discovery**: Highlight development tools and debug endpoints
5. **Enterprise Readiness**: Professional appearance for production deployments

### Success Criteria

| Criterion | Measurement |
|-----------|-------------|
| Banner displays within 100ms of CLI start | Timing measurement |
| Component status updates within 2s of Bun ready | Status polling test |
| Renders correctly in terminals 40-200 columns wide | Visual verification |
| User extensions appear in output | Integration test |
| Non-TTY mode produces valid JSON | Log parsing test |
| Dev mode shows different content than serve mode | Visual comparison |

---

## Acceptance Criteria

### Functional Requirements

#### FR-1: Unicode Block Art Logo

- [ ] Full logo variant displays at terminal width >= 100 columns
- [ ] Compact logo variant displays at terminal width 60-99 columns
- [ ] Minimal "V" variant displays at terminal width < 60 columns
- [ ] Logo uses Unicode block drawing characters (`█`, `▀`, `▄`, etc.)
- [ ] Logo renders in Litestar gold/amber color when color is supported

#### FR-2: Pane-Based Layout

- [ ] Server info pane shows: URL, workers, log level
- [ ] Components pane shows: Bun Engine, Rust IPC, Broker status
- [ ] Status indicators use filled circle (`●`) with color coding
- [ ] No full borders around entire content (panes use section headers with underlines)
- [ ] Sections separated by heavy horizontal line (`━`)

#### FR-3: Component Status Collection

- [ ] Status collected from Bun via `/internal/status` endpoint
- [ ] Status includes: component state, version, uptime, pool sizes
- [ ] Status updates shown within 2 seconds of components becoming ready
- [ ] Graceful handling when Bun not yet started (show "starting...")
- [ ] Graceful handling on status fetch failure (show "unknown")

#### FR-4: TTY vs Non-TTY Modes

- [ ] TTY mode: Unicode art, colors, responsive layout
- [ ] Non-TTY mode: JSON structured log entry with all config
- [ ] Detection via `sys.stdout.isatty()`
- [ ] Color support detection via `TERM` and `COLORTERM` environment variables
- [ ] Unicode support detection via `LANG` environment variable

#### FR-5: Dev vs Serve Mode

- [ ] `vertebra serve`: Production mode display (muted colors, essential info)
- [ ] `vertebra dev`: Development mode display with:
  - "[DEV]" indicator in header
  - Hot reload status and watched paths
  - Debug endpoint URLs (`/__debug__`, `/__reload__`)
  - Brighter accent colors
  - Single worker by default

#### FR-6: User Extension Hooks

- [ ] `@banner_section(name)` decorator registers custom sections
- [ ] Sections support `order` parameter for positioning
- [ ] Section callbacks return `dict[str, str]` of key-value pairs
- [ ] Callback errors caught and displayed as "Error" status
- [ ] Extensions appear after built-in sections

#### FR-7: Color Scheme

- [ ] Primary color: Deep slate blue (#1e3a5f)
- [ ] Accent color: Gold (#d4af37) - **use sparingly** (hard to see on light backgrounds)
- [ ] Success status: Green (#4ade80)
- [ ] Warning status: Yellow (#facc15)
- [ ] Error status: Red (#f87171)
- [ ] Fallback to basic ANSI colors when 256-color not supported
- [ ] Gold reserved for: version number, "[DEV]" badge only
- [ ] Logo and headers use slate blue or default terminal color

### Non-Functional Requirements

#### NFR-1: Performance

- [ ] Initial banner renders within 100ms of CLI invocation
- [ ] Status polling does not block server startup
- [ ] No CPU overhead when banner is complete
- [ ] Memory footprint under 1MB for banner module

#### NFR-2: Compatibility

- [ ] Works on Linux, macOS, and Windows terminals
- [ ] Degrades gracefully in unsupported terminals
- [ ] Functions correctly in CI environments (non-TTY)
- [ ] Compatible with terminal multiplexers (tmux, screen)

#### NFR-3: Maintainability

- [ ] Logo variants stored as Python string constants
- [ ] Colors defined in single configuration dict
- [ ] Extension API stable and documented
- [ ] Test coverage >= 90% for banner module

---

## Technical Approach

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    STARTUP BANNER ARCHITECTURE                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  CLI Entry (serve.py / dev.py)                                  │
│       │                                                          │
│       ├──► BannerRenderer.render(mode, config)                  │
│       │         │                                                │
│       │         ├──► TTY Detection                              │
│       │         │     └──► TTYRenderer / BasicRenderer          │
│       │         │                                                │
│       │         ├──► Logo Selection (width-based)               │
│       │         │     └──► LOGO_FULL / LOGO_COMPACT / LOGO_MIN  │
│       │         │                                                │
│       │         ├──► Pane Rendering                             │
│       │         │     ├──► Server Info Pane                     │
│       │         │     ├──► Components Pane                      │
│       │         │     └──► Extensions Pane                      │
│       │         │                                                │
│       │         └──► Output to stdout                           │
│       │                                                          │
│       └──► StatusCollector.fetch() (async, non-blocking)        │
│                 │                                                │
│                 └──► HTTP GET /internal/status                  │
│                           │                                      │
│                           └──► Bun Server                       │
│                                   └──► Status Handler           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### File Structure Plan

```
src/py/vertebra/cli/
├── __init__.py           # Unchanged
├── serve.py              # Modified: import and call banner.render()
├── dev.py                # New: dev mode command with hot reload
└── banner/               # New: Banner module
    ├── __init__.py       # Public API: render_banner, banner_section
    ├── renderer.py       # BannerRenderer, TTYRenderer, BasicRenderer
    ├── collector.py      # StatusCollector for Bun health checks
    ├── styles.py         # COLORS dict, CHARS dict, color helpers
    ├── hooks.py          # HookRegistry, banner_section decorator
    └── logos.py          # LOGO_FULL, LOGO_COMPACT, LOGO_MINIMAL

src/ts/src/server/
├── status.ts             # New: /internal/status endpoint handler
└── index.ts              # Modified: register status handler
```

### API Design

#### Python Public API

```python
# vertebra/cli/banner/__init__.py

from vertebra.cli.banner.renderer import render_banner
from vertebra.cli.banner.hooks import banner_section

__all__ = ["render_banner", "banner_section"]
```

```python
# Usage in serve.py
from vertebra.cli.banner import render_banner

def serve(...):
    render_banner(
        mode="serve",
        host=host,
        port=port,
        workers=workers,
        log_level=log_level,
    )
```

```python
# User extension example
from vertebra import banner_section

@banner_section("My App", order=50)
def my_app_status() -> dict[str, str]:
    return {
        "Version": "1.2.3",
        "Environment": os.environ.get("ENV", "development"),
    }
```

#### TypeScript Status Endpoint

```typescript
// src/ts/src/server/status.ts

interface ServerStatus {
  bun: {
    status: "ready" | "starting" | "error";
    version: string;
    uptime_ms: number;
  };
  broker: {
    status: "ready" | "starting" | "error";
    clients: number;
    pools: Record<string, number>;
  };
  ipc: {
    status: "ready" | "starting" | "error";
    channels: number;
  };
}

export function createStatusHandler(
  broker: BrokerServer,
  pool: IpcConnectionPool,
): RequestHandler {
  return async () => {
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
        status: pool.isReady() ? "ready" : "starting",
        channels: pool.channelCount(),
      },
    };
    return Response.json(status);
  };
}
```

### Database Changes

None. This feature does not require database modifications.

### Documentation Updates Required

1. **User Guide**: Add section on banner customization and extensions
2. **API Reference**: Document `@banner_section` decorator
3. **CLI Reference**: Document `--no-banner` flag
4. **Contributing Guide**: Add banner styling guidelines

---

## Testing Strategy

### Unit Tests (90%+ Coverage Target)

```python
# src/py/tests/unit/cli/banner/test_renderer.py

class TestTTYDetection:
    def test_tty_returns_tty_renderer(self, monkeypatch):
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
        renderer = BannerRenderer()
        assert isinstance(renderer._impl, TTYRenderer)

    def test_non_tty_returns_basic_renderer(self, monkeypatch):
        monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
        renderer = BannerRenderer()
        assert isinstance(renderer._impl, BasicRenderer)


class TestResponsiveLayout:
    def test_full_logo_at_100_columns(self):
        assert get_logo_variant(100) == "full"

    def test_compact_logo_at_80_columns(self):
        assert get_logo_variant(80) == "compact"

    def test_minimal_logo_at_40_columns(self):
        assert get_logo_variant(40) == "minimal"


class TestColorFallback:
    def test_256_color_term(self, monkeypatch):
        monkeypatch.setenv("TERM", "xterm-256color")
        assert get_color_mode() == "256"

    def test_basic_color_term(self, monkeypatch):
        monkeypatch.setenv("TERM", "dumb")
        assert get_color_mode() == "basic"


class TestExtensionHooks:
    def test_section_registration(self):
        registry = HookRegistry()

        @registry.section("Test")
        def test_section():
            return {"Key": "Value"}

        sections = registry.collect()
        assert len(sections) == 1
        assert sections[0][0] == "Test"

    def test_section_ordering(self):
        registry = HookRegistry()
        registry.register("B", lambda: {}, order=20)
        registry.register("A", lambda: {}, order=10)
        sections = registry.collect()
        assert [s[0] for s in sections] == ["A", "B"]

    def test_callback_error_handling(self):
        registry = HookRegistry()
        registry.register("Broken", lambda: 1/0)
        sections = registry.collect()
        assert "Error" in sections[0][1]
```

```typescript
// src/ts/tests/server/status.test.ts

describe("Status Handler", () => {
  it("returns ready status when all components healthy", async () => {
    const handler = createStatusHandler(mockBroker, mockPool);
    const response = await handler(new Request("http://localhost/internal/status"));
    const status = await response.json();

    expect(status.bun.status).toBe("ready");
    expect(status.broker.status).toBe("ready");
    expect(status.ipc.status).toBe("ready");
  });

  it("returns starting status when broker initializing", async () => {
    mockBroker.isReady.mockReturnValue(false);
    const handler = createStatusHandler(mockBroker, mockPool);
    const response = await handler(new Request("http://localhost/internal/status"));
    const status = await response.json();

    expect(status.broker.status).toBe("starting");
  });
});
```

### Integration Tests

```python
# src/py/tests/integration/cli/test_banner.py

class TestBannerIntegration:
    def test_banner_renders_before_bun_ready(self, capsys):
        """Banner should display immediately without waiting for Bun."""
        render_banner(mode="serve", host="127.0.0.1", port=8000, workers=4)
        captured = capsys.readouterr()
        assert "127.0.0.1:8000" in captured.out
        assert "starting" in captured.out.lower()

    def test_banner_with_user_extension(self, capsys):
        """User extensions should appear in banner output."""
        @banner_section("Custom")
        def custom():
            return {"Test": "Value"}

        render_banner(mode="serve", host="127.0.0.1", port=8000, workers=4)
        captured = capsys.readouterr()
        assert "Custom" in captured.out
        assert "Test" in captured.out

    def test_dev_mode_shows_debug_endpoints(self, capsys):
        """Dev mode should show debug endpoint URLs."""
        render_banner(mode="dev", host="127.0.0.1", port=8000, workers=1)
        captured = capsys.readouterr()
        assert "[DEV]" in captured.out
        assert "__debug__" in captured.out
```

### Visual Verification Tests

Create a test script that outputs banner in various terminal widths:

```python
# tools/test_banner_widths.py

import os
import sys

sys.path.insert(0, "src/py")

from vertebra.cli.banner import render_banner

for width in [120, 100, 80, 60, 40]:
    print(f"\n{'='*width}")
    print(f"WIDTH: {width}")
    print(f"{'='*width}")
    os.environ["COLUMNS"] = str(width)
    render_banner(mode="serve", host="127.0.0.1", port=8000, workers=4)
```

---

## Implementation Notes

### Pattern Deviations (with Rationale)

1. **No full borders**: Unlike Sanic's MOTD, we avoid full borders around content per user request. This improves copy-paste behavior and narrow terminal rendering.

2. **Async status collection**: Unlike immediate blocking, we render static info first then update with status. This ensures fast startup perception.

3. **Python-only rendering**: Unlike architectures where each component prints its own status, Python CLI owns all terminal output. Bun only provides data via IPC.

### Dependencies

**Python (existing):**
- `click` - CLI framework, already used for commands
- `httpx` - HTTP client for status fetching, already installed

**Python (new):**
- None required

**TypeScript (existing):**
- None new required

### Migration Considerations

This is an additive feature. The existing banner in `serve.py` will be replaced, but the command interface remains unchanged.

**Migration Path:**
1. Add banner module alongside existing code
2. Update `serve.py` to use new banner
3. Remove old `BANNER` constant and `print_banner()` function
4. Add `dev.py` command

### Rollback Strategy

Add `--no-banner` flag to disable banner output:

```python
@click.option("--no-banner", is_flag=True, help="Disable startup banner")
def serve(..., no_banner: bool):
    if not no_banner:
        render_banner(...)
```

---

## Visual Design Samples

### Sample 1: Full Width (>= 100 columns) - Serve Mode

```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█   v0.1.0

Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Listening    http://127.0.0.1:8000
  ▸ Workers      4 processes
  ▸ Log Level    info

Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ● Bun Engine ···················· ready (1.1.38)
  ● Rust IPC ······················ ready (4 channels)
  ● Broker ························ ready (SQL: 10, Redis: 5)

```

### Sample 2: Full Width (>= 100 columns) - Dev Mode

```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█   v0.1.0 [DEV]

Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Listening    http://127.0.0.1:8000
  ▸ Workers      1 process (auto-reload)
  ▸ Log Level    debug

Development ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Hot Reload   enabled (watching: src/, app/)
  ▸ Debug UI     http://127.0.0.1:8000/__debug__
  ▸ Reload       http://127.0.0.1:8000/__reload__

Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ● Bun Engine ···················· ready
  ● Rust IPC ······················ ready
  ● Broker ························ ready

```

### Sample 3: Medium Width (60-99 columns)

```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█   v0.1.0
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█

Server
  http://127.0.0.1:8000 | 4 workers | info

Components
  ● Bun ····· ready   ● IPC ····· ready
  ● Broker ·· ready

```

### Sample 4: Narrow Width (< 60 columns)

```
▐█▌ VERTEBRA v0.1.0
http://127.0.0.1:8000
4 workers | ● ready
```

### Sample 5: Non-TTY Mode (JSON)

```json
{"event":"server.startup","version":"0.1.0","host":"127.0.0.1","port":8000,"workers":4,"log_level":"info","mode":"serve"}
```

### Sample 6: With User Extension

```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█   v0.1.0

Server ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ Listening    http://127.0.0.1:8000
  ▸ Workers      4 processes
  ▸ Log Level    info

Components ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ● Bun Engine ···················· ready
  ● Rust IPC ······················ ready
  ● Broker ························ ready

My Application ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ API Version  v2.1.0
  ▸ Environment  production
  ▸ Features     auth, payments, notifications

```

---

## Verification Checklist

### Functional Verification

- [ ] `make test` passes
- [ ] `make js-test` passes
- [ ] `make lint-all` passes
- [ ] Banner renders at all width breakpoints
- [ ] User extensions appear in output
- [ ] Non-TTY mode produces valid JSON
- [ ] Dev mode shows different content than serve mode

### Visual Verification

- [ ] Logo renders correctly in full width
- [ ] Logo renders correctly in compact width
- [ ] Logo renders correctly in minimal width
- [ ] Colors display correctly in 256-color terminal
- [ ] Colors fallback correctly in basic terminal
- [ ] Status indicators show correct colors

### Performance Verification

- [ ] Banner renders within 100ms of CLI start
- [ ] Status updates within 2s of Bun ready
- [ ] No CPU overhead when banner complete

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Unicode rendering issues on Windows | Medium | Low | Test on Windows Terminal, provide ASCII fallback |
| Color inconsistency across terminals | Medium | Low | Test on popular terminals, document requirements |
| Status endpoint not available | Low | Medium | Show "starting..." gracefully, timeout after 5s |
| Extension callback hangs | Low | High | Timeout callbacks at 1s, show error |
| Logo too wide for terminal | Low | Medium | Strict width detection, fallback to minimal |

---

## Implementation Phases

### Phase 1: Core Banner Infrastructure

**Tasks:**
- Create `src/py/vertebra/cli/banner/` module structure
- Implement `TTYRenderer` with terminal detection
- Create logo variants in `logos.py`
- Implement responsive width detection
- Write unit tests for renderer

**Outputs:**
- Static banner renders at all widths
- TTY/non-TTY modes working
- Test coverage for core module

### Phase 2: Status Collection

**Tasks:**
- Add `/internal/status` endpoint to Bun server
- Implement `StatusCollector` in Python
- Define status protocol with dataclasses
- Handle connection failures gracefully
- Write integration tests

**Outputs:**
- Bun returns health status
- Python fetches and displays status
- Graceful degradation on failure

### Phase 3: Pane Rendering

**Tasks:**
- Implement section header rendering
- Implement Server info pane
- Implement Components pane with status indicators
- Add dotted leader lines for alignment
- Implement color scheme

**Outputs:**
- All panes render correctly
- Status colors working
- Alignment consistent

### Phase 4: User Extensibility

**Tasks:**
- Implement `HookRegistry` class
- Create `@banner_section` decorator
- Add extension collection during render
- Handle callback errors
- Document extension API

**Outputs:**
- Users can add custom sections
- Extensions ordered correctly
- Errors handled gracefully

### Phase 5: Dev Mode and Polish

**Tasks:**
- Create `vertebra dev` command
- Add Development pane with dev-mode info
- Implement `--no-banner` flag
- Add visual verification tests
- Final documentation

**Outputs:**
- Dev mode fully functional
- Banner can be disabled
- Documentation complete

---

## Appendix A: Logo Design Details

### Unicode Block Art Technique

The Vertebra logo uses Unicode block elements to create a distinctive visual identity. Unlike ASCII art which uses slashes and backslashes, Unicode blocks create solid, modern-looking text.

**Character Mapping for "VERTEBRA" in Compact Style:**

```
Letter V: █░█    Letter E: █▀▀    Letter R: █▀█    Letter T: ▀█▀
          ▀▄▀              ██▄              █▀▄              █

Letter B: █▄▄    Letter A: ▄▀█
          █▄█              █▀█
```

**Rendering Considerations:**

1. **Font Support**: Most modern monospace fonts support these characters
2. **Width Consistency**: Each letter block is 3 characters wide
3. **Height Consistency**: Each letter block is 2 lines tall
4. **Color Application**: Color applied per-line, not per-character

### Alternative Logo Styles Considered

**Style A: Full Block Letters (Selected for Full Width)**
```
██╗   ██╗███████╗██████╗ ████████╗███████╗██████╗ ██████╗  █╗
██║   ██║██╔════╝██╔══██╗╚══██╔══╝██╔════╝██╔══██╗██╔══██╗██║
```
Pros: Maximum visual impact, clear letterforms
Cons: Wide (60+ characters), may not fit narrow terminals

**Style B: Compact Block Letters (Selected for Medium Width)**
```
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█
```
Pros: Compact (35 characters), readable, modern
Cons: Less impactful than full block

**Style C: Minimal (Selected for Narrow Width)**
```
▐█▌
```
Pros: Only 3 characters, works everywhere
Cons: Not recognizable as "Vertebra"

### Color Implementation Details

**256-Color Mode (xterm-256color):**
```python
# Using RGB values converted to 256-color palette
GOLD_256 = 178   # Closest to #d4af37 - USE SPARINGLY
SLATE_256 = 24   # Closest to #1e3a5f
GREEN_256 = 78   # Closest to #4ade80
```

**True Color Mode (COLORTERM=truecolor):**
```python
# Direct RGB values
GOLD_RGB = (212, 175, 55)   # Use sparingly - poor visibility on light backgrounds
SLATE_RGB = (30, 58, 95)
GREEN_RGB = (74, 222, 128)
```

**Gold Usage Guidelines:**
- Version number (e.g., "v0.1.0")
- "[DEV]" badge in development mode
- NOT for: logo, section headers, body text

**ANSI Escape Sequences:**
```python
def color_text(text: str, r: int, g: int, b: int) -> str:
    return f"\033[38;2;{r};{g};{b}m{text}\033[0m"

def color_256(text: str, code: int) -> str:
    return f"\033[38;5;{code}m{text}\033[0m"
```

---

## Appendix B: Terminal Compatibility Matrix

| Terminal | Unicode | 256-Color | True Color | Notes |
|----------|---------|-----------|------------|-------|
| macOS Terminal | Yes | Yes | No | Fallback to 256 |
| iTerm2 | Yes | Yes | Yes | Full support |
| Windows Terminal | Yes | Yes | Yes | Full support |
| CMD.exe | Limited | No | No | ASCII fallback |
| GNOME Terminal | Yes | Yes | Yes | Full support |
| Konsole | Yes | Yes | Yes | Full support |
| VS Code Terminal | Yes | Yes | Yes | Full support |
| tmux | Yes | Yes | Partial | Depends on config |
| screen | Yes | Yes | No | Fallback to 256 |
| SSH | Depends | Depends | Depends | Inherits from client |

### Detection Algorithm

```python
def detect_terminal_capabilities() -> TerminalCaps:
    """Detect terminal capabilities for banner rendering."""
    is_tty = sys.stdout.isatty()
    term = os.environ.get("TERM", "")
    colorterm = os.environ.get("COLORTERM", "")
    lang = os.environ.get("LANG", "")

    return TerminalCaps(
        is_tty=is_tty,
        supports_unicode="UTF-8" in lang.upper() or "UTF8" in lang.upper(),
        supports_256_color="256color" in term or is_tty,
        supports_true_color=colorterm in ("truecolor", "24bit"),
        width=shutil.get_terminal_size().columns,
    )
```

---

## Appendix C: Extension API Reference

### Decorator Signature

```python
def banner_section(
    name: str,
    *,
    order: int = 100,
    mode: Literal["all", "serve", "dev"] = "all",
) -> Callable[[SectionCallback], SectionCallback]:
    """
    Register a function as a banner section.

    Args:
        name: Display name for the section header
        order: Sort order (lower = earlier). Built-in sections use 0-50.
        mode: Which modes to display in ("all", "serve", or "dev")

    Returns:
        Decorated function unchanged

    Example:
        @banner_section("Database", order=60)
        def db_status() -> dict[str, str]:
            return {"PostgreSQL": "connected", "Redis": "connected"}
    """
```

### Callback Requirements

Section callbacks must:
1. Take no arguments
2. Return `dict[str, str]` mapping labels to values
3. Complete within 1 second (timeout applied)
4. Handle their own errors (or let them propagate for error display)

### Built-in Section Order

| Section | Order | Mode |
|---------|-------|------|
| Server | 10 | all |
| Development | 20 | dev |
| Components | 30 | all |
| User Extensions | 100+ | all |

---

## Appendix D: Message Protocol Specification

### Status Request/Response

**Request:**
```
GET /internal/status HTTP/1.1
Host: 127.0.0.1:8000
```

**Response:**
```json
{
  "bun": {
    "status": "ready",
    "version": "1.1.38",
    "uptime_ms": 1234
  },
  "broker": {
    "status": "ready",
    "clients": 3,
    "pools": {
      "sql": 10,
      "redis": 5
    }
  },
  "ipc": {
    "status": "ready",
    "channels": 4,
    "latency_p99_us": 8
  }
}
```

### Status Values

| Status | Meaning | Display Color |
|--------|---------|---------------|
| `ready` | Component fully operational | Green |
| `starting` | Component initializing | Yellow |
| `degraded` | Partially operational | Yellow |
| `error` | Component failed | Red |

---

*Word count: 3842 words*
