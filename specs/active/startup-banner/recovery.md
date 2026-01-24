# Recovery Guide: Server Startup Banner System

## Quick Resume

- **Last Phase**: Planning Complete
- **Current Status**: Design approved, ready for implementation
- **PRD Location**: `specs/active/startup-banner/`
- **Design Approval**: User approved compact logo style, gold used sparingly (version, [DEV] badge only)

## Intelligence Context

### Complexity

- **Level**: Medium-High
- **Phases**: 5 implementation phases
- **Risk**: Low (additive feature)

### Stack

- **Languages**: Python 3.12+, TypeScript (Bun)
- **CLI Framework**: Click
- **HTTP Client**: httpx

### Commands

| Action | Command |
|--------|---------|
| Test Python | `make test` |
| Test TypeScript | `make js-test` |
| Lint All | `make lint-all` |
| Build | `make install` |

### Documentation

- **System**: Sphinx
- **Location**: `docs/`
- **Skills**: `~/.claude/skills/vertebra/`

### Similar Implementations

- `src/py/vertebra/cli/serve.py` - Current banner (to replace)
- `src/ts/src/server/logging.ts` - Logging patterns
- `src/ts/src/server/metrics.ts` - Metrics collection
- Sanic `application/motd.py` - Industry pattern

### Patterns Being Followed

- Click CLI patterns for command structure
- TTY detection via `sys.stdout.isatty()`
- Decorator-based registration (like Flask/Litestar plugins)
- msgspec/dataclasses for status protocol

## State Summary

### Completed

- [x] Phase 0: Project Discovery
- [x] Phase 1: Complexity Assessment (Medium-High)
- [x] Phase 2: Pattern Recognition
- [x] Phase 3: Workspace Creation
- [x] Phase 4: Intelligent Analysis (mcp__pal__planner)
- [x] Phase 5: Research (2563 words)
- [x] Phase 6: PRD Writing (3491 words)
- [x] Phase 7: Task Breakdown (21 tasks)
- [x] Phase 8: Recovery Guide (this document)
- [x] Phase 9: Git Verification

### Pending

- [ ] User review and approval of design samples
- [ ] Implementation Phase 1-5
- [ ] Testing and documentation

## Key Design Decisions

### Logo Variants

1. **Full** (>= 100 cols): Unicode block art
2. **Compact** (60-99 cols): Abbreviated blocks
3. **Minimal** (< 60 cols): Single "V" character

### Layout

- **No full borders** - uses section headers with underlines
- **Pane-based** - Server, Components, Extensions sections
- **Responsive** - adapts to terminal width

### Color Scheme (Litestar)

- Primary: Deep slate blue (#1e3a5f)
- Accent: Gold (#d4af37) - **use sparingly** (poor visibility on light backgrounds)
- Gold reserved for: version number, "[DEV]" badge only
- Logo/headers: slate blue or default terminal color
- Status: Green/Yellow/Red for ready/warning/error

### Status Collection

- Bun provides `/internal/status` endpoint
- Python fetches via httpx
- Graceful degradation on failure

### User Extensions

- `@banner_section(name, order)` decorator
- Returns `dict[str, str]` of key-value pairs
- Appears after built-in sections

## Files to Create

```
src/py/vertebra/cli/banner/
├── __init__.py
├── renderer.py
├── collector.py
├── styles.py
├── hooks.py
└── logos.py

src/ts/src/server/
└── status.ts
```

## Files to Modify

- `src/py/vertebra/cli/serve.py` - Use new banner
- `src/py/vertebra/cli/__init__.py` - Add dev command
- `src/ts/src/server/index.ts` - Register status endpoint

## Design Samples Pending Review

The PRD includes 6 visual design samples:

1. Full width serve mode
2. Full width dev mode
3. Medium width layout
4. Narrow width layout
5. Non-TTY JSON output
6. User extension example

User should review these samples before implementation begins.

## Next Steps

1. **User Review**: Review design samples in PRD
2. **Approve Design**: Confirm logo style and color scheme
3. **Run Implementation**: `/implement startup-banner`

## Continuation ID

Planning session ID: `d3eca634-2813-480c-8b5b-e41103cfbe27`

Use this ID to resume planning if needed:
```
mcp__pal__planner with continuation_id: d3eca634-2813-480c-8b5b-e41103cfbe27
```
