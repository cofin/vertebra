# Recovery Guide: Server Startup Banner System

## Quick Resume

- **Last Phase**: Implementation Complete
- **Current Status**: Phase 1-5 complete, ready for testing and review
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
- [x] Implementation Phase 1: Core Banner Infrastructure (95 tests passing)
- [x] Implementation Phase 2: Status Collection (TypeScript status endpoint)
- [x] Implementation Phase 3: Pane Rendering (TTY/non-TTY modes)
- [x] Implementation Phase 4: User Extensibility (@banner_section decorator)
- [x] Implementation Phase 5: serve.py integration (--no-banner flag)

### Pending

- [x] Testing agent (96% coverage - exceeds 90% target)
- [ ] Review agent (quality gates)

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

## Files Created

```
src/py/vertebra/cli/banner/
├── __init__.py          ✓ Created
├── renderer.py          ✓ Created
├── collector.py         ✓ Created
├── styles.py            ✓ Created
├── hooks.py             ✓ Created
└── logos.py             ✓ Created

src/ts/src/server/
└── status.ts            ✓ Created

src/py/tests/unit/cli/banner/
├── __init__.py          ✓ Created
├── test_styles.py       ✓ Created
├── test_logos.py        ✓ Created
├── test_hooks.py        ✓ Created
├── test_renderer.py     ✓ Created
└── test_collector.py    ✓ Created

src/ts/tests/unit/
└── status.test.ts       ✓ Created

tools/
└── test_banner_widths.py ✓ Created
```

## Files Modified

- `src/py/vertebra/cli/serve.py` - Uses new banner ✓
- `src/ts/src/server/index.ts` - Registers status endpoint ✓

## Design Samples Pending Review

The PRD includes 6 visual design samples:

1. Full width serve mode
2. Full width dev mode
3. Medium width layout
4. Narrow width layout
5. Non-TTY JSON output
6. User extension example

User should review these samples before implementation begins.

## Current Status

**Testing Complete** - 96% coverage achieved

Tests summary:
- Python: 114 banner-specific tests passing
- TypeScript: 6 status endpoint tests passing
- Coverage: 96% (target: 90%)
- All tests green

Breakdown by module:
- `collector.py`: 100% coverage (21 tests)
- `hooks.py`: 100% coverage (14 tests)
- `logos.py`: 100% coverage (19 tests)
- `renderer.py`: 96% coverage (32 tests)
- `styles.py`: 92% coverage (28 tests)
- `status.ts`: 100% coverage (6 tests)

## Next Steps

Ready for Docs & Vision agent:
- Update documentation
- Run quality gate
- Capture new patterns
- Archive spec

## Continuation ID

Planning session ID: `d3eca634-2813-480c-8b5b-e41103cfbe27`

Use this ID to resume planning if needed:
```
mcp__pal__planner with continuation_id: d3eca634-2813-480c-8b5b-e41103cfbe27
```
