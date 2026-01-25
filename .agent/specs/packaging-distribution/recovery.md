# Recovery Guide: Packaging and Distribution System

## Quick Resume

- **Last Phase**: Planning Complete
- **Current Status**: Design approved, ready for implementation
- **PRD Location**: `specs/active/packaging-distribution/`
- **Complexity**: High (12+ checkpoints)

## Intelligence Context

### Complexity

- **Level**: High
- **Phases**: 6 implementation phases
- **Risk**: Medium (cross-platform builds, multi-language coordination)

### Stack

- **Languages**: Python 3.12+, TypeScript (Bun), Rust
- **CLI Framework**: Click + rich-click
- **Build Tools**: maturin, hatch, PyApp
- **HTTP Client**: httpx

### Commands

| Action | Command |
|--------|---------|
| Test Python | `make test` |
| Test TypeScript | `make js-test` |
| Lint All | `make lint-all` |
| Build | `make install` |
| Build Engine | `bun run build:engine` |

### Documentation

- **System**: Sphinx
- **Location**: `docs/`
- **Skills**: `~/.claude/skills/vertebra/`

### Similar Implementations

- `src/py/vertebra/build/bundler.py` - Existing build orchestration
- `src/py/vertebra/build/standalone.py` - PBS download manager
- `src/py/vertebra/build/tools/build-engine.ts` - Engine compilation
- `src/py/vertebra/cli/init.py` - Project scaffolding
- DMA `bundle_python.py` - PyApp pre-bundling pattern
- Hatch `build-hatch.yml` - Cross-platform CI

### Patterns Being Followed

- PyApp with PYAPP_SKIP_INSTALL=true (pre-bundled deps)
- PBS (Python Build Standalone) for portable runtime
- Bun compile: true for single-file engine
- Click CLI patterns for command structure

## State Summary

### Completed

- [x] Phase 0: Project Discovery
- [x] Phase 1: Complexity Assessment (High)
- [x] Phase 2: Pattern Recognition
- [x] Phase 3: Workspace Creation
- [x] Phase 4: Intelligent Analysis (mcp__pal__thinkdeep)
- [x] Phase 5: Research (2400 words)
- [x] Phase 6: PRD Writing (3484 words)
- [x] Phase 7: Task Breakdown (23 tasks)
- [x] Phase 8: Recovery Guide (this document)
- [ ] Phase 9: Git Verification

### Pending

- [ ] Implementation Phase 1-6
- [ ] Testing and documentation
- [ ] CI/CD pipeline creation

## Key Design Decisions

### Build Modes

1. **Directory Build** (`vertebra build`): Creates dist/ with PBS + engine + deps
2. **One-File Build** (`vertebra build --onefile`): Creates self-extracting PyApp executable

### PyApp Configuration

- `PYAPP_SKIP_INSTALL=true` - Dependencies pre-bundled
- `PYAPP_FULL_ISOLATION=true` - Full distribution copies
- `PYAPP_DISTRIBUTION_EMBED=true` - No runtime download
- `PYAPP_EXEC_SPEC=vertebra.cli:main` - Entry point

### Target Platforms

- linux-x64, linux-arm64
- darwin-arm64, darwin-x64
- windows-x64

### Template System

- `default/` - Minimal (existing)
- `api/` - REST API scaffold
- `web/` - Full web app with landing page

### Color Scheme (Litestar)

- Primary: `#1e3a5f` (deep slate blue)
- Accent: `#d4af37` (gold - use sparingly)
- Background: `#f8fafc` (light gray)

## Files to Create

```
src/py/vertebra/
├── build/
│   ├── directory.py       # NEW: Directory bundler
│   ├── onefile.py         # NEW: PyApp builder
│   └── engine.py          # NEW: Engine manager
├── cli/
│   └── engine.py          # NEW: engine subcommand
└── templates/
    ├── api/               # NEW: API template
    └── web/               # NEW: Web template
        ├── pyproject.toml.jinja2
        ├── app.py.jinja2
        ├── handlers.py.jinja2
        ├── templates/
        │   ├── base.html.jinja2
        │   └── index.html.jinja2
        └── static/
            └── styles.css

.github/workflows/
└── release-binaries.yml   # NEW: CI workflow
```

## Files to Modify

- `src/py/vertebra/cli/build.py` - Enhance with --onefile, --target
- `src/py/vertebra/cli/init.py` - Add --template option
- `src/py/vertebra/cli/__init__.py` - Register engine subcommand

## Next Steps

1. **Run Implementation**: `/implement packaging-distribution`

## Critical Checkpoints

### Cross-Compilation Warning

Cross-compilation for multiple platforms (especially Linux ARM64 from x64, and Windows from Linux/macOS) is known to be complex. If you encounter major issues with:

- Rust cross-compilation (maturin + cargo)
- Bun cross-compilation targets
- PyApp targeting different platforms
- glibc version compatibility
- Code signing requirements

**STOP and discuss options with the user before investing significant time.** Potential alternatives include:

1. **Native builds only** - Build on actual target platforms via CI runners
2. **Subset of platforms** - Start with linux-x64 + darwin-arm64, add others later
3. **Docker-based builds** - Use platform-specific containers for cross builds
4. **GitHub Actions matrix** - Let CI handle cross-platform instead of local builds

## Research Highlights

### DMA Pattern (Key Insight)

The Database Assessment project's `bundle_python.py` demonstrates the optimal approach:
1. Download PBS archive
2. Install ALL dependencies into PBS site-packages
3. Copy application code
4. Wrap with PyApp using PYAPP_SKIP_INSTALL=true

This avoids the slow pip install at runtime.

### Hatch Binary Target

Hatch's binary target configuration is already in pyproject.toml:
```toml
[tool.hatch.build.targets.binary]
scripts = ["vertebra"]
pyapp-version = "0.27.0"
```

### Build Performance

- Cold build: 35-150 seconds
- Warm build (cached PBS/engine): 15-80 seconds
- Runtime first launch: 1-3 seconds (extraction)
- Runtime subsequent: <100ms

## Continuation ID

Research session used mcp__pal__thinkdeep for analysis.
No continuation ID needed - planning is complete.
