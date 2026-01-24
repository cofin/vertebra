# PRD-9 Recovery Guide: Build System & Distribution Experience

**Created:** 2026-01-19
**Purpose:** Enable session recovery and implementation continuity

---

## Quick Context

This PRD defines Vertebra's build system and distribution experience:

1. **Native Python embedding** using `pyembed` crate (not external PyApp)
2. **Bun.build() compilation** for standalone executables
3. **Rust supervisor** for GIL-free process management
4. **Project scaffolding** with `vertebra init` command
5. **Unified build** with `vertebra build --one-file`

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Python embedding | pyembed crate | Native API, no source patching |
| Bun compilation | Bun.build() API | Programmatic, cross-platform |
| Supervisor language | Rust | GIL-free, supports uvloop/rloop |
| Build config location | pyproject.toml `[tool.vertebra]` | Standard Python tooling |
| Runtime config location | Python code (`app.py`) | Type-safe, environment-aware |
| Project structure | `src/py/<name>/`, `src/ts/` | Monorepo organization |
| Default profile | modern (x86-64-v3) | Best performance, 2013+ CPUs |

---

## File Inventory

### PRD Workspace
```
specs/active/build-distribution-experience/
├── prd.md              # Main PRD document (3200+ words)
├── tasks.md            # Implementation task breakdown
├── recovery.md         # This file
└── research/
    └── plan.md         # Technical research (2400+ words)
```

### Files to Create (Implementation)
```
src/rs/vertebra-py/src/
├── supervisor.rs       # Rust supervisor (Phase 1)
└── embedded.rs         # pyembed integration (Phase 5)

src/py/vertebra/
├── build/
│   ├── __init__.py     # Builder exports (Phase 4)
│   ├── config.py       # BuildConfig class (Phase 4)
│   └── builder.py      # Builder implementation (Phase 4)
├── cli/
│   ├── init.py         # vertebra init command (Phase 3)
│   └── build.py        # vertebra build command (Phase 4)
└── templates/
    └── default/        # Project templates (Phase 3)

tools/
├── build-engine.ts     # Bun compilation script (Phase 2)
└── bundle-standalone.py # Standalone packager (Phase 5)
```

---

## Implementation Phases

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | Not Started | Rust Supervisor |
| 2 | Not Started | Bun Engine Compilation |
| 3 | Not Started | Project Scaffolding |
| 4 | Not Started | Unified Build Command |
| 5 | Not Started | pyembed Integration (required for --one-file) |
| 6 | Not Started | CI/CD Pipeline |
| 7 | Not Started | npm Package (Optional) |

**Critical Path:** 1 → 2 → 5 → 4 → 6

---

## Consensus Review Summary

PRD reviewed by Gemini 3 Pro and GPT 5.2. Key changes incorporated:

| Issue | Resolution |
|-------|------------|
| Wheel CPU compatibility | PyPI = baseline only; modern = CI artifacts |
| Supervisor spawning | Use sys.executable, not "python" |
| Windows process mgmt | Job objects for child lifecycle |
| Config contradiction | Removed vertebra.toml, use Python objects |
| Cross-platform scope | CI = all targets; local = host only |
| PyPI size limits | Split: vertebra + vertebra-engine packages |
| pyembed mechanics | Added resource layout specification |

---

## User Requirements Summary

From conversation with user:

1. **Zero-build installation**: `pip install vertebra` gives everything
2. **Native integration**: No external PyApp cloning/patching
3. **Bun --compile support**: Integrate with Bun's single-file build
4. **Optimized builds**: Modern by default, baseline for legacy
5. **Offline builds**: Must work without network
6. **Large binary OK**: ~80MB acceptable for UX
7. **Pools in Python code**: NOT in config files (pyproject.toml or vertebra.toml)
8. **Project structure**: `src/py/<name>/`, `src/ts/` pattern (like litestar-fullstack-spa)

---

## Configuration Philosophy

### Build Config (pyproject.toml)
```toml
[tool.vertebra]
profile = "modern"  # or "baseline", "native"

[tool.vertebra.build]
minify = true
bytecode = true
```

### Runtime Config (Python code)
```python
# src/py/myapp/app.py
from vertebra import Vertebra

app = Vertebra()

app.configure_pool(
    name="postgres",
    driver="bun-postgres",
    url=os.environ["DATABASE_URL"],
    max_connections=20,
)
```

**Separation rationale:**
- Build config = project metadata, rarely changes
- Runtime config = environment-specific, needs Python flexibility

---

## Testing Strategy

1. **Unit tests**: Each component independently
2. **Integration tests**: `vertebra build --one-file` produces working binary
3. **Event loop tests**: Verify uvloop/rloop compatibility
4. **Offline tests**: Network-isolated builds work
5. **Cross-platform tests**: All target platforms build and run

---

## Dependencies

### External References
- [Bun Single-file Executables](https://bun.sh/docs/bundler/executables)
- [pyembed Documentation](https://pyoxidizer.readthedocs.io/)
- [python-build-standalone](https://github.com/indygreg/python-build-standalone)

### Internal Dependencies
- PRD-2: Type Bridge (for TypeScript generation)
- PRD-8: Production Readiness (overlapping CI/CD)

---

## Recovery Prompts

If resuming this work, use these prompts:

### Start Implementation
```
Read the PRD at specs/active/build-distribution-experience/prd.md and tasks.md.
Begin implementing Phase 1 (Rust Supervisor) starting with task 1.1.
```

### Continue Implementation
```
Read specs/active/build-distribution-experience/tasks.md and identify the next
incomplete task. Continue implementation from where it left off.
```

### Review Progress
```
Review all files in specs/active/build-distribution-experience/ and summarize
the current implementation status. Identify any blockers or missing components.
```

---

## Quality Gates

Each phase must pass these gates before proceeding:

1. **All tests pass**: `make test-all`
2. **Type checks pass**: `make lint-all`
3. **No regressions**: Existing functionality unaffected
4. **Documentation**: README and docstrings updated

---

## Contact Points

- **PRD Author**: AI Assistant
- **Design Authority**: User (Cody)
- **Reference Repo**: /home/cody/code/litestar/litestar-fullstack-spa (project structure)
- **Reference Bundler**: GoogleCloudPlatform/database-assessment (PyApp pattern)
