# Recovery Guide for Native Pipeline Litestar Compatibility

## Quick Resume

Last phase: 8 (Recovery)
Current status: Planning Complete
PRD location: specs/active/native-pipeline-litestar-compat/

## Intelligence Context

Complexity: complex
Stack: Python 3.12+, Bun/TypeScript, Rust
Commands:
- Test: `make test`
- Lint: `make lint` / `make lint-all`
- Build: `make rs-build`, `make js-test`, `make rs-test`

Documentation:
- System: None (Markdown docs in `docs/`)
- Location: `docs/`
- Skills: `~/.codex/skills/vertebra/SKILL.md`, plus Litestar/Vite/Bun/Inertia skills

Similar implementations:
- `src/py/vertebra/extensions/litestar/native.py`
- `src/py/vertebra/extensions/litestar/compiler.py`
- `src/py/vertebra/server/worker.py`
- `src/ts/src/ipc/asgi.ts`
- `src/ts/src/server/router.ts`

Patterns being followed:
- Conservative native routing with explicit ASGI fallback
- Compatibility matrix-driven support
- Static asset interception only for safe Vite output

## State Summary

### Completed
- PRD and research written for native pipeline compatibility
- Analysis and task breakdown created

### Pending
- Implement compatibility classifier and handler resolution fixes
- Expand native response/parameter support
- Add hybrid static file interception
- Build test coverage and compatibility matrix
- Update docs and skills

## Next Steps

Run `/implement native-pipeline-litestar-compat` to begin implementation.
