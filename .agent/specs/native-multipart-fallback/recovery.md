# Recovery Guide for Native Multipart Fallback

## Quick Resume

Last phase: 8
Current status: Planning Complete
PRD location: specs/active/native-multipart-fallback/

## Intelligence Context

Complexity: complex
Stack: Python 3.12+, TypeScript (Bun), Rust
Commands:
- Test: make test
- Lint: make lint
- Build: make rs-build

Documentation:
- System: None (Markdown docs only)
- Location: docs/
- Skills: .claude/skills/vertebra/

Similar implementations:
- src/py/vertebra/extensions/litestar/native.py
- src/ts/src/ipc/asgi.ts
- src/py/vertebra/server/worker.py
- src/ts/src/server/index.ts
- specs/active/native-pipeline-litestar-compat/enhancements.md

Patterns being followed:
- Capability-first routing with explicit ASGI fallback for multipart
- Shared-memory data plane reserved for bounded payloads
- Structured logging for fallback reasons

## State Summary

### Completed
- Phase 0: Project discovery and command detection
- Phase 1: Complexity assessment (complex, 12+ checkpoints)
- Phase 2: Pattern recognition and similar implementations identified
- Phase 3: Workspace created under specs/active/native-multipart-fallback/
- Phase 4: Analysis written to research/analysis.md
- Phase 5: Research written to research/plan.md (2000+ words)
- Phase 6: PRD written to prd.md (3200+ words)
- Phase 7: Task breakdown written to tasks.md
- Phase 8: Recovery guide written

### Pending
- Implement tasks in tasks.md
- Update docs and skills
- Run validation commands

## Next Steps

Run `/implement native-multipart-fallback` to begin implementation.
