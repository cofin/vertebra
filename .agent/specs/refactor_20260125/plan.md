# Implementation Plan - Python Package Restructuring

## Phases

### Phase 1: Base & Utilities
- [x] Task: Create `vertebra.common` package (exceptions, protocols, serializers) 7d01447
- [x] Task: Move `exceptions.py` to `vertebra.common.exceptions` 7d01447
- [x] Task: Move `protocols.py` to `vertebra.common.protocols` 7d01447
- [x] Task: Move `serializers/` to `vertebra.common.serializers` 7d01447
- [x] Task: Keep `utils/`, `typing.py`, `_typing.py` at top level 40eb33f
- [x] Task: Update imports in moved files to reflect new locations 7d01447
- [x] Task: Remove backwards-compatible re-exports, update all imports c972930
- [x] Task: `di/` already at `vertebra.di` (no move needed, self-contained)
- [x] Task: Flow - User Manual Verification 'Base & Utilities' (Protocol in workflow.md)
- [x] Task: Checkpoint - Verify Phase 1 complete and create recovery point 5f5391e

### Phase 2: Independent Features
- [x] Task: Create `vertebra.security` package 5df459a
- [x] Task: Move `security.py` to `vertebra.security.client` 5df459a
- [x] Task: Move `auth/` to `vertebra.security.auth` 5df459a
- [x] Task: `storage/` already at `vertebra.storage` (no move needed)
- [x] Task: `tasks/` already at `vertebra.tasks` (no move needed)
- [x] Task: `events/` already at `vertebra.events` (no move needed)
- [x] Task: `mcp/` already at `vertebra.mcp` (no move needed)
- [x] Task: `procedures/` already at `vertebra.procedures` (no move needed)
- [x] Task: Update imports for all Phase 2 moves 5df459a
- [x] Task: Flow - User Manual Verification 'Independent Features' (skipped - minimal changes)
- [x] Task: Checkpoint - Verify Phase 2 complete 5df459a

### Phase 3: Core Infrastructure (DB, Msg, State)
- [ ] Task: Create `vertebra.database` package
- [ ] Task: Move `drivers/` to `vertebra.database.backends`
- [ ] Task: Move `extensions/sqlspec` to `vertebra.database.interfaces`
- [ ] Task: Create `vertebra.messaging` package
- [ ] Task: Move `resources/broker.py` to `vertebra.messaging.broker`
- [ ] Task: Move `ipc/` to `vertebra.messaging.transport`
- [ ] Task: Move `protocol/` to `vertebra.messaging.protocol`
- [ ] Task: Create `vertebra.state` package
- [ ] Task: Consolidate `state.py`, `state_shadow.py`, `sync/` into `vertebra.state`
- [ ] Task: Update imports for all Phase 3 moves
- [ ] Task: Flow - User Manual Verification 'Core Infrastructure' (Protocol in workflow.md)
- [ ] Task: Checkpoint - Verify Phase 3 complete and create recovery point

### Phase 4: Runtime & Web
- [ ] Task: Create `vertebra.core` package
- [ ] Task: Move `app.py` to `vertebra.core.lifecycle`
- [ ] Task: Move `supervisor.py` to `vertebra.core.supervisor`
- [ ] Task: Move `context.py` to `vertebra.core.context`
- [ ] Task: Move `server/worker.py` to `vertebra.core.worker`
- [ ] Task: Move `main.py` to `vertebra.core.entrypoint`
- [ ] Task: Create `vertebra.web` package
- [ ] Task: Move `middleware/` to `vertebra.web.middleware`
- [ ] Task: Move `guards/` to `vertebra.web.guards`
- [ ] Task: Move `controllers/` to `vertebra.web.controllers`
- [ ] Task: Move `routing/` to `vertebra.web.routing`
- [ ] Task: Update imports for all Phase 4 moves
- [ ] Task: Flow - User Manual Verification 'Runtime & Web' (Protocol in workflow.md)
- [ ] Task: Checkpoint - Verify Phase 4 complete and create recovery point

### Phase 5: Tooling & Final Cleanup
- [ ] Task: Move `cli/` to `vertebra.cli`
- [ ] Task: Move `build/` to `vertebra.build`
- [ ] Task: Rename `types/` → `vertebra.codegen` (TypeBridge feature)
- [ ] Task: Audit `extensions/` to ensure it only contains integrations.
  - [ ] Task: Retain `extensions/sqlalchemy`
  - [ ] Task: Retain `extensions/advanced_alchemy`
  - [ ] Task: Retain `extensions/litestar`
- [ ] Task: Update `pyproject.toml` entry points
- [ ] Task: Run full test suite to verify everything
- [ ] Task: Flow - User Manual Verification 'Tooling & Final Cleanup' (Protocol in workflow.md)
- [ ] Task: Checkpoint - Verify Phase 5 complete and create recovery point
