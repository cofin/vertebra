# Implementation Plan - Python Package Restructuring

## Phases

### Phase 1: Base & Utilities
- [ ] Task: Create `vertebra.common` package and sub-packages (`types`, `utils`, `exceptions`, `protocols`)
- [ ] Task: Move `types/` content and `typing.py` to `vertebra.common.types`
- [ ] Task: Move `utils/` content to `vertebra.common.utils`
- [ ] Task: Move `exceptions.py` to `vertebra.common.exceptions`
- [ ] Task: Move `protocols.py` to `vertebra.common.protocols`
- [ ] Task: Move `serializers/` to `vertebra.common.serializers`
- [ ] Task: Update imports in moved files to reflect new locations
- [ ] Task: Update imports in rest of codebase to point to `vertebra.common`
- [ ] Task: Move `di/` to `vertebra.di` (ensure it uses `common`)
- [ ] Task: Flow - User Manual Verification 'Base & Utilities' (Protocol in workflow.md)
- [ ] Task: Checkpoint - Verify Phase 1 complete and create recovery point

### Phase 2: Independent Features
- [ ] Task: Create `vertebra.security` package
- [ ] Task: Move `security.py` to `vertebra.security.client`
- [ ] Task: Move `auth/` to `vertebra.security.auth`
- [ ] Task: Move `storage/` to `vertebra.storage`
- [ ] Task: Move `tasks/` to `vertebra.tasks`
- [ ] Task: Move `events/` to `vertebra.events`
- [ ] Task: Move `mcp/` to `vertebra.mcp`
- [ ] Task: Move `procedures/` to `vertebra.procedures`
- [ ] Task: Update imports for all Phase 2 moves
- [ ] Task: Flow - User Manual Verification 'Independent Features' (Protocol in workflow.md)
- [ ] Task: Checkpoint - Verify Phase 2 complete and create recovery point

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
- [ ] Task: Audit `extensions/` to ensure it only contains integrations.
  - [ ] Task: Retain `extensions/sqlalchemy`
  - [ ] Task: Retain `extensions/advanced_alchemy`
  - [ ] Task: Retain `extensions/litestar`
- [ ] Task: Update `pyproject.toml` entry points
- [ ] Task: Run full test suite to verify everything
- [ ] Task: Flow - User Manual Verification 'Tooling & Final Cleanup' (Protocol in workflow.md)
- [ ] Task: Checkpoint - Verify Phase 5 complete and create recovery point
