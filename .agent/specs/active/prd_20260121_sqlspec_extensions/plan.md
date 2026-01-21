# Plan: Optimized SQLSpec Extensions for Vertebra Drivers

## Phase 1: Foundation & PostgreSQL Implementation
- [x] Task: Create directory structure for PostgreSQL extensions. [f3b174e]
- [x] Task: Implement PostgreSQL ADK Store (\`sqlspec/postgres/adk/store.py\`). [3e62509]
- [x] Task: Implement PostgreSQL Event Store (\`sqlspec/postgres/events/store.py\`). [3e62509]
- [x] Task: Implement PostgreSQL Litestar Store (\`sqlspec/postgres/litestar/store.py\`). [3e62509]
- [x] Task: Write tests for PostgreSQL extensions. [3e62509]
- [x] Task: Checkpoint - Verify Phase 1 complete and create recovery point. [checkpoint: 3e62509]
- [x] Task: Flow - User Manual Verification 'Foundation & PostgreSQL Implementation' (Protocol in workflow.md).

## Phase 2: SQLite Implementation
- [x] Task: Create directory structure for SQLite extensions. [a832ef5]
- [x] Task: Implement SQLite ADK Store (\`sqlspec/sqlite/adk/store.py\`). [7659038]
- [x] Task: Implement SQLite Event Store (\`sqlspec/sqlite/events/store.py\`). [7659038]
- [x] Task: Implement SQLite Litestar Store (\`sqlspec/sqlite/litestar/store.py\`). [7659038]
- [x] Task: Write tests for SQLite extensions. [7659038]
- [x] Task: Checkpoint - Verify Phase 2 complete and create recovery point. [checkpoint: 7659038]
- [x] Task: Flow - User Manual Verification 'SQLite Implementation' (Protocol in workflow.md).

## Phase 3: MySQL Implementation
- [ ] Task: Create directory structure for MySQL extensions.
- [ ] Task: Implement MySQL ADK Store (`sqlspec/mysql/adk/store.py`).
- [ ] Task: Implement MySQL Event Store (`sqlspec/mysql/events/store.py`).
- [ ] Task: Implement MySQL Litestar Store (`sqlspec/mysql/litestar/store.py`).
- [ ] Task: Write tests for MySQL extensions.
- [ ] Task: Checkpoint - Verify Phase 3 complete and create recovery point.
- [ ] Task: Flow - User Manual Verification 'MySQL Implementation' (Protocol in workflow.md).

## Phase 4: Finalization
- [ ] Task: Ensure all `__init__.py` files correctly export the new stores.
- [ ] Task: Run full test suite for all dialects.
- [ ] Task: Final quality gate check (typing, linting).
- [ ] Task: Checkpoint - Verify Phase 4 complete and create recovery point.
- [ ] Task: Flow - User Manual Verification 'Finalization' (Protocol in workflow.md).
