# Python Package Restructuring & Sprawl Reduction

## Overview
A comprehensive refactoring of `src/py/vertebra` to eliminate module sprawl and consolidate overlapping responsibilities. The goal is to move from a flat, organic structure to a strict **Functional/Feature-based** hierarchy.

## Research Reference
- **Consensus Analysis:** Gemini 3 Pro (Neutral) and Claude Opus 4.5 (Neutral) - Validated the "Functional with Layered Internals" approach.
- **Full Scan Analysis:** Mapped 30+ top-level modules/files to 10 distinct Feature Domains.

## Functional Requirements
### Must Have
- [ ] **Create Common Layer (`vertebra.common`):**
    - `types/`, `typing.py`, `_typing.py` -> `vertebra.common.types`
    - `utils/` -> `vertebra.common.utils`
    - `exceptions.py` -> `vertebra.common.exceptions`
    - `protocols.py` -> `vertebra.common.protocols`
    - `serializers/` -> `vertebra.common.serializers`

- [ ] **Create Core Runtime (`vertebra.core`):**
    - `app.py` -> `vertebra.core.lifecycle`
    - `supervisor.py` -> `vertebra.core.supervisor`
    - `context.py` -> `vertebra.core.context`
    - `server/worker.py` -> `vertebra.core.worker`
    - `main.py` -> `vertebra.core.entrypoint`

- [ ] **Unified Database Domain (`vertebra.database`):**
    - `extensions/sqlspec` -> `vertebra.database.interfaces`
    - `drivers/` -> `vertebra.database.backends` (Merge with extensions logic)

- [ ] **Unified Messaging Domain (`vertebra.messaging`):**
    - `resources/broker.py` -> `vertebra.messaging.broker`
    - `ipc/` -> `vertebra.messaging.transport`
    - `protocol/` -> `vertebra.messaging.protocol`

- [ ] **Unified Security Domain (`vertebra.security`):**
    - `security.py` -> `vertebra.security.client`
    - `auth/` -> `vertebra.security.auth`

- [ ] **Unified Web Domain (`vertebra.web`):**
    - `middleware/` -> `vertebra.web.middleware`
    - `guards/` -> `vertebra.web.guards`
    - `controllers/` -> `vertebra.web.controllers`
    - `routing/` -> `vertebra.web.routing`

- [ ] **Retain Extensions (`vertebra.extensions`):**
    - `extensions/sqlalchemy` (Retain as optional integration)
    - `extensions/advanced_alchemy` (Retain as optional integration)
    - `extensions/litestar` (Retain as optional integration)

- [ ] **New Top-Level Feature Packages:**
    - `vertebra.di` (Move existing `di/`)
    - `vertebra.cli` (Move existing `cli/`)
    - `vertebra.build` (Move existing `build/`)
    - `vertebra.mcp` (Move existing `mcp/`)
    - `vertebra.procedures` (Move existing `procedures/`)
    - `vertebra.storage` (Move existing `storage/`)
    - `vertebra.state` (Consolidate `state.py`, `state_shadow.py`, `sync/`)
    - `vertebra.tasks` (Move existing `tasks/`)
    - `vertebra.events` (Move existing `events/`)

### Should Have
- [ ] **Strict Import Linting:** Enforce `Feature -> Core -> Common` dependency flow.

## Technical Approach
### Recommended Structure
```text
src/py/vertebra/
├── common/                # [BASE] Types, Utils, Serializers, Exceptions
├── core/                  # [RUNTIME] Lifecycle, Supervisor, Worker, Context
├── di/                    # [FEATURE] Dependency Injection
├── security/              # [FEATURE] Auth, Crypto, Security Client
├── database/              # [FEATURE] SQLSpec Interfaces, Drivers (Backends)
├── extensions/            # [OPTIONAL] Third-party Integrations (SQLAlchemy, AA, Litestar)
├── messaging/             # [FEATURE] IPC, Broker, Protocol
├── state/                 # [FEATURE] State Management (Sync, Shadow)
├── storage/               # [FEATURE] Blob/File Storage
├── web/                   # [FEATURE] HTTP Server, Routing, Middleware
├── mcp/                   # [FEATURE] Model Context Protocol
├── procedures/            # [FEATURE] RPC/Procedures
├── tasks/                 # [FEATURE] Background Tasks
├── events/                # [FEATURE] Event Bus
├── cli/                   # [TOOLING] CLI Entry Points
└── build/                 # [TOOLING] Build System
```

### Migration Strategy
1.  **Phase 1: Base & Utils:** Move `common`, `di`, `serializers`.
2.  **Phase 2: Independent Features:** Move `security`, `storage`, `tasks`, `events`, `mcp`, `procedures`.
3.  **Phase 3: Core Infrastructure:** Move `messaging`, `state`, `database`.
4.  **Phase 4: Runtime & Web:** Move `web`, `core`.
5.  **Phase 5: Tooling:** Move `cli`, `build`, cleanup `extensions`.

### Files to Modify
- *All files* in `src/py/vertebra/` will be moved.
- `src/py/tests/` (Massive update to imports).

## Acceptance Criteria
- [ ] Root directory `src/py/vertebra` contains ONLY the 16 packages listed in "Recommended Structure" + `__init__.py`.
- [ ] `drivers/` is merged into `database/backends`.
- [ ] `extensions/` only contains optional third-party integrations.
- [ ] All unit tests pass.

## Out of Scope
- Functional changes to logic.

## Dependencies
### Internal Dependencies
- `native-app-framework` work must be coordinated with `web` and `core` moves.

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Import Cycles | Critical | Critical | Strict phased migration; `TYPE_CHECKING` guards. |
| Merge Conflicts | High | High | Freeze other python development during migration if possible. |

### Recovery Strategy
**Rollback Trigger:** Test suite failure after Phase 3 that cannot be resolved.
**Rollback Steps:** `git reset --hard` to pre-migration state.

## Open Questions
- None.
