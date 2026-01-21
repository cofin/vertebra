# PRD: Optimized SQLSpec Extensions for Vertebra Drivers

## Overview
Implement optimized ADK, Events, and Litestar session store extensions for Vertebra's SQL drivers (Postgres, MySQL, SQLite). These extensions will be located within the dialect-specific subpackages of `vertebra.extensions.sqlspec` to mirror the internal structure of the `sqlspec` library's adapters.

## Research Reference
No prior research conducted (Direct implementation from `sqlspec` internal patterns).

## Functional Requirements
### Must Have
- [ ] **Dialect-Specific ADK Stores:** Implement `ADKStore` and `ADKMemoryStore` for `postgres`, `mysql`, and `sqlite` drivers.
- [ ] **Dialect-Specific Litestar Stores:** Implement `LitestarStore` for `postgres`, `mysql`, and `sqlite` drivers.
- [ ] **Dialect-Specific Event Stores:** Implement event storage backends for `postgres`, `mysql`, and `sqlite`.
- [ ] **Vertebra Driver Integration:** Stores must utilize Vertebra's `AsyncDriver` and `SyncDriver` connection/session provisioning.
- [ ] **Package Structure:** Follow the `vertebra.extensions.sqlspec.<dialect>.<extension>.store` pattern.

### Should Have
- [ ] **Optimized SQL:** Use dialect-specific optimizations (e.g., `JSONB` and `ON CONFLICT` for Postgres, `JSON` and `INSERT OR REPLACE` for SQLite).
- [ ] **Strict Typing:** All implementations must pass Vertebra's strict type checking.

## Non-Functional Requirements
- **Performance:** Ensure zero-copy or minimal-overhead data handling where possible.
- **Consistency:** Maintain parity with the original `sqlspec` adapter implementations.

## Technical Approach
### Recommended Implementation
- Mirror the `sqlspec` library's internal adapter structure inside Vertebra's extension folders.
- For each dialect (`postgres`, `mysql`, `sqlite`):
  - `vertebra/extensions/sqlspec/<dialect>/adk/store.py`
  - `vertebra/extensions/sqlspec/<dialect>/events/store.py`
  - `vertebra/extensions/sqlspec/<dialect>/litestar/store.py`
- Base classes from `sqlspec.extensions.{adk,events,litestar}` will be subclassed.
- Connection provisioning will be adapted from `config.provide_session()`/`provide_connection()` to work with Vertebra's broker-backed drivers.

### Files to Modify
- `src/py/vertebra/extensions/sqlspec/postgres/adk/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/postgres/events/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/postgres/litestar/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/mysql/adk/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/mysql/events/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/mysql/litestar/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/sqlite/adk/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/sqlite/events/store.py` (New)
- `src/py/vertebra/extensions/sqlspec/sqlite/litestar/store.py` (New)

## Acceptance Criteria
- [ ] ADK session and event CRUD operations pass for all three drivers.
- [ ] Litestar session storage (get/set/delete) passes for all three drivers.
- [ ] Tables are correctly created/migrated for each dialect.
- [ ] Type checking passes for all new modules.

## Out of Scope
- Redis session store.
- Advanced Alchemy dialect extensions.
- Non-SQLSpec Litestar integration bits (handled in `vertebra.extensions.litestar`).

## Dependencies
- `sqlspec`
- `litestar` (for Litestar store protocols)
- `google-adk` (for ADK record types)

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| SQL Dialect Incompatibilities | Low | Medium | Use `sqlspec` base SQL queries as templates. |
| Connection Provisioning Mismatch | Medium | High | Thoroughly test with Vertebra's `provide_session` context managers. |

### Recovery Strategy
**Rollback Trigger:** Persistent failures in basic CRUD operations after implementation.
**Rollback Steps:**
1. Revert changes to the `sqlspec` extension directory.
2. Ensure no orphaned database tables remain.

**Safe Checkpoints:**
- After Phase 1: `checkpoint/prd_20260121_sqlspec_extensions/1`
- After Phase 2: \`checkpoint/prd_20260121_sqlspec_extensions/2\`
- After Phase 3: \`checkpoint/prd_20260121_sqlspec_extensions/3\`
- After Phase 4: [Description of safe state]
