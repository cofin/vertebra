# Refactor Serialization to Core Package

## Overview
This PRD defines the refactoring of Vertebra's serialization logic. Currently, serialization is either scattered in `_internal` or planned for `extensions`. This is architecturally incorrect as serialization is a core capability. This effort will move serialization to a top-level `vertebra.serializers` package, consolidate existing logic, and implement a clean, unified API.

## Functional Requirements
### Must Have
- [ ] Create `src/py/vertebra/serializers/` package.
- [ ] Move/Implement `VertebraCodec` (MessagePack) in `vertebra.serializers.msgpack`.
- [ ] Move/Implement JSON utilities in `vertebra.serializers.json`.
- [ ] Consolidate `vertebra._internal.serialization` into the new package.
- [ ] Ensure all 30+ types (Arrow, DateTime, UUID, etc.) are supported in the core serializers.
- [ ] Update all references in `database-orm-integration` to point to the new location.

## Technical Approach
### Recommended Implementation
- **New Location:** `src/py/vertebra/serializers/`
- **Structure:**
    - `__init__.py`: Exports `VertebraCodec`, `encode_json`, `decode_json`.
    - `msgpack.py`: `msgspec.msgpack` extensions.
    - `json.py`: `msgspec.json` extensions.
    - `types.py`: Type definitions and protocols.
    - `registry.py`: Central registry for type hooks (if dynamic registration is needed).

### Files to Modify
- **Delete:** `src/py/vertebra/_internal/serialization.py`
- **Create:** `src/py/vertebra/serializers/*`
- **Update:** `src/py/vertebra/extensions/sqlspec/` (imports)
- **Update:** `src/py/vertebra/mcp/` (imports)

## Acceptance Criteria
- [ ] `from vertebra.serializers import VertebraCodec` works.
- [ ] `database-orm-integration` PRD plan is updated.
- [ ] Tests in `src/py/tests/unit/test_serialization_*.py` pass with new imports.

## Dependencies
### Internal Dependencies
- `database-orm-integration` PRD (needs to be updated).

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Import cycles | Medium | High | Use `TYPE_CHECKING` blocks and careful module organization. |
| Breaking internal APIs | High | Medium | Search and replace all usages of `_internal.serialization`. |

## Recovery Strategy
**Rollback Trigger:** Test suite failure or circular import errors.
**Rollback Steps:**
1. Revert file moves.
2. Restore `_internal/serialization.py`.
