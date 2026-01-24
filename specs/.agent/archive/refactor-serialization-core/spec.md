# Refactor Serialization to Core Package

## Status: COMPLETED

## Overview
This PRD defined the refactoring of Vertebra's serialization logic. Serialization has been moved to a top-level `vertebra.serializers` package with a clean, unified API.

## Functional Requirements
### Must Have
- [x] Create `src/py/vertebra/serializers/` package.
- [x] Implement `VertebraCodec` (MessagePack) and `JSONCodec` in `vertebra.serializers.codecs`.
- [x] Consolidate JSON and MessagePack into single `codecs.py` module.
- [x] Remove `vertebra._internal/` directory entirely.
- [x] Ensure all 30+ types (Arrow, DateTime, UUID, etc.) are supported in the core serializers.
- [x] Update all references across codebase to use new locations.

## Technical Approach
### Final Implementation
- **Location:** `src/py/vertebra/serializers/`
- **Structure:**
    - `__init__.py`: Re-exports from `codecs.py`.
    - `codecs.py`: Unified `JSONCodec`, `MessagePackCodec`, `VertebraCodec` (alias), and helper functions.
- **Related modules:**
    - `vertebra.utils.encoders`: Shared `default_encoder` function for msgspec.
    - `vertebra.protocols`: `JSONSerializer` and `Serializer` protocols.
    - `vertebra.typing`: Public typing facade with dependency flags.

### Files Modified
- **Deleted:** `src/py/vertebra/_internal/` (entire directory)
- **Deleted:** `src/py/vertebra/serializers/json.py`, `msgpack.py`, `encoders.py`, `types.py`
- **Created:** `src/py/vertebra/serializers/codecs.py`
- **Created:** `src/py/vertebra/utils/` directory with `module_loader.py`, `dependencies.py`, `encoders.py`
- **Created:** `src/py/vertebra/protocols.py` (consolidated 21 protocols)
- **Created:** `src/py/vertebra/typing.py` (public typing facade)
- **Created:** `src/py/vertebra/_typing.py` (internal native stubs)
- **Created:** `src/py/vertebra/exceptions.py` (`MissingDependencyError`)
- **Updated:** All imports across `extensions/`, `drivers/`, `resources/`, `tests/`

## Acceptance Criteria
- [x] `from vertebra.serializers import VertebraCodec` works.
- [x] `from vertebra.serializers.codecs import JSONCodec, MessagePackCodec` works.
- [x] Tests pass with new imports (199 tests passing).

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
