# Analysis: Optional Dependency Handling

## Complexity Assessment

**Classification: Medium**
- 70+ extension files to audit
- 4 distinct extension categories
- Multiple patterns to unify
- Requires typing module updates
- Compound dependency handling

**Estimated Checkpoints: 8**

## Pattern Recognition Summary

### Similar Implementations Found

1. `vertebra/extensions/litestar/plugin.py` - Try/except with stub classes and MissingDependencyError
2. `vertebra/extensions/advanced_alchemy/_config.py` - Try/except with object fallback
3. `vertebra/serializers/litestar.py` - Conditional feature enhancement with dependency_flag
4. `vertebra/utils/module_loader.py` - Core dependency checking infrastructure

### Key Patterns Identified

| Pattern | Use Case | Recommendation |
|---------|----------|----------------|
| try/except + object fallback | Never | Remove - causes runtime surprises |
| try/except + stub classes | Rare | Replace with ensure_dependency() |
| dependency_flag() conditional | Optional features | Keep - correct for enhancements |
| Direct import | Required deps | Keep - natural Python semantics |
| ensure_dependency() | Entry points | Add - best DX for errors |

## Architectural Decision Record

### Decision: Use ensure_dependency() at Extension Entry Points

**Context:** Extensions currently mix multiple patterns for handling optional dependencies, leading to inconsistent error messages and potential runtime failures.

**Decision:** Standardize on `ensure_dependency()` calls at extension `__init__.py` entry points, removing try/except patterns that defer errors or provide object fallbacks.

**Consequences:**
- Positive: Clear, actionable error messages
- Positive: Fail-fast behavior at import time
- Positive: Simpler code without stub classes
- Negative: Minor refactoring effort
- Negative: Some TYPE_CHECKING blocks may be needed for type hints

### Decision: Centralize Dependency Flags

**Context:** Dependency flags like ADVANCED_ALCHEMY_INSTALLED are defined multiple times across different files.

**Decision:** All dependency flags should be defined once in `vertebra.utils.dependencies` and re-exported where needed.

**Consequences:**
- Positive: Single source of truth
- Positive: Consistent naming
- Positive: Easier to maintain
- Negative: Minor import changes needed

## Risk Analysis

### Low Risk
- Adding new flags to dependencies.py
- Adding ensure_dependency() calls to __init__.py files
- Re-exporting flags from _typing.py

### Medium Risk
- Refactoring litestar/plugin.py to remove stub classes
- Refactoring advanced_alchemy/_config.py to remove object fallback
- Ensuring type checkers still work after removing stubs

### Mitigation
- Comprehensive test coverage for import scenarios
- TYPE_CHECKING blocks for type-only imports
- Incremental rollout by extension category

## Implementation Sequence

1. Add new flags to dependencies.py (Low risk, foundation)
2. Update _typing.py re-exports (Low risk)
3. Refactor advanced_alchemy/_config.py and _storage.py (Medium risk)
4. Refactor litestar/plugin.py (Medium risk)
5. Add guards to sqlspec compound extensions (Low risk)
6. Add tests for import behavior (Required)
7. Update documentation (Required)
