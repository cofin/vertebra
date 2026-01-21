# PRD: Optimize Arrow Dependencies

## Overview
Currently, Vertebra depends on `pyarrow` for Arrow IPC and zero-copy data transfer. While powerful, `pyarrow` is a heavy dependency (~100MB+). This PRD aims to transition core zero-copy operations to `nanoarrow`, a lightweight C-based implementation of the Arrow C Data Interface and IPC format. `pyarrow` will be moved to an optional dependency group for users who need advanced Arrow compute features, significantly reducing the default installation size of Vertebra.

## Research Reference
No prior research conducted in Flow, but codebase analysis shows `nanoarrow` mentions in `specs/active/database/prd.md` and `src/py/vertebra/resources/broker.py`.

## Functional Requirements
### Must Have
- [ ] Add `nanoarrow` to required dependencies in `pyproject.toml`.
- [ ] Move `pyarrow` to an optional `arrow` dependency group.
- [ ] Update `src/py/vertebra/resources/broker.py` to use `nanoarrow` for core Arrow IPC serialization/deserialization when `pyarrow` is absent.
- [ ] Ensure the Python client can decode Arrow IPC streams using `nanoarrow`.

### Should Have
- [ ] Automatic fallback to `pyarrow` if installed, favoring its more mature implementation for complex schemas.
- [ ] Clear error messages when a user attempts to use a feature that *requires* `pyarrow` but it is not installed.

### Nice to Have
- [ ] Benchmarking comparison between `nanoarrow` and `pyarrow` for core serialization tasks in Vertebra.

## Non-Functional Requirements
- **Performance:** Maintain or improve zero-copy performance for simple schemas.
- **Bundle Size:** Default installation size should decrease by ~100MB.

## Technical Approach
### Recommended Implementation
1.  **Dependency Update:** Use `uv` to add `nanoarrow>=0.5.0` to dependencies and move `pyarrow` to `[project.optional-dependencies]`.
2.  **Abstraction Layer:** Implement a lightweight internal utility that wraps `nanoarrow` and `pyarrow` imports, providing a unified interface for creating and consuming Arrow streams.
3.  **Client Update:** Modify the client-side response handling to prefer `nanoarrow` for basic data reconstruction.

### Libraries/APIs to Use
- `nanoarrow`: For lightweight Arrow C Data and IPC handling.
- `pyarrow`: (Optional) for advanced compute and complex schema handling.

### Files to Modify
- `pyproject.toml`
- `src/py/vertebra/resources/broker.py`
- `src/py/vertebra/drivers/broker.py`
- `src/py/vertebra/extensions/sqlspec/provider.py`

## Acceptance Criteria
- [ ] `pip install vertebra` (without extras) does not install `pyarrow`.
- [ ] Core IPC functionality works correctly using only `nanoarrow`.
- [ ] `pip install vertebra[arrow]` installs `pyarrow`.
- [ ] All existing tests in `src/py/tests/unit/sqlspec/test_arrow_threshold.py` pass or are updated to reflect the new dependency structure.

## Out of Scope
- Implementing full Arrow compute functionality in `nanoarrow`.
- Removing `pyarrow` support entirely.

## Dependencies
### Internal Dependencies
- None

### External Dependencies
- `nanoarrow`
- `pyarrow` (Optional)

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| `nanoarrow` missing features | Medium | Medium | Maintain `pyarrow` as an optional dependency and implement clear fallbacks. |
| Performance regression | Low | Medium | Run existing benchmarks to verify latency and throughput. |

### Recovery Strategy
**Rollback Trigger:** Significant performance regression (>10%) or breaking changes in data integrity that cannot be quickly patched.
**Rollback Steps:**
1. Revert changes to `pyproject.toml` to make `pyarrow` a required dependency.
2. Revert code changes in `broker.py` and `drivers/broker.py`.
3. Re-verify with tests.

**Safe Checkpoints:**
- After Phase 1: Dependencies updated but code still supports both.
- After Phase 2: Core logic migrated but feature-flagged or switchable.

## Open Questions
- Are there specific edge-case schemas currently handled by `pyarrow` that `nanoarrow` might struggle with?
