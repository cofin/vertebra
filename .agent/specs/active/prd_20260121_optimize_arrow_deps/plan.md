# Implementation Plan: Adopt SQLSpec Typing Patterns

This plan outlines the adoption of SQLSpec's typing patterns for optional dependency handling in Vertebra.

## Phase 1: Core Infrastructure

**Goal:** Create the foundational typing utilities following SQLSpec's patterns.

- [ ] Task: Create module loader utilities
    - [ ] Create `src/py/vertebra/_internal/module_loader.py`
    - [ ] Implement `module_available()` with caching
    - [ ] Implement `OptionalDependencyFlag` class with `__bool__`
    - [ ] Implement `dependency_flag()` factory function
    - [ ] Implement `reset_dependency_cache()` for testing
    - [ ] Add `ensure_*` functions for each optional dependency

- [ ] Task: Create Arrow typing module
    - [ ] Create `src/py/vertebra/_internal/arrow.py`
    - [ ] Define `PYARROW_INSTALLED` flag
    - [ ] Define `ArrowTableProtocol` with runtime_checkable
    - [ ] Define `ArrowRecordBatchProtocol`
    - [ ] Define `ArrowSchemaProtocol`
    - [ ] Add conditional imports with stub fallbacks

- [ ] Task: Create consolidated typing facade
    - [ ] Create `src/py/vertebra/_internal/typing.py`
    - [ ] Re-export arrow types and flags
    - [ ] Add `NUMPY_INSTALLED`  flags
    - [ ] Add `NumpyArrayProtocol` stub
    - [ ] Add `__all__` with complete exports

- [ ] Task: Update pyproject.toml
    - [ ] Add `[project.optional-dependencies]` section
    - [ ] Add `arrow = ["pyarrow>=14.0.0"]`
    - [ ] Add `full = ["pyarrow",  "numpy"]`

- [ ] Task: Flow - User Manual Verification 'Phase 1: Core Infrastructure'

## Phase 2: Broker Client Updates

**Goal:** Update the broker client to use the new typing patterns.

- [ ] Task: Update resources/broker.py
    - [ ] Import from `vertebra._internal.arrow`
    - [ ] Replace `try/except ImportError` with `ensure_pyarrow()`
    - [ ] Add type annotations using `ArrowTable` (Protocol or real)
    - [ ] Update `_decode_result()` for async client
    - [ ] Update `_decode_result()` for sync client
    - [ ] Update `_decode_arrow_bytes()` helper

- [ ] Task: Update drivers/broker.py
    - [ ] Import Protocol types for annotations
    - [ ] Update `VertebraArrowCursor` type hints
    - [ ] Update `VertebraAsyncArrowCursor` type hints
    - [ ] Ensure `import pyarrow` only happens after flag check

- [ ] Task: Write tests for typing utilities
    - [ ] Test `PYARROW_INSTALLED` flag behavior
    - [ ] Test `ensure_pyarrow()` raises correct error
    - [ ] Test module caching behavior
    - [ ] Test type checking passes without pyarrow

- [ ] Task: Flow - User Manual Verification 'Phase 2: Broker Client Updates'

## Phase 3: SQLSpec Provider Updates

**Goal:** Update the SQLSpec provider with extension support patterns.

- [ ] Task: Update provider.py
    - [ ] Import from `vertebra._internal.arrow`
    - [ ] Use `ensure_pyarrow()` in `_maybe_encode_arrow()`
    - [ ] Use `ensure_pyarrow()` in `_write_arrow_region()`
    - [ ] Add debug logging when pyarrow unavailable

- [ ] Task: Create DriverFeatures TypedDict
    - [ ] Create `VertebraDriverFeatures` base TypedDict
    - [ ] Add JSON serializer/deserializer options
    - [ ] Add `enable_json_codecs` flag 
    - [ ] Add `arrow_threshold_rows` and `arrow_threshold_bytes`

- [ ] Task: Add extension registration hooks
    - [ ] Create `register_json_codecs()` function 
    - [ ] Add `_init_connection()` hook in config

- [ ] Task: Flow - User Manual Verification 'Phase 3: SQLSpec Provider Updates'

## Phase 4: Type Checking Validation

**Goal:** Ensure type checkers pass without optional dependencies.

- [ ] Task: Pyright validation
    - [ ] Run `pyright src/py/vertebra` without pyarrow
    - [ ] Fix any type errors
    - [ ] Verify Protocol usage is correct

- [ ] Task: Mypy validation
    - [ ] Run `mypy src/py/vertebra` without pyarrow
    - [ ] Add necessary `type: ignore` comments
    - [ ] Document any intentional suppressions

- [ ] Task: Test suite validation
    - [ ] Run tests with pyarrow installed
    - [ ] Run subset of tests without pyarrow
    - [ ] Verify error messages are correct

- [ ] Task: Flow - User Manual Verification 'Phase 4: Type Checking Validation'

## Phase 5: Documentation and Cleanup

**Goal:** Document the patterns and clean up any loose ends.

- [ ] Task: Update CLAUDE.md
    - [ ] Document typing pattern location
    - [ ] Document how to add new optional dependencies
    - [ ] Document extension support pattern

- [ ] Task: Add inline documentation
    - [ ] Docstrings in module_loader.py
    - [ ] Docstrings in arrow.py
    - [ ] Usage examples in typing.py

- [ ] Task: Final review
    - [ ] Verify all acceptance criteria met
    - [ ] Run full test suite
    - [ ] Run linters

- [ ] Task: Flow - User Manual Verification 'Phase 5: Documentation and Cleanup'

## Summary of Key Files

| File | Purpose |
|------|---------|
| `_internal/module_loader.py` | Lazy dependency checking (copied/adapted from SQLSpec) |
| `_internal/arrow.py` | Arrow-specific types and protocols |
| `_internal/typing.py` | Public facade for all optional dep types |
| `resources/broker.py` | Updated with ensure_pyarrow() |
| `drivers/broker.py` | Updated with Protocol types |
| `extensions/sqlspec/provider.py` | Updated with ensure_pyarrow() |
| `pyproject.toml` | Optional dependency groups |

## Key Patterns Reference

```python
# 1. Check availability (lazy, cached)
if PYARROW_INSTALLED:
    import pyarrow as pa
    # use pyarrow

# 2. Require with clear error
ensure_pyarrow()  # Raises MissingDependencyError if not installed

# 3. Type annotations that work without import
def process(table: ArrowTable) -> list[dict[str, Any]]:
    # ArrowTable is Protocol if pyarrow not installed
    return table.to_pylist()

# 4. Extension features via TypedDict
config = VertebraPostgresConfig(
    driver_features={ 
        "enable_json_codecs": True,
    }
)
```
