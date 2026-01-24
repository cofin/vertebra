# Implementation Plan: Adopt SQLSpec Typing Patterns

This plan outlines the adoption of SQLSpec's typing patterns for optional dependency handling in Vertebra.

## Phase 1: Core Infrastructure

**Goal:** Create the foundational typing utilities following SQLSpec's patterns.

- [x] Task: Create module loader utilities
    - [x] Create `src/py/vertebra/_internal/module_loader.py`
    - [x] Implement `module_available()` with caching
    - [x] Implement `OptionalDependencyFlag` class with `__bool__`
    - [x] Implement `dependency_flag()` factory function
    - [x] Implement `reset_dependency_cache()` for testing
    - [x] Add `ensure_*` functions for each optional dependency

- [x] Task: Create Arrow typing module
    - [x] Create `src/py/vertebra/_internal/arrow.py`
    - [x] Define `PYARROW_INSTALLED` flag
    - [x] Define `ArrowTableProtocol` with runtime_checkable
    - [x] Define `ArrowRecordBatchProtocol`
    - [x] Define `ArrowSchemaProtocol`
    - [x] Add conditional imports with stub fallbacks

- [x] Task: Create consolidated typing facade
    - [x] Create `src/py/vertebra/_internal/typing.py`
    - [x] Re-export arrow types and flags
    - [x] Add `NUMPY_INSTALLED`  flags
    - [x] Add `NumpyArrayProtocol` stub
    - [x] Add `__all__` with complete exports

- [x] Task: Update pyproject.toml
    - [x] Add `[project.optional-dependencies]` section
    - [x] Add `arrow = ["pyarrow>=14.0.0"]`
    - [x] Add `full = ["pyarrow",  "numpy"]`

- [x] Task: Flow - User Manual Verification 'Phase 1: Core Infrastructure'

## Phase 2: Broker Client Updates

**Goal:** Update the broker client to use the new typing patterns.

- [x] Task: Update resources/broker.py
    - [x] Import from `vertebra._internal.arrow`
    - [x] Replace `try/except ImportError` with `ensure_pyarrow()`
    - [x] Add type annotations using `ArrowTable` (Protocol or real)
    - [x] Update `_decode_result()` for async client
    - [x] Update `_decode_result()` for sync client
    - [x] Update `_decode_arrow_bytes()` helper

- [x] Task: Update drivers/broker.py
    - [x] Import Protocol types for annotations
    - [x] Update `VertebraArrowCursor` type hints
    - [x] Update `VertebraAsyncArrowCursor` type hints
    - [x] Ensure `import pyarrow` only happens after flag check

- [x] Task: Write tests for typing utilities
    - [x] Test `PYARROW_INSTALLED` flag behavior
    - [x] Test `ensure_pyarrow()` raises correct error
    - [x] Test module caching behavior
    - [x] Test type checking passes without pyarrow

- [x] Task: Flow - User Manual Verification 'Phase 2: Broker Client Updates'

## Phase 3: SQLSpec Provider Updates

**Goal:** Update the SQLSpec provider with extension support patterns.

- [x] Task: Update provider.py
    - [x] Import from `vertebra._internal.arrow`
    - [x] Use `ensure_pyarrow()` in `_maybe_encode_arrow()`
    - [x] Use `ensure_pyarrow()` in `_write_arrow_region()`
    - [x] Add debug logging when pyarrow unavailable

- [x] Task: Create DriverFeatures TypedDict
    - [x] Create `VertebraDriverFeatures` base TypedDict
    - [x] Add JSON serializer/deserializer options
    - [x] Add `enable_json_codecs` flag
    - [x] Add `arrow_threshold_rows` and `arrow_threshold_bytes`
    - [ ] NOTE: Removed pgvector support due to compatibility concerns (see implementation notes)

- [x] Task: Add extension registration hooks
    - [x] Create `register_json_codecs()` function
    - [x] Add `_init_connection()` hook in config

- [x] Task: Flow - User Manual Verification 'Phase 3: SQLSpec Provider Updates'

## Phase 4: Type Checking Validation

**Goal:** Ensure type checkers pass without optional dependencies.

- [x] Task: Pyright validation
    - [x] Run `pyright src/py/vertebra` without pyarrow
    - [x] Fix any type errors
    - [x] Verify Protocol usage is correct

- [x] Task: Mypy validation
    - [x] Run `mypy src/py/vertebra` without pyarrow
    - [x] Add necessary `type: ignore` comments
    - [x] Document any intentional suppressions

- [x] Task: Test suite validation
    - [x] Run tests with pyarrow installed
    - [x] Run subset of tests without pyarrow
    - [x] Verify error messages are correct

- [x] Task: Flow - User Manual Verification 'Phase 4: Type Checking Validation'

## Phase 6: Post-Completion Fixes

**Goal:** Address feedback on unused ignores and add Polars support.

- [x] Task: Fix mypy unused-ignore issues
    - [x] Add `unused-ignore` hint to type ignore comments in `arrow.py` and `typing.py`

- [x] Task: Add Polars support
    - [x] Update `module_loader.py` with `ensure_polars()`
    - [x] Update `typing.py` with `POLARS_INSTALLED` and `PolarsDataFrameProtocol`
    - [x] Update `pyproject.toml` optional dependencies

- [x] Task: Add Pandas support
    - [x] Update `module_loader.py` with `ensure_pandas()`
    - [x] Update `typing.py` with `PANDAS_INSTALLED` and `PandasDataFrameProtocol`
    - [x] Update `pyproject.toml` optional dependencies

- [x] Task: Flow - User Manual Verification 'Phase 6: Post-Completion Fixes'

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
