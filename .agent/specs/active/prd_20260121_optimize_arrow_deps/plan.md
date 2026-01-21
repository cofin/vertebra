# Implementation Plan: Optimize Arrow Dependencies

This plan outlines the transition from `pyarrow` to `nanoarrow` as the core required dependency for Arrow IPC in Vertebra.

## Phase 1: Dependency Management
Goal: Update the project configuration to introduce `nanoarrow` and make `pyarrow` optional.

- [ ] Task: Update `pyproject.toml` dependencies
    - [ ] Add `nanoarrow>=0.5.0` to main dependencies
    - [ ] Create `arrow` optional dependency group containing `pyarrow`
    - [ ] Remove `pyarrow` from main dependencies
- [ ] Task: Verify dependency isolation
    - [ ] Create a clean virtual environment and install `vertebra`
    - [ ] Verify `pyarrow` is NOT installed
    - [ ] Verify `nanoarrow` IS installed
- [ ] Task: Flow - User Manual Verification 'Phase 1: Dependency Management' (Protocol in workflow.md)

## Phase 2: Core IPC Abstraction
Goal: Implement an internal utility to handle Arrow operations with either `nanoarrow` or `pyarrow`.

- [ ] Task: Create Arrow utility module
    - [ ] Write tests for a new `vertebra._internal.arrow` module that handles imports and basic stream creation
    - [ ] Implement `vertebra._internal.arrow` with `nanoarrow` as the primary engine and `pyarrow` as fallback
- [ ] Task: Update Broker resource logic
    - [ ] Write tests for `src/py/vertebra/resources/broker.py` Arrow handling without `pyarrow`
    - [ ] Refactor `broker.py` to use the new `vertebra._internal.arrow` utility
- [ ] Task: Update Driver logic
    - [ ] Write tests for `src/py/vertebra/drivers/broker.py` Arrow reconstruction
    - [ ] Refactor `drivers/broker.py` to support `nanoarrow`-based reconstruction
- [ ] Task: Flow - User Manual Verification 'Phase 2: Core IPC Abstraction' (Protocol in workflow.md)

## Phase 3: Integration and Performance Validation
Goal: Ensure the entire system works with the new dependency model and monitor performance.

- [ ] Task: Update existing SQLSpec provider
    - [ ] Write tests for `sqlspec` provider with `nanoarrow`
    - [ ] Update `src/py/vertebra/extensions/sqlspec/provider.py`
- [ ] Task: Full Integration Test Run
    - [ ] Execute all Arrow-related tests in a environment without `pyarrow`
    - [ ] Execute all Arrow-related tests in a environment WITH `pyarrow`
- [ ] Task: Benchmarking
    - [ ] Run `benchmarks/py_serialization.py` and compare results
- [ ] Task: Flow - User Manual Verification 'Phase 3: Integration and Performance Validation' (Protocol in workflow.md)
