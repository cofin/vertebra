# Implementation Plan - Fix Supervisor Signature Mismatch

## Phase 1: Test & Reproduce
- [x] Task: Create a unit test `src/py/tests/unit/supervisor/test_run_supervisor.py`
    - [x] Mock `NativeSupervisor` and `Supervisor` classes.
    - [x] Call `run_supervisor` with `drain_timeout_ms` and `shutdown_timeout_ms` arguments.
    - [x] Verify that it currently raises `TypeError`.
- [x] Task: Verify the failure by running the test
    - [x] Run `pytest src/py/tests/unit/supervisor/test_run_supervisor.py` and confirm failure.
- [x] Task: Checkpoint - Verify Phase 1 complete and create recovery point [checkpoint: e6b1e09]
- [ ] Task: Flow - User Manual Verification 'Test & Reproduce' (Protocol in workflow.md)

## Phase 2: Implementation
- [x] Task: Update `run_supervisor` in `src/py/vertebra/supervisor.py`
    - [x] Add `drain_timeout_ms: int = 5000` and `shutdown_timeout_ms: int = 30000` to the function signature.
    - [x] Update the `env` dictionary inside the function to include:
        - `VERTEBRA_DRAIN_TIMEOUT_MS`: `str(drain_timeout_ms)`
        - `VERTEBRA_SHUTDOWN_TIMEOUT_MS`: `str(shutdown_timeout_ms)`
- [x] Task: Verify the fix with the unit test
    - [x] Run `pytest src/py/tests/unit/supervisor/test_run_supervisor.py` and confirm it passes.
    - [x] Assert that `os.environ` was updated with the correct values during the call.
- [x] Task: Verify with `examples/mcp-tools/app.py`
    - [x] Run `uv run vertebra serve examples/mcp-tools/app.py` and confirm the application starts without crashing.
- [x] Task: Checkpoint - Verify Phase 2 complete and create recovery point [checkpoint: 049e1dc]
- [ ] Task: Flow - User Manual Verification 'Implementation' (Protocol in workflow.md)

## Phase 3: Documentation & Standards
- [x] Task: Verify touch points in other modules
    - [x] Review `src/py/vertebra/cli/serve.py` to ensure it passes arguments correctly.
    - [x] Review `src/py/vertebra/extensions/litestar/cli.py` to ensure it passes arguments correctly.
- [x] Task: Run project-wide linting and type checking
    - [x] Run `make py-lint` and `make lint-all`.
- [x] Task: Checkpoint - Verify Phase 3 complete and create recovery point [checkpoint: 049e1dc]
- [x] Task: Flow - User Manual Verification 'Documentation & Standards' (Protocol in workflow.md)
