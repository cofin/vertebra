# Implementation Plan - Fix Startup Module Import and Polish CLI

## Phase 1: Diagnosis and Fix

- [ ] Task: Reproduce the `ModuleNotFoundError` with a test case or manual run.
    - [ ] Create a reproduction script or rely on the user's provided command.
- [ ] Task: Fix the import logic in `src/py/vertebra/server/worker.py`.
    - [ ] Modify `_import_app` to handle file paths correctly (e.g., split path, add to `sys.path`).
    - [ ] Verify the fix by running the example app.
- [ ] Task: Flow - User Manual Verification 'Phase 1: Diagnosis and Fix' (Protocol in workflow.md)

## Phase 2: CLI Polish and Testing

- [ ] Task: Review CLI error handling for imports.
    - [ ] Add better user feedback if an app module cannot be found.
- [ ] Task: Add an integration test for the CLI starting an app.
    - [ ] Ensure this scenario is covered in the test suite to prevent regression.
- [ ] Task: Flow - User Manual Verification 'Phase 2: CLI Polish and Testing' (Protocol in workflow.md)
