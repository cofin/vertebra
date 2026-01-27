# Implementation Plan: Security & User Framework

## Phase 1: Bun IPC Security Hooks (Rust & TS/JS)
**Goal:** Expose Bun's native password hashing and crypto utilities to Python via the IPC Broker.

- [x] Task: Define Security Command IDs in shared protocol definitions.
- [x] Task: Implement `HASH_PASSWORD` and `VERIFY_PASSWORD` handlers in the Bun Broker (`src/ts/src/broker`).
    - [x] Use `Bun.password.hash` and `Bun.password.verify`.
- [x] Task: Implement `GENERATE_RANDOM` and `CSRF_SIGN` handlers in the Bun Broker.
- [x] Task: Write Python unit tests for the security IPC client.
- [x] Task: Implement `vertebra.security` Python module using the IPC client to call Bun hooks.
- [x] Task: Checkpoint - Verify Phase 1 complete and create recovery point [checkpoint: 23a05d2]
- [x] Task: Flow - User Manual Verification 'Phase 1: Bun IPC Security Hooks' (Protocol in workflow.md)

## Phase 2: Core User Framework (Python)
**Goal:** Implement the foundational User models and proxy logic, defaulting to `msgspec`.

- [x] Task: Implement `VertebraUserMixin` in `vertebra.auth.models` with first-class `msgspec.Struct` support.
    - [x] Support for standard fields (`id`, `is_authenticated`, `is_anonymous`).
- [x] Task: Implement `AnonymousUser` class with standard defaults (as a `msgspec.Struct`).
- [x] Task: Write unit tests for the `LazyUserProxy` using `msgspec` models.
- [x] Task: Implement `LazyUserProxy` to handle claim hydration and fetch-on-demand logic.
- [x] Task: Implement `contextvars` based global state in `vertebra.auth.state` for universal user access.
- [x] Task: Checkpoint - Verify Phase 2 complete and create recovery point [checkpoint: 654f611]
- [ ] Task: Flow - User Manual Verification 'Phase 2: Core User Framework' (Protocol in workflow.md)

## Phase 3: Broker-Centric Authentication (TS & Python)
**Goal:** Implement the auth validation at the Broker layer and consumption in Python.

- [x] Task: Update Bun Broker to support JWT and Google IAP header validation.
    - [x] Implement JWKS caching for Google/GitHub.
- [x] Task: Implement HTTP-only cookie extraction and CSRF validation in the Bun Broker.
- [x] Task: Fix Supervisor Import Mismatch (ImportError) in `src/py/vertebra/supervisor.py`. [d0a9ab0]
- [x] Task: Write integration tests for Broker-to-Python claim passing. [9de32f1]
    - *Status:* Tests run but fail assertion (`is_authenticated=False`). Broker is responding but claims are not propagating or validation failing. Also experiencing SHM `File exists` errors on re-runs. Fixed `Supervisor` import and Litestar compatibility in test env.
    - *Status:* Integration tests are written (`test_broker_auth.py`) but currently failing with timeouts. Need to debug server startup/communication in test environment.
    - *Status:* Fixed timeouts by ensuring FDs are passed via stdio (ProcessManager) and fixing worker entrypoint import. Tests passing.
- [x] Task: Implement `AuthMetadata` extraction in `vertebra.py` to hydrate the `User` object from Broker signals.
- [x] Task: Implement Refresh Token rotation flow in the Broker and Python client. [7d2fe5c]
- [x] Task: Checkpoint - Verify Phase 3 complete and create recovery point [checkpoint: f9d4aa4]
- [x] Task: Flow - User Manual Verification 'Phase 3: Broker-Centric Authentication' (Protocol in workflow.md)

## Phase 4: Permissions & Scopes
**Goal:** Add granular authorization capabilities.

- [x] Task: Implement Scope and Permission check logic in `VertebraUserMixin`. [4e726b1]
    - [x] `has_scope(scope: str)` and `has_perm(perm: str)`.
- [x] Task: Write tests for Granular Permissions. [4e726b1]
- [x] Task: Checkpoint - Verify Phase 4 complete and create recovery point [checkpoint: 4e726b1]
- [ ] Task: Flow - User Manual Verification 'Phase 4: Permissions & Scopes' (Protocol in workflow.md)

## Phase 5: Vertebra Native Integration
**Goal:** Plug the security framework into the Vertebra Native runtime.

- [ ] Task: Implement `NativeAuthMiddleware` for Vertebra Native mode.
- [ ] Task: Implement `current_user` utility/dependency for Native handlers.
- [ ] Task: Write end-to-end tests for a "Protected Route" in Native mode.
- [ ] Task: Checkpoint - Verify Phase 5 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 5: Vertebra Native Integration' (Protocol in workflow.md)

## Phase 6: Quality & Documentation
**Goal:** Finalize the PRD with documentation and benchmarks.

- [ ] Task: Benchmark IPC Hashing vs. Native Python `argon2-cffi`.
- [ ] Task: Update `docs/architecture/` with the new Security Framework details.
- [ ] Task: Checkpoint - Verify Phase 6 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 6: Quality & Documentation' (Protocol in workflow.md)
