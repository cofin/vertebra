# Fix Supervisor Signature Mismatch

## Overview
The `serve` CLI command and Litestar extension pass `drain_timeout_ms` and `shutdown_timeout_ms` to the `run_supervisor` function. However, the `run_supervisor` definition in `src/py/vertebra/supervisor.py` does not accept these arguments, causing a `TypeError` and crashing the application on startup.

This PRD addresses this mismatch by updating `run_supervisor` to accept these arguments and properly propagate them to the underlying runtime via environment variables, ensuring the `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS` configuration values reach the Bun broker.

## Research Reference
No prior research conducted

## Functional Requirements
### Must Have
- [ ] Update `run_supervisor` signature to accept `drain_timeout_ms` (int) and `shutdown_timeout_ms` (int).
- [ ] Ensure `drain_timeout_ms` is propagated to the environment as `VERTEBRA_DRAIN_TIMEOUT_MS`.
- [ ] Ensure `shutdown_timeout_ms` is propagated to the environment as `VERTEBRA_SHUTDOWN_TIMEOUT_MS`.
- [ ] Validate that `examples/mcp-tools/app.py` starts successfully without `TypeError`.

### Should Have
- [ ] Add default values for these arguments in `run_supervisor` to maintain backward compatibility if called without them (defaults: 5000ms and 30000ms).

## Non-Functional Requirements
- **Stability:** Ensure no regression in existing supervisor startup logic.

## Technical Approach
### Recommended Implementation
The `run_supervisor` function in `src/py/vertebra/supervisor.py` serves as the bridge between Python CLI arguments and the Rust/Bun runtime.

1.  **Update Signature:** Modify `run_supervisor` to accept `drain_timeout_ms` and `shutdown_timeout_ms` as keyword-only arguments.
2.  **Environment Propagation:** Inside `run_supervisor`, add these values to the `env` dictionary (or `os.environ`) using the keys `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS`. This aligns with `src/ts/src/server/config.ts` which reads these environment variables.

### Files to Modify
- `src/py/vertebra/supervisor.py`

## Acceptance Criteria
- [ ] `uv run vertebra serve examples/mcp-tools/app.py` runs without `TypeError`.
- [ ] The TypeScript server receives the configured timeouts (verifiable via logs or behavior if verifiable, otherwise just crash avoidance).

## Out of Scope
- Modifying the Rust supervisor core (unless strictly necessary, but python-side fix should suffice).
- Refactoring `Supervisor` class internals.

## Dependencies
### Internal Dependencies
- `src/py/vertebra/cli/serve.py` (Caller)
- `src/ts/src/server/config.ts` (Consumer of env vars)

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Environment Variable Collision | Low | Low | Use explicit `VERTEBRA_` prefix (already standard). |
| Argument Conflict | Low | Low | `run_supervisor` signature update is additive. |

### Recovery Strategy
**Rollback Trigger:** `run_supervisor` fails to start or other dependent commands fail.
**Rollback Steps:**
1. Revert changes to `src/py/vertebra/supervisor.py`.

**Safe Checkpoints:**
- After Phase 1: `checkpoint/fix-supervisor-signature-mismatch/1`
- After Phase 2: `checkpoint/fix-supervisor-signature-mismatch/2`
- After local verification of `examples/mcp-tools/app.py` startup.

## Open Questions
- None.
