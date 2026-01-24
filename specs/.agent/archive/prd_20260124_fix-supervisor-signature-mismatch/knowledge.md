# Knowledge Capture: Fix Supervisor Signature Mismatch

## Patterns Discovered
- CLI arguments are passed through `run_supervisor` to the underlying Rust/Bun components via environment variables using the `VERTEBRA_` prefix.
- Standardized propagation of CLI timeouts (`drain` and `shutdown`) to runtime components.

## Lessons Learned
- Helper functions like `run_supervisor` that wrap complex lifecycles must be updated in sync with CLI options to avoid `TypeError`.
- Integration points (CLI -> Supervisor -> Runtime) are high-risk areas for signature mismatches during architectural shifts.

## Decisions Made
- Used environment variables (`VERTEBRA_DRAIN_TIMEOUT_MS`, `VERTEBRA_SHUTDOWN_TIMEOUT_MS`) for propagation to avoid immediate changes to the `Supervisor` class or `NativeSupervisor` C-ABI, matching existing Bun-side expectations.

## Reusable Components
- None.

## Gotchas & Warnings
- `run_supervisor` is used by both the core CLI (`vertebra serve`) and framework extensions (e.g., Litestar plugin); signature changes must be verified project-wide.