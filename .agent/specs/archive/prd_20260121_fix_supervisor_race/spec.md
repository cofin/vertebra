# Product Specification: Fix Supervisor Signal Race

## Problem
The `NativeSupervisor` in `src/rs/vertebra-py/src/supervisor.rs` uses `signalfd` to detect `SIGCHLD` (child process exit). It temporarily blocks signals, checks the file descriptor, and then unblocks them.

If a child process exits while signals are unblocked (e.g., during Python execution between calls to `wait_next_event`), the `SIGCHLD` is delivered to the process and consumed by the default handler (or ignored). When `wait_next_event` is called again, it creates a *new* `signalfd`. This new descriptor will not report the signal that has already been delivered.

As a result, the supervisor hangs indefinitely in `wait_next_event`, waiting for a signal that has already happened.

## Solution
Modify `wait_impl` to be robust against missed signals.

1.  **Proactive Reaping**: Regardless of whether `signalfd` reports `SIGCHLD`, we should perform a non-blocking `waitpid` check (`WNOHANG`) on every poll cycle. This ensures that even if the signal was missed (or consumed elsewhere), we still detect the zombie process.
2.  **Logic Update**:
    *   In `wait_impl`, after the `poll` (whether it returns events or times out), verify if any children have exited using `waitpid(-1, &mut status, WNOHANG)`.
    *   If `waitpid` returns a PID > 0, return the `Exit` event immediately.

## Scope
- **File**: `src/rs/vertebra-py/src/supervisor.rs`
- **Behavior**: The supervisor must correctly report process exits even if the `SIGCHLD` signal was delivered outside the `signalfd` monitoring window.

## Success Criteria
- `test_supervisor.py` passes reliably without timeouts.
- `make test-all` completes successfully.
