# Knowledge Capture: Fix Supervisor Signal Race

## Patterns Discovered
- **Dual Reaping Race**: When mixing `libc::waitpid` (Rust) and `subprocess.Popen.poll()` (Python), both compete to reap child processes. If Python reaps first, Rust's `waitpid` fails or sees nothing, causing missed events if the Python side assumes Rust will handle it.
- **Robust Fallbacks**: Always handle fallbacks. The Python code checked `poll()` but ignored the result with `pass`, assuming "Rust handles it". This assumption is fatal in race conditions.

## Lessons Learned
- **Trust but Verify**: If you have a fallback mechanism (like checking `poll()` on timeout), *use* the result.
- **Process Supervision**: Supervising processes across language boundaries requires careful coordination of who owns the "wait" responsibility.

## Decisions Made
- Modified `supervisor.py` (Python) to actively handle exits detected by `poll()`, serving as a robust fallback for the Native supervisor.
- Kept the `waitpid(WNOHANG)` check in Rust as it improves responsiveness for unmanaged signals.

## Reusable Components
- `NativeSupervisor` pattern is robust but requires the consumer to handle edge cases where signals are intercepted by the host runtime.

## Gotchas & Warnings
- `subprocess.Popen` in Python may install signal handlers or reap processes in ways that interfere with C-level `signalfd` or `waitpid` loops.
