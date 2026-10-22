# Free-Threaded Python 3.14t Review Reference

This reference outlines safety guidelines for PyO3 native extensions under free-threaded Python execution.

---

## Free-Threaded Safety Checklist

1. **Explicit Module Flag**:
   - Every `#[pymodule]` definition must specify `gil_used = false`.
   - Never remove or condition this flag without verified architectural escalation.

2. **Off-GIL Concurrency**:
   - In free-threaded runtimes, multiple Python threads execute simultaneously in the same interpreter.
   - Any mutable native state exposed to Python via `#[pyclass]` must be synchronized with atomics, `parking_lot::Mutex`, or `RwLock`.
   - Never rely on Python GIL re-acquisition for synchronization.

3. **Memory Isolation in `py.detach`**:
   - `py.detach` passes native closures to execute outside Python execution context.
   - Closures must capture only `Send` data and no borrowed Python objects.
