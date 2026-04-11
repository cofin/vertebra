# Parallelism Strategies

Vertebra has **two** active parallelism strategies after the cleanup saga.
A third (sub-interpreters) was scoped out when the speculative Python entry
points were removed in Ch2 of `cleanup_20260411`.

| Strategy | Active when | Where it lives | Selected by |
|---|---|---|---|
| **In-process threads** (default) | workers = 1 or embedded topology | Tokio + Python event loop, single process | `VERTEBRA_TOPOLOGY=embedded` (default) |
| **SHM multi-process** | workers > 1 and SHM transport available | `src/rs/vertebra-core/src/shm.rs`, `src/rs/vertebra-core/src/transport/`, `src/rs/vertebra-http/src/ring_factory.rs`, `src/py/vertebra/server/_native.py` SHM loop | `VERTEBRA_TOPOLOGY=shm` + `--workers N` |
| **Free-threaded Python (3.14t)** | Build compiled with free-threaded PyO3 | `#[cfg(Py_GIL_DISABLED)]` gate in `src/rs/vertebra-py/src/lib.rs:21` | Build-time: must use a 3.14t Python wheel + matching PyO3 config |

## Decision matrix

```
(build_flavor, worker_count, topology) → strategy

(gil_python,    1, embedded) → in-process, single thread Python loop
(gil_python,    N, embedded) → in-process, N tokio workers + 1 Python loop
(gil_python,    N, shm)      → SHM multi-process, Rust parent + N python workers
(free_threaded, 1, embedded) → in-process, free-threaded Python loop (no GIL)
(free_threaded, N, embedded) → in-process, free-threaded Python parallel workers
(free_threaded, N, shm)      → SHM multi-process + free-threaded workers (highest throughput)
```

## Build-time flag: `FREE_THREADED`

There is exactly one compile-time gate for free-threaded Python in the
workspace:

```rust
// src/rs/vertebra-py/src/lib.rs
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;
```

The Python side reads this through:

```python
# src/py/vertebra/server/_native.py
def _is_free_threaded() -> bool:
    try:
        from vertebra._vertebra import FREE_THREADED
        return FREE_THREADED
    except ImportError:
        return False
```

No other crate in the workspace currently branches on `Py_GIL_DISABLED`.
If that ever changes, mirror the constant via `pyo3-build-config` rather
than redefining it.

## Runtime topology: `VERTEBRA_TOPOLOGY`

The SHM vs in-process decision is runtime, not compile-time. See
`src/rs/vertebra-http/src/config.rs` (`Topology::Embedded` / `Topology::Shm`).

- **Embedded** (default): one process, Tokio + Python in the same address
  space via PyO3.
- **Shm**: Rust parent process spawns N Python workers; all request/response
  traffic goes through per-worker SPSC ring pairs created by
  `ring_factory::RingFactory`. FD inheritance protocol is documented in
  `feedback_fd_inheritance_protocol.md`.

## What happened to sub-interpreters?

A PEP 684 sub-interpreter path was prototyped and shipped as dead code
(`src/py/vertebra/server/_ipc/_subinterp.py`, `_ring_reader.py`,
`_shm_async.py`) but never wired into production. It was consumed only by
tests. The cleanup saga (`cleanup_20260411`, Ch2, commit c9cdae8) removed
the files with the understanding that if Vertebra later adopts a sub-
interpreter strategy, it will be designed against the current
`RingFactory` + `BatchSnapshot` API rather than the old prototype.

## CI matrix

- **Required:** CPython 3.12 GIL (current development target)
- **Aspirational:** CPython 3.14t free-threaded — once stable, a separate
  maturin wheel build should exercise the `FREE_THREADED == true` path
- **Not tested:** PyPy, GraalPy (not supported)
