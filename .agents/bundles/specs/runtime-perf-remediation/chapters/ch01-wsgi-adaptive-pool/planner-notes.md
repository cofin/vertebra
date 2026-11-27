# Planner Notes: ch01-wsgi-adaptive-pool

## Files Written

- `chapters/ch01-wsgi-adaptive-pool/spec.md`
- `chapters/ch01-wsgi-adaptive-pool/tasks/t01-wsgi-server-config.md`
- `chapters/ch01-wsgi-adaptive-pool/tasks/t02-wsgi-executor-pool.md`
- `chapters/ch01-wsgi-adaptive-pool/tasks/t03-wsgi-batch-runner-and-exports.md`
- `chapters/ch01-wsgi-adaptive-pool/tasks/t04-python-stubs-and-worker-shutdown.md`
- `chapters/ch01-wsgi-adaptive-pool/tasks/t05-wsgi-adaptive-pool-tests.md`
- `chapters/ch01-wsgi-adaptive-pool/planner-notes.md`

## Task Summaries

- `t01-wsgi-server-config`: Add `WsgiServerConfig` with `root_path` normalization, switch `WsgiEnviron` to `Arc<WsgiServerConfig>`, add `WsgiEnviron::with_config`, and preserve backwards-compatible constructors (`1 file`, ~85 LOC).
- `t02-wsgi-executor-pool`: Implement `WsgiExecutionMode`, `WsgiPoolConfig`, `WsgiBatchItems`, `WsgiExecutor` (lazy pool threads, zero-idle `Condvar` watchdog, atomic inline-to-pool stall handoff, 2 s hysteresis, immediate `submit_raw_inner`), and GIL-safe `shutdown_wsgi_executor` (`2 files`, ~245 LOC).
- `t03-wsgi-batch-runner-and-exports`: Extract `execute_single_wsgi_item`, wire `drive_wsgi_batch` to batch-shared `Arc<WsgiServerConfig>` and `global_wsgi_executor()`, and register `shutdown_wsgi_executor` in `lib.rs` (`2 files`, ~110 LOC).
- `t04-python-stubs-and-worker-shutdown`: Add `shutdown_wsgi_executor` to `_vertebra.pyi`, call it in `Worker.run`'s `finally:` block, and update `WsgiAdapter.spawn_batch` docstring (`3 files`, ~35 LOC).
- `t05-wsgi-adaptive-pool-tests`: Add `src/py/tests/unit/server/test_wsgi_adaptive_pool.py` covering `16 x 30 ms < 150 ms` blocking I/O concurrency, `threading.Barrier` parallel proof, zero-I/O inline fastpath, hysteresis, `500` exception isolation, GIL-safe shutdown/re-init, and `Worker.run` shutdown integration (`1 file`, ~220 LOC).

## Root Spec Issues

None. All contracts (`WsgiExecutionMode`, `WsgiServerConfig`, `VERTEBRA_WSGI_MODE`, `VERTEBRA_WSGI_THREADS`, `ResponseSubmitter::{submit_batch_raw_inner, submit_raw_inner}`) align with root `spec.md`.

## Open Questions

None. Executor of `t02` and `t05` will inspect `src/rs/vertebra-py/src/transport.rs` (and existing WSGI unit test fixtures for `t05`) per each task's `read_set` to confirm exact `ResponseSubmitter::submit_raw_inner` parameter ordering and in-process response capture helpers.
