# Chapters: Server Runtime Correctness (`server-runtime-correctness`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-python-stream-submission-and-flush`](ch01-python-stream-submission-and-flush/spec.md) | `completed` | `4` | PyO3 `submit_stream_frame` / `max_stream_frame_payload`, progressive ASGI/RSGI flush, and native `App` `_dispatch.py` streaming (`c9e7b370`) |
| [`ch02-lifespan-supervisor-and-worker-lanes`](ch02-lifespan-supervisor-and-worker-lanes/spec.md) | `completed` | `4` | Fail-loud `WorkerStartupError` + SHM exit `78`, `vertebra.supervisor` exports, `QueueService` resolution, and `--worker-only` `HealthProbeServer` (`d2da8fa0`) |
| [`ch03-relocatable-server-binary-and-packaging`](ch03-relocatable-server-binary-and-packaging/spec.md) | `active` | `4` | Relocatable `build.rs` + `build_server_binary.py`, `EmbeddedPythonRuntime`, maturin binary wheel include, and opt-in `StandaloneBuilder` `.agents/skills` staging (`ruff` + `mypy` + `test_packaging_and_pgo.py`) |
| [`ch04-wheel-smoke-and-ci`](ch04-wheel-smoke-and-ci/spec.md) | `planned` | `3` | `smoke_vtb_run.py` + `make core-wheel-check`, PR CI smoke job, and release wheel + `linux-arm64` binary workflows (`ruff` + `mypy` + workflow schema validation) |
| [`ch05-fastpath-consolidation`](ch05-fastpath-consolidation/spec.md) | `planned` | `4` | `CompiledRouteBase`, single `__vertebra_fastpath__` resolution, shared `has_custom_exception_handlers`, and copy-free response serialization (`ruff` + `mypy` + framework plugin pytest) |
| [`ch06-runtime-native-cleanup`](ch06-runtime-native-cleanup/spec.md) | `planned` | `3` | Fail-loud `getrandom` + post-fork entropy reseed, `_module_cache` removal, direct `_zero_copy` FFI bindings, and Spanner `batch_write` cleanup (`cargo test -p vertebra-core --lib id::` + unit pytest) |
| [`ch07-docs-and-okf-sync`](ch07-docs-and-okf-sync/spec.md) | `planned` | `4` | Server/CLI/packaging `docs/`, `pyo3-ffi-and-batch-dispatch.md` (`<= 200L`), IPC/runner/worker OKF chapters, `server-runtime-limitations.md`, and `product.md` (`make docs && make review-check`) |
