---
type: Spec
flow_id: server-runtime-correctness
title: Server Runtime Correctness
description: "Complete progressive Python/ASGI/RSGI/native response streaming, fail-loud lifespan startup, supervisor exports and worker-only lanes, wheel-shipped vertebra-server binaries with standalone skill staging, and FastPath/native runtime cleanup."
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-python-stream-submission-and-flush
  - ch02-lifespan-supervisor-and-worker-lanes
  - ch03-relocatable-server-binary-and-packaging
  - ch04-wheel-smoke-and-ci
  - ch05-fastpath-consolidation
  - ch06-runtime-native-cleanup
  - ch07-docs-and-okf-sync
current_chapter: ch01-python-stream-submission-and-flush
current_task: "server-runtime-correctness:ch01:t01"
parent_prd: integration-remediation
plan_revision: 3
plan_commit: 18baadcd
last_verified_checkpoint: 18baadcd
last_reviewed_commit: 18baadcd
created_at: "2026-11-12T23:45:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - server
  - streaming
  - asgi
  - supervisor
  - packaging
  - fastpath
sources:
  - id: bridge-core-rs
    resource: "src/rs/vertebra-core/src/bridge.rs::BatchBridge"
    title: In-process bridge with ResponseStreamFrame and StreamSink landed in 18baadcd
  - id: ring-pair-rs
    resource: "src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair"
    title: SHM VRSF xlarge stream frames and max_stream_frame_payload landed in 18baadcd
  - id: streams-http-rs
    resource: "src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry"
    title: Bounded progressive response stream registry shared across bridge and SHM
  - id: transport-py-rs
    resource: "src/rs/vertebra-py/src/transport.rs::ResponseSubmitter"
    title: PyO3 response submitter awaiting submit_stream_frame and max_stream_frame_payload
  - id: asgi-protocol-rs
    resource: "src/rs/vertebra-py/src/asgi.rs::AsgiProtocol"
    title: Rust ASGI protocol currently buffering more_body chunks until final send
  - id: dispatch-py
    resource: "src/py/vertebra/server/_dispatch.py::_submit_coerced"
    title: Native App response coercion lacking streaming tuple and iterable dispatch
  - id: worker-py
    resource: "src/py/vertebra/server/worker.py::Worker"
    title: Worker startup loop currently swallowing lifespan and RSGI init errors
  - id: supervisor-py
    resource: "src/py/vertebra/supervisor/_supervisor.py::find_server_binary"
    title: Server binary discovery expecting vertebra/bin/vertebra-server
  - id: standalone-py
    resource: "src/py/vertebra/build/packaging/standalone.py::StandaloneBuilder"
    title: Standalone distribution builder currently skipping dot-prefixed .agents/skills
  - id: fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Shared FastPath core awaiting CompiledRouteBase and single-attribute helpers
  - id: id-rs
    resource: "src/rs/vertebra-core/src/id.rs::fill_os_random"
    title: UUID entropy generator with time-seeded fallback and no post-fork reseed
---

# Specification: Server Runtime Correctness

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Native Rust stream transport (`ResponseStreamFrame`, `StreamDelivery`, `BatchBridge::submit_stream_frame`, `VRSF` xlarge SHM ring framing, `ResponseStreamRegistry`, and lossless `shm_reader::drain_worker` routing) is merged on `main` [^bridge-core-rs] [^ring-pair-rs] [^streams-http-rs].
- **Progressive Python, ASGI, RSGI & Native Streaming**: `_vertebra.submit_stream_frame` and `max_stream_frame_payload` connect Rust `AsgiProtocol`, `RsgiHttpProtocol`, Python `AsgiSend`, and native `App` `_dispatch.py` `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` responses to progressive transport frames ([#81](https://github.com/cofin/vertebra/issues/81)) [^transport-py-rs] [^asgi-protocol-rs] [^dispatch-py].
- **Fail-Loud Startup, Supervisor API & Worker Lanes**: Lifespan and `__rsgi_init__` failures raise `WorkerStartupError` (SHM exit `78`, stopping Rust supervisor respawn) ([#84](https://github.com/cofin/vertebra/issues/84)); `vertebra.supervisor` exports its public launcher API ([#83](https://github.com/cofin/vertebra/issues/83)); `vtb workers run` and `vtb run --worker-only` bind `QueueService` and `HealthProbeServer` ([#85](https://github.com/cofin/vertebra/issues/85)) [^worker-py].
- **Wheel Binary & Standalone Skills Staging**: Every `vertebra-core` wheel packages a relocatable `vertebra/bin/vertebra-server` verified by `smoke_vtb_run.py` ([#82](https://github.com/cofin/vertebra/issues/82)), and `StandaloneBuilder` supports opt-in staging of user `.agents/skills` without bundling Vertebra's own repository `.agents/` ([#115](https://github.com/cofin/vertebra/issues/115)) [^supervisor-py] [^standalone-py].
- **FastPath & Native Hygiene**: Host frameworks consolidate onto `CompiledRouteBase` and `__vertebra_fastpath__`, UUID entropy panics on OS failure and reseeds on `fork()`, `_module_cache` and `_zero_copy` Python fallbacks are deleted, and Spanner `batch_write` drops inert `max_commit_delay_ms` [^fastpath-core-py] [^id-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Merged Baseline (`18baadcd`) & Remaining Problem Statement

Commit `18baadcd` ([PR #118](https://github.com/cofin/vertebra/pull/118)) landed the native Rust streaming foundation documented in [tiered-rings-and-batch-bridge.md](../../knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md): `ResponseStreamFrame` and `StreamSink` on `BatchBridge`, ordered `VRSF` (`0x56525346`) frames on the SHM xlarge response ring, `ResponseStreamRegistry` (`deliver_blocking` and `deliver`), and lossless `shm_reader::drain_worker` routing with the dead 16-byte `ResponseSlot` path removed [^bridge-core-rs] [^ring-pair-rs] [^streams-http-rs].

Remaining runtime gaps span five areas:
1. **Python & Protocol Streaming ([#81](https://github.com/cofin/vertebra/issues/81))**: `vertebra-py` still lacks `submit_stream_frame` and `max_stream_frame_payload`; `AsgiProtocol::send` and `RsgiHttpProtocol` accumulate streaming chunks until handler return; `AsgiSend` buffers fallback responses; and native `App` `server/_dispatch.py` coerces `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` via `encode_json_bytes`, raising `TypeError` (`500`) on native SSE endpoints [^transport-py-rs] [^asgi-protocol-rs] [^dispatch-py].
2. **Lifespan, Supervisor & Worker Modes ([#83](https://github.com/cofin/vertebra/issues/83), [#84](https://github.com/cofin/vertebra/issues/84), [#85](https://github.com/cofin/vertebra/issues/85))**: `Worker.run` logs and swallows ASGI lifespan and RSGI `__rsgi_init__` exceptions while `Supervisor::poll_workers` respawns crashing startup workers; `vertebra.supervisor/__init__.py` is docstring-only; `vtb workers run` exits immediately without a bound `QueueService`; and `vtb run --worker-only` sets unread `VERTEBRA_WORKER_ONLY` and launches the HTTP server [^worker-py].
3. **Wheel Binary & Standalone Skills Packaging ([#82](https://github.com/cofin/vertebra/issues/82), [#115](https://github.com/cofin/vertebra/issues/115))**: `vertebra-http/build.rs` embeds build-host absolute rpaths, `pyproject.toml` excludes `vertebra/bin/vertebra-server` from wheels, `release-binaries.yml` lacks `linux-arm64`, and `StandaloneBuilder.build` skips dot-prefixed `.agents/skills` in user applications [^supervisor-py] [^standalone-py].
4. **FastPath & Runtime Hygiene**: Five host frameworks duplicate `CompiledRoute` fields and probe four fastpath alias attributes; `id.rs` falls back to `SystemTime` splitmix without `pthread_atfork` reseeding; `_zero_copy.py` carries `_py_*` fallbacks; `_module_cache` duplicates `sys.modules`; and Spanner `batch_write` accepts an ignored `max_commit_delay_ms` parameter [^fastpath-core-py] [^id-rs].

### 1.2 Non-Goals

- Altering the buffered `AsgiRunner.__call__` return contract or changing `FrameReceiver`'s `Infallible` error type (mid-stream errors emit `FLAG_ERROR` and close the body early, recorded in [server-runtime-limitations.md](../../knowledge/domains/server/server-runtime-limitations.md)).
- Queue backend task registration (`queues-tasks-and-channels`) or local `EmbeddingBroker` / `GemmaEmbedder` event-loop hardening (owned by sibling flow `gcp-embeddings-and-identity`).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **`R1` PyO3 Stream Submission**: Expose `_vertebra.submit_stream_frame(request_id, status, flags, headers, body) -> None`, `_vertebra.max_stream_frame_payload() -> int`, and `STREAM_FLAG_*` constants with detached-GIL backpressure, automatic SHM xlarge payload chunking, and `ConnectionResetError` on closed clients.
- **`R2` Progressive ASGI & RSGI Flush ([#81](https://github.com/cofin/vertebra/issues/81))**: Flush `http.response.start` + non-final `http.response.body`, `http.response.zerocopysend`, and `http.response.sse` chunks plus RSGI `response_stream` `send_bytes`/`send_str` calls immediately in direct-submit mode while preserving zero-copy unary fast paths for single-chunk responses.
- **`R3` Python Fallback & Native `App` Streaming**: Stream `AsgiSend` fallback chunks via injected `stream_submitter` and stream native `App` `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` and streaming `Response` bodies in `server/_dispatch.py` end-to-end across embedded and SHM `vertebra-server` topologies.
- **`R4` Fail-Loud Startup ([#84](https://github.com/cofin/vertebra/issues/84))**: Propagate `WorkerStartupError(prefix, phase)` on lifespan errors, `30s` startup timeouts (cancelling the task), and `__rsgi_init__` failures, unwinding started mounts; exit SHM workers with `EXIT_STARTUP_FAILURE = 78` so `Supervisor::poll_workers` halts respawn and terminates `vertebra-server` non-zero.
- **`R5` Supervisor Exports & Worker Lanes ([#83](https://github.com/cofin/vertebra/issues/83), [#85](https://github.com/cofin/vertebra/issues/85))**: Lazily export `build_server_command`, `find_repo_root`, `find_server_binary`, `run_supervisor`, and `worker_main` from `vertebra.supervisor`; bind `WorkerRunner.resolve_queue_service` across `vtb workers run` (`run_until_stopped=True`) and `--with-workers`; implement `vtb run --worker-only` behind `HealthProbeServer`.
- **`R6` Relocatable Wheel Binary & Smoke Matrix ([#82](https://github.com/cofin/vertebra/issues/82))**: Build per-ABI relocatable `vertebra-server` binaries via `tools/scripts/build_server_binary.py`, resolve `PYTHONHOME` + shared `libpython` in `EmbeddedPythonRuntime`, include `vertebra/bin/*` in core wheels with libpython-excluding repairs, add `linux-arm64` standalone binaries, and gate PR CI + release wheels with `tools/scripts/smoke_vtb_run.py` (`make core-wheel-check`).
- **`R7` Opt-In User `.agents/skills` Standalone Staging ([#115](https://github.com/cofin/vertebra/issues/115))**: Add opt-in `include_agent_skills: bool = False` on `StandaloneBuilder` (`vtb build --include-agent-skills`) staging user `<project>/.agents/skills` into `app/.agents/skills` (excluding non-`skills` `.agents/` state and refusing implicit staging of Vertebra's own repository `.agents/`) and anchoring relative skill lookup to `app_dir` in standalone and `OnefileBuilder` bootstraps.
- **`R8` FastPath Consolidation**: Unify host `CompiledRoute` descriptors onto `CompiledRouteBase` in `_fastpath_core.py`, attach and resolve fastpaths exclusively through `__vertebra_fastpath__`, share one `has_custom_exception_handlers`, and dispatch Starlette/FastAPI `BUFFERED_RESPONSE_TYPES` by exact type without copying `memoryview`/`bytes` bodies.
- **`R9` Runtime & Native Cleanup**: Use `getrandom::fill` with panic-on-failure and `pthread_atfork` `FORK_GENERATION` reseeding in `vertebra-core/src/id.rs`, remove `_module_cache` from `module_loader.py` and `typing.py`, bind `_zero_copy.py` helpers directly from `vertebra._vertebra`, and remove `max_commit_delay_ms` from sync/async Spanner `batch_write`.
- **`R10` Living Docs & OKF Sync**: Synchronize `docs/`, runtime/IPC/FFI OKF chapters (compacting `pyo3-ffi-and-batch-dispatch.md` to `<= 200` lines), `server-runtime-limitations.md`, and `product.md`.

### 2.2 Key Public Signatures

```python
# src/py/vertebra/_vertebra.pyi
STREAM_FLAG_STREAMING: Final[int]
STREAM_FLAG_FINAL: Final[int]
STREAM_FLAG_ERROR: Final[int]
STREAM_FLAG_HEADERS_SENT: Final[int]
STREAM_FLAG_TRAILERS: Final[int]
def max_stream_frame_payload() -> int: ...
def submit_stream_frame(
    request_id: int,
    status: int,
    flags: int,
    headers: Sequence[tuple[bytes | str, bytes | str]] | None,
    body: Buffer,
) -> None: ...

# src/py/vertebra/server/worker.py & supervisor/_supervisor.py & build/packaging/standalone.py
class WorkerStartupError(VertebraError, RuntimeError):
    def __init__(self, prefix: str, phase: Literal["lifespan", "rsgi_init"]) -> None: ...

class EmbeddedPythonRuntime(msgspec.Struct, frozen=True, gc=False):
    home: str
    library_dir: str
    library_file: str | None
    @classmethod
    def detect(cls) -> Self: ...
    def require_shared_library(self, binary: Path) -> None: ...
    def apply(self, env: dict[str, str]) -> None: ...

class StandaloneBuilder:
    def __init__(
        self,
        project_dir: Path,
        target: BuildTarget,
        entrypoint: str = "app:app",
        *,
        server_binary: Path | None = None,
        profile: str = "release",
        include_agent_skills: bool = False,
    ) -> None: ...
```

## 3. Documentation & OKF Knowledge Plan

All public documentation and OKF chapters updated in `ch07-docs-and-okf-sync` stay `<= 200` lines (`<= 40`-line truth block):
- **Public Docs (`t01`–`t02`)**: `docs/usage/server-and-workers.md`, `docs/usage/packaging-and-deployment.md`, `docs/reference/cli.md`, `docs/getting-started/installation.md`, `docs/getting-started/cli.md`, and `docs/reference/build-and-wasm.md`.
- **Architecture, Domain & Product OKF (`t03`–`t04`)**: [tiered-rings-and-batch-bridge.md](../../knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md), [pyo3-ffi-and-batch-dispatch.md](../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md) (`212L -> <= 200L`), [asgi-and-wsgi-runners.md](../../knowledge/architecture/worker/asgi-and-wsgi-runners.md), [worker-and-protocols.md](../../knowledge/domains/server/worker-and-protocols.md), [server-runtime-limitations.md](../../knowledge/domains/server/server-runtime-limitations.md), and [product.md](../../product/product.md).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Tasks | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R3` Streaming | `ch01` (`t01`–`t04`) | `src/rs/vertebra-py/src/{transport,asgi,rsgi/http}.rs`, `src/py/vertebra/server/{_dispatch,_asgi/*}.py` | `cargo test -p vertebra-py` + `stubtest` + unit/integration streaming suites |
| `R4`–`R5` Lifespan & Lanes | `ch02` (`t01`–`t04`) | `src/rs/vertebra-http/src/workers/*`, `src/py/vertebra/{server,supervisor,cli}/*` | `cargo test -p vertebra-http --lib workers` + runner/CLI unit suites |
| `R6`–`R7` Packaging & `#115` | `ch03` (`t01`–`t04`), `ch04` (`t01`–`t03`) | `src/rs/vertebra-http/build.rs`, `tools/scripts/{build_server_binary,smoke_vtb_run}.py`, `standalone.py`, `.github/workflows/*` | `pytest test_packaging_and_pgo.py` + `make core-wheel-check` |
| `R8` FastPath Consolidation | `ch05` (`t01`–`t04`) | `src/py/vertebra/server/{_fastpath_core,_fastpath}.py`, `extensions/{litestar,starlette,fastapi,flask,django}/*` | `mypy` + host framework plugin unit suites |
| `R9` Native & Driver Cleanup | `ch06` (`t01`–`t03`) | `src/rs/vertebra-core/src/id.rs`, `src/py/vertebra/{utils,typing,db/adapters/spanner,extensions/sqlspec}/*` | `cargo test -p vertebra-core --lib id::` + typing/Spanner unit suites |
| `R10` Docs & OKF Sync | `ch07` (`t01`–`t04`) | `docs/**`, `.agents/bundles/{knowledge,product}/**` | `make docs && make review-check` |

## 5. Implementation Plan

- [ ] [ch01-python-stream-submission-and-flush](chapters/ch01-python-stream-submission-and-flush/spec.md) — PyO3 `submit_stream_frame` / `max_stream_frame_payload`, progressive ASGI & RSGI flush ([#81](https://github.com/cofin/vertebra/issues/81)), and native `App` `_dispatch.py` streaming (`4` tasks; `< 45s` gate)
- [ ] [ch02-lifespan-supervisor-and-worker-lanes](chapters/ch02-lifespan-supervisor-and-worker-lanes/spec.md) — Fail-loud `WorkerStartupError` + SHM exit `78` ([#84](https://github.com/cofin/vertebra/issues/84)), `vertebra.supervisor` exports ([#83](https://github.com/cofin/vertebra/issues/83)), and `QueueService` / `--worker-only` lanes ([#85](https://github.com/cofin/vertebra/issues/85)) (`4` tasks; `< 35s` gate)
- [ ] [ch03-relocatable-server-binary-and-packaging](chapters/ch03-relocatable-server-binary-and-packaging/spec.md) — Relocatable `vertebra-server` build, `EmbeddedPythonRuntime`, core wheel inclusion ([#82](https://github.com/cofin/vertebra/issues/82)), and opt-in `StandaloneBuilder` `.agents/skills` staging ([#115](https://github.com/cofin/vertebra/issues/115)) (`4` tasks; `< 25s` gate)
- [ ] [ch04-wheel-smoke-and-ci](chapters/ch04-wheel-smoke-and-ci/spec.md) — `smoke_vtb_run.py`, `make core-wheel-check`, PR CI smoke job, and release wheel/arm64 binary matrix (`3` tasks; `< 30s` gate)
- [ ] [ch05-fastpath-consolidation](chapters/ch05-fastpath-consolidation/spec.md) — Shared `CompiledRouteBase`, single `__vertebra_fastpath__` attribute, `has_custom_exception_handlers`, and copy-free response serialization (`4` tasks; `< 25s` gate)
- [ ] [ch06-runtime-native-cleanup](chapters/ch06-runtime-native-cleanup/spec.md) — Fail-loud `getrandom` + post-fork entropy reseed, `_module_cache` removal, direct `_zero_copy` native bindings, and Spanner `batch_write` signature cleanup (`3` tasks; `< 25s` gate)
- [ ] [ch07-docs-and-okf-sync](chapters/ch07-docs-and-okf-sync/spec.md) — Public `docs/`, `pyo3-ffi-and-batch-dispatch.md` (`<= 200L`), IPC/runner/worker OKF chapters, `server-runtime-limitations.md`, and `product.md` (`4` tasks; `< 35s` gate)

## 6. Current State & Remaining Plan Snapshot

- **Execution Frontier**: Active at `ch01-python-stream-submission-and-flush` (`server-runtime-correctness:ch01:t01`), anchored to `main @ 18baadcd` (`26` open `compact` tasks across `ch01`–`ch07`; prior native bridge/SHM stream transport chapter landed in `18baadcd`).
- **Downstream Unblock**: Completing `ch01` unblocks `mcp-protocol-completeness` `ch03` (`GET /mcp` SSE adapter stream writers) and `http3-and-http-hygiene`.
- **Full Flow Finish Gate (`/flow:finish`)**: `make lint-all && make rs-test && make py-test && make core-wheel-check && make docs-check && make review-check && make review-route BASE=18baadcd`.

[^bridge-core-rs]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — In-process bridge with ResponseStreamFrame and StreamSink landed in 18baadcd.
[^ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair` — SHM VRSF xlarge stream frames and max_stream_frame_payload landed in 18baadcd.
[^streams-http-rs]: `src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry` — Bounded progressive response stream registry shared across bridge and SHM.
[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 response submitter awaiting submit_stream_frame and max_stream_frame_payload.
[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — Rust ASGI protocol currently buffering more_body chunks until final send.
[^dispatch-py]: `src/py/vertebra/server/_dispatch.py::_submit_coerced` — Native App response coercion lacking streaming tuple and iterable dispatch.
[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — Worker startup loop currently swallowing lifespan and RSGI init errors.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` — Server binary discovery expecting vertebra/bin/vertebra-server.
[^standalone-py]: `src/py/vertebra/build/packaging/standalone.py::StandaloneBuilder` — Standalone distribution builder currently skipping dot-prefixed .agents/skills.
[^fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPath core awaiting CompiledRouteBase and single-attribute helpers.
[^id-rs]: `src/rs/vertebra-core/src/id.rs::fill_os_random` — UUID entropy generator with time-seeded fallback and no post-fork reseed.
