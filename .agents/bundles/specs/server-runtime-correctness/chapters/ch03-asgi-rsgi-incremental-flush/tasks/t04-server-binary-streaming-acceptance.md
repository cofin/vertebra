---
type: Task
id: "server-runtime-correctness:ch03:t04"
chapter_id: ch03-asgi-rsgi-incremental-flush
title: Prove incremental streaming through vertebra-server in embedded and SHM topologies
description: Add a server-binary integration suite that launches vertebra-server from the development build against generated ASGI and RSGI apps and asserts that the first body chunk and first SSE event arrive before the app is released, in embedded and SHM topologies, plus closed-client and unary regressions.
status: draft
state: pending
priority: P1
verification_strategy: integration
depends_on:
  - "server-runtime-correctness:ch03:t01"
  - "server-runtime-correctness:ch03:t02"
  - "server-runtime-correctness:ch03:t03"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch03-asgi-rsgi-incremental-flush/spec.md
  - src/py/vertebra/supervisor/_supervisor.py
  - src/py/vertebra/cli/_launch.py
  - src/py/tests/integration/conftest.py
  - src/py/tests/conftest.py
  - src/rs/vertebra-http/src/main.rs
files:
  - src/py/tests/integration/server/__init__.py
  - src/py/tests/integration/server/conftest.py
  - src/py/tests/integration/server/test_vertebra_server.py
tests:
  - src/py/tests/integration/server/test_vertebra_server.py
estimated_loc: 240
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: supervisor-py
    resource: src/py/vertebra/supervisor/_supervisor.py::build_server_command
    title: Server binary resolution and command construction
  - id: launch-py
    resource: src/py/vertebra/cli/_launch.py::ServerLaunchEnv
    title: Server environment assembly including VERTEBRA_TOPOLOGY and VERTEBRA_WORKERS
  - id: main-rs
    resource: src/rs/vertebra-http/src/main.rs::run_server
    title: Embedded and SHM topology startup in the server binary
tags:
  - integration
  - streaming
  - server-binary
---

# Task t04: Prove Incremental Streaming through vertebra-server in Embedded and SHM Topologies

## Objective & Scope Boundary

Create the canonical server-binary integration module `src/py/tests/integration/server/test_vertebra_server.py` (with `__init__.py` and a local `conftest.py`) that writes app modules into `tmp_path`, launches `build_server_command()` with `ServerLaunchEnv(...).to_environ()` on a free port, and reads the response with a raw socket so arrival timing is observable. Each streaming app blocks on a file-based release gate (the test creates `tmp_path / "release"` after asserting the first chunk), proving delivery before completion. Out of scope: wheel installs (`ch07`), lifespan failures (`ch04` adds its cases to this module).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/supervisor/_supervisor.py::{build_server_command,find_server_binary}` [^supervisor-py].
- `src/py/vertebra/cli/_launch.py::ServerLaunchEnv.to_environ` [^launch-py].
- `src/py/tests/integration/conftest.py` — integration fixture conventions.
- `src/py/tests/conftest.py::_cleanup_shm` — autouse `/dev/shm/vtb-*` cleanup.
- `src/rs/vertebra-http/src/main.rs::{run_server,spawn_python_workers}` — startup logs and topology selection [^main-rs].

## Symbol & Signature Contracts

- `src/py/tests/integration/server/conftest.py`:
  - `class ServerProcess(msgspec.Struct)` with `process: subprocess.Popen[bytes]`, `port: int`, `log_path: Path`; methods `def request(self, raw: bytes, *, read_timeout: float = 5.0) -> socket.socket` (connect, send, return the socket for incremental reads) and `def stop(self) -> int` (SIGTERM, wait `10s`, return exit code).
  - `@pytest.fixture def launch_server(tmp_path: Path) -> Iterator[Callable[..., ServerProcess]]` — factory taking `app_source: str`, `topology: Literal["embedded", "shm"]`, `workers: int = 1`, `interface: str = "auto"`, `wait_ready: bool = True`; writes `tmp_path / "srvapp.py"`, builds env via `ServerLaunchEnv(app_path="srvapp:app", host="127.0.0.1", port=port, workers=workers, topology=topology).to_environ()`, sets `VERTEBRA_INTERFACE`, `cwd=tmp_path`, and (when `wait_ready`) waits until `connect` succeeds (`20s` deadline, failing with the captured log); stops every process on teardown. `ServerProcess` also exposes `def wait_exit(self, timeout: float) -> int` and `def log_text(self) -> str` for failure cases added by `ch04`.
  - `def free_port() -> int`.
- `src/py/tests/integration/server/test_vertebra_server.py` — module-level app sources as string constants; parametrized over `topology` (`"embedded"`, `"shm"` with `workers=2`; SHM cases are skipped only on non-POSIX via `pytest.mark.skipif(os.name != "posix", reason=...)`).

## Edge Cases & Error Semantics

- Readiness polling never sleeps longer than `0.05s` per attempt and raises `AssertionError` with the server log tail on timeout.
- Socket reads use `settimeout`; a missing first chunk within `5s` fails the test (the app is still blocked, so buffering would time out).
- Every case releases the gate in `finally` so the server shuts down cleanly.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/server/test_vertebra_server.py`, dependency category `integration` (spawns the development `vertebra-server` built by `make dev`).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| ASGI chunked body | App: start `200` `text/plain`, body `b"first\n"` `more_body=True`, wait for gate, body `b"second\n"` `more_body=False`; `GET /stream HTTP/1.1` | Headers and `b"first\n"` read before the gate exists; after creating the gate, `b"second\n"` and the chunked terminator arrive |
| ASGI SSE | App sends `http.response.body` with `b"data: 1\n\n"` `more_body=True`, waits, then `b"data: 2\n\n"` final; header `content-type: text/event-stream` | `b"data: 1\n\n"` observed before release |
| Starlette StreamingResponse | `starlette.responses.StreamingResponse` over an async generator yielding `b"a"`, awaiting the gate, then `b"b"` | `b"a"` observed before release |
| RSGI stream | RSGI app `proto.response_stream(200, [("content-type","text/plain")])`, `await transport.send_bytes(b"r1")`, wait, `send_bytes(b"r2")`; `interface="rsgi"` | `b"r1"` observed before release |
| Client disconnect | ASGI app streams `b"x"` every `0.05s` and records `ConnectionResetError` or `http.disconnect` into `tmp_path / "disconnected"`; test closes the socket after the first chunk | Marker file exists within `5s` (embedded); SHM case asserts the server keeps serving a following unary request |
| Unary regression | ASGI app returns `b"ok"` in one body message | Full response with `content-length: 2` |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync pytest src/py/tests/integration/server/test_vertebra_server.py -q`
- Final Green + lint:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/integration/server/test_vertebra_server.py -q
  uv run --no-sync ruff check src/py/tests/integration/server
  ```
- Planned local commit: `test(server): verify incremental streaming through vertebra-server`

## Acceptance Criteria

- [ ] ASGI body, ASGI SSE, Starlette streaming, and RSGI stream cases observe the first chunk before the app completes in embedded and SHM topologies (R8).
- [ ] Disconnect and unary regression cases pass (R8).

## Notes & Discoveries

[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::build_server_command` — Server binary resolution and command construction.
[^launch-py]: `src/py/vertebra/cli/_launch.py::ServerLaunchEnv` — Server environment assembly including `VERTEBRA_TOPOLOGY` and `VERTEBRA_WORKERS`.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::run_server` — Embedded and SHM topology startup in the server binary.
