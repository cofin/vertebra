---
type: Task
id: async-loop-and-framework-fastpath:t01
title: "Add unified LoopBackend (uvloop, winloop, rloop, asyncio) selector and remove deprecated EventLoopPolicy APIs"
state: done
priority: P0
verification_strategy: behavior_tdd
depends_on: []
files:
  - pyproject.toml
  - src/py/vertebra/server/_gil_audit.py
  - src/py/vertebra/server/_loops.py
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/cli/_launch.py
  - src/py/vertebra/cli/run.py
  - src/py/vertebra/cli/dev.py
  - docs/reference/cli.md
tests:
  - src/py/tests/unit/server/test_runtime_tuning.py
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 2
claimed_by: agent:executor
claimed_at: 2026-11-02T00:12:20Z
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-02T00:19:06Z
last_verified_commit: 5763c37
verification_evidence: "uv run --no-sync pytest src/py/tests/unit/server/test_runtime_tuning.py -W 'error::DeprecationWarning:vertebra' -q (16 passed); ruff & mypy clean"
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:19:06Z
commit: null
tags: [server, event-loop, uvloop, winloop, rloop, tdd]
---

# Task t01: Add unified `LoopBackend` (`uvloop`, `winloop`, `rloop`, `asyncio`) selector and remove deprecated `EventLoopPolicy` APIs

## Objective
Introduce a factory-based `LoopBackend(StrEnum)` (`auto`, `uvloop`, `winloop`, `rloop`, `asyncio`) in `src/py/vertebra/server/_loops.py` and `--loop` (`VERTEBRA_LOOP`) in `vtb run`/`vtb dev`, upgrade `uvloop>=0.23.0` and add `winloop>=0.8.1` / optional `rloop>=0.5.0` in `pyproject.toml`, and remove deprecated `asyncio.set_event_loop_policy` / `asyncio.DefaultEventLoopPolicy` calls from `src/py/vertebra/server/_gil_audit.py` and `src/py/tests/unit/server/test_runtime_tuning.py`.

## Context
- **Verification Strategy (`behavior_tdd`)**: Write failing unit tests in `src/py/tests/unit/server/test_runtime_tuning.py` asserting that `LoopBackend`, `resolve_loop_backend`, `create_event_loop`, `EventLoopPool`, and `GilAuditReport.quarantine_unsafe_modules()` select the expected loop implementation and respect `quarantined_modules` without calling deprecated `asyncio.get_event_loop_policy()` or `asyncio.set_event_loop_policy()`.
- **Architectural Invariants**:
  - Zero inline `#` comments in Python files; use Google-style docstrings and PEP 585/604/695 types.
  - `uvloop` added free-threaded support in `0.22.0` and `eager_start` + `3.15`/`3.15t` wheels in `0.23.0`; only versions `<= (0, 21, 99)` should be marked GIL-unsafe in `src/py/vertebra/server/_gil_audit.py::detect_unsafe_modules`. Note that `detect_unsafe_modules()` formats entries as `"uvloop==0.21.0"` (`f"{name}=={ver}"`), so loop quarantine checks must match `m.split("==", 1)[0] in {"uvloop", "winloop", "rloop"}`.

## Steps
1. **Write failing unit tests (`RED`)**:
   - In `src/py/tests/unit/server/test_runtime_tuning.py`, update `test_gil_audit_quarantines_unsafe_modules_to_single_shard` and `test_gil_audit_quarantine_forces_single_loop_shard` to remove deprecated `asyncio.get_event_loop_policy()` / `asyncio.DefaultEventLoopPolicy` assertions and instead assert that `create_event_loop(quarantined_modules=frozenset(report.quarantined_modules))` and `EventLoopPool(gil_report=report)` fall back to a standard `asyncio` event loop when `"uvloop==0.21.0"` or `"uvloop"` is in `report.quarantined_modules`, with zero `DeprecationWarning`s under `-W error::DeprecationWarning`.
   - Add `test_loop_backend_resolution_and_cli_launch_env` in `src/py/tests/unit/server/test_runtime_tuning.py` testing `LoopBackend` (`AUTO`, `UVLOOP`, `WINLOOP`, `RLOOP`, `ASYNCIO`), `resolve_loop_backend`, `create_event_loop(backend=...)`, `VERTEBRA_LOOP` environment override, and `ServerLaunchEnv(app_path="app:app", host="127.0.0.1", port=8000, loop="rloop").to_environ()["VERTEBRA_LOOP"] == "rloop"`.
2. **Implement `LoopBackend`, `_gil_audit.py` fix, and `--loop` CLI option (`GREEN`)**:
   - In `pyproject.toml`:
     - Under `[project.dependencies]`, update `"uvloop>=0.23.0; sys_platform != 'win32'"` and add `"winloop>=0.8.1; sys_platform == 'win32'"`.
     - Under `[project.optional-dependencies]`, add `rloop = ["rloop>=0.5.0; sys_platform != 'win32'"]` and include `"rloop>=0.5.0; sys_platform != 'win32'"` in `all`.
     - Under `[[tool.mypy.overrides]]` (for untyped C-extensions alongside `"uvloop"`), add `"winloop"` and `"rloop"`.
   - In `src/py/vertebra/server/_gil_audit.py`:
     - In `GilAuditReport.quarantine_unsafe_modules`, remove `asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())` and set `os.environ["VERTEBRA_QUARANTINED_LOOPS"] = ",".join(sorted({m.split("==", 1)[0] for m in self.quarantined_modules if m.split("==", 1)[0] in {"uvloop", "winloop", "rloop"}}))` when any loop module is quarantined.
     - In `detect_unsafe_modules`, update the `uvloop` version threshold in `known_limits` from `(0, 22, 1)` to `(0, 21, 99)`.
   - In `src/py/vertebra/server/_loops.py`:
     - Define `class LoopBackend(StrEnum)` with `AUTO = "auto"`, `UVLOOP = "uvloop"`, `WINLOOP = "winloop"`, `RLOOP = "rloop"`, `ASYNCIO = "asyncio"`.
     - Implement `resolve_loop_backend(backend: LoopBackend | str | None = None, *, quarantined_modules: frozenset[str] | tuple[str, ...] = frozenset()) -> LoopBackend` checking explicit `backend`, `os.environ.get("VERTEBRA_LOOP", "auto")`, `VERTEBRA_QUARANTINED_LOOPS`, and normalized module prefixes (`m.split("==", 1)[0]`) in `quarantined_modules`.
     - Update `create_event_loop(backend: LoopBackend | str | None = None, *, quarantined_modules: frozenset[str] | tuple[str, ...] = frozenset()) -> asyncio.AbstractEventLoop` and `EventLoopPool.__init__` (adding `loop_backend: LoopBackend | str | None = None` and storing `self._quarantined_modules` from `gil_report.quarantined_modules`) to instantiate `rloop.new_event_loop()`, `winloop.new_event_loop()`, `uvloop.new_event_loop()`, or `asyncio.new_event_loop()` accordingly and attach `asyncio.eager_task_factory` when supported by the loop.
   - In `src/py/vertebra/cli/_launch.py::ServerLaunchEnv`, `src/py/vertebra/cli/run.py::run`, and `src/py/vertebra/cli/dev.py::dev`, add `loop: str = "auto"` and `--loop` (`click.Choice(["auto", "uvloop", "winloop", "rloop", "asyncio"], case_sensitive=False)`) writing `VERTEBRA_LOOP` when set.
3. **Refactor & verify**:
   - Run `uv run --no-sync python -m tools.docs.generate_cli_docs` to sync `docs/reference/cli.md` and verify with `ruff` and `mypy`.

## Verification
```bash
uv run --no-sync pytest src/py/tests/unit/server/test_runtime_tuning.py -W error::DeprecationWarning -q
uv run --no-sync ruff check src/py/vertebra/server/_gil_audit.py src/py/vertebra/server/_loops.py src/py/vertebra/cli/_launch.py src/py/vertebra/cli/run.py src/py/vertebra/cli/dev.py src/py/tests/unit/server/test_runtime_tuning.py
uv run --no-sync mypy src/py/vertebra/server/_gil_audit.py src/py/vertebra/server/_loops.py src/py/vertebra/cli/_launch.py src/py/vertebra/cli/run.py src/py/vertebra/cli/dev.py
```

## Acceptance Criteria
- [x] `LoopBackend`, `resolve_loop_backend`, and `create_event_loop` support `auto`, `uvloop`, `winloop`, `rloop`, and `asyncio` and honor `VERTEBRA_LOOP` and `quarantined_modules` (R1).
- [x] Zero `DeprecationWarning`s are emitted by `_gil_audit.py` or `test_runtime_tuning.py` on Python 3.14/3.15/3.15t (R1).
- [x] `vtb run` and `vtb dev` accept `--loop` and export `VERTEBRA_LOOP` via `ServerLaunchEnv` (R1).

## Notes & Discoveries
- Added `LoopBackend(StrEnum)` (`AUTO`, `UVLOOP`, `WINLOOP`, `RLOOP`, `ASYNCIO`), `resolve_loop_backend`, and `quarantined_modules` propagation through `create_event_loop` and `EventLoopPool`.
- Removed deprecated `asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())` from `GilAuditReport.quarantine_unsafe_modules` and exported `VERTEBRA_QUARANTINED_LOOPS` for quarantined loop extensions (`uvloop <= 0.21.99`).
