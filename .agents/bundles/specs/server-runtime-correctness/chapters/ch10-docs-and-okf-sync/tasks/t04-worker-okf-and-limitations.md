---
type: Task
id: "server-runtime-correctness:ch10:t04"
chapter_id: ch10-docs-and-okf-sync
title: Sync worker and lanes OKF chapters and record server runtime limitations
description: Update the worker-and-protocols and worker-lanes OKF chapters for WorkerStartupError, exit code 78, supervisor exports, EmbeddedPythonRuntime, QueueService resolution, worker-only mode, and HealthProbeServer, and create the server-runtime-limitations chapter recording every caveat of this flow.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "server-runtime-correctness:ch10:t03"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/spec.md
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch09-runtime-native-cleanup/spec.md
  - .agents/bundles/knowledge/domains/server/worker-and-protocols.md
  - .agents/bundles/knowledge/domains/server/worker-lanes.md
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/supervisor/_supervisor.py
files:
  - .agents/bundles/knowledge/domains/server/worker-and-protocols.md
  - .agents/bundles/knowledge/domains/server/worker-lanes.md
  - .agents/bundles/knowledge/domains/server/server-runtime-limitations.md
tests: []
estimated_loc: 180
plan_revision: 2
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
  - id: worker-okf
    resource: .agents/bundles/knowledge/domains/server/worker-and-protocols.md
    title: Python server worker and protocol adapters chapter
  - id: lanes-okf
    resource: .agents/bundles/knowledge/domains/server/worker-lanes.md
    title: Multi-lane worker runner chapter
  - id: limitations-precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Limitations chapter layout precedent
tags:
  - okf
  - limitations
---

# Task t04: Sync Worker and Lanes OKF Chapters and Record Server Runtime Limitations

## Objective & Scope Boundary

Update two OKF chapters and create `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` (`type: Gotcha`, v0.2 frontmatter as in the storage limitations precedent). Out of scope: indexes (`ch11`).

## Bounded Read Manifest

- [Root spec](../../../spec.md) — Sections 1.2 and 2.4.
- `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` [^worker-okf].
- `.agents/bundles/knowledge/domains/server/worker-lanes.md` [^lanes-okf].
- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` [^limitations-precedent].
- `src/py/vertebra/server/worker.py::WorkerStartupError`; `src/py/vertebra/supervisor/_supervisor.py::EmbeddedPythonRuntime`.

## Symbol & Signature Contracts

- `worker-and-protocols.md`: startup failure contract (`WorkerStartupError` phases, ordered unwind, `EXIT_STARTUP_FAILURE = 78`, embedded non-zero exit); `vertebra.supervisor` lazy exports; `find_server_binary` packaged-first order plus `EmbeddedPythonRuntime` (`PYTHONHOME`, library path, fail-loud packaged check); replace the "freshest compiled binary" sentence with both packaged and development resolution.
- `worker-lanes.md`: `WorkerRunner.resolve_queue_service` and `QUEUE_SERVICE_STATE_KEY`; `vtb workers run` lifetime; colocated binding via `VERTEBRA_APP`; `vtb run --worker-only` and `HealthProbeServer` routes and status codes.
- `server-runtime-limitations.md` records, each as a bullet with the governing symbol:
  - Error frames end streamed bodies early without a transport-level abort (`FrameReceiver` is `Infallible`).
  - SHM workers cannot observe client disconnects on `send`; the reader discards frames for closed streams (`ResponseStreamRegistry::deliver`).
  - A slow SHM client stalls the response reader of its own worker ring while delivery awaits capacity (`shm_reader::drain_worker`).
  - Embedded backpressure blocks the producing worker thread with the GIL detached (`ResponseStreamRegistry::deliver_blocking`); calling it on an async runtime thread returns `Closed`.
  - The buffered batch path (`AsgiRunner.__call__`, `_AsgiMountRunner.__call__`) still collects full bodies.
  - `lifespan.shutdown.failed` is logged, not fatal.
  - The packaged binary requires a shared libpython; `PYTHONHOME` is overwritten with `sys.base_prefix` (`EmbeddedPythonRuntime`).
  - Linux wheels link the python-build-standalone libpython at build time and load the user's libpython with the same SONAME at run time; macOS resolves `@rpath/libpython…dylib` through `DYLD_LIBRARY_PATH`, which SIP strips when launched through protected binaries, so `run_supervisor` spawns the server directly.
  - The SHM topology is POSIX-only; Windows streams only in embedded topology.
  - `HealthProbeServer` answers single `GET` requests with `connection: close` only.
  - UUID generation panics when the OS entropy source fails; there is no deterministic fallback (`vertebra_core::id::fill_os_random`).

## Edge Cases & Error Semantics

- Limitations are stated as current facts, not plans.
- No customer, internal product, or workstation path names.

## Public-Seam Test Blueprint

- **Verification category**: `documentation_validation`.

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Limitations frontmatter | Parse YAML frontmatter of the new chapter | `type: Gotcha`, `status: stable`, non-empty `sources` with matching footnotes |
| Coverage | `git grep -c "^- " -- .agents/bundles/knowledge/domains/server/server-runtime-limitations.md` | At least `11` bullets |
| Stale sentence | `git grep -n "always launches the freshest compiled" -- .agents/bundles/knowledge` | No matches |

## Verification

- Initial baseline: `git grep -n "always launches the freshest compiled" -- .agents/bundles/knowledge`
- Final:
  ```bash
  make review-check
  git grep -n "WorkerStartupError" -- .agents/bundles/knowledge/domains/server/worker-and-protocols.md
  git grep -n "resolve_queue_service" -- .agents/bundles/knowledge/domains/server/worker-lanes.md
  ```
- Planned local commit: `docs(okf): document worker startup, lanes, and server runtime limitations`

## Acceptance Criteria

- [ ] Worker and lanes chapters describe startup failure, supervisor exports, runtime resolution, queue binding, and worker-only mode (R18).
- [ ] `server-runtime-limitations.md` records every caveat listed above with symbol anchors (R18).

## Notes & Discoveries

[^worker-okf]: `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` — Python server worker and protocol adapters chapter.
[^lanes-okf]: `.agents/bundles/knowledge/domains/server/worker-lanes.md` — Multi-lane worker runner chapter.
[^limitations-precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations chapter layout precedent.
