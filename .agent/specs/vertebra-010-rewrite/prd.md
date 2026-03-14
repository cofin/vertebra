# Master PRD: Vertebra 0.1.0 — Ghost Framework Rewrite

## Context & North Star

Vertebra has gone through 3 POC libraries (Vertebra v1, Litecore, Bungi/Spikard) across 5 architectural eras. Key lessons:

1. **Rust owns the HTTP hot path** — every successful POC confirms this
2. **Batch-first execution** breaks the per-request FFI ceiling (~200k RPS vs ~88k ASGI)
3. **Shared memory** wins for data-plane/broker, NOT for every HTTP request
4. **ASGI/WSGI are compatibility layers**, not the native protocol
5. **Litecore moved AWAY from shared-memory workers** as the default HTTP path

**Goal:** Clean-slate rewrite of Vertebra 0.1.0 implementing the "Ghost Framework" architecture:
- Rust HTTP server + native routing + batch collection
- Zero-object PyO3 request proxies (lazy field access to Rust memory)
- Vectorized FFI (1 call per N requests, not 1:1)
- WSGI/ASGI as emulation layers over the native batch protocol
- Frontend tooling (Rolldown/Vite, Type Bridge, HMR)

**Performance Target:** 200k+ RPS on hello-world, exceeding WSGI's ~198k ceiling.

## Roadmap (Ordered Execution)

| # | Chapter | Flow ID | Beads | Depends On |
|---|---------|---------|-------|------------|
| 1 | Wipe Slate | `wipe-slate` | `vertebra-w4i.1` | — |
| 2 | Scaffold Base 0.1.0 | `scaffold-base` | `vertebra-w4i.2` | Ch 1 |
| 3 | Project Buildout | `project-buildout` | `vertebra-w4i.3` | Ch 2 |
| 4 | Native IPC Substrate | `native-ipc` | `vertebra-w4i.4` | Ch 2 |
| 5 | Native HTTP & Routing | `native-http` | `vertebra-w4i.5` | Ch 4 |
| 6 | Native FFI & Batch Runner | `native-ffi` | `vertebra-w4i.6` | Ch 4, 5 |
| 7 | WSGI Adapter | `wsgi-adapter` | `vertebra-w4i.7` | Ch 6 |
| 8 | ASGI Adapter | `asgi-adapter` | `vertebra-w4i.8` | Ch 6 |
| 9 | Frontend Integration | `frontend-integration` | `vertebra-w4i.9` | Ch 3, 6 |

## Dependency Graph

```
Ch1 (wipe) → Ch2 (scaffold) ─┬─→ Ch3 (buildout) ───────→ Ch9 (frontend)
                               │                              ↑
                               └─→ Ch4 (ipc) → Ch5 (http) → Ch6 (ffi) ─┬→ Ch7 (wsgi)
                                                                         ├→ Ch8 (asgi)
                                                                         └→ Ch9 (frontend)
```

## Global Constraints

1. **Python 3.12+** baseline — no 3.11 compatibility
2. **Every PRD must end with `make lint` + `make test` passing**
3. **No sockets in data plane** — shared memory only for IPC
4. **Cross-platform parity** (Linux/macOS/Windows) for MVP
5. **Optional dependencies** — use `MissingDependencyError` pattern
6. **Each spec must include exact file paths and code blocks**
7. **Each task must have explicit pass/fail criteria**

## Deferred (Not in This PRD)

- HTTP/3 QUIC transport (research: proxy termination first)
- Data Plane Bypass / SHM-to-Socket streaming (optimization phase)
- Vertebra PyClient WASM SDK (nice-to-have)
- Bun/TypeScript broker layer (replaced by Rust in Ghost Framework)

## Consolidated Spec Mapping

| Original Stub | Consolidated Into |
|---------------|-------------------|
| `wipe-slate_20260314` | Ch 1: wipe-slate |
| `scaffold-base_20260314` | Ch 2: scaffold-base |
| `cli-entrypoints_20260314` | Ch 3: project-buildout |
| `app-interface_20260314` | Ch 3: project-buildout |
| `project-buildout_20260314` | Ch 3: project-buildout |
| `supervisor-lifecycle_20260314` | Ch 3: project-buildout |
| `ipc-substrate_20260314` | Ch 4: native-ipc |
| `native-core_20260314` (Phase 1) | Ch 4: native-ipc |
| `http-server-h2_20260314` | Ch 5: native-http |
| `native-routing_20260314` | Ch 5: native-http |
| `batch-collector_20260314` | Ch 5: native-http |
| `native-core_20260314` (Phase 2) | Ch 5: native-http |
| `pyo3-request-proxy_20260314` | Ch 6: native-ffi |
| `vectorized-ffi-api_20260314` | Ch 6: native-ffi |
| `python-batch-runner_20260314` | Ch 6: native-ffi |
| `native-core_20260314` (Phase 3) | Ch 6: native-ffi |
| `wsgi-adapter_20260314` | Ch 7: wsgi-adapter |
| `wsgi-core-adapter_20260314` | Ch 7: wsgi-adapter |
| `wsgi-extension-support_20260314` | Ch 7: wsgi-adapter |
| `asgi-adapter_20260314` | Ch 8: asgi-adapter |
| `asgi-core-adapter_20260314` | Ch 8: asgi-adapter |
| `asgi-extension-support_20260314` | Ch 8: asgi-adapter |
| `frontend-integration_20260314` | Ch 9: frontend-integration |
| `build-orchestrator_20260314` | Ch 9: frontend-integration |
| `type-bridge-codegen_20260314` | Ch 9: frontend-integration |
| `hmr-dx-experience_20260314` | Ch 9: frontend-integration |

## Post-Scaffold Directory Structure (Reference)

```
vertebra/
├── pyproject.toml              # Python 3.12+, vertebra v0.1.0
├── Makefile                    # install, test, lint, rs-build, rs-test
├── src/
│   ├── py/vertebra/            # Python package
│   │   ├── __init__.py
│   │   ├── cli/                # Ch3: CLI entrypoints
│   │   ├── web/                # Ch3: VertebraApp, routing
│   │   ├── core/               # Ch3/6: supervisor, runner, worker
│   │   │   ├── build/          # Ch9: build orchestrator
│   │   │   └── codegen/        # Ch9: type bridge, HMR
│   │   ├── wsgi/               # Ch7: WSGI adapter
│   │   ├── asgi/               # Ch8: ASGI adapter
│   │   └── _vertebra.pyi       # Ch6: native extension stubs
│   └── rs/                     # Rust workspace
│       ├── Cargo.toml
│       ├── vertebra-core/      # Ch4: IPC substrate
│       ├── vertebra-http/      # Ch5: HTTP server + router
│       └── vertebra-py/        # Ch6: PyO3 bindings
├── src/py/tests/               # Test suite
│   ├── conftest.py
│   ├── test_cli.py             # Ch3
│   ├── test_app.py             # Ch3
│   ├── test_wsgi.py            # Ch7
│   ├── test_asgi.py            # Ch8
│   └── test_codegen.py         # Ch9
└── scratch/                    # Ch1: archived POC code
```
