# Gaps and Improvements Analysis
**Date:** 2026-01-18
**Version:** Pre-Alpha (PRD-3/4 In-Progress)
**Scope:** Source Code (@src) vs Documentation (@docs)

## Executive Summary

Vertebra has successfully established its core architecture: Rust-based IPC/Shared Memory primitives (PRD-1) and Python bindings with a sophisticated Type Bridge (PRD-2). The project is currently bridging the gap between these low-level primitives and high-level application usage (Server & Drivers).

The primary gap is the "Last Mile" integration: wiring the completed low-level drivers into standard ecosystems (SQLSpec, fsspec) and finalizing the Bun-to-Python server protocol (Streaming, Protobuf).

## 1. Architecture & Core Gaps

### Resource Broker Integration (PRD-5)
- **Status:** Scaffolding exists in `src/rs/vertebra-broker`.
- **Gap:** The broker is currently a standalone crate/binary. It is not yet integrated into the `vertebra-py` or `vertebra-js` runtime.
- **Impact:** Connection pooling is currently managed locally per-worker or via simple round-robin in Bun. The promised "global resource management" is missing.
- **Action:** Wire `vertebra-broker` into the Python/Bun IPC initialization flow.

### Python "Server" Entry Point
- **Status:** `src/py/vertebra/server/__init__.py` defines a `ServerBackend` protocol that raises `NotImplementedError`.
- **Gap:** Users cannot currently run `vertebra.serve()` to start the stack. They must run `bun run ...` from the TS workspace.
- **Action:** Implement a Python CLI wrapper that spawns the Bun runtime, effectively reversing the dependency to match the "Python-first" UX goal.

## 2. PRD-3: Bun Server & Protocol

### Missing Protocol Features
- **Protobuf IPC:** Documented as the control-plane format, but unimplemented. Currently relying on Msgpack/JSON.
- **Streaming:** Request-body streaming and `http.disconnect` propagation are listed as "Next Steps".
- **Advanced HTTP:** HTTP/3 and WebSocket IPC bridging are pending.

### Testing
- **Gap:** End-to-end tests for the server lifecycle (startup, request handling, shutdown, crash recovery) are minimal or rely on manual verification.

## 3. PRD-4: Drivers & Ecosystem

### SQLSpec Adapter (Critical)
- **Status:** Drivers for Postgres/SQLite/MySQL exist (`src/py/vertebra/drivers`).
- **Gap:** No adapter for SQLSpec. This is the intended "High Level" ORM layer. Without it, users are stuck with raw DB-API usage.
- **Action:** Implement `vertebra.drivers.sqlspec` adapters.

### fsspec Compliance
- **Status:** `src/py/vertebra/storage` exists with S3/GCS/Local backends.
- **Gap:** Compliance tests for `fsspec` are noted as missing.
- **Action:** Add `fsspec` registry registration and run the official `fsspec` conformance suite.

## 4. API Surface & Polish

### Top-Level Exports
- **Gap:** `vertebra/__init__.py` only exposes `procedures` and `serializers`.
- **Impact:** Users must import `vertebra.drivers` and `vertebra.storage` explicitly.
- **Recommendation:** Expose `drivers` and `storage` (or a unified `connect` entry point) at the top level to improve discoverability.

### Type Hints & Docs
- **Gap:** `docs/api/drivers.md` is good, but lacks examples of *using* the drivers with the Type Bridge (PRD-2).
- **Action:** Add "Cookbook" style docs for common patterns (e.g., "FastAPI + Vertebra + Postgres").

## 5. Testing Infrastructure

### Container Runtime Abstraction
- **Status:** `tools/lib/container.py` exists.
- **Gap:** It is not widely used in `tests/` or `src/py/tests`. Most tests seem to mock the IPC or run locally.
- **Action:** Refactor integration tests to use this abstraction for robust, hermetic environments (testing Postgres/Redis drivers against real containers).

## 6. Code Quality & Standards

### Anti-Patterns
- **Future Imports:** 50+ files usage `from __future__ import annotations`. Per project standards (Python 3.12+), this should be audited and potentially removed if native type evaluation is sufficient, or standardized.
- **Legacy Typing:** Usage of `Optional[...]` detected in tests. Should be modernized to `| None` syntax.
- **Type Checking:** `mypy` reports ~120 errors (mostly missing return types or loose typing). Strict mode compliance is a gap.
