# Dead Code Removal Inventory

**Date:** 2026-01-19
**Status:** Partially Complete

This document tracks dead code identified during the codebase consolidation review.

---

## COMPLETED REMOVALS (2026-01-19)

### 1. Proto Files - REMOVED

**Location:** `src/ts/src/proto/` (DELETED)

| File | Lines | Status |
|------|-------|--------|
| `asgi.proto` | 41 | DELETED |
| `config.proto` | ~20 | DELETED |
| `metrics.proto` | ~20 | DELETED |

**Reason:** Never compiled or imported; native TS types used instead.

---

### 2. Deprecated Filesystem Module - REMOVED

**Location:** `src/py/vertebra/filesystem/` (DELETED)

| File | Lines | Status |
|------|-------|--------|
| `__init__.py` | 35 | DELETED |

**Reason:** Backwards-compat shim with 0 callers; all code uses `vertebra.storage`.

---

### 3. Protobuf Protocol Option - REMOVED

**Location:** `src/ts/src/server/config.ts` and `src/ts/src/server/index.ts`

**Changes Made:**
- `IpcProtocol` type changed from `"msgpack" | "protobuf"` to `"msgpack"`
- Removed dead validation code that threw "Protobuf IPC protocol is not implemented yet"
- Simplified protocol validation

---

## REMAINING: Requires Review

### 4. Scratch Markdown Files (Untracked)

**Location:** Repository root

| File | Size | Reason for Removal |
|------|------|-------------------|
| `onefile.md` | 15KB | Build system notes, untracked in git |
| `onefile_serve.md` | 27KB | Serve process notes, untracked in git |
| `example.md` | 10KB | Example notes, untracked in git |
| `gaps_and_improvements.md` | 4KB | Planning notes, untracked in git |

**Evidence:**
- Git status shows `?? onefile.md` and `?? onefile_serve.md` (untracked)
- Not referenced in any build system or documentation structure
- Information likely covered in `docs/prd/` or `specs/active/`

**Action:**
- Review for any unique content worth preserving to `docs/`
- Delete after review

---

### 5. Empty Test Directories

**Location:** Rust workspace

| Directory | Reason for Removal |
|-----------|-------------------|
| `src/rs/vertebra-core/tests/` | Empty - all tests are inline (mod tests) |
| `src/rs/vertebra-broker/tests/` | Empty - no tests for FFI wrapper |

**Action:** Remove empty directories or add `.gitkeep` if convention requires

---

### 6. Legacy Transport Code Paths

**Location:** `src/py/vertebra/resources/broker.py` and `src/py/vertebra/server/worker.py`

**Patterns Found:**
```python
# broker.py:162
"""Connect using the legacy CFFI-based transport."""

# broker.py:330
"""Receive response using legacy CFFI transport."""

# worker.py:216
await self._handle_legacy_request(message, contexts, writer)

# worker.py:307
async def _handle_legacy_request(...)
```

**Analysis:**
- These "legacy" code paths coexist with new async transport
- May be fallback paths for platforms without eventfd
- Requires careful review before removal

**Action:** Review if legacy paths are still needed for:
- macOS/Windows fallback
- Error recovery scenarios
- Gradual migration support

---

### 7. `from __future__ import annotations` Statements

**Location:** 61+ Python files

**Context:**
- Vertebra requires Python 3.12+ per CLAUDE.md
- Python 3.12+ has PEP 563 postponed evaluation by default
- These imports are only needed for Python 3.7-3.10 compatibility

**Example files:**
- `src/py/vertebra/_internal/async_transport.py`
- `src/py/vertebra/resources/broker.py`
- `src/py/vertebra/server/worker.py`
- And 58+ others

**Action:**
- Option A: Remove all `from __future__ import annotations`
- Option B: Add linter rule to prevent new ones
- **Decision needed:** Confirm Python 3.12+ is truly the minimum

---

## Summary

| Category | Files/Items | Status |
|----------|-------------|--------|
| Proto files | 3 files (~80 lines) | REMOVED |
| Filesystem module | 1 directory (~35 lines) | REMOVED |
| Protobuf protocol option | 2 files (~10 lines) | REMOVED |
| Scratch markdown | 4 files (~57KB) | REVIEW NEEDED |
| Empty test dirs | 2 directories | REVIEW NEEDED |
| Legacy transport | Multiple locations | REVIEW NEEDED |
| Future imports | 61 files | REVIEW NEEDED |

---

## Commands for Remaining Items

```bash
# Scratch markdown files (after review for useful content)
rm onefile.md onefile_serve.md example.md gaps_and_improvements.md

# Empty test directories
rmdir src/rs/vertebra-core/tests/ src/rs/vertebra-broker/tests/
```

---

## Not Dead Code (Clarifications)

The following were investigated but confirmed to be **in use**:

1. **`vertebra-broker` Rust crate** - NOT dead. This is the C dynamic library (`libvertebra_broker.so`) loaded by both Python CFFI and Bun FFI. Contains the `vtb_*` C ABI functions.

2. **`src/ts/src/protocol/` directory** - NOT dead. Contains `serialization.ts` for msgpack encoding, actively imported.

3. **`_internal/async_transport.py`** - NOT dead. Used by `BrokerClient` for new async transport, though `PollingTransport` fallback may be unused.
