# Vertebra 1.0 Release Path

## Overview
This PRD consolidates the final critical path to the Vertebra 1.0 release. It focuses on stabilizing the core data plane (SQLSpec/Arrow), implementing the complete distribution toolchain (including single-file builds), and delivering a full-featured MCP (Model Context Protocol) runtime.

**Key constraints:**
- SQLAlchemy and Advanced Alchemy integrations are explicitly **out of scope**.
- "Bug squashing" is a primary deliverable, with a focus on shared memory safety, process lifecycle, and serialization correctness.
- Code signing/notarization is out of scope for implementation but the **process must be documented**.

## Research Reference
No prior research conducted

## Functional Requirements

### 1. Data Plane Stabilization (SQLSpec + Arrow)
#### Must Have
- [ ] **Arrow IPC Integration:** Automatic switch to Arrow serialization for large result sets (> threshold).
- [ ] **Msgspec Codec Hardening:** Robust encoding/decoding for all core Python types (Date, UUID, Enum, etc.) without corruption.
- [ ] **Serialization Edge Cases:** Fixes for known/potential issues in msgspec decoding logic.
- [ ] **Shared Memory Safety:** Zero-leak guarantee for ring buffers during process crashes or test failures.

### 2. Distribution & Packaging
#### Must Have
- [ ] **Universal Build Command:** `vertebra build` command handling wheels, Docker prep, and single-file builds.
- [ ] **Single-File Executable:** `vertebra build --one-file` producing a standalone binary (bundling Python + Bun + App) using `pyembed` or `python-build-standalone`.
- [ ] **Rust Supervisor:** Robust process management (GIL-release aware) for spawning Bun and Python workers.
- [ ] **Process Lifecycle:** Clean signal handling (SIGINT/SIGTERM) and zombie process prevention in both Dev and Prod modes.
- [ ] **Signing Documentation:** Comprehensive guide on how to sign and notarize the generated binaries for macOS (notarization) and Windows (code signing).

### 3. Server Runtime & MCP
#### Must Have
- [ ] **MCP Stdio Transport:** For CLI and Claude Desktop integration.
- [ ] **MCP SSE Transport:** Server-Sent Events for web clients.
- [ ] **MCP WebSocket Transport:** Full bidirectional streaming for agents.
- [ ] **Python Decorator API:** `@mcp_tool` and `@mcp_resource` for easy exposure of Python functions.
- [ ] **Schema Generation:** Automatic JSON schema generation from Python type hints for MCP tools.

## Non-Functional Requirements
- **Performance:** Arrow serialization must be faster than MessagePack for >1000 rows.
- **Reliability:** No zombie processes left behind after 100 consecutive start/stop cycles.
- **Portability:** Single-file builds must work on Linux (x86_64/aarch64) and macOS (x86_64/arm64). Windows is nice-to-have but not blocking.

## Technical Approach

### Recommended Implementation
1.  **Data Plane:** Finish the `Vertebra*` rename refactor (from previous work) and implement the `nanoarrow` based IPC stream in the Python driver.
2.  **Distribution:** Use `python-build-standalone` for the portable Python runtime. Implement the supervisor in Rust using `pyo3` to manage the lifecycle, releasing the GIL during the "wait" phase to allow the Python web server to run efficiently.
3.  **MCP:** Implement the Protocol in TypeScript (within the Bun Broker) and bridge it to Python via the existing shared memory ring buffer, treating tool calls as a specialized IPC message type.

### Libraries/APIs to Use
- **Python:** `nanoarrow`, `msgspec`, `click`, `rich-click`.
- **Rust:** `pyo3`, `pyembed` (optional/investigate), `signal-hook`.
- **TypeScript:** `zod` (for schema validation), native `Bun.serve` (for HTTP/WS).

### Files to Modify
- `src/py/vertebra/extensions/sqlspec/` (Driver & Configs)
- `src/py/vertebra/extensions/serialization/` (Msgspec codecs)
- `src/rs/vertebra-py/src/supervisor.rs` (New module)
- `src/ts/src/mcp/` (New module)
- `src/py/vertebra/cli.py` (Build commands)

## Acceptance Criteria
- [ ] **SQLSpec:** Querying 10k rows returns correct data via Arrow IPC.
- [ ] **Distribution:** `vertebra build --one-file` produces a binary that runs on a fresh machine without Python installed.
- [ ] **MCP:** A Python function decorated with `@mcp_tool` can be called from Claude Desktop via the single-file binary.
- [ ] **Stability:** Test suite passes 50 consecutive runs without hangs or leftover `/dev/shm` files.
- [ ] **Documentation:** Signing guide for macOS/Windows exists in `docs/distribution/signing.md`.

## Out of Scope
- SQLAlchemy Integration
- Advanced Alchemy Integration
- Actual certificate-based binary signing and notarization (only the process documentation is required).

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **Single-File Complexity:** Bundling Python + Bun + Dependencies is notoriously brittle. | High | High | Use `python-build-standalone` and a strictly controlled import path. |
| **Shared Memory Leaks:** Tests crashing leave `/dev/shm` dirty. | Medium | High | Implement a robust cleanup fixture that runs even on `SIGKILL`. |
| **MCP Protocol Drift:** The MCP spec is evolving. | Medium | Low | Stick to the stable core (Tools/Resources). |

## Recovery Strategy
**Rollback Trigger:** build command fails to produce working artifacts on CI, or data corruption detected in serialization.
**Rollback Steps:**
1. Revert the specific sub-module changes.
2. Use the standard `pip install` flow as a fallback if single-file builds fail.

**Safe Checkpoints:**
- **Checkpoint 1:** SQLSpec + Arrow fully working and tested. (Tag: `checkpoint/prd_20260121_vertebra_1_0_release_path/1`)

## Open Questions
- None currently.
