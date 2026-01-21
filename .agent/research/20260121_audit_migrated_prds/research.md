# Research: Audit and Verification of Migrated Legacy PRDs

## 1. Introduction
**Goal:** Conduct a comprehensive audit of all migrated active PRDs to verify their technical veracity, identify architectural misalignments, and ensure they represent the optimal path forward for Vertebra 1.0.
**Motivation:** Legacy specifications were migrated without deep validation. We need to ensure we aren't carrying forward suboptimal decisions, technical debt, or deprecated patterns. The goal is to build the "fastest and best" system with clean breaks from legacy shims.

## 2. Context Analysis
### Project Fit
- **Product Definition:** High-performance, zero-copy, polyglot backbone.
- **Tech Stack:** Python 3.12+, Rust (PyO3/Napi-rs), Bun, Arrow IPC, Shared Memory.
- **Constraints:** Performance and Security are paramount. No backward compatibility constraints (Clean Breaks).

### Scope
- **Target PRDs:**
    1. `database-orm-integration`
    2. `server-runtime`
    3. `distribution-onefile`
    4. `vertebra-1-0-release`
    5. `documentation`
- **Depth:** Full gamut (High-level to Deep-dive).

## 3. Investigation

### 3.1 Database & ORM Integration Audit
**Focus:** SQLSpec, SQLAlchemy, Advanced Alchemy, Serialization.
**Key Questions:**
1. Is the proposed `Broker*` -> `Vertebra*` rename sufficient, or should we rethink the abstraction entirely?
2. Is the "fairness scheduler" in the broker actually optimal, or does it introduce unnecessary latency?
3. Is the proposed Arrow integration deep enough, or just a surface-level shim?
4. Are the 3 distinct Litestar plugins necessary, or can they be unified?

### 3.2 Server & Runtime Audit
**Focus:** MCP, Health Checks, Observability.
**Key Questions:**
1. Is the Stdio transport implementation robust enough for high-throughput AI agent use?
2. Are the proposed health checks (SELECT 1) sufficient for high-availability production pools?
3. Does the metric collection strategy (Prometheus) introduce locking overhead?

### 3.3 Distribution Audit
**Focus:** PBS, Single-file builds, Supervisor.
**Key Questions:**
1. Is the current supervisor implementation race-free?
2. Does the single-file build process correctly handle native extension linking across platforms?

## 4. Synthesis & Recommendations

### 4.1 Database & ORM Integration (`database-orm-integration`)
**Status:** ⚠️ Partially Stale / Incomplete
- **Finding:** The PRD lists "API Rename (Broker* -> Vertebra*)" as a pending P0 task, but the codebase (`src/py/vertebra/extensions/sqlspec/`) already uses `Vertebra*` prefixes.
- **Finding:** The `serialization` module (`src/py/vertebra/extensions/serialization/`) is completely missing. This is a critical gap for the 1.0 release.
- **Finding:** Storage integration (`_storage.py`) is missing.
- **Recommendation:** Update PRD to mark renames as complete. prioritize `serialization` module implementation immediately.

### 4.2 Server & Runtime (`server-runtime`)
**Status:** ❌ Significant Divergence
- **Finding:** The PRD specifies a full Python MCP package (`src/py/vertebra/mcp/`) with decorators, registry, and schema generation.
- **Finding:** The codebase contains only a single `src/py/vertebra/mcp.py` stub.
- **Finding:** TypeScript implementation (`src/ts/src/mcp/`) is ahead of Python.
- **Recommendation:** The Python MCP implementation needs to be built from scratch to match the spec. The existing `mcp.py` is likely a placeholder.

### 4.3 Distribution & Packaging (`distribution-onefile`)
**Status:** ⚠️ Structural Mismatch
- **Finding:** The PRD calls for a `src/py/vertebra/cli/` package with separate modules (`init.py`, `build.py`, `serve.py`).
- **Finding:** The codebase has a monolithic `src/py/vertebra/cli.py` file.
- **Finding:** The `vertebra init` command is completely missing from the CLI.
- **Recommendation:** Refactor `cli.py` into the specified package structure and implement `vertebra init`.

### 4.4 Strategic Recommendations
1.  **Clean Break on MCP:** Delete `src/py/vertebra/mcp.py` and implement the full package structure defined in the PRD.
2.  **Refactor CLI:** Move `src/py/vertebra/cli.py` to `src/py/vertebra/cli/__init__.py` and split commands into modules to support `vertebra init`.
3.  **Serialization First:** The `serialization` module is a dependency for almost all other extensions (SQLAlchemy, Litestar). It must be implemented before proceeding with further ORM integration.

## 5. Next Steps
- [ ] Update `database-orm-integration` PRD to reflect current state (renames done).
- [ ] Update `server-runtime` PRD to explicitly flag the Python MCP rewrite.
- [ ] Update `distribution-onefile` PRD to include the CLI refactor task.
- [ ] Create a new "Refactor Plan" to execute these structural fixes before adding new features.
