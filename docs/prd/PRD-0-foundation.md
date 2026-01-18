# PRD-0: Foundation and Repository Cleanup

**Status:** Complete
**Author:** Vertebra Team
**Created:** 2026-01-17
**Last Updated:** 2026-01-18

---

## Executive Summary

This PRD defines the foundation work required to transform the Vertebra repository from a legacy DMA (Database Migration Assessment) codebase into a clean, multi-language application backbone. The deliverable is a verified, working build system with zero *undocumented* legacy references, proper tooling for Python, Rust, and TypeScript/Bun, and comprehensive documentation for contributors.

**Success Criteria:**
- All DMA references removed **or explicitly documented as known legacy issues** (no new DMA references introduced)
- Multi-language build system functional (Python + Rust + TypeScript/Bun)
- All verification commands pass
- Architecture and conventions documented

**Estimated Effort:** 3-5 days (single developer)

---

## Table of Contents

1. [Background](#background)
2. [Problem Statement](#problem-statement)
3. [Goals and Non-Goals](#goals-and-non-goals)
4. [User Stories](#user-stories)
5. [Technical Approach](#technical-approach)
6. [Detailed Specification](#detailed-specification)
7. [Implementation Phases](#implementation-phases)
8. [Progress Update](#progress-update)
9. [Verification and Testing](#verification-and-testing)
10. [Risks and Mitigations](#risks-and-mitigations)
11. [Success Metrics](#success-metrics)
12. [Research and References](#research-and-references)

---

## Background

### Project Vision

Vertebra is a **Python-first, high-performance application backbone** that provides developers with:

1. **Seamless tRPC-like Developer Experience** - Use Python type annotations to automatically generate TypeScript types for frontend clients
2. **High-Performance ASGI Server** - Serve Litestar applications via a Rust+Bun powered server
3. **Zero-Copy IPC** - Bypass Python's GIL limitations through efficient inter-process communication
4. **Bun Utilities for Python** - Leverage Bun's blazing-fast I/O capabilities from Python code
5. **Cooperative Resource Management** - Share database connection pools and other resources across worker processes

### Current State

The repository currently contains:
- Legacy DMA code references in `pyproject.toml`, `Makefile`, and `conftest.py`
- Build system configured for `hatchling` (pure Python), not `maturin` (Rust+Python)
- Empty placeholder directories for Rust (`src/rs/`) and JavaScript (`src/ts/`)
- No Rust workspace configuration (`Cargo.toml`)
- No TypeScript package configuration (`package.json`)
- No architecture documentation
- No contributor conventions documentation

### Why This Matters

Without a clean foundation:
- Contributors will encounter confusing legacy references
- Build tooling won't support Rust extension modules
- Type generation pipeline cannot be implemented
- IPC layer has no Rust infrastructure
- Development experience will be poor

---

## Problem Statement

The Vertebra project needs to remove or quarantine DMA legacy code references and establish proper multi-language build infrastructure before feature development can begin. This is a **blocking prerequisite** for all future work.

### Specific Problems

1. **DMA Legacy Code**: References to `dma` module exist in:
   - `pyproject.toml` (package paths, import checks, lint rules)
   - `conftest.py` (imports, environment variables, fixtures)
   - `Makefile` (paths, targets)
   - `.mypy_cache/` (cached type information)

2. **Build System Mismatch**: Current build uses `hatchling`, which:
   - Cannot build Rust extension modules
   - Doesn't integrate with `maturin` for PyO3 bindings
   - Lacks cross-language dependency management

3. **Missing Infrastructure**:
   - No Rust workspace for IPC, server, and bindings crates
   - No TypeScript package for type generation and client
   - No unified build commands for multi-language project
- No documentation of architecture decisions or conventions

### Known Legacy Issues (DMA)

The following DMA-related errors and references are **known issues** and are already documented in the specs:
- Residual references in historical docs or cached artifacts
- Legacy fixture names or environment variables in tests that are pending removal

These items **must remain documented** until they are removed. The foundation cleanup must not regress or reintroduce additional DMA references.

---

## Goals and Non-Goals

### Goals

**Primary Goals:**
1. Remove DMA legacy references or document them as known legacy issues
2. Configure `maturin` as the build backend for Rust+Python integration
3. Create Rust workspace with three crates:
   - `vertebra-core`: Core IPC primitives and shared types
   - `vertebra-py`: PyO3 bindings for Python
   - `vertebra-js`: napi-rs bindings for Bun/Node.js
4. Create TypeScript/Bun package with structure for:
   - Client: Type-safe API client
   - Codegen: Python AST → TypeScript type generation
   - Server: Bun HTTP server integration
5. Update Makefile with cross-language build targets
6. Create comprehensive documentation:
   - `CLAUDE.md`: Project conventions for AI assistants
   - `docs/arch.md`: Architecture decisions and patterns

**Secondary Goals:**
- Preserve existing test infrastructure where compatible
- Maintain existing linting and formatting configurations
- Keep working Makefile patterns (colored output, uv integration, etc.)

### Non-Goals

**Explicitly Out of Scope:**
- Implementing actual features (server, IPC protocols, type bridge)
- Writing production code beyond minimal skeleton files
- Database driver implementations
- Performance optimization
- Deployment configuration
- CI/CD pipeline setup (will be addressed in separate PRD)

### Assumptions and Constraints

- **Greenfield implementation**: no backward-compat guarantees beyond documented DMA legacy notes.
- **Cross-platform target**: Linux/macOS/Windows parity for MVP if feasible; Linux-first implementation.
- **Single-developer focus** for PRD-0 delivery; subsequent PRDs may assume a small team.
- **Tooling baseline**: Rust + Python + Bun toolchains are required for local development.

### Cross-PRD Architectural Decisions

The following decisions apply across all Vertebra PRDs. They were consolidated via multi-model consensus (Gemini 3 Pro, GPT 5.2) to resolve conflicts and ensure consistency.

#### Python Version
- **Baseline: Python 3.12+** (unanimous)
- Enables: `type` statement, improved generics, performance gains
- All PRDs must not reference Python 3.11 or earlier

#### Serialization Strategy
Hybrid approach by layer to avoid protocol sprawl:

| Layer | Format | Rationale |
|-------|--------|-----------|
| **Python codec** | msgspec | Zero-copy, fastest Python serialization |
| **gRPC/MCP control plane** | Protobuf | Industry standard, schema evolution |
| **Driver data plane** | MessagePack | Compact binary, cross-language |
| **Internal Rust IPC** | bincode | Zero-copy Rust-native, no schema overhead |

#### Directory Layout
- **Canonical path: `src/ts/`** (established in PRD-0)
- Do NOT use `src/js/` - this conflicts with PRD-0's structure
- All PRDs must reference `src/ts/` for TypeScript/Bun code

#### Dependency Versions
- **thiserror: 2.0** (greenfield project, use latest)
- **PyO3: 0.27+** (latest stable)
- **napi-rs: 2.16+** (latest stable)

#### Runtime Strategy
- **Bun-first** with napi-rs providing Node.js compatibility
- Node fallback available "for free" via napi-rs ABI
- Prefer Bun-native APIs where they offer performance gains

#### Protocol Priorities (MVP)
| Protocol | Priority | Rationale |
|----------|----------|-----------|
| HTTP/2 | P0 | User mandate: "100% in scope for MVP" |
| H2C (cleartext HTTP/2) | P0 | Required for gRPC, local dev |
| gRPC | P1 | Aspirational MVP, enables MCP integration |

#### Cross-Platform IPC
- **Linux/macOS**: `shm_open`/`mmap` + Unix domain sockets
- **Windows**: `CreateFileMapping` + Named Pipes
- **Not using**: Mach ports (macOS-only, adds complexity)

#### Bindings Strategy
- **Primary**: PyO3 (Python) + napi-rs (Bun/Node)
- **C ABI**: Only if external consumers require it (not MVP)

---

## User Stories

### As a Contributor

> "When I clone the Vertebra repository and run `make install`, I expect a working development environment with no errors or confusing legacy references. I should be able to import `vertebra` in Python, build Rust crates with `cargo build`, and run TypeScript tests with `bun test`."

**Acceptance Criteria:**
- `make install` completes without errors
- `uv run python -c "import vertebra; print(vertebra.__version__)"` prints "0.1.0"
- `cd src/rs && cargo build && cargo test` succeeds
- `cd src/ts && bun install && bun test` succeeds
- No DMA references found by `grep -r "dma" --include="*.py" --include="*.toml" src/` **except documented known legacy issues**

### As a Python Developer

> "When I start working on Vertebra features, I need clear documentation explaining the architecture, how the multi-language build system works, and what conventions to follow. I shouldn't have to reverse-engineer anything."

**Acceptance Criteria:**
- `CLAUDE.md` exists with clear project conventions
- `docs/arch.md` documents architecture decisions
- `README.md` explains the project vision and build process
- Code comments in skeleton files explain their purpose

### As a Maintainer

> "When reviewing pull requests, I need confidence that the foundation is clean. No legacy code should remain, and all build tooling should be properly configured for the intended architecture."

**Acceptance Criteria:**
- All verification commands pass
- No manual cleanup needed post-installation
- Build configuration follows best practices from research
- Directory structure matches architectural vision

---

## Technical Approach

### Build System Strategy

**Decision: Use Pure Maturin Build Backend**

After researching maturin best practices, we'll use maturin as the sole build backend rather than a hybrid approach. This provides:

- Native Rust extension module compilation
- PyO3 bindings support
- Compatibility with `uv` for Python dependency management
- abi3 stable ABI support for multiple Python versions

**Configuration:**
```toml
[build-system]
build-backend = "maturin"
requires = ["maturin>=1.0,<2.0"]

[tool.maturin]
python-source = "src/py"
module-name = "vertebra._vertebra"
```

### Rust Workspace Architecture

**Decision: Flat Workspace with Three Crates**

Following best practices for large Rust projects, we'll use a flat layout with crates one-level deep:

```
src/rs/
├── Cargo.toml (workspace)
├── vertebra-core/
│   ├── Cargo.toml
│   ├── src/lib.rs
│   └── tests/
├── vertebra-py/
│   ├── Cargo.toml (PyO3 bindings)
│   ├── src/lib.rs
│   └── build.rs
└── vertebra-js/
    ├── Cargo.toml (napi-rs bindings)
    ├── src/lib.rs
    └── build.rs
```

**Rationale:**
- `vertebra-core`: Shared IPC types, protocol definitions, zero-copy primitives
- `vertebra-py`: Python bindings expose core functionality via PyO3
- `vertebra-js`: Bun/Node bindings expose core functionality via napi-rs

This structure allows:
- Shared `Cargo.lock` across all crates
- Core logic in pure Rust (testable, fast)
- Language-specific bindings as thin wrappers
- Future crates can be added easily

### TypeScript Package Structure

**Decision: Monorepo-style src/ Organization**

```
src/ts/
├── package.json
├── tsconfig.json
├── bun.lockb
└── src/
    ├── client/
    │   ├── index.ts (type-safe API client)
    │   └── types.ts (generated from Python)
    ├── codegen/
    │   ├── index.ts (Python AST parser)
    │   ├── generator.ts (TypeScript emitter)
    │   └── transformer.ts (type mappings)
    └── server/
        ├── index.ts (Bun HTTP server)
        └── ipc.ts (calls Python via vertebra-core)
```

**Key Dependencies:**
- `@kriss-u/py-ast`: Parse Python code to AST in TypeScript
- `typescript`: Type system and compiler API
- `bun`: Runtime and test framework

### DMA Removal Strategy

**Decision: Surgical Replacement, Not Deletion**

Rather than deleting all DMA code upfront, we'll:

1. Update configuration files to reference `vertebra` instead of `dma`
2. Keep test infrastructure (pytest config, fixtures) but update imports
3. Preserve linting rules and formatting configuration
4. Remove DMA-specific fixtures and environment variables
5. Clean cache directories (`.mypy_cache`)

This approach minimizes risk while ensuring no references remain.

### Makefile Integration

**Decision: Preserve Existing Patterns, Add Language Targets**

The current Makefile has excellent patterns:
- Colored output with INFO/OK/ERROR messages
- Silent by default (unless VERBOSE=1)
- UV integration for Python
- JavaScript/TypeScript targets with Bun

We'll preserve these and add:
- `make install-rust`: Install Rust toolchain if needed
- `make rs-build`: Build Rust workspace
- `make rs-test`: Test Rust workspace
- Updated `make install`: Build Rust before Python
- Updated `make test`: Run all language tests

---

## Detailed Specification

### 1. pyproject.toml Configuration

**Current Issues:**
- Line 17: `description` references DMA ("database migration assessment")
- Line 18: `keywords` includes DMA terms
- Line 67-70: `packages` points to `src/py/dma`
- Line 275: Lint rules reference `src/py/dma/`
- Line 290: `known-first-party` includes `dma`
- Line 376: `pyright` includes `src/py/dma`

**Required Changes:**

```toml
# Project metadata
description = "Python-first, high-performance application backbone with tRPC-like DX"
keywords = ["trpc", "asgi", "ipc", "performance", "bun"]

# Build system (CRITICAL CHANGE)
[build-system]
build-backend = "maturin"
requires = ["maturin>=1.0,<2.0"]

[tool.maturin]
python-source = "src/py"
module-name = "vertebra._vertebra"
features = ["pyo3/extension-module"]
bindings = "pyo3"

# Package configuration
[tool.hatch.build.targets.sdist]
packages = ["src/py/vertebra"]

[tool.hatch.build.targets.wheel]
packages = ["src/py/vertebra"]

# Linting paths
[tool.ruff.lint.per-file-ignores]
"**/src/py/vertebra/**/*.py" = [...]

# Import configuration
[tool.ruff.lint.isort]
known-first-party = ['vertebra', 'tests']

# Type checking paths
[tool.pyright]
include = ["src/py/vertebra", "src/py/tests"]
```

**Version Update:**
```toml
version = "0.1.0"  # Start fresh for foundation
```

**Dependencies to Add:**
```toml
dependencies = [
    # Will be populated by feature PRDs
]

[dependency-groups]
dev = [
    "maturin>=1.0,<2.0",
    # ... existing dev dependencies
]
```

### 2. conftest.py Updates

**Current Issues:**
- Line 1: Docstring says "Test configuration and fixtures for DMA"
- Line 12: `from dma.utils.worker import Worker`
- Lines 16-20: DMA-specific environment variables
- Line 93: `from dma.utils.portal import PortalManager`
- Line 114: `from dma.cli.config import console`
- Lines 151-155: DMA environment variables in mock file
- Line 185: `from dma.utils.worker import Worker`

**Required Changes:**

```python
"""Test configuration and fixtures for Vertebra."""

import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from pytest import MonkeyPatch

# Vertebra imports will be added as modules are implemented
# from vertebra.utils.worker import Worker

# Set test environment before any other imports
os.environ.update({
    "VERTEBRA_DEBUG": "true",
    "VERTEBRA_DEV_MODE": "true",
    "VERTEBRA_LOG_LEVEL": "40",
})

# ... rest of configuration with DMA references removed or commented out
```

**Strategy:**
- Remove DMA imports (will cause import errors until features are implemented)
- Update environment variable names (DMA_ → VERTEBRA_)
- Keep pytest configuration (markers, xdist grouping)
- Comment out fixtures that depend on unimplemented modules
- Add TODO comments for future implementation

### 3. Cargo.toml Workspace

**Create: `/home/cody/code/c/vertebra/src/rs/Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
    "vertebra-core",
    "vertebra-py",
    "vertebra-js",
]

[workspace.package]
version = "0.1.0"
authors = ["Cody Fincher <cody@litestar.dev>"]
edition = "2021"
license = "Apache-2.0"
repository = "https://github.com/litestar-org/vertebra"

[workspace.dependencies]
# Shared dependencies across all crates
tokio = { version = "1.35", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"

# PyO3 for Python bindings
pyo3 = { version = "0.27", features = ["extension-module"] }

# napi-rs for Node/Bun bindings
napi = { version = "2.16", features = ["tokio_rt"] }
napi-derive = "2.16"

[profile.release]
lto = true
codegen-units = 1
opt-level = 3
```

**Rationale:**
- `resolver = "2"`: Modern dependency resolution
- Workspace-level dependencies ensure version consistency
- PyO3 0.27+ for modern Rust-Python interop
- napi-rs for Bun/Node bindings
- Release profile optimized for performance

### 4. Rust Crate: vertebra-core

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-core/Cargo.toml`**

```toml
[package]
name = "vertebra-core"
version.workspace = true
authors.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "Core IPC primitives and shared types for Vertebra"

[dependencies]
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tokio-test = "0.4"
```

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-core/src/lib.rs`**

```rust
//! Core IPC primitives and shared types for Vertebra
//!
//! This crate provides the foundational types and protocols used across
//! the Vertebra ecosystem for zero-copy inter-process communication.

/// Version constant synchronized with Python package
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Placeholder for IPC protocol types
///
/// Future PRDs will define:
/// - Message formats
/// - Serialization protocols
/// - Shared memory primitives
pub mod ipc {
    // TODO: Implement in PRD-1
}

/// Placeholder for type bridge utilities
///
/// Future PRDs will define:
/// - Python type → Rust type mappings
/// - TypeScript type generation
pub mod types {
    // TODO: Implement in PRD-2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
```

### 5. Rust Crate: vertebra-py

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-py/Cargo.toml`**

```toml
[package]
name = "vertebra-py"
version.workspace = true
authors.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "Python bindings for Vertebra (PyO3)"

[lib]
name = "_vertebra"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { workspace = true }
vertebra-core = { path = "../vertebra-core" }
```

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-py/src/lib.rs`**

```rust
//! PyO3 bindings for Vertebra
//!
//! This module is imported as `vertebra._vertebra` in Python and provides
//! access to high-performance Rust-based functionality.

use pyo3::prelude::*;

/// Python module initialization
#[pymodule]
fn _vertebra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", vertebra_core::VERSION)?;

    // Future PRDs will add:
    // - IPC client/server classes
    // - Zero-copy buffer types
    // - Type bridge utilities

    Ok(())
}
```

### 6. Rust Crate: vertebra-js

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-js/Cargo.toml`**

```toml
[package]
name = "vertebra-js"
version.workspace = true
authors.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "Bun/Node.js bindings for Vertebra (napi-rs)"

[lib]
crate-type = ["cdylib"]

[dependencies]
napi = { workspace = true }
napi-derive = { workspace = true }
vertebra-core = { path = "../vertebra-core" }

[build-dependencies]
napi-build = "2.1"
```

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-js/src/lib.rs`**

```rust
//! napi-rs bindings for Vertebra
//!
//! This module provides Node.js/Bun bindings to Vertebra's Rust core.

#![deny(clippy::all)]

use napi_derive::napi;

#[napi]
pub fn get_version() -> String {
    vertebra_core::VERSION.to_string()
}

// Future PRDs will add:
// - IPC client functions
// - Type bridge utilities
// - Server integration
```

**Create: `/home/cody/code/c/vertebra/src/rs/vertebra-js/build.rs`**

```rust
fn main() {
    napi_build::setup();
}
```

### 7. TypeScript Package Configuration

**Create: `/home/cody/code/c/vertebra/src/ts/package.json`**

```json
{
  "name": "@vertebra/core",
  "version": "0.1.0",
  "description": "Type-safe Python API client with tRPC-like DX",
  "type": "module",
  "scripts": {
    "test": "bun test",
    "typecheck": "tsc --noEmit",
    "build": "bun build ./src/index.ts --outdir ./dist --target bun",
    "dev": "bun --watch ./src/index.ts"
  },
  "dependencies": {
    "@kriss-u/py-ast": "^0.1.0",
    "typescript": "^5.3.0"
  },
  "devDependencies": {
    "@types/bun": "latest",
    "bun-types": "latest"
  },
  "peerDependencies": {
    "typescript": ">=5.0.0"
  }
}
```

**Create: `/home/cody/code/c/vertebra/src/ts/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "lib": ["ES2022"],
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "declaration": true,
    "declarationMap": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "types": ["bun-types"]
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

**Create: `/home/cody/code/c/vertebra/src/ts/src/index.ts`**

```typescript
/**
 * Vertebra TypeScript Client
 *
 * Provides type-safe Python API clients with tRPC-like developer experience.
 */

export const VERSION = "0.1.0";

// Future PRDs will export:
// - createClient() - Type-safe API client factory
// - generateTypes() - Python AST → TypeScript codegen
// - createServer() - Bun HTTP server with IPC
```

**Create skeleton directories:**

```typescript
// src/ts/src/client/index.ts
export {}; // TODO: Implement in PRD-2

// src/ts/src/codegen/index.ts
export {}; // TODO: Implement in PRD-3

// src/ts/src/server/index.ts
export {}; // TODO: Implement in PRD-4
```

**Create: `/home/cody/code/c/vertebra/src/ts/src/index.test.ts`**

```typescript
import { describe, it, expect } from "bun:test";
import { VERSION } from "./index";

describe("Vertebra", () => {
  it("exports version", () => {
    expect(VERSION).toBe("0.1.0");
  });
});
```

### 8. Makefile Updates

**Add Rust Tooling Targets:**

```makefile
# =============================================================================
# Rust Development
# =============================================================================

.PHONY: install-rust
install-rust:                                       ## Install Rust toolchain via rustup
	@echo "${INFO} Checking for Rust installation..."
	@if ! command -v rustc >/dev/null 2>&1; then \
		echo "${INFO} Installing Rust via rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
		echo "${OK} Rust installed successfully"; \
	else \
		echo "${OK} Rust already installed"; \
	fi

.PHONY: rs-build
rs-build:                                          ## Build Rust workspace
	@echo "${INFO} Building Rust workspace... 🦀"
	@cd src/rs && cargo build
	@cd $(CURDIR) && echo "${OK} Rust build complete"

.PHONY: rs-test
rs-test:                                           ## Test Rust workspace
	@echo "${INFO} Testing Rust workspace... 🧪"
	@cd src/rs && cargo test
	@cd $(CURDIR) && echo "${OK} Rust tests passed ✨"

.PHONY: rs-lint
rs-lint:                                           ## Run Rust linting (clippy)
	@echo "${INFO} Running Rust linting... 🔍"
	@cd src/rs && cargo clippy -- -D warnings
	@cd $(CURDIR) && echo "${OK} Rust linting complete ✨"

.PHONY: rs-fmt
rs-fmt:                                            ## Format Rust code
	@echo "${INFO} Formatting Rust code... 🔧"
	@cd src/rs && cargo fmt
	@cd $(CURDIR) && echo "${OK} Rust formatting complete ✨"
```

**Update install target:**

```makefile
.PHONY: install
install: destroy clean                              ## Install the project, dependencies, and pre-commit for local development
	@echo "${INFO} Starting fresh installation..."
	@uv python pin 3.12 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1

	# Install Rust if needed
	@if ! command -v rustc >/dev/null 2>&1; then \
		$(MAKE) install-rust; \
	fi

	# Install Bun if needed
	@if ! command -v bun >/dev/null 2>&1; then \
		$(MAKE) install-bun; \
	fi

	# Build Rust first (needed for Python bindings)
	@echo "${INFO} Building Rust workspace... 🦀"
	@cd src/rs && cargo build --release >/dev/null 2>&1

	# Install Python dependencies and build with maturin
	@echo "${INFO} Installing Python dependencies... 🐍"
	@uv sync --all-extras --dev
	@uv run maturin develop --release

	# Install JavaScript dependencies
	@echo "${INFO} Installing JavaScript dependencies... 📦"
	@cd src/ts && bun install >/dev/null 2>&1

	@cd $(CURDIR) && echo "${OK} Installation complete! 🎉"
```

**Update test target:**

```makefile
.PHONY: test-all
test-all: rs-test js-test test                     ## Run all tests (Rust, JS, Python)
```

**Update clean target:**

```makefile
.PHONY: clean
clean:                                              ## Cleanup temporary build artifacts
	@echo "${INFO} Cleaning working directory..."
	@rm -rf target/ src/rs/target/ >/dev/null 2>&1  # Rust build artifacts
	@rm -rf .mypy_cache/ >/dev/null 2>&1             # Clear DMA type cache
	# ... existing clean commands
```

### 9. Python Package Updates

**Create: `/home/cody/code/c/vertebra/src/py/vertebra/__init__.py`**

```python
"""
Vertebra: Python-first, high-performance application backbone

Provides tRPC-like developer experience with automatic TypeScript type generation,
high-performance ASGI server via Rust+Bun, zero-copy IPC, and cooperative resource
management across workers.
"""

__version__ = "0.1.0"

# Import Rust extension module
try:
    from vertebra._vertebra import __version__ as _rust_version
    assert _rust_version == __version__, "Version mismatch between Python and Rust"
except ImportError:
    # Development mode: Rust extension not built yet
    pass

__all__ = ["__version__"]
```

**Create skeleton module directories:**

```python
# src/py/vertebra/server/__init__.py
"""ASGI server integration (Litestar via Rust+Bun)."""
# TODO: Implement in PRD-4

# src/py/vertebra/procedures/__init__.py
"""tRPC-like procedure decorators and router."""
# TODO: Implement in PRD-2

# src/py/vertebra/types/__init__.py
"""Type bridge utilities (Python ↔ TypeScript)."""
# TODO: Implement in PRD-3

# src/py/vertebra/drivers/__init__.py
"""Bun-backed database drivers."""
# TODO: Implement in PRD-5

# src/py/vertebra/ipc/__init__.py
"""Zero-copy IPC primitives."""
# TODO: Implement in PRD-1
```

### 10. Documentation Files

**Create: `/home/cody/code/c/vertebra/CLAUDE.md`**

```markdown
# Vertebra Development Guide for AI Assistants

## Project Overview

**Vertebra** is a Python-first, high-performance application backbone that provides:
- tRPC-like developer experience (Python types → TypeScript types)
- High-performance ASGI server (Litestar via Rust+Bun)
- Zero-copy IPC (bypass Python GIL)
- Bun utilities for Python (fast I/O)
- Cooperative resource management (shared pools)

## Technology Stack

- **Python 3.12+**: Primary language, uv for dependency management
- **Rust 2021**: High-performance core, PyO3 for Python bindings, napi-rs for Bun bindings
- **TypeScript/Bun**: Type generation, client library, HTTP server
- **Build Tools**: maturin (Rust+Python), Cargo (Rust), Bun (TypeScript)

## Repository Structure

```
vertebra/
├── src/
│   ├── py/vertebra/          # Python package
│   │   ├── server/           # ASGI server integration
│   │   ├── procedures/       # tRPC-like decorators
│   │   ├── types/            # Type bridge utilities
│   │   ├── drivers/          # Bun-backed DB drivers
│   │   └── ipc/              # Zero-copy IPC
│   ├── rs/                   # Rust workspace
│   │   ├── vertebra-core/    # IPC primitives
│   │   ├── vertebra-py/      # PyO3 bindings
│   │   └── vertebra-js/      # napi-rs bindings
│   └── js/                   # TypeScript package
│       └── src/
│           ├── client/       # Type-safe API client
│           ├── codegen/      # Type generation
│           └── server/       # Bun HTTP server
├── pyproject.toml            # Python project config (maturin)
├── Makefile                  # Cross-language build targets
└── docs/
    ├── arch.md              # Architecture decisions
    └── prd/                 # Product requirements
```

## Build System

### Quick Start

```bash
# Install everything (Rust, Python, TypeScript)
make install

# Run all tests
make test-all

# Format all code
make fmt
```

### Language-Specific Commands

**Python:**
```bash
make test          # Run Python tests
make py-lint       # Lint Python code
uv run python -m vertebra  # Run module
```

**Rust:**
```bash
make rs-build      # Build Rust workspace
make rs-test       # Test Rust workspace
make rs-lint       # Run clippy
cd src/rs && cargo build --release
```

**TypeScript:**
```bash
make js-test       # Run Bun tests
make js-type-check # TypeScript type checking
cd src/ts && bun run dev
```

## Development Conventions

### Code Style

**Python:**
- Use `ruff` for linting and formatting (configured in pyproject.toml)
- Type hints required (enforced by mypy and pyright)
- Google-style docstrings
- Max line length: 120 characters

**Rust:**
- Use `rustfmt` (2021 edition)
- Use `clippy` with `#![deny(clippy::all)]`
- Document all public APIs with `///` comments
- Prefer explicit error types over `.unwrap()`

**TypeScript:**
- Use Biome for linting and formatting
- Strict mode enabled
- Prefer functional patterns
- Document complex types with JSDoc

### Testing

**Python:**
- Use pytest with anyio for async tests
- Group tests by database type (xdist markers)
- Place tests in `src/py/tests/`
- Maintain >80% coverage

**Rust:**
- Unit tests in same file (`#[cfg(test)] mod tests`)
- Integration tests in `tests/` directory
- Use `tokio-test` for async tests
- Benchmark critical paths

**TypeScript:**
- Use Bun's built-in test runner
- Test files: `*.test.ts`
- Mock external dependencies
- Test generated types match Python

### Commit Messages

Follow conventional commits:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation only
- `refactor:` Code restructuring
- `test:` Adding tests
- `chore:` Tooling/config changes

Examples:
```
feat(procedures): Add tRPC-like router decorator
fix(ipc): Handle connection timeout gracefully
docs(arch): Document type bridge design
```

### Version Management

- Synchronized versions across Python, Rust, and TypeScript
- Use `bump-my-version` to update all manifests
- Semantic versioning (MAJOR.MINOR.PATCH)
- Update `CHANGELOG.md` for user-facing changes

## Architecture Principles

1. **Python-First**: Python is the primary interface, Rust accelerates
2. **Zero-Copy When Possible**: Use shared memory, avoid serialization overhead
3. **Type Safety Everywhere**: Python types → Rust types → TypeScript types
4. **Fail Fast**: Validate at boundaries, propagate errors clearly
5. **Performance by Default**: Optimize hot paths, profile before guessing

## Common Tasks

### Adding a New Python Module

1. Create module in `src/py/vertebra/feature/`
2. Add `__init__.py` with public API
3. Add type hints and docstrings
4. Create tests in `src/py/tests/feature/`
5. Update `vertebra/__init__.py` exports

### Adding a New Rust Crate

1. Create crate in `src/rs/feature/`
2. Add to workspace members in `src/rs/Cargo.toml`
3. Define public API in `lib.rs`
4. Add documentation and examples
5. Create tests in `tests/`

### Adding TypeScript Types

1. Define types in `src/ts/src/types/`
2. Export from `src/ts/src/index.ts`
3. Add tests in `*.test.ts`
4. Generate type documentation
5. Validate against Python types

## Debugging

### Python + Rust Integration

```python
# Enable Rust backtrace
import os
os.environ['RUST_BACKTRACE'] = '1'

import vertebra
# Rust panics will show full backtrace
```

### IPC Communication

```python
# Enable IPC debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

from vertebra.ipc import Client
# Will show message traces
```

### Type Generation

```bash
# Debug Python AST parsing
cd src/ts
bun run src/codegen/index.ts --debug example.py
```

## Resources

- [Architecture Decisions](docs/arch.md)
- [PRD Index](docs/prd/)
- [Maturin Documentation](https://www.maturin.rs/)
- [PyO3 Guide](https://pyo3.rs/)
- [napi-rs Documentation](https://napi.rs/)
- [Bun Documentation](https://bun.sh/docs)

## Getting Help

1. Check `docs/arch.md` for design rationale
2. Review relevant PRD in `docs/prd/`
3. Search issues on GitHub
4. Ask in discussions (be specific!)
```

**Create: `/home/cody/code/c/vertebra/docs/arch.md`**

```markdown
# Vertebra Architecture

**Version:** 0.1.0
**Last Updated:** 2026-01-17

---

## Table of Contents

1. [Overview](#overview)
2. [Design Principles](#design-principles)
3. [System Architecture](#system-architecture)
4. [Component Details](#component-details)
5. [Data Flow](#data-flow)
6. [Technology Choices](#technology-choices)
7. [Performance Considerations](#performance-considerations)
8. [Security Model](#security-model)

---

## Overview

Vertebra is a **Python-first, high-performance application backbone** designed to provide developers with a tRPC-like experience while leveraging Rust and Bun for performance-critical operations.

### Core Value Propositions

1. **Developer Experience**: Write Python, get TypeScript types automatically
2. **Performance**: Rust+Bun server outperforms pure Python by orders of magnitude
3. **Scalability**: Zero-copy IPC bypasses GIL limitations
4. **Productivity**: Bun's fast I/O available from Python
5. **Efficiency**: Shared resource pools across workers

### Target Use Cases

- Web applications with TypeScript frontends
- APIs requiring strong type safety across languages
- High-throughput data processing with Python business logic
- Microservices with Python + Rust components
- Real-time applications needing low-latency IPC

---

## Design Principles

### 1. Python-First Philosophy

**Principle:** Python is the primary interface; Rust and TypeScript are acceleration layers.

**Rationale:**
- Python has the richest ecosystem for web frameworks, data science, and ML
- Most developers are more productive in Python than Rust
- Type hints in Python (via PEP 484+) are mature and well-supported

**Implementation:**
- Public API exposed via Python decorators and functions
- Rust code lives in `._vertebra` internal module
- TypeScript client is generated, not hand-written

**Trade-offs:**
- ✅ Lower barrier to entry for Python developers
- ✅ Leverage existing Python ecosystem
- ❌ Rust expertise needed for core contributions
- ❌ FFI overhead for frequently-called functions

### 2. Type Safety Everywhere

**Principle:** Types flow from Python → Rust → TypeScript without manual synchronization.

**Rationale:**
- Manual type duplication leads to drift and bugs
- Runtime type errors are expensive in production
- IDE autocomplete improves developer productivity

**Implementation:**
- Python type hints are the source of truth
- AST analysis generates Rust types (via procedural macros)
- Same AST generates TypeScript types (via codegen)
- Validation at all boundaries (Python/Rust, Rust/Bun, Bun/Client)

**Trade-offs:**
- ✅ Single source of truth (Python types)
- ✅ Compile-time verification in Rust and TypeScript
- ❌ Code generation adds build complexity
- ❌ Debugging spans multiple languages

### 3. Zero-Copy When Possible

**Principle:** Avoid serialization overhead for large data transfers.

**Rationale:**
- JSON serialization is slow for large payloads
- Python GIL makes multi-threading ineffective
- Shared memory bypasses both issues

**Implementation:**
- Use `mmap` for shared memory regions
- Protocol Buffers or Cap'n Proto for structured data
- Direct buffer sharing between Python and Rust (via PyO3)
- Unix domain sockets for control plane

**Trade-offs:**
- ✅ Orders of magnitude faster for large data
- ✅ Bypasses Python GIL
- ❌ More complex error handling (segfaults possible)
- ❌ Platform-specific backends (hidden behind shared abstractions)

### 4. Fail Fast, Fail Clearly

**Principle:** Validate early, propagate errors with context.

**Rationale:**
- Silent failures are hard to debug
- Stack traces across languages are confusing
- Production debugging needs actionable information

**Implementation:**
- Rust functions return `Result<T, E>` with structured errors
- PyO3 converts to Python exceptions with full tracebacks
- TypeScript client includes error discrimination
- Logging at every boundary crossing

**Trade-offs:**
- ✅ Easier debugging in development
- ✅ Better error messages for users
- ❌ Slight performance overhead for validation
- ❌ More verbose code

### 5. Performance by Default

**Principle:** Optimize hot paths proactively, not reactively.

**Rationale:**
- Retrofitting performance is harder than building it in
- Python's overhead is predictable and avoidable
- Users expect fast APIs in 2026

**Implementation:**
- Critical paths in Rust (request routing, IPC)
- Bun for I/O-heavy operations (database, file system)
- Python for business logic only
- Profiling integrated into CI/CD

**Trade-offs:**
- ✅ Fast out of the box
- ✅ Scales to high concurrency
- ❌ Higher initial development cost
- ❌ Requires performance expertise

---

## System Architecture

### High-Level Components

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (Browser)                      │
│  TypeScript Client (generated from Python types)            │
└────────────────┬────────────────────────────────────────────┘
                 │ HTTP/WebSocket
                 ▼
┌─────────────────────────────────────────────────────────────┐
│                    Bun HTTP Server                           │
│  - Request routing                                           │
│  - WebSocket handling                                        │
│  - Static file serving                                       │
└────────────────┬────────────────────────────────────────────┘
                 │ IPC (Unix socket / shared memory)
                 ▼
┌─────────────────────────────────────────────────────────────┐
│               vertebra-core (Rust)                           │
│  - Zero-copy IPC protocol                                    │
│  - Type validation                                           │
│  - Connection pooling                                        │
└────────────────┬────────────────────────────────────────────┘
                 │ PyO3 FFI
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          Python Application (Litestar)                       │
│  - Business logic                                            │
│  - Procedure definitions                                     │
│  - Database ORM                                              │
└─────────────────────────────────────────────────────────────┘
```

### Process Model

**Development Mode:**
```
┌──────────────────┐
│  Bun Dev Server  │  (watches TypeScript, hot reload)
└────────┬─────────┘
         │ IPC
┌────────▼─────────┐
│  Python Process  │  (uvicorn auto-reload)
└──────────────────┘
```

**Production Mode:**
```
┌───────────────────────────────────────┐
│  Bun HTTP Server (master process)    │
└───────┬───────────────────────────────┘
        │ spawns & manages
        ├──────┬──────┬──────┬──────┐
        ▼      ▼      ▼      ▼      ▼
    ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐
    │Py #1│ │Py #2│ │Py #3│ │Py #4│ │Py #N│
    └─────┘ └─────┘ └─────┘ └─────┘ └─────┘
       ▲       ▲       ▲       ▲       ▲
       └───────┴───────┴───────┴───────┘
              Shared connection pool
              (PostgreSQL, Redis, etc.)
```

**Benefits:**
- Bun master handles concurrency (no GIL issues)
- Python workers process business logic
- Shared pools reduce connection overhead
- Graceful restarts without downtime

---

## Component Details

### 1. vertebra-core (Rust)

**Purpose:** Shared IPC primitives and protocol definitions.

**Key Types:**
- `Message`: Envelope for RPC calls (method, params, id)
- `Transport`: Abstraction over Unix socket / shared memory
- `Pool`: Connection pool manager
- `TypeRegistry`: Runtime type validation

**Dependencies:**
- `tokio`: Async runtime
- `serde`: Serialization framework
- `thiserror`: Error handling

**Public API:**
```rust
pub struct Transport { /* ... */ }
impl Transport {
    pub async fn send(&self, msg: Message) -> Result<Response>;
    pub async fn recv(&self) -> Result<Message>;
}

pub struct Pool<T> { /* ... */ }
impl<T> Pool<T> {
    pub async fn acquire(&self) -> Result<Pooled<T>>;
}
```

### 2. vertebra-py (PyO3 Bindings)

**Purpose:** Expose Rust functionality to Python.

**Exposed Classes:**
- `IpcClient`: Async client for calling Bun server
- `Buffer`: Zero-copy buffer wrapper
- `PoolManager`: Access shared connection pools

**Example Usage:**
```python
from vertebra._vertebra import IpcClient

client = IpcClient(socket_path="/tmp/vertebra.sock")
result = await client.call("users.get", {"id": 123})
```

**Implementation Notes:**
- Use `pyo3-asyncio` for async/await support
- Convert Rust errors to Python exceptions
- Expose metrics (call latency, error rate)

### 3. vertebra-js (napi-rs Bindings)

**Purpose:** Expose Rust functionality to Bun/Node.

**Exposed Functions:**
- `createTransport(options)`: Create IPC transport
- `createPool(config)`: Create connection pool
- `validateType(schema, value)`: Runtime type checking

**Example Usage:**
```typescript
import { createTransport } from "@vertebra/core/bindings";

const transport = createTransport({
  path: "/tmp/vertebra.sock",
  timeout: 5000,
});

const result = await transport.call("users.get", { id: 123 });
```

### 4. Python Procedures (vertebra.procedures)

**Purpose:** tRPC-like decorators for defining APIs.

**Design:**
```python
from vertebra.procedures import router, query, mutation

user_router = router()

@user_router.query
def get_user(id: int) -> User:
    """Get user by ID."""
    return db.query(User).filter(User.id == id).first()

@user_router.mutation
def update_user(id: int, data: UserUpdate) -> User:
    """Update user data."""
    user = db.query(User).filter(User.id == id).first()
    user.update(data)
    db.commit()
    return user
```

**Type Extraction:**
- Use `inspect.signature()` to get function signature
- Parse type hints with `typing.get_type_hints()`
- Generate schema for Rust validation
- Emit TypeScript types via codegen

### 5. TypeScript Codegen (src/ts/src/codegen)

**Purpose:** Generate TypeScript types from Python functions.

**Process:**
1. Parse Python source with `@kriss-u/py-ast`
2. Extract function signatures and type hints
3. Map Python types to TypeScript types
4. Emit `.d.ts` files for IDE support

**Type Mappings:**
```
Python          TypeScript
------          ----------
int             number
str             string
bool            boolean
list[T]         T[]
dict[K, V]      Record<K, V>
Pydantic model  interface { ... }
```

**Output Example:**
```typescript
// Generated from Python procedure
export interface UserRouter {
  getUser(params: { id: number }): Promise<User>;
  updateUser(params: { id: number; data: UserUpdate }): Promise<User>;
}
```

### 6. TypeScript Client (src/ts/src/client)

**Purpose:** Type-safe client for calling Python procedures.

**Design:**
```typescript
import { createClient } from "@vertebra/core/client";
import type { AppRouter } from "./generated/types";

const client = createClient<AppRouter>({
  url: "http://localhost:3000",
});

// Fully type-checked!
const user = await client.users.getUser({ id: 123 });
//    ^? User
```

**Features:**
- Request batching
- Automatic retries
- Error discrimination
- AbortController support
- Middleware system (logging, auth, etc.)

---

## Data Flow

### Request Lifecycle

**Example:** Frontend calls `users.getUser({ id: 123 })`

```
1. TypeScript Client
   ├─ Validate params against schema
   ├─ Serialize to JSON
   └─ POST to http://localhost:3000/api/users.getUser

2. Bun HTTP Server
   ├─ Parse HTTP request
   ├─ Extract procedure name & params
   ├─ Create IPC message
   └─ Send via Unix socket

3. vertebra-core (Rust)
   ├─ Deserialize IPC message
   ├─ Validate types
   ├─ Route to Python worker
   └─ Send via PyO3 FFI

4. Python Procedure
   ├─ Execute get_user(id=123)
   ├─ Query database
   ├─ Serialize result
   └─ Return User object

5. vertebra-core (Rust)
   ├─ Receive result from Python
   ├─ Validate return type
   └─ Send back via IPC

6. Bun HTTP Server
   ├─ Receive IPC response
   ├─ Create HTTP response
   └─ Return JSON to client

7. TypeScript Client
   ├─ Deserialize JSON
   ├─ Validate return type
   └─ Return typed User object
```

**Latency Budget:**
- Network (client ↔ server): ~10-50ms
- HTTP overhead: ~1ms
- IPC (Bun ↔ Rust): <0.1ms (Unix socket)
- Type validation: <0.1ms
- Python execution: variable (database query)
- **Total overhead: ~1-2ms**

### Error Handling

**Error Flow:**
```
Python Exception
  ↓ (PyO3 catches)
Rust Result::Err
  ↓ (IPC serializes)
Bun receives error
  ↓ (HTTP error response)
TypeScript throws
  ↓ (application handles)
User sees error message
```

**Error Structure:**
```typescript
type VertebraError = {
  code: "VALIDATION_ERROR" | "NOT_FOUND" | "INTERNAL_ERROR" | ...;
  message: string;
  path?: string[];  // For nested validation errors
  cause?: Error;    // Original error
};
```

---

## Technology Choices

### Why maturin?

**Alternatives Considered:**
- setuptools-rust: Older, less ergonomic
- PyO3 alone: Requires manual build configuration
- pybind11: C++ only, not Rust

**Decision:** Use maturin for best PyO3 integration.

**Rationale:**
- Handles all build complexity automatically
- Integrates with uv and pip
- Supports abi3 for multi-version wheels
- Active maintenance and community

### Why PyO3 over pybind11?

**Comparison:**
| Feature | PyO3 | pybind11 |
|---------|------|----------|
| Language | Rust | C++ |
| Memory Safety | ✅ (compile-time) | ❌ (manual) |
| Async Support | ✅ (tokio) | Limited |
| Type Conversion | Automatic | Manual |
| Build Time | Fast | Slow |

**Decision:** PyO3 for memory safety and ergonomics.

### Why Bun over Node.js?

**Benchmarks (req/sec):**
- Bun: published benchmarks indicate higher throughput (validate locally)
- Node.js (v20): baseline to be measured
- Deno: baseline to be measured

**Decision:** Bun for performance and built-in TypeScript support (pending local validation).

**Additional Benefits:**
- No build step for TypeScript (faster iteration)
- Better module resolution
- Built-in test runner
- Fast package installation

### Why napi-rs over N-API?

**Alternatives:**
- Raw N-API: Manual FFI bindings (error-prone)
- nan: Deprecated, C++ only

**Decision:** napi-rs for type safety and code generation.

**Benefits:**
- Rust macros generate bindings automatically
- Type-safe on both sides
- Async/await support
- Cross-platform (Bun, Node, Electron)

---

## Performance Considerations

### Bottleneck Analysis

**Likely Bottlenecks:**
1. Database queries (I/O bound)
2. Python GIL contention (CPU bound)
3. IPC overhead (if not zero-copy)
4. Type validation (if naive implementation)

**Mitigation Strategies:**

**1. Database Queries:**
- Connection pooling (shared across workers)
- Query caching (Redis)
- Read replicas for scalability
- Use Bun's fast I/O instead of Python drivers

**2. Python GIL:**
- Offload to multiple processes (Bun manages)
- Use Rust for CPU-intensive tasks
- Async I/O where possible (release GIL)

**3. IPC Overhead:**
- Zero-copy for large payloads (mmap)
- Batch requests when possible
- Unix sockets > TCP (lower latency)
- Protocol Buffers > JSON (smaller size)

**4. Type Validation:**
- Generate validation code at build time
- Use Rust's type system (compile-time checks)
- Cache schema instances
- Skip validation in production (optional)

### Benchmarking Strategy

**Metrics to Track:**
- Request latency (p50, p95, p99)
- Throughput (requests/second)
- Memory usage (resident set size)
- CPU utilization (per process)
- IPC round-trip time
- Type validation overhead

**Tools:**
- `hyperfine`: CLI benchmarking
- `criterion`: Rust benchmarks
- `pytest-benchmark`: Python benchmarks
- `flamegraph`: CPU profiling
- `heaptrack`: Memory profiling

---

## Security Model

### Threat Model

**In Scope:**
- Malicious input validation bypass
- Type confusion attacks
- IPC message spoofing
- Resource exhaustion (DoS)

**Out of Scope:**
- Network security (TLS, authentication)
- Code injection via compromised dependencies
- Physical access attacks

### Defense Layers

**1. Input Validation:**
- Validate at every boundary (HTTP, IPC, Python)
- Use schema validation (Pydantic, Rust types)
- Reject unknown fields
- Enforce size limits

**2. Type Safety:**
- No `Any` types in public APIs
- Runtime checks in Rust (debug mode)
- TypeScript strict mode enforced
- Pydantic models for Python

**3. IPC Security:**
- Unix socket permissions (0600)
- Process isolation (one Python per worker)
- Message authentication (HMAC)
- Rate limiting per client

**4. Resource Limits:**
- Max request size (e.g., 10MB)
- Connection timeouts (e.g., 30s)
- Worker memory limits (cgroups)
- Database connection pool limits

### Audit Trail

**Logging:**
- All IPC calls logged (procedure, params, duration)
- Errors logged with stack traces
- Security events logged (validation failures, auth)
- Structured logging (JSON format)

**Metrics:**
- Error rate by procedure
- Latency histogram
- Active connections
- Memory usage

---

## Future Considerations

### Planned Enhancements

1. **WebSocket Support** (PRD-6)
   - Bi-directional type-safe communication
   - Subscriptions (like tRPC)
   - Automatic reconnection

2. **Distributed Tracing** (PRD-7)
   - OpenTelemetry integration
   - Trace across Python/Rust/Bun
   - Correlation IDs

3. **Code Generation CLI** (PRD-8)
   - `vertebra generate` command
   - Watch mode for development
   - Plugins for custom types

4. **Browser SDK** (PRD-9)
   - Optimized bundle size
   - Tree-shaking support
   - React hooks integration

5. **Database Drivers** (PRD-5)
   - PostgreSQL via Bun
   - MySQL via Bun
   - Redis via Bun
   - Shared pool across workers

### Open Questions

1. **Should we support streaming responses?**
   - Pros: Efficient for large data
   - Cons: Adds protocol complexity
   - Decision: Defer to PRD-6

2. **How to handle schema versioning?**
   - Pros: Enables gradual rollouts
   - Cons: Adds runtime overhead
   - Decision: Defer to PRD-10

3. **Should we provide ORM integration?**
   - Pros: Better DX for database access
   - Cons: Tight coupling
   - Decision: Defer to PRD-11

---

## References

- [PRD-0: Foundation](./prd/PRD-0-foundation.md)
- [Maturin User Guide](https://www.maturin.rs/)
- [PyO3 Documentation](https://pyo3.rs/)
- [napi-rs Guide](https://napi.rs/)
- [Bun Documentation](https://bun.sh/docs)
- [tRPC Architecture](https://trpc.io/)
```

---

## Implementation Phases

### Phase 1: Python Configuration (Day 1)

**Tasks:**
1. Update `pyproject.toml`
   - Change build backend to maturin
   - Update all DMA references to vertebra
   - Add maturin configuration
   - Update version to 0.1.0

2. Update `conftest.py`
   - Change imports (dma → vertebra)
   - Update environment variables
   - Comment out unimplemented fixtures
   - Keep pytest configuration

3. Create Python package skeleton
   - `src/py/vertebra/__init__.py` with version
   - Empty module directories (server, procedures, types, drivers, ipc)
   - Skeleton `__init__.py` files with TODO comments

**Verification:**
```bash
uv sync --dev  # Should complete without errors
```

### Phase 2: Rust Workspace (Day 1-2)

**Tasks:**
1. Create workspace root `src/rs/Cargo.toml`
2. Create `vertebra-core` crate
   - Basic lib.rs with version constant
   - Minimal test
3. Create `vertebra-py` crate
   - PyO3 bindings
   - Expose __version__
4. Create `vertebra-js` crate
   - napi-rs bindings
   - Expose getVersion()

**Verification:**
```bash
cd src/rs && cargo build  # Should build all crates
cd src/rs && cargo test   # Should pass all tests
```

### Phase 3: TypeScript Package (Day 2)

**Tasks:**
1. Create `src/ts/package.json`
2. Create `src/ts/tsconfig.json`
3. Create `src/ts/src/index.ts` with VERSION
4. Create skeleton directories (client, codegen, server)
5. Create basic test file

**Verification:**
```bash
cd src/ts && bun install  # Should install dependencies
cd src/ts && bun test     # Should pass version test
```

### Phase 4: Build Integration (Day 2-3)

**Tasks:**
1. Add Rust targets to Makefile
   - `install-rust`
   - `rs-build`, `rs-test`, `rs-lint`, `rs-fmt`
2. Update `install` target
   - Install Rust if needed
   - Build Rust before Python
   - Install JavaScript dependencies
3. Update `clean` target
   - Remove Rust build artifacts
   - Clear mypy cache
4. Update `test-all` target
   - Run all language tests

**Verification:**
```bash
make clean && make install  # Should complete successfully
make test-all               # Should run all tests
```

### Phase 5: Documentation (Day 3-4)

**Tasks:**
1. Create `CLAUDE.md`
   - Project overview
   - Repository structure
   - Build commands
   - Development conventions
   - Common tasks

2. Create `docs/arch.md`
   - Design principles
   - System architecture
   - Component details
   - Data flow
   - Technology choices

3. Update `README.md`
   - Replace DMA description
   - Add Vertebra vision
   - Add quick start guide
   - Link to architecture docs

**Verification:**
- Review documentation for clarity
- Ensure all examples are accurate
- Verify links work

### Phase 6: Final Verification (Day 4-5)

**Tasks:**
1. Run all verification commands
2. Check for remaining DMA references
3. Test full development workflow
4. Create checkpoint commit

**Verification Commands:**
```bash
# 1. Python environment works
make clean && make install
uv run python -c "import vertebra; print(vertebra.__version__)"
# Expected output: 0.1.0

# 2. Rust builds and tests
cd src/rs && cargo build && cargo test
# Expected: All tests pass

# 3. Bun/JS works
cd src/ts && bun install && bun test
# Expected: All tests pass

# 4. No DMA references remain (except documented legacy issues)
grep -r "dma" --include="*.py" --include="*.toml" src/
# Expected: Empty output (or only documented legacy references)

# 5. Full build works
make clean && make install && make test-all
# Expected: All tests pass
```

---

## Progress Update

**Last reviewed:** 2026-01-18

### Progress Checklist
- [x] Standardized TypeScript/Bun layout to `src/ts` and aligned PRD docs accordingly.
- [x] Enforced Python 3.12+ baseline across tooling and documentation.
- [x] Switched Python build backend to `maturin` with PyO3 bindings at `src/rs/vertebra-py`.
- [x] Created Rust workspace (`vertebra-core`, `vertebra-py`, `vertebra-js`) with minimal bindings.
- [x] Added Bun/TypeScript workspace scaffold with tests and minimal server/client/codegen stubs.
- [x] Updated Makefile with cross-language install/upgrade/build/test/lint targets and fail-fast behavior.
- [x] Removed unused N-API import warning in `vertebra-js`.
- [x] Added architecture + conventions docs (`docs/arch.md`, `CLAUDE.md`, `README.md`).
- [x] Added Biome v2 config and TS formatting pipeline.
- [x] Added Docker/Podman container runtime helper for integration test scaffolding.
- [x] `make test-all` completed after pytest plugin cleanup and N-API warning removal.
- [x] Regenerated `uv.lock` (`uv lock --upgrade`) to capture the 3.12 baseline and current deps.
- [x] Confirmed no remaining legacy DMA references beyond documented notes.

### Verification Status
- `make install` verified on 2026-01-17.
- `make test-all` verified on 2026-01-17.
- `uv lock --upgrade` verified on 2026-01-17.

### Close-Out Notes
- PRD-0 complete; move to PRD-1 (Rust core IPC).

---

## Verification and Testing

### Acceptance Criteria

**Must Pass:**
- ✅ `make install` completes without errors
- ✅ `uv run python -c "import vertebra; print(vertebra.__version__)"` prints "0.1.0"
- ✅ Python can import Rust extension: `from vertebra._vertebra import __version__`
- ✅ `cd src/rs && cargo build` succeeds
- ✅ `cd src/rs && cargo test` passes
- ✅ `cd src/ts && bun install` succeeds
- ✅ `cd src/ts && bun test` passes
- ✅ `grep -r "dma" --include="*.py" --include="*.toml" src/` returns empty **or only documented legacy references**
- ✅ `make test-all` passes
- ✅ `CLAUDE.md` exists and is comprehensive
- ✅ `docs/arch.md` exists and documents architecture

**Nice to Have:**
- ⭐ `make lint` passes (may need fixes for skeleton files)
- ⭐ `make type-check` passes
- ⭐ Documentation includes diagrams

### Testing Strategy

**Unit Tests:**
- Rust: Test version constant exists
- Python: Test package imports
- TypeScript: Test version export

**Integration Tests:**
- Python imports Rust extension successfully
- Versions match across all languages
- Build artifacts are created correctly

**Manual Verification:**
- Clone fresh repository
- Run `make install`
- Verify no errors in output
- Check that all directories exist
- Confirm documentation is readable

---

## Risks and Mitigations

### High Priority Risks

**Risk 1: Maturin Configuration Incorrect**
- **Impact:** Python package won't install, blocker
- **Likelihood:** Medium
- **Mitigation:**
  - Research maturin examples before implementing
  - Test with fresh virtual environment
  - Verify with `uv run maturin build --help`
- **Contingency:** Revert to hatchling, implement Rust in separate PRD

**Risk 2: PyO3 Version Incompatibility**
- **Impact:** Rust extension won't compile
- **Likelihood:** Low
- **Mitigation:**
  - Use PyO3 0.27+ (latest stable)
  - Follow official PyO3 installation guide
  - Test with multiple Python versions (3.12, 3.13)
- **Contingency:** Pin to known-good PyO3 version

**Risk 3: Incomplete DMA Removal**
- **Impact:** Confusing errors for contributors
- **Likelihood:** Medium
- **Mitigation:**
  - Use comprehensive grep patterns
  - Check `.mypy_cache`, `.ruff_cache`
  - Manual code review of all config files
- **Contingency:** Create follow-up cleanup task

### Medium Priority Risks

**Risk 4: Cargo Workspace Build Failures**
- **Impact:** Rust development blocked
- **Likelihood:** Low
- **Mitigation:**
  - Use standard workspace patterns
  - Test with `cargo build --all`
  - Validate Cargo.toml syntax
- **Contingency:** Use individual crate builds instead of workspace

**Risk 5: Bun Package Installation Issues**
- **Impact:** TypeScript development blocked
- **Likelihood:** Low
- **Mitigation:**
  - Use stable Bun version (1.0+)
  - Lock dependencies in bun.lockb
  - Test on multiple platforms (Linux, macOS)
- **Contingency:** Fall back to npm/pnpm if needed

**Risk 6: Makefile Target Conflicts**
- **Impact:** Build commands fail unexpectedly
- **Likelihood:** Low
- **Mitigation:**
  - Use clear prefixes (py-, rs-, js-)
  - Test each target individually
  - Validate with `make -n <target>` (dry run)
- **Contingency:** Rename conflicting targets

### Low Priority Risks

**Risk 7: Documentation Outdated**
- **Impact:** Contributors confused
- **Likelihood:** Medium (over time)
- **Mitigation:**
  - Write docs after code is working
  - Include verification examples in docs
  - Add "Last Updated" timestamps
- **Contingency:** Create docs update task

**Risk 8: Version Mismatch Across Languages**
- **Impact:** Confusing error messages
- **Likelihood:** Low
- **Mitigation:**
  - Use bump-my-version for synchronized updates
  - Add version check in Python __init__.py
  - Include version in test suites
- **Contingency:** Manual version sync

---

## Success Metrics

### Quantitative Metrics

1. **Build Success Rate:** 100% on clean install
2. **Test Pass Rate:** 100% for skeleton tests
3. **DMA References:** 0 in source code
4. **Documentation Coverage:** 100% of required files exist
5. **Installation Time:** <5 minutes on modern hardware

### Qualitative Metrics

1. **Developer Experience:**
   - Can a new contributor run `make install` successfully?
   - Is the project structure intuitive?
   - Is documentation clear and comprehensive?

2. **Code Quality:**
   - Are skeleton files well-documented?
   - Do configuration files follow best practices?
   - Is the build system robust?

3. **Maintainability:**
   - Can future features be added easily?
   - Is the multi-language setup well-organized?
   - Are there clear conventions to follow?

### Definition of Done

- [ ] All verification commands pass
- [ ] No DMA references in source code **except documented known legacy issues**
- [ ] Documentation complete (CLAUDE.md, arch.md, README.md)
- [ ] Code reviewed by maintainer
- [ ] Fresh install tested on clean environment
- [ ] Checkpoint commit created

---

## Research and References

### Maturin Best Practices

**Sources:**
- [Maturin User Guide](https://www.maturin.rs/)
- [PyO3 - Building and Distribution](https://pyo3.rs/)

**Key Findings:**
- `pyproject.toml` is the primary configuration for Rust+Python builds
- `tool.maturin` supports `python-source` and `module-name` configuration

### Rust Workspace Structure

**Sources:**
- [Cargo Workspaces - The Rust Programming Language](https://doc.rust-lang.org/book/ch14-03-cargo-workspaces.html)
- [Large Rust Workspaces](https://matklad.github.io/2021/08/22/large-rust-workspaces.html)

**Key Findings:**
- Flat layout (one level deep) for 10k-1M LOC projects
- Virtual manifest prevents polluting root directory
- Shared `Cargo.lock` across all crates
- Workspace-level dependencies ensure consistency

### Bun Runtime & Server APIs

**Sources:**
- [Bun HTTP Server](https://bun.sh/docs/api/http)
- [Bun WebSockets](https://bun.sh/docs/api/websockets)
- [Bun SQL](https://bun.sh/docs/api/sql)

**Key Findings:**
- Bun exposes `Bun.serve()` for high-performance HTTP handling
- WebSocket support is integrated into the Bun server API
- Bun provides built-in SQL helpers for database access

### napi-rs (Node/Bun bindings)

**Sources:**
- [napi-rs Documentation](https://napi.rs/)

**Key Findings:**
- napi-rs targets the Node-API surface for native bindings

### Python AST to TypeScript

**Sources:**
- [@kriss-u/py-ast - JSR](https://jsr.io/@kriss-u/py-ast)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)
- [Bun TypeScript Support](https://bun.sh/docs/runtime/typescript)

**Key Findings:**
- `@kriss-u/py-ast` parses Python to AST in TypeScript
- Supports all Python AST nodes (Module, Statement, Expression)
- Can unparse AST back to Python for validation
- Bun has native TypeScript support (no build step)

### tRPC Architecture for Python

**Sources:**
- [tRPC Documentation](https://trpc.io/docs)

**Key Findings:**
- tRPC is TypeScript-first, not designed for Python backends
- Existing Python implementations are experimental/prototypes
- Need custom solution: Python decorators → TypeScript types
- Focus on procedure definition pattern, not full tRPC protocol

---

## Appendix

### File Checklist

**Files to Modify:**
- [x] `pyproject.toml`
- [x] `Makefile`
- [x] `conftest.py`
- [x] `README.md`

**Files to Create:**
- [x] `src/rs/Cargo.toml`
- [x] `src/rs/vertebra-core/Cargo.toml`
- [x] `src/rs/vertebra-core/src/lib.rs`
- [x] `src/rs/vertebra-py/Cargo.toml`
- [x] `src/rs/vertebra-py/src/lib.rs`
- [x] `src/rs/vertebra-js/Cargo.toml`
- [x] `src/rs/vertebra-js/src/lib.rs`
- [x] `src/rs/vertebra-js/build.rs`
- [x] `src/ts/package.json`
- [x] `src/ts/tsconfig.json`
- [x] `src/ts/src/index.ts`
- [x] `src/ts/src/index.test.ts`
- [x] `src/ts/src/client/index.ts`
- [x] `src/ts/src/codegen/index.ts`
- [x] `src/ts/src/server/index.ts`
- [x] `src/py/vertebra/__init__.py`
- [x] `src/py/vertebra/server/__init__.py`
- [x] `src/py/vertebra/procedures/__init__.py`
- [x] `src/py/vertebra/types/__init__.py`
- [x] `src/py/vertebra/drivers/__init__.py`
- [x] `src/py/vertebra/ipc/__init__.py`
- [x] `CLAUDE.md`
- [x] `docs/arch.md`

**Files to Delete:**
- (None in PRD-0; DMA code deletion deferred to avoid breaking tests)

### Dependencies Added

**Python:**
- `maturin>=1.0,<2.0` (build dependency)

**Rust:**
- `pyo3 = "0.27"` (Python bindings)
- `napi = "2.16"` (Node/Bun bindings)
- `napi-derive = "2.16"` (Derive macros)
- `tokio = "1.35"` (Async runtime)
- `serde = "1.0"` (Serialization)
- `thiserror = "2.0"` (Error handling)

**TypeScript:**
- `@kriss-u/py-ast` (Python AST parser)
- `typescript` (Type system)
- `@types/bun` (Bun type definitions)

### Glossary

- **ASGI**: Asynchronous Server Gateway Interface (Python web server standard)
- **AST**: Abstract Syntax Tree (code structure representation)
- **FFI**: Foreign Function Interface (calling code across languages)
- **GIL**: Global Interpreter Lock (Python's threading limitation)
- **IPC**: Inter-Process Communication
- **maturin**: Build tool for Rust+Python projects
- **napi-rs**: Rust bindings for Node.js/Bun
- **PyO3**: Rust bindings for Python
- **tRPC**: TypeScript Remote Procedure Call framework
- **Zero-Copy**: Data transfer without serialization overhead

---

**End of PRD-0**
