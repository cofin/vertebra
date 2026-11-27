---
type: Task
id: "runtime-perf-remediation:ch01:t01"
chapter_id: ch01-wsgi-adaptive-pool
title: Share WsgiServerConfig across WsgiEnviron
description: Add WsgiServerConfig and refactor WsgiEnviron to store Arc<WsgiServerConfig> while preserving existing constructors.
status: stable
state: completed
priority: P0
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
  - src/rs/vertebra-py/src/wsgi/environ.rs
files:
  - src/rs/vertebra-py/src/wsgi/environ.rs
tests:
  - src/rs/vertebra-py/src/wsgi/environ.rs
estimated_loc: 85
plan_revision: 2
plan_commit: "86527cf1"
commit: "b110db7d"
sources:
  - id: ch01-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
    title: Chapter 01 adaptive WSGI pool specification
  - id: src-wsgi-environ
    resource: src/rs/vertebra-py/src/wsgi/environ.rs::WsgiEnviron
    title: Lazy PEP 3333 WSGI environ mapping
tags:
  - wsgi
  - rust
  - performance
---

# Share WsgiServerConfig across WsgiEnviron

## Objective & Scope Boundary

Eliminate three per-request `String` allocations in `WsgiEnviron` by storing batch-shared server metadata in `Arc<WsgiServerConfig>` [^ch01-spec] [^src-wsgi-environ]. Keep `WsgiEnviron::new`, `WsgiEnviron::with_root_path`, and `WsgiEnviron::from_parts` backwards-compatible. Do not modify `runner.rs`, `pool.rs`, or Python files in this task.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` (Section 2.2 `WsgiServerConfig` contract)
- `src/rs/vertebra-py/src/wsgi/environ.rs`

## Contracts & Edge Cases

- Implement `WsgiServerConfig` and `WsgiServerConfig::new` per `spec.md` Section 2.2 with the exact four public fields (`root_path`, `server_name`, `server_port`, `url_scheme`) so struct literals remain valid [^ch01-spec].
- Normalize `root_path` inside `WsgiServerConfig::new`: `"/"` becomes `""`, and trailing `'/'` characters are trimmed (`"/api/"` -> `"/api"`).
- Replace the four metadata fields on `WsgiEnviron` with `config: Arc<WsgiServerConfig>`.
- Add `WsgiEnviron::with_config(batch: Arc<BatchSnapshot>, index: usize, config: Arc<WsgiServerConfig>) -> Self`.
- Delegate `WsgiEnviron::new` and `WsgiEnviron::with_root_path` to `Self::with_config`.
- Update `build_static_key` (`SCRIPT_NAME`, `PATH_INFO`, `SERVER_NAME`, `SERVER_PORT`, `wsgi.url_scheme`) to read from `self.config`.

## Test Matrix

| Test Case | Assertion |
| :--- | :--- |
| `WsgiServerConfig::new` root normalization | `"/"` -> `""`, `"/v1/"` -> `"/v1"`, `"/v1///"` -> `"/v1"`, `""` -> `""`. |
| Shared `Arc<WsgiServerConfig>` reuse | Creating multiple `WsgiEnviron::with_config` instances increments `Arc::strong_count` without cloning strings. |
| Backwards-compatible constructors | `WsgiEnviron::new` and `WsgiEnviron::with_root_path` preserve `SCRIPT_NAME`, `PATH_INFO`, `SERVER_NAME`, `SERVER_PORT`, and `wsgi.url_scheme` values. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi::environ
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
```

## Acceptance Criteria

- `WsgiEnviron` stores `Arc<WsgiServerConfig>` and exposes `WsgiEnviron::with_config`.
- `WsgiEnviron::new`, `WsgiEnviron::with_root_path`, and `WsgiEnviron::from_parts` compile and pass unchanged callers.
- Rust unit tests in `environ.rs` and `clippy -D warnings` pass.

[^ch01-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` — Chapter 01 adaptive WSGI pool specification.
[^src-wsgi-environ]: `src/rs/vertebra-py/src/wsgi/environ.rs::WsgiEnviron` — Lazy PEP 3333 WSGI environ mapping.
