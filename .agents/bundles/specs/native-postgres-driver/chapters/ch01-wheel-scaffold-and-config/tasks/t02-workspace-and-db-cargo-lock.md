---
type: Task
id: "native-postgres-driver:ch01:t02"
chapter_id: ch01-wheel-scaffold-and-config
title: Register workspace member, add vertebra-db socket2/half pins, and lock Cargo.lock
description: Register vertebra-postgres in src/rs/Cargo.toml, add socket2 =0.6.5 and half =2.7.1 to src/rs/vertebra-db/Cargo.toml, and resolve src/rs/Cargo.lock so workspace builds pass with --locked.
status: stable
state: completed
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch01:t01"
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/src/lib.rs
files:
  - src/rs/Cargo.toml
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/Cargo.lock
tests: []
estimated_loc: 45
plan_revision: 4
plan_commit: 45ff9563
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T20:16:22.351888+00:00"
last_verified_commit: 9e128425
verification_evidence: "exit 0: manifest assertions; pre-existing lock package/version identities preserved; workspace cargo check --locked; workspace all-targets Clippy --features tls,compression,gcp --locked -- -D warnings; crate cargo test --locked. PYO3_PYTHON=${PWD}/.venv/bin/python; CARGO_TARGET_DIR=/tmp/vtb-cargo-target; UV_NO_SYNC=1."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T20:16:22.351888+00:00"
commit: 9e128425
sources:
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Rust workspace members manifest
  - id: db-cargo
    resource: src/rs/vertebra-db/Cargo.toml
    title: Shared vertebra-db crate dependencies
  - id: ws-lock
    resource: src/rs/Cargo.lock
    title: Workspace Cargo lockfile pinning x509-parser 0.18.1, arrow 60.1.0, half 2.7.1, and socket2 0.6.5
tags:
  - postgres
  - cargo
  - workspace
  - lockfile
---

# Task t02: Register Workspace Member, Add `vertebra-db` `socket2`/`half` Pins, and Lock `Cargo.lock`

## Objective & Scope Boundary

Register `"vertebra-postgres"` in `src/rs/Cargo.toml` `[workspace].members`, add `socket2 = { version = "=0.6.5", features = ["all"] }` and `half = "=2.7.1"` to `src/rs/vertebra-db/Cargo.toml`, and update `src/rs/Cargo.lock` so the entire Rust workspace (including `vertebra-postgres` and `vertebra-db`) compiles and passes Clippy with `--locked` [^ws-cargo] [^db-cargo] [^ws-lock]. Maturin wheel packaging, `_postgres.pyi`, and `Makefile` targets belong to `native-postgres-driver:ch01:t03` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/Cargo.toml` — `[workspace].members` list [^ws-cargo].
- `src/rs/vertebra-db/Cargo.toml` — `[dependencies]` table for `vertebra-db` [^db-cargo].
- `src/rs/Cargo.lock` — Existing locked crate versions (including `x509-parser` `0.18.1`, `arrow-*` `60.1.0`, `half` `2.7.1`, `socket2` `0.6.5`) [^ws-lock].
- `src/rs/vertebra-postgres/Cargo.toml` — Crate manifest created in `ch01:t01`.
- `src/rs/vertebra-postgres/src/lib.rs` — PyO3 module created in `ch01:t01`.

## Symbol & Signature Contracts

- `src/rs/Cargo.toml`:
  - Append `"vertebra-postgres"` to `[workspace].members`.
- `src/rs/vertebra-db/Cargo.toml`:
  - Add `half = "=2.7.1"` and `socket2 = { version = "=0.6.5", features = ["all"] }` to `[dependencies]` in alphabetical order (pre-staging TCP keepalive socket controls for `ch06:t02` and `f16` `halfvec` codecs for `ch12:t01` so neither downstream task mutates `Cargo.toml` or `Cargo.lock`).
- `src/rs/Cargo.lock`:
  - Resolve workspace lock entries for `vertebra-postgres` and `vertebra-db` via `cargo metadata` / `cargo check --manifest-path src/rs/Cargo.toml --workspace` without running `cargo update` across unrelated workspace dependencies.

## Edge Cases & Error Semantics

- Never run `cargo update` without `-w` / package scoping: unrelated locked crate versions in `src/rs/Cargo.lock` must remain unchanged.
- Verify `--locked` across `--workspace` AND `--all-targets --features tls,compression,gcp` so CI `lint-all` and `rs-test` pass immediately after this commit without regenerating `Cargo.lock`.

## Public-Seam Test Blueprint

- **Canonical gate**: `tomllib` manifest checks plus `--locked` workspace `cargo check`, `cargo clippy`, and `cargo test` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Workspace & `vertebra-db` TOML pins | Parse `src/rs/Cargo.toml` and `src/rs/vertebra-db/Cargo.toml` with `tomllib` | `"vertebra-postgres"` in `workspace["members"]`; `vertebra-db` `dependencies["half"] == "=2.7.1"` and `dependencies["socket2"] == {"version": "=0.6.5", "features": ["all"]}`. |
| Locked workspace compilation | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` | Exits `0` without touching `src/rs/Cargo.lock`. |
| Locked all-targets Clippy & crate test | `cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,compression,gcp --locked -- -D warnings` and `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked` | Both exit `0` with zero warnings and clean `git diff src/rs/Cargo.lock`. |

## Verification

Update the manifests, resolve `src/rs/Cargo.lock`, and confirm all `--locked` gates exit `0`:

```bash
uv run --no-sync python -c '
import pathlib, tomllib
ws = tomllib.loads(pathlib.Path("src/rs/Cargo.toml").read_text())
assert "vertebra-postgres" in ws["workspace"]["members"]
db = tomllib.loads(pathlib.Path("src/rs/vertebra-db/Cargo.toml").read_text())
assert db["dependencies"]["half"] == "=2.7.1"
assert db["dependencies"]["socket2"] == {"version": "=0.6.5", "features": ["all"]}
'
cargo check --manifest-path src/rs/Cargo.toml --workspace --locked
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,compression,gcp --locked -- -D warnings
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
```

Commit as `build(postgres): register workspace member and lock Cargo dependencies`.

## Acceptance Criteria

- [x] `src/rs/Cargo.toml` includes `"vertebra-postgres"` in `[workspace].members` and `src/rs/vertebra-db/Cargo.toml` pins `socket2 = { version = "=0.6.5", features = ["all"] }` and `half = "=2.7.1"` (R1).
- [x] `src/rs/Cargo.lock` is committed in sync such that `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` and `--locked` workspace Clippy exit `0` (R1).

## Notes & Discoveries

- [2026-11-07] Registered the crate and resolved 17 new registry package entries without changing any existing locked package/version/source identity. Native gates select the isolated Python 3.12 via `PYO3_PYTHON`; the default system Python 3.14 has no linkable `libpython3.14`.

- [2026-11-07 16:40] Locking `socket2` and `half` in `vertebra-db` alongside `vertebra-postgres`'s `postgres-protocol`, `x509-parser = "=0.18.1"`, `rsa` `["pem", "sha2"]`, `rustls-pemfile`, `half`, and `arrow-*` pins in `ch01:t02` prevents unowned `src/rs/Cargo.lock` churn across Chapters 02–28 (the only task that touches `Cargo.toml` + `Cargo.lock` later is `ch17:t04` when wiring `jiff = "=0.2.39"`).

[^ws-cargo]: `src/rs/Cargo.toml` — Workspace crate member registry.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — Shared `vertebra-db` dependency table.
[^ws-lock]: `src/rs/Cargo.lock` — Workspace dependency lockfile.
