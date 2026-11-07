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
plan_revision: 6
plan_commit: 8833b064
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T22:40:44Z"
last_verified_commit: d809be01
verification_evidence:
  - "Direct fallible-iterator =0.2.0 manifest and lock-edge assertions: exit 0; all existing package/version/source/checksum identities unchanged."
  - "UV_NO_SYNC=1; PYO3_PYTHON=${PWD}/.venv/bin/python; CARGO_TARGET_DIR=/tmp/vtb-cargo-target."
  - "Workspace cargo check --locked: exit 0; workspace all-target Clippy --features tls,compression,gcp --locked -- -D warnings: exit 0."
  - "flock /tmp/vertebra-flow-shm-tests.lock cargo test -p vertebra-postgres --locked: exit 0; 9 codec and 7 configuration tests."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T22:40:44Z"
commit: d809be01
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

Workspace membership and the shared `socket2`/`half` pins satisfy the contracts below. The task-02 lock entry records the direct `fallible-iterator` edge for `vertebra-postgres` at its existing locked version `0.2.0`, paired atomically with task 01's crate-manifest declaration. Workspace and `vertebra-db` manifests retain their verified contents; unrelated package identities stay unchanged.

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

Baseline and final native gates select the project interpreter with `PYO3_PYTHON=${PWD}/.venv/bin/python`, `UV_NO_SYNC=1`, and the PostgreSQL Cargo target directory. The current change is the coupled direct-dependency/lock-edge delta; scaffold files and workspace membership retain their verified contracts.

```bash
uv run --no-sync python - <<'PY'
from pathlib import Path
import subprocess
import tomllib
manifest = tomllib.loads(Path("src/rs/vertebra-postgres/Cargo.toml").read_text())
assert manifest["dependencies"]["fallible-iterator"] == "=0.2.0"
previous = tomllib.loads(subprocess.check_output(["git", "show", "8833b064:src/rs/Cargo.lock"], text=True))
current = tomllib.loads(Path("src/rs/Cargo.lock").read_text())
def identities(data):
    return {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in data["package"]}
assert identities(previous) == identities(current)
package = next(p for p in current["package"] if p["name"] == "vertebra-postgres")
assert any(dep == "fallible-iterator" or dep == "fallible-iterator 0.2.0" for dep in package["dependencies"])
PY
```

```bash
cargo check --manifest-path src/rs/Cargo.toml --workspace --locked
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,compression,gcp --locked -- -D warnings
flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
```

Resolve the two-file delta with scoped/offline Cargo metadata before running the final locked gates. Compare `(name, version, source, checksum)` package identities against `git show 8833b064:src/rs/Cargo.lock`; all identities stay equal and the PostgreSQL package gains its direct `fallible-iterator` edge at existing version `0.2.0`.

Tasks 01 and 02 share one signed source commit, `fix(postgres): declare diagnostic iterator prerequisite`, covering only the crate manifest and Cargo lockfile. Both worksheets record that source SHA and their respective ownership after the complete gates pass. Fresh correctness and independent quality review reclose chapter 01 before the blocked session task resumes.

## Acceptance Criteria

- [x] The direct `fallible-iterator = "=0.2.0"` prerequisite is present and its existing-version lock edge passes the static native delta gates (R1).

- [x] `src/rs/Cargo.toml` includes `"vertebra-postgres"` in `[workspace].members` and `src/rs/vertebra-db/Cargo.toml` pins `socket2 = { version = "=0.6.5", features = ["all"] }` and `half = "=2.7.1"` (R1).
- [x] `src/rs/Cargo.lock` is committed in sync such that `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` and `--locked` workspace Clippy exit `0` (R1).

## Notes & Discoveries

- The PostgreSQL lock entry includes the direct `fallible-iterator` dependency edge at existing version `0.2.0`; the prerequisite repair changes no package versions and shares its signed two-file delta with task 01.

- Native gates select the project interpreter through `PYO3_PYTHON=${PWD}/.venv/bin/python`; extension tests require a matching linkable `libpython`.
- Upfront `socket2`, `half`, protocol, TLS, X.509, and Arrow pins keep dependency resolution in this worksheet; the timezone-codec worksheet owns the later `jiff` dependency.

[^ws-cargo]: `src/rs/Cargo.toml` — Workspace crate member registry.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — Shared `vertebra-db` dependency table.
[^ws-lock]: `src/rs/Cargo.lock` — Workspace dependency lockfile.
