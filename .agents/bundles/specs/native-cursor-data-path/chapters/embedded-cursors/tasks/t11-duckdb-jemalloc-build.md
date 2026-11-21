---
type: Task
id: "native-cursor-data-path:embedded:t11"
chapter_id: embedded-cursors
title: "Build the bundled DuckDB engine with jemalloc"
description: "Switch vertebra-duckdb from the duckdb crate feature bundled to bundled-cmake so the bundled engine is built with DuckDB's jemalloc extension on Linux, and record the DuckDB benchmark sample against the PyPI comparator."
worksheet_format: full
status: draft
state: deferred
deferred_by: "user"
deferred_reason: "User decision (2026-11-20): keep bundled in vertebra-duckdb and place DuckDB on the benchmark allow-list until upstream libduckdb-sys ships jemalloc in its crates.io package (bundled-cmake excludes duckdb-sources; download-lib lacks checksums, breaks musllinux/offline builds, and requires dylib vendoring)."
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t05"
read_set:
  - "src/rs/vertebra-duckdb/Cargo.toml"
  - ".github/workflows/release.yaml:43-185"
  - ".github/workflows/ci.yml:414-447"
  - "Makefile:160-195"
  - "Makefile:272-285"
  - "src/py/tests/unit/db/test_duckdb_driver.py:1-40"
  - "tools/benchmarks/adapters/sql.py:353-440"
files:
  - src/rs/vertebra-duckdb/Cargo.toml
  - src/rs/Cargo.lock
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 60
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T16:13:29Z
sources:
  - id: duckdb-manifest
    resource: src/rs/vertebra-duckdb/Cargo.toml
    title: "vertebra-duckdb manifest with the duckdb crate features"
  - id: release-workflow
    resource: .github/workflows/release.yaml
    title: "Wheel build matrix for the optional extensions"
tags: [duckdb, build, allocator, benchmark]
---

# Build the bundled DuckDB engine with jemalloc

## Objective & Scope Boundary

After this task the `_duckdb` extension built on 64-bit glibc Linux allocates through DuckDB's jemalloc extension, as the PyPI `duckdb` wheel does. This runs before the first DuckDB cursor task, so the cursor work is measured on the allocator the comparator uses.

Measured context: sync `SELECT 1` is 426 us against 245 us for the PyPI `duckdb` comparator today. Preloading jemalloc into the current build cut a 314 us sample to 250 us.

In scope: one feature name in `src/rs/vertebra-duckdb/Cargo.toml`, the lock file, one test, and the benchmark sample.

Out of scope: workflow files, the `Makefile`, DuckDB Rust sources, and the Python drivers. No build site needs a new install step, as the table below shows.

## Bounded Read Manifest

- `src/rs/vertebra-duckdb/Cargo.toml` — the `duckdb` dependency line.[^duckdb-manifest]
- `.github/workflows/release.yaml:43-185` — the wheel build matrix and its Linux setup command.[^release-workflow]
- `.github/workflows/ci.yml:414-447` — the optional wheel check job that runs the DuckDB wheel check.
- `Makefile:160-195`, `272-285` — `dev`, `build`, `duckdb-test`, and `duckdb-wheel-check`.
- `src/py/tests/unit/db/test_duckdb_driver.py:1-40` — imports and test style.
- `tools/benchmarks/adapters/sql.py:353-440` — DuckDB capture.

## Symbol & Signature Contracts

`src/rs/vertebra-duckdb/Cargo.toml`, the `duckdb` dependency. Only the feature name changes:

```toml
duckdb = { version = "=1.20000.1", default-features = false, features = ["bundled-cmake", "appender-arrow"] }
```

`src/rs/Cargo.lock` is regenerated with `(cd src/rs && cargo check -p vertebra-duckdb)` and committed. Expected diff:

| Lock entry | Change |
| --- | --- |
| `libduckdb-sys` | its dependency list gains `cmake` and `which` |
| `cmake` | no new entry; version `0.1.58` is already locked for another crate |
| `which` | a second entry at major version `8`, plus any crate it needs that is not locked yet. The existing `4.4.2` entry stays for its current user |

Facts read from the `libduckdb-sys` `1.20000.1` sources that this change relies on:

| Fact | Detail |
| --- | --- |
| Feature definition | `bundled-cmake` enables `bundled`, the build dependencies `cmake` and `which`, and the `parquet` feature |
| Allocator | The CMake build sets `ENABLE_JEMALLOC` to `ON`. DuckDB turns jemalloc on for 64-bit, non-musl Linux targets only. Elsewhere the setting does nothing |
| Opt-out | The environment variable `DUCKDB_DISABLE_JEMALLOC=1` at build time forces the standard allocator. No build site sets it |
| Generator | Ninja is used when `ninja` is on `PATH`; otherwise the default CMake generator |
| Extensions | `json` and `icu` stay skipped, as today. `parquet` becomes part of the static build |
| Windows | The CMake build emits deep object paths. A long build directory can fail with compiler error `C1083` |

Build sites. Each must have a `cmake` binary on `PATH`. None needs a new install step.

| Build site | Where | `cmake` |
| --- | --- | --- |
| `make dev`, `make build`, `make duckdb-test`, `make duckdb-wheel-check`, `make rs-test` on a developer host | `Makefile` | Required on the host. A missing binary fails the build script with the error from the `cmake` crate |
| Lint, Python test, Rust test, conformance, and optional wheel check jobs | `.github/workflows/ci.yml`, hosted Ubuntu runners | Preinstalled on the hosted runner image |
| macOS Rust test job | `.github/workflows/ci.yml`, hosted macOS runner | Preinstalled on the hosted runner image |
| Benchmark job | `.github/workflows/benchmark.yml`, hosted Ubuntu runner | Preinstalled on the hosted runner image |
| Release wheels, Linux | `.github/workflows/release.yaml`, inside the manylinux and musllinux build images | Shipped in those images |
| Release wheels, macOS and Windows | `.github/workflows/release.yaml`, hosted runners | Preinstalled on the hosted runner image |
| Release binaries | `.github/workflows/release-binaries.yml` | Not affected. The server binary does not depend on `libduckdb-sys` |
| Source distribution | `.github/workflows/release.yaml` | Not compiled there. A user who builds from the source distribution needs `cmake` |

The runner and build image contents come from the image documentation, not from a file in this repository. The verification below runs `cmake --version` on the host that executes this task.

## Edge Cases & Error Semantics

- A build without `cmake` fails. There is no fallback to the `bundled` feature.
- jemalloc is active on 64-bit glibc Linux only. macOS, Windows, and musllinux wheels keep the system allocator. The driver limitations page records this in `cleanup-and-checks`.
- The release workflow builds musllinux wheels because it sets no build filter. Those wheels build with `cmake` and without jemalloc.
- The Windows path caveat cannot be checked on a Linux host. This task records it under Notes & Discoveries and changes no workflow. A `C1083` failure in the Windows release wheel job is handled in a follow-up, with `DUCKDB_DISABLE_UNITY=1` as the known remedy.
- No query exposes the allocator in DuckDB `2.0.0`. `PRAGMA version` reports the version only, and `duckdb_extensions()` has no `jemalloc` row even in a build that uses it. The check is the marker text `<jemalloc>:`, which jemalloc compiles into the binary. The PyPI `duckdb` extension module contains it, and the current `_duckdb` extension does not.
- A debug build from `make dev` also links jemalloc, so the test below holds for debug and release builds.
- Existing DuckDB behavior, including extension loading and the Arrow paths, is covered by `make duckdb-test`. Any test that changes result is reported, not edited.

## Public-Seam Test Blueprint

Add one test to `src/py/tests/unit/db/test_duckdb_driver.py`.

`test_duckdb_extension_links_jemalloc_on_glibc_linux`:

- `expected = sys.platform == "linux" and platform.libc_ver()[0] == "glibc" and platform.machine() in {"x86_64", "aarch64"}`.
- Read `pathlib.Path(vertebra._duckdb.__file__).read_bytes()` and assert `(b"<jemalloc>:" in data) is expected`.

| Case | Host | Expected |
| --- | --- | --- |
| Before the feature change | 64-bit glibc Linux | The test fails: the marker is absent |
| After the feature change | 64-bit glibc Linux | The test passes: the marker is present |
| After the feature change | macOS, Windows, musl Linux | The test passes: the marker is absent and `expected` is `False` |

## Verification

```bash
cmake --version
uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -q -k links_jemalloc
(cd src/rs && cargo check -p vertebra-duckdb)
(cd src/rs && cargo tree -p vertebra-duckdb -e features -i libduckdb-sys | grep bundled-cmake)
make build
strings -a "$(uv run --no-sync python -c 'import vertebra._duckdb as m; print(m.__file__)')" | grep -c "<jemalloc>:"
make duckdb-test
make duckdb-wheel-check
unzip -p /tmp/vtb-wheels-duckdb/vertebra_duckdb-*.whl 'vertebra/_duckdb*' | strings -a | grep -c "<jemalloc>:"
(cd src/rs && cargo clippy -p vertebra-duckdb --all-targets -- -D warnings)
PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine duckdb --dsn ":memory:" --iterations 2000 --warmup 200 --output "$(mktemp -d)/embedded-duckdb-allocator.json"
```

- The first `pytest` run happens before the manifest edit and fails. Run it again after `make build`.
- Both `grep -c` commands print `1` or more.
- Run the capture three times and keep the median per case. Each report goes to a temporary directory outside the repository.

## Acceptance Criteria

- [ ] The `duckdb` dependency uses `bundled-cmake`, and the lock file diff matches the table above.
- [ ] `make build` succeeds, and the built extension and the built wheel both contain the jemalloc marker.
- [ ] The new test passes, and `make duckdb-test` and `make duckdb-wheel-check` pass.
- [ ] The benchmark medians for `vertebra-duckdb-sync` and the `duckdb` comparator on the single-row select are recorded under Notes & Discoveries, next to the 426 us and 245 us starting figures.
- [ ] No workflow file and no `Makefile` line has a diff.

## Notes & Discoveries

Record here: the benchmark medians, the lock file entries that were added, and the Windows path caveat as an open risk for the release wheel job.

### Blocked on 2026-11-20: `bundled-cmake` does not build from the crates.io package

- `cmake --version` on the host prints `4.3.1`, and `ninja` is at `/usr/bin/ninja`.
- The new test was added first and failed as planned: the marker `<jemalloc>:` is absent from the current `_duckdb` extension.
- After the feature change, `(cd src/rs && cargo check -p vertebra-duckdb)` fails in the `libduckdb-sys` build script:
  `build_bundled_cmake.rs:19: bundled-cmake requires a duckdb-rs checkout with DuckDB sources at duckdb-sources/CMakeLists.txt`.
- Cause, read from the `libduckdb-sys` `1.20000.1` registry sources:
  - `Cargo.toml.orig` line 19 has `exclude = ["duckdb-sources"]`, so the published package ships no CMake tree.
  - `build_bundled_cmake.rs` uses the fixed relative path `duckdb-sources`. No environment variable overrides it.
  - The shipped `duckdb.tar.gz` holds the sources for the `cc` build only and contains no `CMakeLists.txt`.
- The "Facts read from the `libduckdb-sys` sources" table is correct about the feature definition and missed this exclusion. The feature works only when `libduckdb-sys` is built from a duckdb-rs Git checkout that has the DuckDB submodule.
- Lock file change seen before the revert: one new entry, `which 8.0.6`, and `libduckdb-sys` gained `cmake` and `which`. This matches the expected table.
- All three edits (manifest, lock file, test) are reverted. The tree has no diff from this task. The test is not left in place because it fails on the current build.
- Not run: `make build`, `make duckdb-test`, `make duckdb-wheel-check`, `make lint`, the benchmark capture. The Windows path caveat stays unchecked.

### Second dispatch on 2026-11-20T18:00Z: block still holds, no edits made

- The worksheet was handed out again at `plan_revision: 3` with `state: blocked` and no change to the plan. `bb spec preflight native-cursor-data-path --chapter embedded-cursors --json` returns `"ok": true` with an empty `ready_tasks` list, so this task is not ready.
- Checked again against the registry copy of `libduckdb-sys` `1.20000.1`, read-only: `Cargo.toml.orig` line 19 is `exclude = ["duckdb-sources"]`, the package has no `duckdb-sources` directory, `build_bundled_cmake.rs` line 16 uses the fixed path `duckdb-sources`, and `duckdb.tar.gz` contains zero `CMakeLists.txt` files.
- `cmake --version` prints `4.3.1`. No manifest, lock file, or test edit was made, and no build, test, lint, or benchmark command was run.

Options for the refine step, none tried here:

| Option | Cost |
| --- | --- |
| Git dependency (or `[patch.crates-io]`) on duckdb-rs at the `1.20000.1` tag | Cargo must fetch the DuckDB submodule. `release.yaml` `validate_release_manifest` blocks upstream Git pins in `[tool.uv.sources]`; whether a Cargo Git pin is acceptable for release wheels and the source distribution needs a decision |
| Vendor the DuckDB CMake tree and a patched `libduckdb-sys` in the repository | Large tree in the repository and a fork to maintain |
| Keep `bundled` and link jemalloc as the Rust global allocator of the `_duckdb` cdylib | Does not reach DuckDB's C++ `malloc` calls, so it is unlikely to give the measured gain |
| Keep `bundled` and document `LD_PRELOAD` of jemalloc for benchmarks only | No change to shipped wheels |

[^duckdb-manifest]: `src/rs/vertebra-duckdb/Cargo.toml`
[^release-workflow]: `.github/workflows/release.yaml`
