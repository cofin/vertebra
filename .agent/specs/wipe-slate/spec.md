# Flow: wipe-slate

**Beads ID:** vertebra-w4i.1
**Flow ID:** wipe-slate
**Chapter:** 1 of 22 — Saga 1: Clean Slate & Foundation
**Status:** active

---

## Specification

### Context

Vertebra 0.1.0 "Ghost Framework" is a complete ground-up rewrite. The current
repository contains a working POC codebase accumulated across multiple
exploration sprints. That code is reference material, not a foundation. It must
be archived — not deleted — so implementation insights remain accessible during
the rebuild without polluting the new structure.

This chapter performs exactly one thing: move all POC code into `scratch/` and
verify only the preserved items remain at the repository root.

### What Is Being Archived

The following top-level directories and files will be moved to `scratch/`:

| Source path | Destination in `scratch/` |
|---|---|
| `src/py/` | `scratch/src-py/` |
| `src/rs/` | `scratch/src-rs/` |
| `src/ts/` | `scratch/src-ts/` |
| `src/wasm/` | `scratch/src-wasm/` |
| `src/` (empty shell after moves) | removed |
| `benchmarks/` | `scratch/benchmarks/` |
| `tools/` | `scratch/tools/` |
| `examples/` | `scratch/examples/` |
| `docs/` | `scratch/docs/` |
| `specs/` | `scratch/specs/` |
| `dist/` | `scratch/dist/` |
| `tmp/` | `scratch/tmp/` |
| `Makefile` | `scratch/Makefile` |
| `pyproject.toml` | `scratch/pyproject.toml` |
| `uv.lock` | `scratch/uv.lock` |
| `Cargo.toml` (root, if present) | `scratch/Cargo.toml` |
| `Cargo.lock` (root, if present) | `scratch/Cargo.lock` |
| `CLAUDE.md` | `scratch/CLAUDE.md` |
| `AGENTS.md` | `scratch/AGENTS.md` |
| `GEMINI.md` | `scratch/GEMINI.md` |
| `server.log` | `scratch/server.log` |
| `server_multi.log` | `scratch/server_multi.log` |
| `server_release.log` | `scratch/server_release.log` |

Note: `CLAUDE.md` and `AGENTS.md` are project instruction files tied to the old
codebase and its conventions. They will be rewritten from scratch for 0.1.0.
Moving them to `scratch/` keeps them accessible as reference while clearing the
way for new files.

### What Is Preserved (NOT Moved)

The following items stay at the repository root and must not be touched:

- `.agent/` — AI agent context, flows, specs, research
- `.beads/` — Beads tracking database
- `.claude/` — Claude agent configuration
- `.gemini/` — Gemini agent configuration
- `.github/` — CI/CD workflows
- `.gitignore`
- `.gitattributes` (if present)
- `.pre-commit-config.yaml`
- `.python-version`
- `LICENSE`
- `README.md`
- `.mypy_cache/` — ignored by git, harmless to leave
- `.pytest_cache/` — ignored by git, harmless to leave
- `.venv/` — ignored by git, harmless to leave
- `.tmp/` — ignored by git, harmless to leave
- `.vscode/` — editor config, harmless to leave

### Why Not Delete

POC code contains working implementations of ring buffer plumbing, ASGI bridge
logic, type IR extraction, and benchmark harnesses. During rebuilds those
references prevent re-inventing existing solutions. `scratch/` is gitignored
for production but available locally.

### Gate Conditions

- `make lint` is NOT applicable — there is no source to lint after archive.
- `make test` is NOT applicable — there is no test suite at root after archive.
- The gate is structural: verify preserved items exist and archived items are
  gone from their original locations.

---

## Implementation Plan

### Phase 1: Create Archive Root

- [ ] 1.1 Create the `scratch/` directory and add it to `.gitignore`
  **Files:**
  - `/home/cody/code/c/vertebra/scratch/` (new directory)
  - `/home/cody/code/c/vertebra/.gitignore` (append entry)

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mkdir -p scratch
  echo '' >> .gitignore
  echo '# POC archive — local reference only' >> .gitignore
  echo 'scratch/' >> .gitignore
  ```

  **Pass:** `scratch/` exists; `.gitignore` contains `scratch/`; `git check-ignore -v scratch/` outputs a match.
  **Fail:** Directory not created; `.gitignore` not updated; `git status` shows `scratch/` as an untracked addition.

---

### Phase 2: Archive Python Source

- [ ] 2.1 Move `src/py/` to `scratch/src-py/`
  **Files:**
  - Source: `src/py/` (contains `vertebra/` package and `tests/`)
  - Destination: `scratch/src-py/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv src/py scratch/src-py
  ```

  **Pass:** `scratch/src-py/vertebra/__init__.py` exists; `src/py/` does not exist.
  **Fail:** Either path state is reversed; move produced an error.

---

### Phase 3: Archive Rust Source

- [ ] 3.1 Move `src/rs/` to `scratch/src-rs/`
  **Files:**
  - Source: `src/rs/` (workspace with vertebra-core, vertebra-py, vertebra-engine, vertebra-broker, vertebra-server)
  - Destination: `scratch/src-rs/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv src/rs scratch/src-rs
  ```

  **Pass:** `scratch/src-rs/Cargo.toml` exists; `scratch/src-rs/vertebra-core/` exists; `src/rs/` does not exist.
  **Fail:** Source directory still present; Cargo.toml not found at destination.

  Note: The `src/rs/target/` build artifact directory will move with `src/rs/`. It
  is gitignored and very large. After the move you may optionally delete
  `scratch/src-rs/target/` to reclaim disk space:
  ```bash
  rm -rf scratch/src-rs/target
  ```

---

### Phase 4: Archive TypeScript/Bun Source

- [ ] 4.1 Move `src/ts/` to `scratch/src-ts/` (if it exists)
  **Files:**
  - Source: `src/ts/` (Bun broker, drivers, IPC, MCP, HTTP server)
  - Destination: `scratch/src-ts/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  # Check existence first; src/ts may already be absent on this branch
  [ -d src/ts ] && mv src/ts scratch/src-ts || echo "src/ts not present, skipping"
  ```

  **Pass:** If `src/ts/` existed before: `scratch/src-ts/` exists and `src/ts/` does not. If it did not exist: step is a no-op and no error is raised.
  **Fail:** `mv` errors out; destination not created when source was present.

- [ ] 4.2 Move `src/wasm/` to `scratch/src-wasm/` (if it exists)
  **Files:**
  - Source: `src/wasm/` (vertebra_wasm Python package)
  - Destination: `scratch/src-wasm/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  [ -d src/wasm ] && mv src/wasm scratch/src-wasm || echo "src/wasm not present, skipping"
  ```

  **Pass:** If source existed: `scratch/src-wasm/vertebra_wasm/__init__.py` exists and `src/wasm/` does not.
  **Fail:** `mv` errors out when source was present.

- [ ] 4.3 Remove the now-empty `src/` shell
  **Files:**
  - `src/` (should be empty after all sub-moves)

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  # Only remove if empty; fail loudly if anything remains
  rmdir src 2>/dev/null && echo "src/ removed" || echo "src/ still has contents — check manually"
  ```

  **Pass:** `src/` does not exist.
  **Fail:** `rmdir` fails because `src/` still contains files (indicates a missed sub-directory).

---

### Phase 5: Archive Support Directories

- [ ] 5.1 Move `benchmarks/` to `scratch/benchmarks/`
  **Files:**
  - Source: `benchmarks/` (arrow_vs_msgspec.py, ipc_fast_path.py, ipc_performance.py, mpmc_contention.py, native_litestar.py, py_ipc_latency.py, py_serialization.py, research_threshold.py, security_hashing.py, results/, servers/)
  - Destination: `scratch/benchmarks/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv benchmarks scratch/benchmarks
  ```

  **Pass:** `scratch/benchmarks/ipc_performance.py` exists; `benchmarks/` does not exist.
  **Fail:** Source still present; destination empty.

- [ ] 5.2 Move `tools/` to `scratch/tools/`
  **Files:**
  - Source: `tools/` (ipc_consumer.py, ipc_producer.py, deploy/, lib/, mysql/, postgres/, redis/, scripts/, rs_coverage.sh, rs_tsan.sh, rs_valgrind.sh, test_app.py, test_*.py)
  - Destination: `scratch/tools/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv tools scratch/tools
  ```

  **Pass:** `scratch/tools/__init__.py` exists; `tools/` does not exist.
  **Fail:** Source still present; destination not created.

- [ ] 5.3 Move `examples/` to `scratch/examples/`
  **Files:**
  - Source: `examples/` (hello-world/, mcp-tools/, pool-health-demo/, __init__.py)
  - Destination: `scratch/examples/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv examples scratch/examples
  ```

  **Pass:** `scratch/examples/hello-world/` exists; `examples/` does not exist.
  **Fail:** Source still present.

- [ ] 5.4 Move `docs/` to `scratch/docs/`
  **Files:**
  - Source: `docs/` (api/, architecture/, distribution/, grafana/, mcp/, observability/, pools/, arch.md, cli.md, progress.md, protocol.md)
  - Destination: `scratch/docs/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv docs scratch/docs
  ```

  **Pass:** `scratch/docs/arch.md` exists; `docs/` does not exist.
  **Fail:** Source still present.

- [ ] 5.5 Move `specs/` to `scratch/specs/`
  **Files:**
  - Source: `specs/` (active/performance-100k-rps/ and all sub-artifacts)
  - Destination: `scratch/specs/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv specs scratch/specs
  ```

  **Pass:** `scratch/specs/active/` exists; `specs/` does not exist.
  **Fail:** Source still present.

- [ ] 5.6 Move `dist/` to `scratch/dist/` (if it exists)
  **Files:**
  - Source: `dist/` (distribution/ sub-directory)
  - Destination: `scratch/dist/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  [ -d dist ] && mv dist scratch/dist || echo "dist/ not present, skipping"
  ```

  **Pass:** If source existed: `scratch/dist/` exists and `dist/` does not.
  **Fail:** `mv` errors out when source was present.

- [ ] 5.7 Move `tmp/` to `scratch/tmp/` (if it exists)
  **Files:**
  - Source: `tmp/` (asgi_source.py, broker.err/out/stderr/stdout, example.md, gaps_and_improvements.md, moving_forward.md, pytest-of-cody/, skills/, socketify_source.py, test-frontend/, verify_phase1.py, VERTEBRA.md)
  - Destination: `scratch/tmp/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  [ -d tmp ] && mv tmp scratch/tmp || echo "tmp/ not present, skipping"
  ```

  **Pass:** If source existed: `scratch/tmp/` exists and `tmp/` does not.
  **Fail:** `mv` errors out when source was present.

---

### Phase 6: Archive Root-Level Config and Build Files

- [ ] 6.1 Move `Makefile` to `scratch/Makefile`
  **Files:**
  - Source: `Makefile`
  - Destination: `scratch/Makefile`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv Makefile scratch/Makefile
  ```

  **Pass:** `scratch/Makefile` exists; `Makefile` does not exist at root.
  **Fail:** Source still present; destination absent.

- [ ] 6.2 Move `pyproject.toml` to `scratch/pyproject.toml`
  **Files:**
  - Source: `pyproject.toml`
  - Destination: `scratch/pyproject.toml`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv pyproject.toml scratch/pyproject.toml
  ```

  **Pass:** `scratch/pyproject.toml` exists; `pyproject.toml` does not exist at root.
  **Fail:** Source still present.

- [ ] 6.3 Move `uv.lock` to `scratch/uv.lock`
  **Files:**
  - Source: `uv.lock`
  - Destination: `scratch/uv.lock`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  mv uv.lock scratch/uv.lock
  ```

  **Pass:** `scratch/uv.lock` exists; `uv.lock` does not exist at root.
  **Fail:** Source still present.

- [ ] 6.4 Move root-level `Cargo.toml` and `Cargo.lock` to `scratch/` (if present)
  **Files:**
  - Source: `Cargo.toml`, `Cargo.lock` (root-level, if they exist)
  - Destination: `scratch/Cargo.toml`, `scratch/Cargo.lock`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  [ -f Cargo.toml ] && mv Cargo.toml scratch/Cargo.toml || echo "Cargo.toml not at root, skipping"
  [ -f Cargo.lock ] && mv Cargo.lock scratch/Cargo.lock || echo "Cargo.lock not at root, skipping"
  ```

  **Pass:** Root no longer contains `Cargo.toml` or `Cargo.lock`; if they existed, copies are in `scratch/`.
  **Fail:** `mv` errors; files remain at root.

- [ ] 6.5 Move AI instruction files to `scratch/` (`CLAUDE.md`, `AGENTS.md`, `GEMINI.md`)
  **Files:**
  - Source: `CLAUDE.md`, `AGENTS.md`, `GEMINI.md`
  - Destination: `scratch/CLAUDE.md`, `scratch/AGENTS.md`, `scratch/GEMINI.md`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  [ -f CLAUDE.md ] && mv CLAUDE.md scratch/CLAUDE.md || echo "CLAUDE.md not present, skipping"
  [ -f AGENTS.md ] && mv AGENTS.md scratch/AGENTS.md || echo "AGENTS.md not present, skipping"
  [ -f GEMINI.md ] && mv GEMINI.md scratch/GEMINI.md || echo "GEMINI.md not present, skipping"
  ```

  **Pass:** `CLAUDE.md`, `AGENTS.md`, and `GEMINI.md` do not exist at root; their archived copies exist in `scratch/`.
  **Fail:** Files remain at root; `scratch/` copies absent.

  Note: `CLAUDE.md` and `AGENTS.md` will be recreated in the next chapter
  (scaffold-base) with 0.1.0-era content.

- [ ] 6.6 Move stale log files to `scratch/`
  **Files:**
  - Source: `server.log`, `server_multi.log`, `server_release.log` (root-level, if present)
  - Destination: `scratch/server.log`, `scratch/server_multi.log`, `scratch/server_release.log`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  for f in server.log server_multi.log server_release.log; do
    [ -f "$f" ] && mv "$f" "scratch/$f" || echo "$f not present, skipping"
  done
  ```

  **Pass:** No `.log` files remain at the repository root; archived copies exist in `scratch/`.
  **Fail:** Log files remain at root.

---

### Phase 7: Verification

- [ ] 7.1 Run post-wipe verification script

  **Commands:**
  Save and run the following script from the repository root:

  ```bash
  cd /home/cody/code/c/vertebra
  python3 - <<'PYEOF'
  import os, sys

  ROOT = "/home/cody/code/c/vertebra"

  # Items that MUST still exist (preserved)
  MUST_EXIST = [
      ".agent",
      ".beads",
      ".claude",
      ".gemini",
      ".github",
      ".gitignore",
      ".pre-commit-config.yaml",
      ".python-version",
      "LICENSE",
      "README.md",
      "scratch",
  ]

  # Items that must NOT exist at root (were archived)
  MUST_NOT_EXIST = [
      "src",
      "benchmarks",
      "tools",
      "examples",
      "docs",
      "specs",
      "dist",
      "Makefile",
      "pyproject.toml",
      "uv.lock",
      "CLAUDE.md",
      "AGENTS.md",
      "GEMINI.md",
      "server.log",
      "server_multi.log",
      "server_release.log",
  ]

  # Items archived into scratch/ that must be present there
  MUST_BE_IN_SCRATCH = [
      "scratch/src-py/vertebra/__init__.py",
      "scratch/src-rs/Cargo.toml",
      "scratch/src-rs/vertebra-core",
      "scratch/benchmarks",
      "scratch/tools",
      "scratch/examples",
      "scratch/docs/arch.md",
      "scratch/specs/active",
      "scratch/Makefile",
      "scratch/pyproject.toml",
      "scratch/uv.lock",
  ]

  errors = []

  for item in MUST_EXIST:
      path = os.path.join(ROOT, item)
      if not os.path.exists(path):
          errors.append(f"MISSING (must exist):    {item}")

  for item in MUST_NOT_EXIST:
      path = os.path.join(ROOT, item)
      if os.path.exists(path):
          errors.append(f"STILL PRESENT (must be gone): {item}")

  for item in MUST_BE_IN_SCRATCH:
      path = os.path.join(ROOT, item)
      if not os.path.exists(path):
          errors.append(f"NOT IN SCRATCH (missing): {item}")

  if errors:
      print("VERIFICATION FAILED")
      for e in errors:
          print(" ", e)
      sys.exit(1)
  else:
      print("VERIFICATION PASSED — wipe-slate complete")
      sys.exit(0)
  PYEOF
  ```

  **Pass:** Script exits 0 and prints `VERIFICATION PASSED — wipe-slate complete`.
  **Fail:** Script exits non-zero and lists specific missing or unexpected items; each listed item must be investigated and resolved before this phase passes.

- [ ] 7.2 Confirm git sees only expected changes

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  git status --short
  ```

  **Pass:** All moved tracked files appear as `D` (deleted from their original path) or `R` (renamed). No unexpected `?` (untracked) entries appear outside of `scratch/`. `.gitignore` appears as `M` (modified).
  **Fail:** Tracked files outside `scratch/` appear as `M` unexpectedly; `.agent/`, `.beads/`, `.github/` are not present in `git status` output (would indicate accidental deletion).

- [ ] 7.3 Confirm `.gitignore` suppresses `scratch/`

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra
  git check-ignore -v scratch/
  ```

  **Pass:** Output shows the `.gitignore` rule matching `scratch/`, e.g. `.gitignore:NNN:scratch/	scratch/`.
  **Fail:** No output (scratch/ not ignored), meaning it will appear in `git status` as a large untracked directory.

---

### Phase 8: Commit

- [ ] 8.1 Stage and commit the wipe

  **Commands:**
  ```bash
  cd /home/cody/code/c/vertebra

  # Stage all deletions of tracked source files
  git add -u

  # Stage the .gitignore change
  git add .gitignore

  git commit -m "$(cat <<'EOF'
  chore(wipe-slate): Archive POC codebase to scratch/ for 0.1.0 rebuild

  Moves all pre-rewrite source code, build configs, and docs into scratch/
  so the Ghost Framework rebuild starts from a clean root. Preserved items:
  .agent/, .beads/, .claude/, .gemini/, .github/, .gitignore,
  .pre-commit-config.yaml, .python-version, LICENSE, README.md.

  Beads: vertebra-w4i.1
  EOF
  )"
  ```

  **Pass:** `git log --oneline -1` shows the commit; `git status` is clean.
  **Fail:** Pre-commit hooks reject the commit (investigate hook output); `git status` shows uncommitted changes.

---

## Quick-Reference: All Move Commands (Sequential)

For implementers who want a single-block execution path. Run from
`/home/cody/code/c/vertebra`:

```bash
set -euo pipefail
ROOT=/home/cody/code/c/vertebra
cd "$ROOT"

# 1. Create scratch and update .gitignore
mkdir -p scratch
printf '\n# POC archive — local reference only\nscratch/\n' >> .gitignore

# 2. Python source
mv src/py scratch/src-py

# 3. Rust source
mv src/rs scratch/src-rs
# Optionally reclaim disk: rm -rf scratch/src-rs/target

# 4. TypeScript / WASM source (conditional)
[ -d src/ts ]   && mv src/ts   scratch/src-ts
[ -d src/wasm ] && mv src/wasm scratch/src-wasm

# 5. Remove empty src/ shell
rmdir src

# 6. Support directories
mv benchmarks scratch/benchmarks
mv tools      scratch/tools
mv examples   scratch/examples
mv docs       scratch/docs
mv specs      scratch/specs
[ -d dist ] && mv dist scratch/dist
[ -d tmp ]  && mv tmp  scratch/tmp

# 7. Root-level build and config files
mv Makefile      scratch/Makefile
mv pyproject.toml scratch/pyproject.toml
mv uv.lock       scratch/uv.lock
[ -f Cargo.toml ] && mv Cargo.toml scratch/Cargo.toml
[ -f Cargo.lock ] && mv Cargo.lock scratch/Cargo.lock

# 8. AI instruction files
[ -f CLAUDE.md ] && mv CLAUDE.md scratch/CLAUDE.md
[ -f AGENTS.md ] && mv AGENTS.md scratch/AGENTS.md
[ -f GEMINI.md ] && mv GEMINI.md scratch/GEMINI.md

# 9. Stale logs
for f in server.log server_multi.log server_release.log; do
  [ -f "$f" ] && mv "$f" "scratch/$f"
done

echo "All moves complete. Run verification next."
```

---

## Completion Checklist

- [ ] `scratch/` created and gitignored
- [ ] `src/` fully archived (`src-py/`, `src-rs/`, `src-ts/`, `src-wasm/` in scratch)
- [ ] `src/` removed from root
- [ ] `benchmarks/`, `tools/`, `examples/`, `docs/`, `specs/` archived
- [ ] `dist/`, `tmp/` archived (if present)
- [ ] `Makefile`, `pyproject.toml`, `uv.lock` archived
- [ ] Root Cargo files archived (if present)
- [ ] `CLAUDE.md`, `AGENTS.md`, `GEMINI.md` archived
- [ ] Stale `.log` files archived
- [ ] Verification script exits 0
- [ ] `git check-ignore -v scratch/` confirms suppression
- [ ] Commit created with Beads ID in message

**Next chapter:** `scaffold-base` — initialize the 0.1.0 `uv` workspace and Cargo workspace from scratch.
