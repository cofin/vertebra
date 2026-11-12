---
type: Task
id: "storage-integrity-and-streaming:ch04:t01"
chapter_id: ch04-wheel-check-docs-and-okf
title: Add make storage-wheel-check and document it
description: Add a storage-wheel-check Make target asserting the vertebra-storage wheel ships only vertebra/_storage*.so and vertebra/_storage.pyi, and list it in AGENTS.md and the workflow knowledge chapter.
status: draft
state: pending
priority: P2
verification_strategy: static_validation
depends_on:
  - "storage-integrity-and-streaming:ch03:t03"
read_set:
  - Makefile
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
  - src/rs/vertebra-storage/pyproject.toml
files:
  - Makefile
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
tests: []
estimated_loc: 30
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: makefile
    resource: Makefile
    title: postgres-wheel-check and duckdb-wheel-check recipes
  - id: storage-pyproject
    resource: src/rs/vertebra-storage/pyproject.toml
    title: vertebra-storage maturin module-name vertebra._storage
tags:
  - makefile
  - packaging
---

# Task t01: Add `make storage-wheel-check` and Document It

## Objective & Scope Boundary

1. `Makefile`: after `postgres-wheel-check`, add [^makefile]:

```make
.PHONY: storage-wheel-check
storage-wheel-check:                                ## Build vertebra-storage wheel and verify payload isolation
	@rm -rf /tmp/vertebra-storage-wheel-check
	@uv build src/rs/vertebra-storage --wheel --out-dir /tmp/vertebra-storage-wheel-check
	@uv run --no-sync python -c 'import glob, re, zipfile; whls = glob.glob("/tmp/vertebra-storage-wheel-check/vertebra_storage-*.whl"); assert len(whls) == 1, whls; vtb = [n for n in zipfile.ZipFile(whls[0]).namelist() if n.startswith("vertebra/")]; assert len(vtb) == 2 and "vertebra/_storage.pyi" in vtb and any(re.fullmatch(r"vertebra/_storage\..*\.so", n) for n in vtb), vtb'
	@echo "${OK} vertebra-storage wheel verified"
```

2. `AGENTS.md` Quick Verification Commands: add `make storage-wheel-check # Verify standalone vertebra-storage wheel ships only vertebra/_storage*.so and _storage.pyi` after `oracle-wheel-check`.
3. `workflow.md`: add `make storage-wheel-check` to the target list sentence and the command block next to `make duckdb-wheel-check`.

## Bounded Read Manifest

- `Makefile` wheel-check recipes; `AGENTS.md` command block; `workflow.md` target list and command block; `vertebra-storage/pyproject.toml` (`name = "vertebra-storage"`, `module-name = "vertebra._storage"`) [^storage-pyproject].

## Symbol & Signature Contracts

- Target `storage-wheel-check`; wheel glob `vertebra_storage-*.whl`.

## Edge Cases & Error Semantics

- Stale output directory: removed before build.
- Extra entry such as `vertebra/__init__.py`: assertion prints the entry list and exits non-zero.

## Public-Seam Test Blueprint

- **Canonical gate**: `make storage-wheel-check`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Dry run | `make -n storage-wheel-check` | contains `uv build src/rs/vertebra-storage --wheel` |
| Real build | `make storage-wheel-check` | prints `vertebra-storage wheel verified` |
| Help | `make help` | lists `storage-wheel-check` |

## Verification

- `make -n storage-wheel-check | grep -q 'uv build src/rs/vertebra-storage --wheel'`
- `make storage-wheel-check`
- `make review-check`

## Acceptance Criteria

- [ ] The storage wheel isolation check exists and is documented (R7).

## Notes & Discoveries

- [2026-11-12 01:30] The release workflow builds `vertebra-storage` as a standalone wheel; no local gate checked its payload before this task.

[^makefile]: `Makefile` — Wheel-check recipes.
[^storage-pyproject]: `src/rs/vertebra-storage/pyproject.toml` — Package and module names.
