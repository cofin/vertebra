---
type: Task
id: "native-sqlite-driver:ch05:t01"
flow_id: native-sqlite-driver
chapter_id: ch05-docs-and-knowledge-sync
title: "Document vertebra-core[sqlite] Usage, Stubs Reference, and Changelog"
description: "Update docs/usage/databases.md, docs/reference/native-stubs.md (heading_level: 4), and docs/changelog/index.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch04:t04"
files:
  - docs/usage/databases.md
  - docs/reference/native-stubs.md
  - docs/changelog/index.md
tests: []
read_set:
  - docs/usage/databases.md
  - docs/reference/native-stubs.md
  - docs/changelog/index.md
estimated_loc: 140
tags:
  - docs
  - sqlite
  - changelog
sources:
  - id: docs-db
    resource: docs/usage/databases.md
    title: "Database drivers usage guide"
  - id: docs-stubs
    resource: docs/reference/native-stubs.md
    title: "Native extension stub API reference"
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: "Release changelog"
---

# Task `t01`: Document vertebra-core[sqlite] Usage, Stubs Reference, and Changelog

## Objective & Scope Boundary

Document `vertebra-core[sqlite]`, `SyncSQLitePipeline`/`AsyncSQLitePipeline`, `backup_to`/`restore_from`, and `vertebra._sqlite` stubs in `docs/` per [`../spec.md` §2](../spec.md) [^docs-db] [^docs-stubs] [^docs-changelog].

## Bounded Read Manifest

- `docs/usage/databases.md` — SQLite configuration, WAL, pipeline, and Arrow examples.
- `docs/reference/native-stubs.md` — TOC structure and `heading_level: 4` mkdocstrings blocks.
- `docs/changelog/index.md` — `Unreleased` changelog section.

## Contracts & Edge Cases

- Format install blocks with `=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"` tabs.
- Keep `docs/reference/native-stubs.md` within `<= 14` secondary TOC items and `< 120_000` HTML bytes using `heading_level: 4`.
- **Edge Case**: Avoid internal `frozen=True, gc=False` boilerplate in public `msgspec.Struct` code examples.

## Test Matrix

- `make docs` builds cleanly and passes `verify_docs_artifacts.py`.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] `make docs` passes with `vertebra._sqlite` and `vertebra-core[sqlite]` documented.
- [ ] Zero regressions in the chapter fast verification gate.

[^docs-db]: `docs/usage/databases.md` — Database drivers usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
