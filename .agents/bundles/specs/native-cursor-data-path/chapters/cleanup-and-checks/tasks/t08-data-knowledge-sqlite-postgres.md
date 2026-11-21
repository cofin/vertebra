---
type: Task
id: "native-cursor-data-path:cleanup:t08"
chapter_id: cleanup-and-checks
title: "Data knowledge chapters: SQLite and PostgreSQL"
description: "Update the SQLite and PostgreSQL knowledge chapters for lease scope, the location of bind planning, and the names this spec removed."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:cleanup:t04"
  - "native-cursor-data-path:cleanup:t05"
read_set:
  - ".agents/bundles/knowledge/domains/data/sqlite-limitations.md:1-61"
  - ".agents/bundles/knowledge/domains/data/postgres-limitations.md:1-70"
  - ".agents/bundles/knowledge/domains/data/postgres-native-driver.md:98-157"
files:
  - .agents/bundles/knowledge/domains/data/sqlite-limitations.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/domains/data/postgres-native-driver.md
tests: []
estimated_loc: 70
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlite-limitations
    resource: .agents/bundles/knowledge/domains/data/sqlite-limitations.md
    title: "SQLite native driver limitations"
  - id: postgres-native
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: "PostgreSQL native extension chapter"
tags: [knowledge, sqlite, postgres, docs]
---

# t08 — Data knowledge chapters: SQLite and PostgreSQL

## Objective & Scope Boundary

The three chapters describe the tree after this spec. A reader learns where leases are taken and where bind planning lives, and meets no deleted name.[^sqlite-limitations][^postgres-native]

In scope: the three files in `files:`. Out of scope: code, other knowledge chapters, and benchmark numbers (t07 owns those).

## Bounded Read Manifest

- `sqlite-limitations.md` in full (61 lines).
- `postgres-limitations.md` in full (70 lines).
- `postgres-native-driver.md:98-157`, with attention to section 2 "Codecs, Extended Query, Pools & Pipelines" and section 4 "Python Driver Layer".
- The Notes sections of t04 and t05 for the final keep or delete result per name.

## Contracts & Edge Cases

`sqlite-limitations.md`:

1. State that the SQLite query path takes no broker lease and does not touch the shared-memory statement cache. Remove any sentence that says it does.
2. State that statement classification happens in the extension and that `cursor.description is None` marks a statement without a result set.
3. Remove or reword every mention of `classify_sql_statement` on the Python side and of the broker-less bypass branch.
4. Add one `sources` entry with a matching footnote. Its resource is the path `src/rs/vertebra-sqlite/src/py_client.rs`, then `::`, then the class name `SyncSQLiteCursor`.

`postgres-limitations.md`:

1. State that the broker lease is taken when a connection is checked out of the pool and returned at release, and that a worker holds the lease for as long as it holds the connection.
2. State that named-parameter rewriting still works through the native driver and is done in Rust.

`postgres-native-driver.md`:

1. Section 2: add that placeholder rewriting and the bind plan are cached per SQL text in the Rust statement cache, and that the pooled one-shot path backs `cursor.execute` in autocommit.
2. Section 4: rewrite the description of `binds.py`, `types.py`, and `project_query_rows` to match what remains after `network-engines`. If a module was deleted, remove it from the section heading and text, and remove its `sources` entry and footnote together.
3. Add the cursor classes `SyncPostgresCursor` and `AsyncPostgresCursor` with their defining file under `src/rs/vertebra-postgres/src/` to `sources`, with a footnote.

All three files:

- Every `sources[]` id keeps a matching `[^id]` footnote, and every footnote keeps a `sources[]` entry.
- Every `resource` path exists. Check each with `test -e`.
- The truth block stays at 40 lines or fewer.
- Add a `verified` entry with the task commit.
- Name no workstation path and no internal tool.
- Write in present tense. Do not describe what the code used to do.

## Test Matrix

| Check | Expected |
| --- | --- |
| Must-be-gone names in the three files | No match |
| Names deleted per t04 and t05 notes in the three files | No match |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |
| Every `sources[].resource` path | Exists |

## Verification

```bash
cd .agents/bundles/knowledge/domains/data
for name in classify_sql_statement _lookup_or_cache_statement project_query_rows is_row_returning_sql NATIVE_CURSOR_ENGINES; do
  git grep -nw -- "$name" -- sqlite-limitations.md postgres-limitations.md postgres-native-driver.md
done
cd -
make review-check
```

A name that t04 or t05 recorded as kept may still match; every other name prints nothing.

## Acceptance Criteria

- [x] The three chapters state the lease scope and the bind planning location as specified.
- [x] No deleted name remains in the three files.
- [x] Sources and footnotes match one to one and every path exists.
- [x] `make review-check` passes and the prose lint reports no finding.

## Notes & Discoveries

- Updated `sqlite-limitations.md`, `postgres-limitations.md`, and `postgres-native-driver.md` in present tense to document SQLite's lease-free, statement-cache-free native cursor path (`src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor`), PostgreSQL's connection-checkout lease scope and Rust named-parameter rewriting (`bind_plan.rs`, `vertebra._postgres.compile_binds`), and `SyncPostgresCursor` / `AsyncPostgresCursor` in `src/rs/vertebra-postgres/src/cursor.rs`.
- Removed stale references to `_lookup_or_cache_statement`, `NATIVE_CURSOR_ENGINES`, `project_query_rows`, and `is_read_only_sql` (reworded to lexical read-only check) across the three files.
- `verified[0].commit` on all three chapters uses current `HEAD` (`06e99fbf`) because this job leaves Git uncommitted for the end-of-chapter commit.
- `make lint` was not run against the whole polyglot tree because parallel workers hold uncommitted edits across Python/Rust/docs files; documentation verification (`bb design check`, `make review-check`, source/footnote/path validation, and `git grep`) all passed with exit code `0`.

[^sqlite-limitations]: `.agents/bundles/knowledge/domains/data/sqlite-limitations.md`
[^postgres-native]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md`
