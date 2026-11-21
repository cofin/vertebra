---
type: Task
id: "native-cursor-data-path:cleanup:t10"
chapter_id: cleanup-and-checks
title: "Extension knowledge chapters"
description: "Update the SQLSpec and SQLAlchemy knowledge chapters to describe the native cursor as the single seam under both wrappers and to drop the deleted layers."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:cleanup:t04"
read_set:
  - ".agents/bundles/knowledge/domains/extensions/sqlspec-limitations.md:1-116"
  - ".agents/bundles/knowledge/domains/extensions/sqlalchemy-limitations.md:1-96"
  - ".agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md:127-164"
  - ".agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md:189-235"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver"
files:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-limitations.md
  - .agents/bundles/knowledge/domains/extensions/sqlalchemy-limitations.md
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
tests: []
estimated_loc: 80
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlspec-limitations
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-limitations.md
    title: "SQLSpec adapter, proxy, and store limitations"
  - id: sqlalchemy-limitations
    resource: .agents/bundles/knowledge/domains/extensions/sqlalchemy-limitations.md
    title: "SQLAlchemy dialects, DB-API, and Alembic limitations"
  - id: extensions-chapter
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: "SQLSpec, Advanced Alchemy, and SQLAlchemy dialect extensions"
tags: [knowledge, sqlspec, sqlalchemy, docs]
---

# t10 — Extension knowledge chapters

## Objective & Scope Boundary

The three extension chapters describe one seam: the wrapper validates and caches, then calls the engine's native cursor. No chapter describes the deleted Python layers.[^sqlspec-limitations][^sqlalchemy-limitations][^extensions-chapter]

Out of scope: code, the SQLSpec proxy and provider text, Advanced Alchemy, storage, and caching sections.

## Bounded Read Manifest

- `sqlspec-limitations.md` in full (116 lines).
- `sqlalchemy-limitations.md` in full (96 lines).
- `sqlspec-alchemy-and-dialects.md:127-164` for the truth block and section 1, and `189-235` for section 4 on the `+vtb` dialects and the footnotes.
- `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver` as it stands after `network-engines`, for the method names that remain.
- The Notes section of t04.

## Contracts & Edge Cases

`sqlspec-limitations.md`:

1. State that `with_cursor` yields the engine's native cursor, so a SQLSpec cache hit performs one native `execute`.
2. State that the adapter passes SQL and parameters through unchanged, and that value conversion SQLSpec does not cover lives in a type converter on the statement config.
3. Remove limitations that existed only because of the deleted layers: typed-parameter unwrapping, the three-shape method probing, per-row `dict` building, and the per-call exception handler object.
4. Keep every limitation about the proxy, the provider, and the stores.

`sqlalchemy-limitations.md`:

1. State that the DB-API module supplies module globals, type objects, constructors, `connect`, and transaction control, and that `connection.cursor()` returns the native cursor.
2. State that autocommit follows the connection's transaction state and that no Python code scans SQL text.
3. State that the DB-API exception names are the `vertebra._vertebra` classes and that the extension raises them with no translation. State the deviation from PEP 249: `InterfaceError` is a subclass of `DatabaseError`, and `Error` is `DatabaseError`.
4. Remove limitations tied to the Python tokenizer, row re-tupling, or the error translator.

`sqlspec-alchemy-and-dialects.md`:

1. Section 1: add one bullet that names the native cursor as the single seam under SQLSpec and SQLAlchemy for the eight SQL engines.
2. Section 4: describe the sync dialects and `pool.py` as unchanged, reaching the native cursor through the DB-API connection, and `async_adapter.py` as the one changed file, which no longer calls `cursor.__aenter__()`.
3. Update truth-block bullets that describe the deleted layers. Keep the block at 40 lines or fewer.

All three files:

- Remove a `sources` entry and its footnote together when its symbol no longer exists. Repoint to the nearest surviving symbol when the surrounding claim still holds.
- Every `resource` path and symbol exists. Check symbols with `git grep -n "class <Name>\|def <name>"`.
- Add a `verified` entry with the task commit.
- Present tense. No workstation path, no internal tool name, and no benchmark number.

## Test Matrix

| Check | Expected |
| --- | --- |
| Must-be-gone names in the three files | No match |
| `SyncDBAPICursor`, `AsyncDBAPICursor`, `execute_tuples` | Match only when t04 or t05 recorded the name as kept |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |

## Verification

```bash
cd .agents/bundles/knowledge/domains/extensions
for name in is_row_returning_sql is_read_only_sql is_insert_sql coerce_dbapi_rows_and_desc _ErrorTranslator \
  unwrap_typed_parameters _unwrap_typed_parameters _normalize_bound_params _compile_for_native \
  _fetch_rows_with_columns _translate_native_error _translate_errors NATIVE_CURSOR_ENGINES; do
  git grep -nw -- "$name" -- sqlspec-limitations.md sqlalchemy-limitations.md sqlspec-alchemy-and-dialects.md
done
cd -
make review-check
```

The loop prints nothing.

## Acceptance Criteria

- [ ] Each chapter states the cursor seam as specified.
- [ ] No must-be-gone name remains in the three files.
- [ ] Sources and footnotes match one to one and every symbol exists.
- [ ] `make review-check` passes and the prose lint reports no finding.

[^sqlspec-limitations]: `.agents/bundles/knowledge/domains/extensions/sqlspec-limitations.md`
[^sqlalchemy-limitations]: `.agents/bundles/knowledge/domains/extensions/sqlalchemy-limitations.md`
[^extensions-chapter]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`
