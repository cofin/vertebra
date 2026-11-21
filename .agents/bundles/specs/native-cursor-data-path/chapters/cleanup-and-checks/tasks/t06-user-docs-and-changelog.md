---
type: Task
id: "native-cursor-data-path:cleanup:t06"
chapter_id: cleanup-and-checks
title: "User docs and changelog for the cursor path"
description: "Update the database user guide and the changelog so they describe the native cursor and the Oracle relay switch and name no removed symbol."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:cleanup:t04"
  - "native-cursor-data-path:cleanup:t05"
read_set:
  - "docs/usage/databases.md:85-125"
  - "docs/usage/databases.md:262-300"
  - "docs/changelog/index.md:1-45"
  - "docs/reference/db.md:115-122"
files:
  - docs/usage/databases.md
  - docs/changelog/index.md
tests: []
estimated_loc: 60
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: user-db-docs
    resource: docs/usage/databases.md
    title: "User guide for native drivers and the DB-API module"
  - id: changelog
    resource: docs/changelog/index.md
    title: "Changelog with the Unreleased section"
tags: [docs, database, changelog]
---

# t06 — User docs and changelog for the cursor path

## Objective & Scope Boundary

A reader of the database guide sees the surface that exists after this spec: `client.cursor()`, the PEP 249 members, and `OracleConfig.enable_reset_relay`. No page names a deleted symbol.[^user-db-docs]

In scope: `docs/usage/databases.md` and `docs/changelog/index.md`. `docs/reference/db.md` renders `OracleConfig` from its docstring and needs no edit; confirm the rendered page lists the new field. Out of scope: knowledge chapters and docstrings.

## Bounded Read Manifest

- `docs/usage/databases.md:85-125` for the driver method list (line 91) and the DB-API paragraph (line 118).
- `docs/usage/databases.md:262-300` for the Oracle section.
- `docs/changelog/index.md:1-45` for the Unreleased section; line 18 names `is_row_returning_sql`.[^changelog]
- `docs/reference/db.md:115-122` for the `OracleConfig` directive.
- The Notes sections of t04 and t05 for the final keep or delete result per name.

## Contracts & Edge Cases

`docs/usage/databases.md`:

1. Driver method list near line 91: list only methods that still exist per the t04 and t05 notes. Add one bullet titled **Native cursor** that states `driver.cursor()` returns a PEP 249-shaped cursor with `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, `description`, `rowcount`, `lastrowid`, `arraysize`, and `close`, that rows are tuples, and that the async driver returns awaitable forms of the methods.
2. DB-API paragraph near line 118: name the classes that still exist. State that `connection.cursor()` returns the engine's native cursor and that `description is None` means the statement returned no rows. Remove `SyncDBAPICursor` and `AsyncDBAPICursor` if t04 deleted them. State that the DB-API exception names are the `vertebra.db` driver exception classes, that specific classes such as `QueryCancelledError` reach DB-API callers unchanged, and that `InterfaceError` is a subclass of `DatabaseError`, which differs from PEP 249.
3. Oracle section near line 273: add one bullet titled **Reset relay** stating that `OracleConfig(enable_reset_relay=False)` connects to the resolved address directly, that the default is `True`, and that the relay provides the break fallback path.

`docs/changelog/index.md`, Unreleased section:

1. In the existing DB-API entry (line 18), remove `is_row_returning_sql` from the name list.
2. Under `### Added`, append: native PEP 249-shaped cursors on the eight SQL extensions through `client.cursor()`, and `OracleConfig.enable_reset_relay`.
3. Under `### Changed` (create the heading after `### Added` if it is absent), append: SQLSpec, SQLAlchemy, and the DB-API module run statements through the native cursor; broker leases are taken per connection checkout; embedded engines take none; the DB-API exception names are the native driver classes, so `InterfaceError` is a subclass of `DatabaseError` and `Error` is `DatabaseError`; the bundled DuckDB engine uses jemalloc on 64-bit glibc Linux.
4. Under `### Removed` (create after `### Changed` if absent), append one entry listing the removed public names from the t04 and t05 notes. Private names are not listed.

Edge cases:

- State no benchmark number in user docs.
- Keep each changelog entry to one bullet in the style of its neighbors.
- Name no workstation path and no internal tool.
- A removed name may stay in a released changelog section. The Unreleased section is the only one edited.

## Test Matrix

| Check | Command | Expected |
| --- | --- | --- |
| No removed name in user docs | The `git grep` loop in Verification | No output |
| Docs build | `make docs` | Exit `0` |
| Relay field rendered | `grep -c enable_reset_relay site/reference/db/index.html` | `1` or more |
| Prose lint | The planning toolkit's prose lint on `docs/usage/databases.md` | No finding |

## Verification

```bash
for name in is_row_returning_sql is_read_only_sql is_insert_sql coerce_dbapi_rows_and_desc \
  unwrap_typed_parameters NATIVE_CURSOR_ENGINES _ErrorTranslator; do
  git grep -nw -- "$name" -- docs ':!docs/changelog'
done
git grep -nw -- is_row_returning_sql -- docs/changelog/index.md
make docs
grep -c enable_reset_relay site/reference/db/index.html
```

The first two commands print nothing.

## Acceptance Criteria

- [x] `docs/usage/databases.md` describes the cursor, the DB-API cursor source, and the relay switch.
- [x] Every name deleted by t04 or t05 is absent from `docs/` outside released changelog sections.
- [x] The changelog has Added, Changed, and Removed entries for this spec.
- [ ] `make docs` passes and the reference page shows `enable_reset_relay`.

## Notes & Discoveries

- Updated `docs/usage/databases.md`:
  - Added the **Native cursor** bullet under `Unified Driver Contract & provide_driver` describing `driver.cursor()`, the PEP 249 members, tuple rows, and async awaitable forms.
  - Updated the `vertebra.db.dbapi` paragraph to list only existing classes (`SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPIConnection`, `AsyncDBAPIModule`), document that `connection.cursor()` returns the native cursor and `description is None` means no rows, and document that DB-API exception names are the `vertebra.db` driver classes (`QueryCancelledError` reaches callers unchanged; `InterfaceError` subclasses `DatabaseError`).
  - Added the **Reset relay** bullet under the Oracle section documenting `OracleConfig(enable_reset_relay=False)`, the `True` default, and the break fallback path.
- Updated `docs/changelog/index.md` (`Unreleased` section only):
  - Removed `is_row_returning_sql` from the existing DB-API entry under `### Added` (omitted from `### Removed` because it was never in a published release and verification checks for zero occurrences in `docs/changelog/index.md`).
  - Appended `### Added`, `### Changed`, and `### Removed` bullets matching t04 and t05 public symbol removals.
- Verification & caveats:
  - Both `git grep` checks printed zero output across `docs/` (excluding released changelog) and `docs/changelog/index.md`.
  - `bb design check docs/usage/databases.md docs/changelog/index.md` returned `No findings.`.
  - `UV_NO_SYNC=1 UV_CACHE_DIR=/tmp/uv-cache make docs` passed `tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts` and `tools.docs.generate_cli_docs --check`, then exited because `zensical` is not installed in `.venv` (`--group doc` is not synced in this offline environment). Confirmed `enable_reset_relay` is present in `OracleConfig.__struct_fields__` and `docs/reference/db.md` renders `vertebra.db.adapters.oracle.OracleConfig`; run `make docs` and `grep -c enable_reset_relay site/reference/db/index.html` at the end-of-chapter check once the `doc` group is available.
  - `make lint` was not run because only Markdown files were modified and sibling chapter tasks are running concurrently in the workspace.

[^user-db-docs]: `docs/usage/databases.md`
[^changelog]: `docs/changelog/index.md`
