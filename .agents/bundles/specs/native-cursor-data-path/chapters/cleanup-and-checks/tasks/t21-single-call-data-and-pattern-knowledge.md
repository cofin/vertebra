---
type: Task
id: "native-cursor-data-path:cleanup:t21"
chapter_id: cleanup-and-checks
title: "Data and pattern knowledge for the single-call methods"
description: "Record the four single-call methods and the driver methods that use them in the database drivers chapter, and record the completed awaitable and the exact-type bind check in the Rust FFI patterns chapter."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:single:t27"
read_set:
  - ".agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md:1-38"
  - ".agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md:156-200"
  - ".agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md:1-22"
  - ".agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md:110-176"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:285-320"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md"
  - "src/rs/vertebra-core/src/py_async.rs"
  - "src/rs/vertebra-sqlite/src/value.rs::BoundParams"
  - "src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver"
files:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
tests: []
estimated_loc: 60
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: drivers-chapter
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and cloud dialers chapter"
  - id: ffi-chapter
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: "Rust FFI and GIL discipline patterns chapter"
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec: single-call methods, completed awaitables, and the bind fast path"
tags: [knowledge, database, pyo3, docs]
---

# t21 — Data and pattern knowledge for the single-call methods

## Objective & Scope Boundary

Someone reading the driver chapter learns that a native-driver query is one native call, which call it is, and what it returns. Someone writing an extension finds the completed awaitable and the exact-type bind check as patterns to follow.[^drivers-chapter][^ffi-chapter]

This task runs after `native-cursor-data-path:single:t27`, which closes `single-call-paths`.

In scope: the two knowledge chapters. Out of scope: code, benchmark numbers (t07), the SQLSpec cache-hit executor and ADR-006 (t22), and every other chapter.

## Bounded Read Manifest

- `database-drivers-and-dialers.md:1-38` for frontmatter and `verified`, and `156-200` for the truth block and the sections "Polymorphic Registry, `CommonDriverMixin` & Cloud Auth" and "Relational, Analytical & Cache Adapters".
- `ffi-and-gil-discipline.md:1-22` for frontmatter, and `110-176` for the truth block, the section on releasing the GIL, and the section on interned keys and the shared async module of `vertebra-core`.
- Root spec lines `285-320`: "Single-call methods (R16)", "Completed awaitables (R17)", and "Bind fast path (R19)".[^root-spec]
- The `single-call-paths` chapter spec, for the class and function names that chapter delivered.
- `src/rs/vertebra-core/src/py_async.rs` for the completed awaitable class as built. `src/rs/vertebra-sqlite/src/value.rs::BoundParams` for the bind check as built. `SyncDatabaseDriver` for the driver methods as built.

## Contracts & Edge Cases

Take every class, function, and method name from the code as it stands, not from this worksheet. Check each with `git grep -n "<name>" -- src`. A name the code does not have is not written.

`database-drivers-and-dialers.md`:

1. Truth block, bullet "Uniform Cursor Surface, Checkout Leases, RESP & Cloud-Auth Paths". Replace the clause that says `select`, `select_one`, `execute`, and `execute_with_description` call the cursor. The new clause states: on the SQLite, DuckDB, PostgreSQL, and Oracle native drivers each query method makes one native call, or awaits one awaitable, to one of the four single-call methods; `cursor()` stays for the DB-API module, SQLSpec, and callers that fetch in steps.
2. Section "Polymorphic Registry, `CommonDriverMixin` & Cloud Auth", bullet "Uniform Driver Surface & `CommonDriverMixin`". Add a table after the bullet list of that section:

   | Single-call method | Returns | Driver methods that call it |
   | --- | --- | --- |
   | `fetch_rows(sql, params=())` | `(rows, names)`; `([], ())` with no result set | `execute_with_description`, `execute_tuples`, and the `select` family when `schema_type` is set |
   | `fetch_dicts(sql, params=())` | One `dict` per row, keyed by column name | `select`, `select_one`, `select_one_or_none` without `schema_type` |
   | `fetch_value(sql, params=())` | First column of the first row; `NotFoundError` with no row | `select_value` |
   | `execute_dml(sql, params=())` | `(rowcount, lastrowid)` | `execute` |

   Confirm each row of the third column against the four drivers before writing it. Where a driver differs, write what the code does and name the driver.
3. After the table, three sentences: the methods exist on each client class of the four extensions and on the PostgreSQL and Oracle pool classes; a pool method checks a connection out, runs the statement, and returns the connection inside the extension; the methods run the statement through the runner the cursor uses and keep no cursor state.
4. State that MySQL, SQL Server, Spanner, and BigQuery native drivers run their query methods on the cursor.
5. State one Oracle limit of the single-call methods: out-bind values and session-context tracking come back only from `execute_dml`, which the driver's `execute` calls. The three fetch methods return rows only. A caller that needs out-binds from a statement that also returns rows uses the cursor.

`ffi-and-gil-discipline.md`:

1. The section whose heading starts "Intern Repeated Keys". Add one paragraph headed in bold `Completed awaitables`. It states: an async method whose result needs no I/O returns the completed awaitable class from the `py_async` module of `vertebra-core`; its `__await__` finishes at once with the stored value or raises the stored error; no Tokio task is spawned and the event loop is not re-entered; the stub return type stays `Awaitable[T]`; every async cursor `fetchone`, `fetchmany`, `fetchall`, and a `close` with nothing pending use it.
2. The section whose heading starts "Release the GIL Around Blocking Work". Add one bullet headed in bold `Exact-type bind check`. It states: parameter extraction tests the exact type (`tuple`, `list`, `dict`) before any protocol cast; an empty container returns the no-parameter value without allocating; subclasses and other sequences and mappings take the protocol path after the exact checks fail.
3. Truth block: add one bullet that names both patterns in one sentence. Keep the block at 40 lines or fewer.

Both files:

- Sources and footnotes match one to one. `ffi-and-gil-discipline.md` already has a source for `vertebra-core/src/py_async.rs`; cite it. Add a source only when a new footnote cites it, and give it a `resource` that exists.
- Add a `verified` entry with the task commit.
- Present tense. No workstation path, no internal tool name, and no benchmark number.
- Relative links use the depth the file already uses.

## Test Matrix

| Check | Expected |
| --- | --- |
| `fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml` in the drivers chapter | Each present |
| The completed awaitable class name in the patterns chapter | Present, and it matches a `#[pyclass]` name in `src/rs/vertebra-core/src/py_async.rs` |
| Truth block length in both files | 40 lines or fewer |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |

## Verification

```bash
grep -n "state:" .agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md | head -1
for name in fetch_rows fetch_dicts fetch_value execute_dml; do
  grep -c -- "$name" .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  git grep -c -- "fn $name" -- src/rs/vertebra-sqlite/src src/rs/vertebra-postgres/src | head -2
done
grep -n "Completed awaitables\|Exact-type bind check" .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
make review-check
```

The first command prints `state: completed`. Each `grep -c` prints `1` or more. The last `grep` prints two lines or more.

## Acceptance Criteria

- [ ] The drivers chapter names the four single-call methods, what each returns, and which driver methods call each, checked against the code.
- [ ] The patterns chapter records the completed awaitable and the exact-type bind check.
- [ ] Every symbol named in the new text exists in `src/`.
- [ ] `make review-check` passes and the prose lint reports no finding.

[^drivers-chapter]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`
[^ffi-chapter]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`
[^root-spec]: `.agents/bundles/specs/native-cursor-data-path/spec.md`
