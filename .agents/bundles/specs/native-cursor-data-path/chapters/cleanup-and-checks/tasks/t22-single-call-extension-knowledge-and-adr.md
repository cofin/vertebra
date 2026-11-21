---
type: Task
id: "native-cursor-data-path:cleanup:t22"
chapter_id: cleanup-and-checks
title: "Extension knowledge and ADR-006 for the single-call path"
description: "Record the SQLSpec cache-hit executor in the extensions chapter and state in ADR-006 that the cursor and the single-call methods share one statement runner in each extension."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:single:t27"
read_set:
  - ".agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md:127-170"
  - ".agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md:194-240"
  - ".agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md:1-77"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:306-320"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver"
  - "src/rs/vertebra-sqlite/src/py_client.rs"
files:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md
tests: []
estimated_loc: 50
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: extensions-chapter
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: "SQLSpec, Advanced Alchemy, and SQLAlchemy dialect extensions"
  - id: adr-006
    resource: .agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md
    title: "ADR-006: native cursor and connection-scoped leases"
  - id: sqlspec-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
    title: "SQLSpec adapter base that registers the cache-hit executor"
tags: [knowledge, sqlspec, decisions, docs]
---

# t22 — Extension knowledge and ADR-006 for the single-call path

## Objective & Scope Boundary

The extensions chapter says how a cached SQLSpec statement runs: through a registered cache-hit executor that makes one driver call. ADR-006 says the cursor and the single-call methods are two entry points to one statement runner, so the decision still covers every query path.[^extensions-chapter][^adr-006]

This task runs after `native-cursor-data-path:single:t27`, which closes `single-call-paths`.

In scope: the two knowledge files. Out of scope: code, the driver and pattern chapters (t21), `sqlspec-limitations.md`, `sqlalchemy-limitations.md`, the decision index, and benchmark numbers.

## Bounded Read Manifest

- `sqlspec-alchemy-and-dialects.md:127-170` for the truth block and section 1, and `194-240` for section 4 and the footnotes.
- ADR-006 in full (77 lines).
- Root spec lines `306-320`: "SQLSpec cache-hit executor (R18)" and "Bind fast path (R19) and cursor factory (R20)".
- The `single-call-paths` chapter spec, for the names that chapter delivered.
- `BaseSyncDriver` and `BaseAsyncDriver` as built, for the executor attribute names and the two callables in its state.[^sqlspec-base]
- `src/rs/vertebra-sqlite/src/py_client.rs` for the name of the shared statement runner in the reference extension.

## Contracts & Edge Cases

Take every attribute, class, and function name from the code as it stands. Check each with `git grep -n "<name>" -- src`. A name the code does not have is not written.

`sqlspec-alchemy-and-dialects.md`:

1. Section 1, after the bullet that names the native cursor as the single seam. Add one bullet headed in bold `Cache-hit executor`. It states:
   - Both adapter bases register a cache-hit executor and its state with the upstream driver when they are constructed.
   - The state holds two callables chosen once: a row fetch with the signature of `fetch_rows` and a statement run with the signature of `execute_dml`.
   - For SQLite and DuckDB the callables are the client's native methods. For the network engines they are the native driver's methods, which route to the pinned session or the pool.
   - The executor reads whether the cached statement returns rows, makes one call, and builds the result once with the upstream constructors.
   - A native error is mapped by the adapter's existing mapper and raised with the native error as its cause.
   - `with_cursor`, `collect_rows`, and `resolve_rowcount` stay for the paths upstream does not route through the executor.
2. Section 4. Add one sentence: the DB-API connection hands out cursors through a callable bound to the driver's native cursor factory, rebuilt when `autocommit` changes, and a driver without `cursor()` is rejected with `InterfaceError` when the connection is built.
3. Truth block: change the bullet about the native cursor seam so it also says a cached SQLSpec statement makes one driver call through the cache-hit executor. Keep the block at 40 lines or fewer.

`adr-006-native-cursor-and-connection-scoped-leases.md`:

1. Truth block: add one bullet. `The cursor and the single-call methods (fetch_rows, fetch_dicts, fetch_value, execute_dml) share one statement runner in each extension; neither is a second execution path.` Cite the existing `sqlite-cursor` footnote.
2. `## Decision`: add one paragraph after the first. It states: each extension has one statement runner; the cursor stores the runner's outcome as cursor state; the single-call methods return the same outcome directly and store nothing; native-driver query methods call the single-call methods, and the DB-API module and SQLSpec hook paths call the cursor; bind planning, statement caching, and error conversion happen in the runner, so they are the same on both entry points.
3. `## Alternatives Rejected`: add one row. Alternative: `Separate fast-path functions beside the cursor, each with its own bind and decode code`. Trade-off: `Two implementations of statement execution per extension that drift apart; a fix to one path misses the other.`
4. `## Consequences`: add one bullet: a change to how a statement runs is made once, in the runner, and both the cursor tests and the single-call tests cover it.
5. Leave the title, the status, and the three existing decisions as they are. Do not add a second decision record.

Both files:

- Sources and footnotes match one to one. Add no new source unless a new footnote cites it.
- Add a `verified` entry with the task commit.
- Present tense. No workstation path, no internal tool name, and no benchmark number.

## Test Matrix

| Check | Expected |
| --- | --- |
| `Cache-hit executor` in the extensions chapter | Present |
| The executor attribute name written in the chapter | Matches a name in `src/py/vertebra/extensions/sqlspec/_driver.py` |
| `statement runner` in ADR-006 | Present in the truth block, the decision, and the consequences |
| `.agents/bundles/knowledge/decisions/index.md` | No diff |
| Truth block length in both files | 40 lines or fewer |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |

## Verification

```bash
grep -n "state:" .agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md | head -1
grep -n "Cache-hit executor" .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
grep -c "statement runner" .agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md
git diff --stat -- .agents/bundles/knowledge/decisions/index.md
make review-check
```

The first command prints `state: completed`. The second prints one line or more. The third prints `3` or more. The fourth prints nothing.

## Acceptance Criteria

- [ ] The extensions chapter describes the cache-hit executor as specified, with names taken from the code.
- [ ] ADR-006 states that the cursor and the single-call methods share one statement runner, in its truth block, decision, rejected alternatives, and consequences.
- [ ] Sources and footnotes match one to one and every symbol exists.
- [ ] `make review-check` passes and the prose lint reports no finding.

[^extensions-chapter]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`
[^adr-006]: `.agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md`
[^sqlspec-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver`
