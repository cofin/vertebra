---
type: Task
id: "native-cursor-data-path:cleanup:t12"
chapter_id: cleanup-and-checks
title: "ADR-006: native cursor and connection-scoped leases"
description: "Record the decisions to put the PEP 249-shaped cursor in each PyO3 extension, to scope broker leases to connection checkout, and to keep one exception hierarchy with a stated deviation from PEP 249."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:cleanup:t04"
read_set:
  - ".agents/bundles/knowledge/decisions/adr-005-persistent-object-store-ttl-envelopes.md:1-60"
  - ".agents/bundles/knowledge/decisions/index.md"
  - ".agents/bundles/knowledge/index.md:103-110"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-200"
files:
  - .agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md
  - .agents/bundles/knowledge/decisions/index.md
  - .agents/bundles/knowledge/index.md
tests: []
estimated_loc: 90
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: adr-005
    resource: .agents/bundles/knowledge/decisions/adr-005-persistent-object-store-ttl-envelopes.md
    title: "Most recent decision record, used as the format reference"
  - id: decisions-index
    resource: .agents/bundles/knowledge/decisions/index.md
    title: "Decision record index"
tags: [knowledge, adr, decisions, docs]
---

# t12 — ADR-006: native cursor and connection-scoped leases

## Objective & Scope Boundary

The two hard-to-reverse choices in this spec are written down where the next person will look: why the cursor lives in Rust, and why leases follow connections.[^decisions-index]

In scope: one new file and two index entries. Out of scope: every other knowledge chapter and all code.

## Bounded Read Manifest

- `adr-005-persistent-object-store-ttl-envelopes.md:1-60` for frontmatter keys, the truth block, and the four H2 headings.[^adr-005]
- `decisions/index.md` in full.
- `knowledge/index.md:103-110` for the decisions list.
- Root spec lines `150-200` for the cursor, error, and lease contracts.

## Contracts & Edge Cases

New file `.agents/bundles/knowledge/decisions/adr-006-native-cursor-and-connection-scoped-leases.md`.

Frontmatter:

```yaml
type: Decision
title: 'ADR-006: Native Cursor and Connection-Scoped Leases'
description: "Each PyO3 SQL extension owns a PEP 249-shaped cursor, and broker leases are taken per connection checkout."
status: stable
tags:
- decisions
- adr
- database
- dbapi
- leases
sources:
- id: sqlite-cursor
  resource: src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor
  title: Reference native cursor implementation
- id: dbapi-module
  resource: src/py/vertebra/db/dbapi/_sync.py
  title: DB-API exception classes, module globals, and connection
- id: sqlspec-base
  resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
  title: SQLSpec adapter base yielding the native cursor
- id: driver-common
  resource: src/py/vertebra/db/driver/_common.py::CommonDriverMixin
  title: Lease acquisition at connection checkout
verified:
- by: agent:flow-executor
  at: '<UTC time of the task commit>'
  commit: <task commit sha>
```

Before writing, confirm each `resource` symbol exists with `git grep -n "struct SyncSQLiteCursor\|class BaseSyncDriver\|class CommonDriverMixin"`. If `network-engines` moved lease acquisition out of `CommonDriverMixin`, point `driver-common` at the function t04 recorded as the checkout call site.

Body, in this order:

1. `# ADR-006: Native Cursor and Connection-Scoped Leases`
2. A truth block of five bullets, each with a footnote:
   - Each of the eight SQL extensions exposes a sync and an async cursor with the PEP 249 shape; rows are tuples built once in Rust.
   - `description`, `rowcount`, and `lastrowid` come from the engine reply; Python does not scan SQL text.
   - There is one exception hierarchy. The DB-API module binds its PEP 249 names to the `vertebra._vertebra` classes, and each extension raises them through its existing error conversion.
   - Deviation from PEP 249: `InterfaceError` is a subclass of `DatabaseError`, and `Error` is the same class as `DatabaseError`.
   - Network pools take one broker lease per connection checkout; embedded engines take none.
3. `## Context`: wrappers validate, convert, and cache; the Python layers between them and the extension repeated that work on every query; on embedded engines one interpreted frame costs about as much as the native call. State the cost in words. Do not copy benchmark figures here; link to the driver limitations chapter for measured results.
4. `## Decision`: the three decisions, each in two or three sentences. The third is the single exception hierarchy. State the deviation from PEP 249 in full, and state that specific classes such as `QueryCancelledError` and `SerializationFailure` reach DB-API callers unchanged.
5. `## Alternatives Rejected`:
   - A slim Python cursor class over the native `fetch_all`. Rejected because it keeps one interpreted frame per `execute` and per `fetchall`.
   - Keeping the Python helpers as fallbacks. Rejected because two paths must then be kept correct.
   - Per-statement leases with a cheaper lookup. Rejected because the cost stays on every statement and embedded engines do not need a lease.
   - Separate DB-API exception classes with a per-call translator, or a second error table in each extension. Rejected because both add work to every error and flatten specific classes to their PEP 249 parent.
6. `## Consequences`:
   - One more public class pair per extension in the stubs and in stubtest.
   - A worker holds its lease for as long as it holds the connection; fairness is decided at checkout.
   - The lease is owned by a Rust guard on the checked-out connection or the client, and the guard's drop returns it. Close, an error, task cancellation, and a driver dropped without `close()` all release it.
   - Acquire is best effort: it waits up to the broker timeout, and a refused or timed-out acquire lets the connection proceed without a lease.
   - A single-client driver (MySQL, SQL Server, unpooled Oracle) holds its lease for the life of the client. An idle open connection occupies a broker slot.
   - The DB-API connection keeps begin-on-execute through the cursor's lazy-begin mode, so the transaction opens inside the extension.
   - Native-driver conveniences with no wrapper above them (PostgreSQL named parameters, Oracle out-binds) are implemented in Rust.
   - Per-statement work the wire needs is cached per SQL text in the extension.
   - The Oracle relay stays on by default and has a switch.
   - `except dbapi.DatabaseError` also catches interface errors, and `dbapi.Error` cannot be told apart from `dbapi.DatabaseError`. Code that needs to separate them catches `InterfaceError` first.
7. Footnotes for the four source ids.

Index edits:

- `decisions/index.md`: append one bullet in the style of the ADR-005 entry: `- **[ADR-006: Native Cursor and Connection-Scoped Leases](adr-006-native-cursor-and-connection-scoped-leases.md)** — Why each SQL extension owns a PEP 249-shaped cursor and why broker leases follow connection checkout.` Remove the blank line that separates ADR-005 from the list above it so the list is contiguous.
- `knowledge/index.md`: append `  - [ADR-006: Native Cursor and Connection-Scoped Leases](decisions/adr-006-native-cursor-and-connection-scoped-leases.md)` after the ADR-005 line.

Edge cases:

- No standalone `---` line in the body.
- No workstation path, no internal tool name, and no comparator name.
- Present tense throughout.

## Test Matrix

| Check | Expected |
| --- | --- |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |
| Both index files link to the new file | One match each |
| Every `sources[].resource` path | Exists |

## Verification

```bash
grep -c "adr-006-native-cursor-and-connection-scoped-leases.md" .agents/bundles/knowledge/decisions/index.md .agents/bundles/knowledge/index.md
test -e src/rs/vertebra-sqlite/src/py_client.rs && test -e src/py/vertebra/db/dbapi/_sync.py && test -e src/py/vertebra/extensions/sqlspec/_driver.py && test -e src/py/vertebra/db/driver/_common.py
make review-check
```

## Acceptance Criteria

- [ ] ADR-006 exists with the frontmatter, truth block, and four sections above.
- [ ] Both indexes link to it.
- [ ] Every cited symbol exists.
- [ ] `make review-check` passes and the prose lint reports no finding.

[^adr-005]: `.agents/bundles/knowledge/decisions/adr-005-persistent-object-store-ttl-envelopes.md`
[^decisions-index]: `.agents/bundles/knowledge/decisions/index.md`
