---
type: Task
id: "native-cursor-data-path:cleanup:t09"
chapter_id: cleanup-and-checks
title: "Data knowledge chapters: leases and driver surface"
description: "Update the broker lease chapter and the drivers-and-dialers chapter for per-connection leases, the uniform cursor surface, and the Oracle relay switch."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:cleanup:t04"
read_set:
  - ".agents/bundles/knowledge/domains/data/broker-and-shm-leases.md"
  - ".agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md:160-180"
  - "src/py/vertebra/db/driver/_common.py::CommonDriverMixin"
files:
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
tests: []
estimated_loc: 50
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: broker-leases
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: "Broker and shared-memory lease chapter"
  - id: drivers-dialers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and dialers chapter"
tags: [knowledge, leases, database, docs]
---

# t09 — Data knowledge chapters: leases and driver surface

## Objective & Scope Boundary

Both chapters name symbols this spec audits. After this task they describe per-connection leases and the cursor as the uniform driver surface.[^broker-leases][^drivers-dialers]

The root spec does not list these two files. They are added because a search for the audited names finds them. Out of scope: code and every other chapter.

## Bounded Read Manifest

- `broker-and-shm-leases.md`: run `grep -n "_lookup_or_cache_statement\|_acquire_broker_lease\|_release_broker_lease\|_BrokerLeasePlan\|per-query\|per-statement\|per statement" <file>` and read ten lines around each hit, plus the truth block.
- `database-drivers-and-dialers.md:160-180` for the bullet "Uniform Driver Surface & `CommonDriverMixin`" at line 169, plus the truth block.
- `src/py/vertebra/db/driver/_common.py::CommonDriverMixin` as it stands after t04, for the helper names that remain.
- The Notes section of t04 for remaining lease call sites.

## Contracts & Edge Cases

`broker-and-shm-leases.md`:

1. State the lease contract: network pools take one broker lease when a connection is checked out and return it at release. Fairness between workers is decided at checkout. Embedded engines (SQLite, DuckDB) take no lease. State that a Rust guard owns the lease and returns it on drop, that acquire is best effort, and that a single-client driver holds its lease while the connection is open, idle or not.
2. Replace any description of a lease taken or a statement-cache lookup made per query.
3. Name only helpers that t04 recorded as kept. Remove `_BrokerLeasePlan` if t04 deleted it.
4. The SQLSpec proxy and provider lease paths are unchanged by this spec. Leave their text alone.

`database-drivers-and-dialers.md`:

1. In the uniform driver surface bullet, state that every SQL driver exposes `cursor()` returning the extension's PEP 249-shaped cursor and that `select`, `select_one`, `execute`, and `execute_with_description` call it.
2. Remove any method name that t04 or t05 deleted.
3. In the Oracle dialer text, add that `OracleConfig.enable_reset_relay=False` connects directly and starts no relay thread or local socket, and that the default is `True`.

Both files:

- Sources and footnotes match one to one, and every `resource` path exists.
- The truth block stays at 40 lines or fewer.
- Add a `verified` entry with the task commit.
- Present tense. No workstation path and no internal tool name.

## Test Matrix

| Check | Expected |
| --- | --- |
| Names deleted per t04 notes in the two files | No match |
| Text "per query" or "per statement" next to "lease" in `broker-and-shm-leases.md` | Only in a sentence stating that no such lease exists |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |

## Verification

```bash
git grep -nw -e _BrokerLeasePlan -e NATIVE_CURSOR_ENGINES -e query_with_columns -- .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
git grep -n "enable_reset_relay" -- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
make review-check
```

The first command prints only names that t04 recorded as kept. The second prints at least one line.

## Acceptance Criteria

- [ ] The lease chapter states the per-connection contract and the embedded-engine exemption.
- [ ] The driver chapter names `cursor()` and the relay switch.
- [ ] No deleted name remains in the two files.
- [ ] `make review-check` passes and the prose lint reports no finding.

[^broker-leases]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
[^drivers-dialers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`
