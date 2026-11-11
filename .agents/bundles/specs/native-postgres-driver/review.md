---
type: Review
flow_id: native-postgres-driver
title: Native PostgreSQL planning review
description: Adjudication and validation of the complete-driver implementation plan.
status: stable
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: Shared PostgreSQL wire frame helpers in vertebra-db
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python PostgreSQL driver target cutover seam
tags:
  - postgres
  - planning
  - driver
---

# Planning review

<!-- truth: start -->
- **Adjudicated Plan Baseline**: All 22 planning review findings are adopted across `spec.md`, `contracts.md`, and `t01`–`t102` at `state: planned`, `plan_revision: 2`, and `plan_commit: 9af810f5` [^pg-core-rs] [^pg-driver-py].
- **Scope of Evidence**: Planning review validates contracts, dependency order, and worksheet completeness; runtime, live-service, wheel, and benchmark evidence are produced during `/flow:implement native-postgres-driver`.
<!-- truth: end -->

This review covers the specification and worksheets. Runtime, wheel, live-service
and performance acceptance remain pending until implementation. The complete
driver is delivered in one PR; worksheets define individual local commits.

## Adjudication

All findings below are ADOPT. No product decision remains escalated.

| Finding | Correction and verification contract |
| --- | --- |
| Cold statement and backend lifetime | Resolve required parameter metadata before Parse; execute cold text results before optional enrichment; retain completed results on later failure and never replay SQL. Named preflight pins or re-Parses. |
| SCRAM downgrade detection | Prefer uses GS2 y when binding is usable but PLUS is absent; actual wire assertions verify downgrade rejection. |
| Pipeline depended on future driver helpers | Early native pipeline takes explicit preparation/release callbacks; later adapter supplies its compiler. |
| Incomplete public value records | Contracts freeze units, discriminants, array bounds, vector/bit carriers, JSON null and nested conversion. |
| Static login token expiry | Require supplied real expiry and declared scopes; reject unusable credentials before transmission. |
| Feature-gated cancellation test could run empty | Register the target with required pyo3 features and verify named behavioral tests actually execute. |
| Pool constructor inventory | Enumerate cache, pooler, limits and timeout options with canonical configuration identity tests. |
| Pipeline could commit before context exit | Validate every operation before frames; reject transaction controls and raw same-scope API escapes; caught errors retain rollback disposition. |
| AlloyDB token expiry check was unreachable | Split verified TLS from metadata exchange; check scoped login material between those stages and test held-TLS expiry. |
| Missing production event hook owner | Specify dedicated-client hooks, structural protocols, uniform async publication and bounded registry cleanup on both driver facades. |
| Durable checkpoint diverged from reconnect state | Fence yields while saving/acknowledging; reconcile uncertain outcomes from the durable LSN and event counter together. |
| pgvector version was not pinned | Own fixtures use verified versioned image digests rather than floating built-in tags. |
| Local services contradicted TLS default | Explicitly disable TLS for owned local fixtures; retain separate certificate and identity acceptance. |
| Session-dependent bind lexing | Reject ambiguous ordinary backslash-quote forms during emulation; require E strings and respect numeric-token identifier boundaries. |
| Numeric passthrough contradicted pre-I/O coverage validation | Pass numeric-only SQL and ordered values unchanged; validate sequence shape/resource limits locally and leave SQL syntax and parameter coverage to the server. |
| Benchmark acceptance was unresolved | Freeze reference versions, capture schema, owned service lifecycle, sample counts and numerical comparison thresholds. |
| DSN defaults were unresolved | Freeze environment/OS precedence, explicit-empty behavior, strict percent decoding and adapter TLS defaults. |
| Python cutover broke the legacy async driver | Implement permanent private modules, switch both public exports atomically, then delete characterized legacy classes before conformance. |
| Command timeout forwarding lacked a native owner | Add one optional native command I/O deadline and bounded cancellation retirement; forward it per command and explicitly bound notification publication. |
| Cancellation acceptance conflated caller outcomes | Explicit cancel verifies actual server QueryCancelledError/57014; task cancellation verifies asyncio.CancelledError plus independent native cleanup and physical retirement. |
| Unresolved temporal display text was unsafe to rebind | Preserve output text/context; reject typed unresolved binds before submission. Input abbreviation configuration is independent of output settings. |
| Stale IDs and review-only prerequisites | Renumber the dependency graph, regenerate requirement/checklist links and remove staging notes from normative worksheets. |

## Review and validation evidence

- Core correctness review: three bounded rounds; all six original Important findings, two second-round Important findings and associated reference/signature issues are resolved in the normalized worksheets.
- Data and acceptance correctness review: two rounds; all seven original Important findings and final lexer/activation-reference corrections are resolved.
- Fresh quality review: cancellation caller outcomes, strict static-token construction and researched cross-extension ownership prose are reconciled with the public contracts.
- Structural validation passes: 102 pending worksheets, `state: planned` anchors (`plan_revision: 2`, `plan_commit: 9af810f5`), a serial dependency graph, 27 covered requirements, seven phase checkpoints, matching checklist/files, resolved document references and 103 shell blocks checked with bash -n.
- Repository make review-check and git diff --check pass after installation. All new planning documents also pass whitespace checks. These checks validate planning material and repository review configuration; implementation acceptance remains pending.

Plan state is `planned` at `plan_revision: 2` and `plan_commit: 9af810f5`. The next canonical command is
`/flow:implement native-postgres-driver`, starting with
[t01](tasks/t01-package-scaffold.md). This document claims no production build,
native/live service, installed-wheel, free-threaded or performance acceptance.

[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — Shared PostgreSQL wire frame helpers in vertebra-db.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver target cutover seam.
