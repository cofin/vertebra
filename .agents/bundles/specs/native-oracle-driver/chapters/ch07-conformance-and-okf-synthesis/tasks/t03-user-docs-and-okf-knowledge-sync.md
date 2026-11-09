---
type: Task
id: "native-oracle-driver:ch07:t03"
flow_id: native-oracle-driver
chapter_id: ch07-conformance-and-okf-synthesis
title: "Task 03: Synchronize User Documentation, README, and AGENTS.md"
description: Update docs/usage/databases.md, README.md, and AGENTS.md to document vertebra-core[oracle], vertebra-oracle (_oracle), Oracle 26ai codecs, Autonomous Wallet mTLS, delayed-commit pipelines, VPD tenancy, and Make targets.
status: stable
state: completed
priority: P1
owner: null
depends_on:
  - "native-oracle-driver:ch07:t02"
files:
  - docs/usage/databases.md
  - docs/reference/db.md
  - docs/reference/database-capabilities.md
  - docs/reference/native-stubs.md
  - README.md
  - AGENTS.md
tests: []
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md
  - docs/usage/databases.md
  - README.md
  - AGENTS.md
verification_strategy: documentation_validation
estimated_loc: 150
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T18:31:49Z"
last_verified_commit: 549fdf73
verification_evidence: "make docs-check (docstring scan, CLI docs check, zensical build, postbuild, verify_docs_artifacts, live zensical serve HTTP 200), wc -l AGENTS.md (65 <= 100), and review_setup validate passed"
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T18:31:49Z"
completed_at: "2026-11-09T18:31:49Z"
commit_sha: null
commit: null
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public contracts for vertebra-oracle and Python Oracle adapters
  - id: docs-db
    resource: docs/usage/databases.md
    title: User-facing database drivers and adapters guide
  - id: repo-readme
    resource: README.md
    title: Repository overview and optional driver extras catalog
  - id: repo-agents
    resource: AGENTS.md
    title: Root agent instructions and quick verification command table
tags:
  - oracle
  - documentation
  - zensical
---

# Task 03: Synchronize User Documentation, README, and AGENTS.md

## Objective & Scope Boundary

Update `docs/usage/databases.md`, `README.md`, and `AGENTS.md` in present tense to document the shipped `vertebra-oracle` (`vertebra._oracle`, `vertebra-core[oracle]`) native driver, Oracle 26ai `VECTOR` and `OSON` codecs, Autonomous Database Wallet mTLS, DRCP pooling, delayed-commit `pipeline()`, VPD trusted-package and `CLIENT_IDENTIFIER` tenancy, and `OracleNotificationSource` [^ora-contracts] [^docs-db].

- **In Scope**:
  - `docs/usage/databases.md`: Add linked `pymdownx.tabbed` installation blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) for `vertebra-core[oracle]`, concise usage examples (`OracleConfig` descriptor precedence, Autonomous Wallet mTLS, 26ai `VECTOR`/`OSON`, delayed-commit `pipeline()`, VPD trusted context package + `SET_IDENTIFIER` tenancy, and `OracleNotificationSource`), and a conceptual Mermaid diagram (`4`–`6` nodes).
  - `README.md`: Register `vertebra-oracle` (`_oracle`), the `[oracle]` optional extra, and `oracle-test` / `oracle-wheel-check` targets.
  - `AGENTS.md`: Include `vertebra-oracle` (`_oracle`) in the crate list and `make dev` description while keeping `AGENTS.md` `<= 100` lines.
- **Out of Scope**:
  - OKF knowledge base chapters under `.agents/bundles/` (owned by `native-oracle-driver:ch07:t04`).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Shipped public API signatures and invariants.
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md` — Section 2.3 and Section 3 documentation requirements.
3. `docs/usage/databases.md` — Existing PostgreSQL, MySQL, SQL Server, BigQuery, Spanner, and Oracle sections.
4. `README.md` — Existing package table and command summary.
5. `AGENTS.md` — Root agent instructions (`<= 100`-line hard cap).

## Symbol & Signature Contracts

### Documentation Style & Structural Invariants

- Installation tabs in `docs/usage/databases.md` must use `=== "uv"`, `=== "pip"`, `=== "pdm"`, and `=== "poetry"`.
- Public documentation code examples must declare `class Model(msgspec.Struct):` without internal `frozen=True, gc=False` boilerplate and pass configuration arguments inline rather than creating single-use intermediate variables.
- Mermaid diagrams in `docs/usage/databases.md` must stay conceptual (`4`–`6` nodes).
- `AGENTS.md` must remain `<= 100` lines with zero standalone `---` horizontal rules.

## Edge Cases & Error Semantics

- **Present-Tense Accuracy**: Describe `vertebra-oracle` (`vertebra._oracle`) as the active pure-Rust Oracle 26ai Thin driver (`oracledb = "=26.0.0-beta.5"`, zero Instant Client dependency) without historical migration notes or "previously stubbed" commentary.
- **Line-Cap Enforcement on `AGENTS.md`**: Verify `wc -l AGENTS.md` is `<= 100` lines before committing.

## Public-Seam Test Blueprint

- Run `make docs-check` to build the Zensical documentation site, validate all internal links and API docstrings, and verify the live server responds with `HTTP 200`.
- Run `make review-check` to validate `AGENTS.md` line count and repository hygiene gates.

## Verification

```bash
make docs-check
make review-check
```

## Acceptance Criteria

- [x] `docs/usage/databases.md` documents `vertebra-core[oracle]` with `pymdownx.tabbed` install tabs, concise inline `OracleConfig` / `SyncOracleDriver` / `AsyncOracleDriver` / `SyncOraclePool` / `pipeline()` / VPD tenancy / `OracleNotificationSource` examples, and a `4`–`6`-node conceptual Mermaid diagram [^docs-db].
- [x] `README.md` and `AGENTS.md` (`<= 100` lines) list `vertebra-oracle` (`_oracle`) and its verification targets [^repo-readme] [^repo-agents].
- [x] `make docs-check` and `make review-check` exit `0`.

## Notes & Discoveries

Also synchronized `docs/reference/db.md` (`summary: false` on `OracleConfig` to keep `reference/db/index.html` under the 120 KB reference artifact budget and updated the Oracle VPD tenancy note), `docs/reference/database-capabilities.md` (`TenantConnectionScope`, `AsyncTenantConnectionScope`, `OracleNotifyConfig`, `OracleNotificationSource`), and `docs/reference/native-stubs.md` (`vertebra._oracle` row).

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for `vertebra-oracle` and Python Oracle adapters.
[^docs-db]: `docs/usage/databases.md` — User-facing database drivers and adapters guide.
[^repo-readme]: `README.md` — Repository overview and optional driver extras catalog.
[^repo-agents]: `AGENTS.md` — Root agent instructions and quick verification command table.
