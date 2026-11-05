---
type: Guide
title: Database Foundation Review Evidence
description: Adjudication, verification boundaries, and completion evidence for the database foundation.
status: stable
tags: [db, review, verification]
---

# Database Foundation Review Evidence

## Range and correctness review

Range: `f741701..4ce057e`. Isolated Python, PyO3, runtime, and packaging reviews completed. Correctness re-verification resolves all accepted blockers; live PostgreSQL and GCP service acceptance remains outside this evidence.

| Finding | Outcome | Verdict | Commit |
| --- | --- | --- | --- |
| PY-01 | Cancellation cleanup | ADOPT; resolved | `9ed6723` |
| PY-02 | Transactional DBAPI ownership | ADOPT; resolved | `9ed6723` |
| PY-03 | DuckDB affected counts | ADOPT; resolved | `9ed6723` |
| PY-04 | Asyncpg transaction lifecycle | ADOPT; resolved | `9ed6723` |
| PY-05 | SQLite RETURNING completion | ADOPT; resolved | `9ed6723` |
| PY-06 | Asyncpg affected counts | ADOPT; resolved | `9ed6723` |
| PY-07 | Spanner DBAPI statement semantics | ADOPT; resolved | `4ce057e` |
| PY-08 | BigQuery permanent DDL policy | ADOPT; resolved | `4ce057e` |
| DOC-01 | Companion release artifacts | ADOPT; resolved | `abfe042` |
| DOC-02 | Source-built wheel ownership | ADOPT; resolved | `abfe042` |
| RT-NEW | Adjacent pre-existing GCP HTTP classification | ADOPT; resolved | `f2d35c3` |
| DOC-03 | Parent/index lifecycle drift | ADOPT; reconcile at finish/archive | Pending archive |

## Verification

make py-test (1738 passed, 69 skipped, 170 xfailed); make rs-test (473 passed, 1 ignored); make ts-test; make lint-all; make docs; isolated wheel and sdist rebuild/install/uninstall probes (exit 0). CARGO_TARGET_DIR and TMPDIR use disk-backed paths to avoid host temporary-filesystem quota.

Hosted CI run `41935822926` is green at antecedent `989ebbd`; the local remediation commits have no hosted run. Local companion artifacts use CPython 3.12 Linux debug builds; the hosted release platform matrix remains unrun. Strict expected failures and service-dependent skips are retained for their owning native-driver flows.

## Quality review

Mandatory fresh quality/debloat review is pending on the committed checkpoint.
