# Active Specifications & Research (`specs/`)

Active specifications and promoted spec-scoped research notes anchored at `main @ 473e1f2e` (`broker-pool-sharing` archived at `246d7d0e`; covers [#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96), [#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98)); synthesized documentation lives in [`../knowledge/`](../knowledge/index.md) and bundle activity is logged in [`../log.md`](../log.md).

## 1. Master PRDs (`state: active`)

- [`integration-remediation`](integration-remediation/spec.md) — Downward `vertebra.db` layering, SQLSpec `0.71.1+` adapter parity, SQLAlchemy/Advanced Alchemy/native caching/Django `6.0+` integration, native queues/tasks/channels, Spanner interactive `ReadWrite` transactions, progressive server streaming, HTTP/3, embeddings/OIDC identity, MCP `2026-07-28` completeness, and strict typing hygiene.

## 2. Planned Executable Specifications (`state: planned`)

- [`spanner-queue-and-rw-transactions`](spanner-queue-and-rw-transactions/spec.md) — Interactive multi-statement Spanner `ReadWrite` transactions (`BeginTransaction`/`seqno`/`precommit_token`/`Commit`/`Rollback` with `SpannerAbortedError` retry) in `vertebra-spanner` and `SyncSpannerQueueStore`/`AsyncSpannerQueueStore` across GoogleSQL and Spangres (`has_chapters: false`, `4` tasks; unblocked with `queues-tasks-and-channels` archived at `1094ca5b`).
- [`strict-typing-hygiene`](strict-typing-hygiene/spec.md) — Pruning `40` redundant `ignore_missing_imports` patterns (of `52`) in `pyproject.toml`, fixing the masked `litestar_queues.records` import, adding the `ServerRunner` stderr regression test, and enabling `mypy` across `src/py/tests` (`has_chapters: false`, `4` tasks; depends on `spanner-queue-and-rw-transactions`; covers [#59](https://github.com/cofin/vertebra/issues/59)).

## 3. Promoted Spec Research (`state: promoted`)

- [`audit-findings_20261103.md`](integration-remediation/research/audit-findings_20261103.md) — Audit findings across database extensions, framework, Google Cloud drivers, MCP/WebMCP, and `vertebra.db` (`integration-remediation`).
- [`google-cloud-rust_20261103.md`](integration-remediation/research/google-cloud-rust_20261103.md) — Evaluation of official `googleapis/google-cloud-rust` crates for Spanner, BigQuery Storage, and auth (`integration-remediation`).
