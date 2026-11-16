# Active Specifications & Research (`specs/`)

Active specifications and promoted spec-scoped research notes anchored at `main @ 473e1f2e` (`broker-pool-sharing` archived at `246d7d0e`; covers [#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96), [#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98)); synthesized documentation lives in [`../knowledge/`](../knowledge/index.md) and bundle activity is logged in [`../log.md`](../log.md).

## 1. Master PRDs (`state: active`)

- [`integration-remediation`](integration-remediation/spec.md) — Downward `vertebra.db` layering, SQLSpec `0.71.1+` adapter parity, SQLAlchemy/Advanced Alchemy/native caching/Django `6.0+` integration, native queues/tasks/channels, Spanner interactive `ReadWrite` transactions, progressive server streaming, HTTP/3, embeddings/OIDC identity, MCP `2026-07-28` completeness, and strict typing hygiene.

## 2. Planned Executable Specifications (`state: planned`)

- [`queues-tasks-and-channels`](queues-tasks-and-channels/spec.md) — `SyncQueueStore`/`AsyncQueueStore` (`SyncPostgresQueueStore`/`AsyncPostgresQueueStore`, `SyncOracleQueueStore`/`AsyncOracleQueueStore`), native `vertebra._redis` and `vertebra._valkey` queue backends, `litestar-queues>=0.14.0`, Django `6.0+` `TaskBackend` with `Container` DI, and `SyncNotificationSource`/`AsyncNotificationSource` distributed channels plus `ValkeyStore`/`RedisStore` ([`3 chapters`](queues-tasks-and-channels/chapters/index.md), `12` tasks; unblocked with `sqlspec-adapter-parity` archived at `4fad78c5`).
- [`spanner-queue-and-rw-transactions`](spanner-queue-and-rw-transactions/spec.md) — Interactive multi-statement Spanner `ReadWrite` transactions (`BeginTransaction`/`seqno`/`precommit_token`/`Commit`/`Rollback` with `SpannerAbortedError` retry) in `vertebra-spanner` and `SyncSpannerQueueStore`/`AsyncSpannerQueueStore` across GoogleSQL and Spangres (`has_chapters: false`, `4` tasks; depends on `queues-tasks-and-channels`).
- [`strict-typing-hygiene`](strict-typing-hygiene/spec.md) — Pruning `40` redundant `ignore_missing_imports` patterns (of `52`) in `pyproject.toml`, fixing the masked `litestar_queues.records` import, adding the `ServerRunner` stderr regression test, and enabling `mypy` across `src/py/tests` (`has_chapters: false`, `4` tasks; depends on `queues-tasks-and-channels`, `spanner-queue-and-rw-transactions`; covers [#59](https://github.com/cofin/vertebra/issues/59)).

## 3. Promoted Spec Research (`state: promoted`)

- [`audit-findings_20261103.md`](integration-remediation/research/audit-findings_20261103.md) — Audit findings across database extensions, framework, Google Cloud drivers, MCP/WebMCP, and `vertebra.db` (`integration-remediation`).
- [`google-cloud-rust_20261103.md`](integration-remediation/research/google-cloud-rust_20261103.md) — Evaluation of official `googleapis/google-cloud-rust` crates for Spanner, BigQuery Storage, and auth (`integration-remediation`).
