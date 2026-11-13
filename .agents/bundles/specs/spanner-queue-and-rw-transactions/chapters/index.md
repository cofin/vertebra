# Chapters: Cloud Spanner Interactive ReadWrite Transactions & Queue Store (`spanner-queue-and-rw-transactions`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-interactive-rw-transactions`](ch01-interactive-rw-transactions/spec.md) | `planned` | `3` | Implement interactive multi-statement `ReadWrite` transactions (`TransactionSelector::Id` + `seqno` + `precommit_token` + `SpannerAbortedError` retries) in `vertebra-spanner` and Python Spanner drivers. |
| [`ch02-spanner-queue-store-and-docs`](ch02-spanner-queue-store-and-docs/spec.md) | `planned` | `3` | Implement `SyncSpannerQueueStore`/`AsyncSpannerQueueStore` across GoogleSQL and Spangres, fix `SpannerQueueBackend` claim/lease/leak defects, and sync docs/OKF. |
