# Chapters: Native Queues, Tasks, Channels, and Stores (`queues-tasks-and-channels`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-queue-store-capabilities-and-native-stores`](ch01-queue-store-capabilities-and-native-stores/spec.md) | `planned` | `3` | Split `QueueStore` into `@runtime_checkable SyncQueueStore`/`AsyncQueueStore` and build `Sync/AsyncPostgresQueueStore` & `Sync/AsyncOracleQueueStore`. |
| [`ch02-first-party-queue-backends-and-service`](ch02-first-party-queue-backends-and-service/spec.md) | `planned` | `3` | Add `QueueBackendSettings`, drop `TaskEnvelope` `gc=False`, delete duplicate cron helpers, and auto-wire PostgreSQL, Oracle, Redis, and Valkey backends. |
| [`ch03-worker-runner-scheduler-and-di`](ch03-worker-runner-scheduler-and-di/spec.md) | `planned` | `3` | Implement `WorkerRunner` with per-task `Container.scope()` DI, distributed `CronScheduler` leases, and Starlette/FastAPI/Flask queue wiring. |
| [`ch04-litestar-queues-integration`](ch04-litestar-queues-integration/spec.md) | `planned` | `3` | Bump `litestar-queues>=0.14.0` and wire `SHMQueueBackend`, engine-neutral `StoreQueueBackend`, `queue_store_class`/`events_store_class`, and `Container` DI. |
| [`ch05-django-tasks-and-cache-integration`](ch05-django-tasks-and-cache-integration/spec.md) | `planned` | `3` | Implement Django `6.0+` `TaskBackend` (`enqueue` via `SyncQueueStore`, `aenqueue` via `AsyncQueueStore`), `vertebra_worker`, and native Django cache/channels. |
| [`ch06-distributed-channels-and-valkey-stores`](ch06-distributed-channels-and-valkey-stores/spec.md) | `planned` | `3` | Wire `SyncNotificationSource`/`AsyncNotificationSource` into `ChannelBus` and Litestar `ChannelsPlugin` across 5 backends, and consolidate `ValkeyStore`. |
| [`ch07-queues-channels-docs-and-okf`](ch07-queues-channels-docs-and-okf/spec.md) | `planned` | `3` | Synchronize `docs/` and `distributed-queues-and-email.md`, `litestar.md`, and `server-runtime-limitations.md`. |
