# `vertebra.framework.queues` — Background Task Queues & Cron Scheduler

Synchronous and asynchronous background task queues (`QueueService`, `QueueConfig`, `QueueBackendSettings`), capability store protocols (`SyncQueueStore`, `AsyncQueueStore`, `QueueEntry`), retry policies (`TaskRetryPolicy`), dead-letter tracking (`DeadLetterEntry`), task results (`TaskResultRecord`), distributed cron leases (`CronScheduler`), multi-lane worker execution (`WorkerRunner`), and pluggable storage backends (`MemoryQueueBackend`, `PostgreSQLQueueBackend`, `OracleQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `SpannerQueueBackend`).

| Component | Classes | Purpose |
| --- | --- | --- |
| **Queue Core & Runner** | `QueueConfig`, `QueueBackendSettings`, `TaskRetryPolicy`, `TaskEnvelope`, `TaskResultRecord`, `DeadLetterEntry`, `QueueService`, `WorkerRunner` | Enqueue typed tasks (`enqueue` / `enqueue_sync`) onto `io`, `cpu`, or `agent` lanes with scoped DI, heartbeats, exponential backoff, and dead-letter retention |
| **Queue Store Protocols** | `SyncQueueStore`, `AsyncQueueStore`, `QueueEntry` | `@runtime_checkable` engine-local queue persistence contracts (`vertebra.db.driver.capabilities`) implemented by `SyncPostgresQueueStore`/`AsyncPostgresQueueStore`, `SyncOracleQueueStore`/`AsyncOracleQueueStore`, and `SyncSpannerQueueStore`/`AsyncSpannerQueueStore` |
| **Cron Scheduler** | `CronSchedule`, `CronScheduler` | Execute recurring cron schedules with slot-keyed distributed leader lease coordination |
| **Storage Backends** | `MemoryQueueBackend`, `PostgreSQLQueueBackend`, `OracleQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `SpannerQueueBackend` | Persist and claim task envelopes with auto-constructed native `SKIP LOCKED` stores (`vertebra-core[postgres]`, `vertebra-core[oracle]`), interactive `ReadWrite` hash-sharded `FOR UPDATE` stores (`vertebra-core[spanner]`), or native Streams consumer groups (`vertebra-core[redis]`, `vertebra-core[valkey]`). |

## Queue Configuration, Store Protocols & Service

::: vertebra.framework.queues.base.QueueConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.framework.queues.base.QueueBackendSettings
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.capabilities.QueueEntry
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.capabilities.SyncQueueStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.capabilities.AsyncQueueStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.framework.queues.base.TaskRetryPolicy
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.framework.queues.base.TaskEnvelope
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.framework.queues.base.DeadLetterEntry
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.framework.queues.base.QueueService
    options:
      heading_level: 3

::: vertebra.server.lanes.WorkerRunner
    options:
      heading_level: 3

## Cron Scheduling (`vertebra.framework.queues.scheduler`)

::: vertebra.framework.queues.base.CronSchedule
    options:
      heading_level: 4
      members: false

::: vertebra.framework.queues.scheduler.CronScheduler
    options:
      heading_level: 3

## Queue Backends

::: vertebra.framework.queues.base.MemoryQueueBackend
    options:
      heading_level: 3

::: vertebra.framework.queues.postgres.PostgreSQLQueueBackend
    options:
      heading_level: 3
      members:
        - enqueue
        - dequeue
        - ack
        - nack

::: vertebra.framework.queues.redis.RedisQueueBackend
    options:
      heading_level: 3
      members:
        - enqueue
        - dequeue
        - ack
        - nack

::: vertebra.framework.queues.valkey.ValkeyQueueBackend
    options:
      heading_level: 3
      members:
        - enqueue
        - dequeue
        - ack
        - nack

::: vertebra.framework.queues.oracle.OracleQueueBackend
    options:
      heading_level: 3
      members:
        - enqueue
        - dequeue
        - ack
        - nack

::: vertebra.framework.queues.spanner.SpannerQueueBackend
    options:
      heading_level: 3
      members:
        - enqueue
        - dequeue
        - ack
        - nack
