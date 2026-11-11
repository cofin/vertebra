# `vertebra.framework.queues` — Background Task Queues & Cron Scheduler

Asynchronous background task queues (`QueueService`, `QueueConfig`), retry policies (`TaskRetryPolicy`), dead-letter tracking (`DeadLetterEntry`), distributed cron leases (`CronScheduler`), multi-lane worker execution (`WorkerRunner`), and pluggable storage backends (`MemoryQueueBackend`, `PostgreSQLQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `OracleQueueBackend`, `SpannerQueueBackend`).

| Component | Classes | Purpose |
| --- | --- | --- |
| **Queue Core & Runner** | `QueueConfig`, `TaskRetryPolicy`, `TaskEnvelope`, `DeadLetterEntry`, `QueueService`, `WorkerRunner` | Enqueue typed tasks onto `io`, `cpu`, or `agent` lanes with exponential backoff and dead-letter retention |
| **Cron Scheduler** | `CronSchedule`, `CronScheduler` | Execute recurring cron schedules with distributed leader lease coordination |
| **Storage Backends** | `MemoryQueueBackend`, `PostgreSQLQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `OracleQueueBackend`, `SpannerQueueBackend` | Persist and claim task envelopes with `SKIP LOCKED` tables (PostgreSQL, Oracle), Redis Streams consumer groups (Redis, Valkey), or hash-sharded `FOR UPDATE` claims (Spanner). `PostgreSQLQueueBackend`, `OracleQueueBackend`, and `SpannerQueueBackend` require `executor=`; `RedisQueueBackend` and `ValkeyQueueBackend` require the `redis` package and a hosted `redis://` / `rediss://` URL or `client=` (`valkey://`, `valkeys://`, and hostless `unix://` URLs require a `redis://` / `rediss://` host URL or an injected `client=`); otherwise construction raises `MissingDependencyError`, `ImproperConfigurationError`, or `ValueError`. |

## Queue Configuration, Dead-Letter Entries & Service

::: vertebra.framework.queues.base.QueueConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.queues.base.TaskRetryPolicy
    options:
      heading_level: 4
      members: false

::: vertebra.framework.queues.base.TaskEnvelope
    options:
      heading_level: 4
      members: false

::: vertebra.framework.queues.base.DeadLetterEntry
    options:
      heading_level: 4
      members: false

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

::: vertebra.framework.queues.redis.ValkeyQueueBackend
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
