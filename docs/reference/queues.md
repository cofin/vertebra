# Background Task Queues & Cron Scheduler

`vertebra.framework.queues` provides sync and async task queues, retry and dead-letter policies, distributed cron leases, multi-lane worker execution, and pluggable backends for Memory, PostgreSQL, Oracle, Redis, Valkey, and Spanner.

| Component | Purpose |
| --- | --- |
| **Queue Core & Runner** | Enqueue typed tasks onto `io`, `cpu`, or `agent` lanes with scoped DI, heartbeats, exponential backoff, and dead-letter retention. |
| **Queue Store Protocols** | Runtime-checkable persistence contracts implemented by the PostgreSQL, Oracle, and Spanner queue stores. |
| **Cron Scheduler** | Run recurring cron schedules with slot-keyed distributed leader leases. |
| **Storage Backends** | Store and claim task envelopes using `SKIP LOCKED` SQL tables, hash-sharded Spanner transactions, or Redis and Valkey Streams consumer groups. |

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

## Cron Scheduling

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
