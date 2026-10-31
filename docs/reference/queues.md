# `vertebra.framework.queues` — Background Task Queues & Cron Scheduler

Asynchronous background task queues, retry policies, dead-letter tracking, distributed cron leases, and storage backends.

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

::: vertebra.framework.queues.postgres.PostgresQueueBackend
    options:
      heading_level: 3

::: vertebra.framework.queues.redis.RedisQueueBackend
    options:
      heading_level: 3

::: vertebra.framework.queues.redis.ValkeyQueueBackend
    options:
      heading_level: 3

::: vertebra.framework.queues.oracle.OracleQueueBackend
    options:
      heading_level: 3
