# Database Capabilities

Engine adapters own notification, change-stream, and database tool implementations.
Shared tenant values remain engine-neutral; session SQL belongs to the owning adapter.

## Agent Tool Providers

Pass a provider to [`build_tools`](agent.md) to expose typed operations through the generic
agent bridge. Creating providers and registering their tools opens no database connection.

::: vertebra.db.adapters.bigquery.tools.BigQueryToolProvider
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.spanner.tools.SpannerToolProvider
    options:
      heading_level: 4
      members: false
      summary: false

## Tenant Scope, Notifications & Change Streams

::: vertebra.db.rls.TenantScopeConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.rls.TenantConnectionScope
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.rls.AsyncTenantConnectionScope
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.capabilities.SyncNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.capabilities.AsyncNotificationSource
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

::: vertebra.db.adapters.postgres.notify.PostgresNotifyConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.notify.SyncPostgresNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.notify.AsyncPostgresNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.queue.SyncPostgresQueueStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.queue.AsyncPostgresQueueStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.change_stream.PostgresChangeConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.change_stream.PostgresChangeSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.change_stream.PostgresCheckpoint
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.change_stream.MemoryPostgresCheckpointStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.embeddings.PostgresEmbeddingPushdown
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.notify.OracleNotifyConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.notify.SyncOracleNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.notify.AsyncOracleNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.queue.SyncOracleQueueStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.queue.AsyncOracleQueueStore
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.redis.notify.SyncRedisNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.redis.notify.AsyncRedisNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.valkey.notify.SyncValkeyNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.valkey.notify.AsyncValkeyNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.spanner.change_stream.SyncSpannerChangeStreamSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.spanner.change_stream.AsyncSpannerChangeStreamSource
    options:
      heading_level: 4
      members: false
      summary: false
