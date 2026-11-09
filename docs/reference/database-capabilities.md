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

::: vertebra.db.adapters.postgres.notify.PostgresNotifyConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.postgres.notify.PostgresNotificationSource
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.notify.OracleNotifyConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.oracle.notify.OracleNotificationSource
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
