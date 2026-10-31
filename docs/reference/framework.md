# `vertebra.framework` — Dependency Injection, Security, OpenAPI & Realtime

Core framework primitives for dependency injection, security guards, OpenAPI and AsyncAPI specification generation, real-time `LiveState` synchronization, typed RPC procedures, and transactional email delivery.

## Dependency Injection & Auto-Wiring (`vertebra.framework.di`, `vertebra.framework.autowire`)

::: vertebra.framework.di.Provide
    options:
      heading_level: 3

::: vertebra.framework.di.Inject
    options:
      heading_level: 4
      members: false

::: vertebra.framework.di.Container
    options:
      heading_level: 3

::: vertebra.framework.di.DependencyGraph
    options:
      heading_level: 4
      members: false

::: vertebra.framework.di.RequestScope
    options:
      heading_level: 4
      members: false

::: vertebra.framework.autowire.AutowireLoader
    options:
      heading_level: 4
      members: false

## Security & Guards (`vertebra.framework.security`)

::: vertebra.framework.security.Principal
    options:
      heading_level: 4
      members: false

::: vertebra.framework.security.SecurityConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.security.Guard
    options:
      heading_level: 3

## OpenAPI & AsyncAPI (`vertebra.framework.openapi`, `vertebra.framework.asyncapi`)

::: vertebra.framework.openapi.OpenAPIConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.openapi.OpenAPISpecBuilder
    options:
      heading_level: 3

::: vertebra.framework.asyncapi.AsyncAPIConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.asyncapi.AsyncAPISpecBuilder
    options:
      heading_level: 4
      members: false

## Real-Time `LiveState`, Channels & Procedures (`vertebra.framework.live`, `vertebra.framework.channels`, `vertebra.framework.procedures`)

::: vertebra.framework.live.LiveState
    options:
      heading_level: 3

::: vertebra.framework.live.LiveStateRoom
    options:
      heading_level: 4
      members: false

::: vertebra.framework.channels.ChannelBus
    options:
      heading_level: 4
      members: false

::: vertebra.framework.procedures.Procedure
    options:
      heading_level: 4
      members: false

::: vertebra.framework.procedures.ProcedureRouter
    options:
      heading_level: 3

## Email & Stores (`vertebra.framework.email`, `vertebra.framework.stores`)

::: vertebra.framework.email.base.EmailService
    options:
      heading_level: 3

::: vertebra.framework.stores.ResponseCacheStore
    options:
      heading_level: 3

::: vertebra.framework.stores.NamespacedStore
    options:
      heading_level: 4
      members: false
