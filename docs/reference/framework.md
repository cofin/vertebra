# `vertebra.framework` — Dependency Injection, Security, OpenAPI & Realtime

Core framework primitives for dependency injection, security guards, OpenAPI and AsyncAPI specification generation, real-time `LiveState` synchronization, typed RPC procedures, and transactional email delivery.

## Dependency Injection (`vertebra.framework.di`)

::: vertebra.framework.di.Provide
    options:
      heading_level: 3

::: vertebra.framework.di.Container
    options:
      heading_level: 3

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

## Real-Time `LiveState` & Procedures (`vertebra.framework.live`, `vertebra.framework.procedures`)

::: vertebra.framework.live.LiveState
    options:
      heading_level: 3

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
