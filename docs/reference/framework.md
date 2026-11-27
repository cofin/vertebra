# Framework Primitives

Core primitives in `vertebra.framework` for dependency injection, security guards, OpenAPI and AsyncAPI specs, live state sync, typed RPC procedures, email, and key-value stores. See [Framework Features](../usage/framework-features.md) for usage examples.

## Dependency Injection

`Provide` and `Container` compile a static dependency graph at route registration time. They support sync, async, and generator-scoped dependencies without per-request reflection.

::: vertebra.framework.di.Provide
    options:
      heading_level: 3

::: vertebra.framework.di.Container
    options:
      heading_level: 3

## Security & Guards

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

## OpenAPI & AsyncAPI

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

## Live State & Procedures

::: vertebra.framework.live.LiveState
    options:
      heading_level: 3

::: vertebra.framework.procedures.ProcedureRouter
    options:
      heading_level: 3

## Email & Stores

::: vertebra.framework.email.base.EmailService
    options:
      heading_level: 3

::: vertebra.framework.stores.KeyValueStore
    options:
      heading_level: 3
