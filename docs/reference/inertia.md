# `vertebra.framework.inertia` & `vertebra.framework.ssr` — Inertia v3, QuickJS SSR & HTMX

Server-driven single-page application primitives for Inertia v3 (`InertiaConfig`, `InertiaResponse`, `SyncInertiaMiddleware`, `AsyncInertiaMiddleware`), pooled QuickJS server-side rendering (`SyncEmbeddedSSRClient`, `AsyncEmbeddedSSRClient`, `SyncSidecarSSRClient`, `AsyncSidecarSSRClient`), and HTMX request/response helpers (`HTMXRequest`, `HTMXResponse`). See [Frontend, Inertia & SSR](../usage/frontend-and-ssr.md) for usage examples.

| Subsystem | Classes | Purpose |
| --- | --- | --- |
| **Inertia v3 Core** | `InertiaConfig`, `InertiaResponse`, `SyncInertiaMiddleware`, `AsyncInertiaMiddleware` | Negotiate `X-Inertia` JSON vs. initial HTML shell responses, asset version mismatches (`409 Conflict`), and partial reloads |
| **Inertia v3 Props** | `DeferredProp`, `MergeProp`, `DeepMergeProp`, `OnceProp`, `ScrollProp`, `OptionalProp` | Control deferred evaluation, client-side array/object merging, infinite scroll pagination, and lazy props |
| **QuickJS SSR** | `SSRConfig`, `SSRResult`, `SyncEmbeddedSSRClient`, `AsyncEmbeddedSSRClient`, `SyncSidecarSSRClient`, `AsyncSidecarSSRClient` | Pre-render initial page HTML inside the embedded Rust QuickJS pool (`vertebra._bundler.SSRPool`) or an external Node/Bun sidecar |
| **HTMX Helpers** | `HxSwap`, `HTMXRequest`, `HTMXResponse` | Inspect `HX-*` request headers and emit `HX-Trigger`, `HX-Retarget`, `HX-Push-Url`, and out-of-band swap responses |

## Inertia v3 Configuration & Responses

::: vertebra.framework.inertia.InertiaConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.InertiaResponse
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.SyncInertiaMiddleware
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.AsyncInertiaMiddleware
    options:
      heading_level: 3

## Inertia v3 Prop Descriptors

::: vertebra.framework.inertia.DeferredProp
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.MergeProp
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.DeepMergeProp
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.OnceProp
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.ScrollProp
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.OptionalProp
    options:
      heading_level: 4
      members: false

## QuickJS Server-Side Rendering (`vertebra.framework.ssr`)

::: vertebra.framework.ssr.SSRConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.ssr.SSRResult
    options:
      heading_level: 4
      members: false

::: vertebra.framework.ssr.SyncEmbeddedSSRClient
    options:
      heading_level: 4
      members: false

::: vertebra.framework.ssr.AsyncEmbeddedSSRClient
    options:
      heading_level: 3

::: vertebra.framework.ssr.SyncSidecarSSRClient
    options:
      heading_level: 4
      members: false

::: vertebra.framework.ssr.AsyncSidecarSSRClient
    options:
      heading_level: 4
      members: false

## HTMX Request & Response Helpers (`vertebra.framework.htmx`)

::: vertebra.framework.htmx.HxSwap
    options:
      heading_level: 4
      members: false

::: vertebra.framework.htmx.HTMXRequest
    options:
      heading_level: 3

::: vertebra.framework.htmx.HTMXResponse
    options:
      heading_level: 3
