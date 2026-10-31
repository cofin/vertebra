# `vertebra.framework.inertia` & `vertebra.framework.ssr` — Inertia v3, QuickJS SSR & HTMX

Server-driven single-page application primitives for Inertia v3 (`InertiaConfig`, `InertiaResponse`, `InertiaMiddleware`), pooled QuickJS server-side rendering (`EmbeddedSsrClient`, `SidecarSsrClient`), and HTMX request/response helpers (`HtmxRequest`, `HtmxResponse`). See [Frontend, Inertia & SSR](../usage/frontend-and-ssr.md) for usage examples.

| Subsystem | Classes | Purpose |
| --- | --- | --- |
| **Inertia v3 Core** | `InertiaConfig`, `InertiaResponse`, `InertiaMiddleware` | Negotiate `X-Inertia` JSON vs. initial HTML shell responses, asset version mismatches (`409 Conflict`), and partial reloads |
| **Inertia v3 Props** | `DeferredProp`, `MergeProp`, `DeepMergeProp`, `OnceProp`, `ScrollProp`, `OptionalProp` | Control deferred evaluation, client-side array/object merging, infinite scroll pagination, and lazy props |
| **QuickJS SSR** | `SsrConfig`, `SsrResult`, `EmbeddedSsrClient`, `SidecarSsrClient` | Pre-render initial page HTML inside the embedded Rust QuickJS pool (`vertebra._bundler.SsrPool`) or an external Node/Bun sidecar |
| **HTMX Helpers** | `HxSwap`, `HtmxRequest`, `HtmxResponse` | Inspect `HX-*` request headers and emit `HX-Trigger`, `HX-Retarget`, `HX-Push-Url`, and out-of-band swap responses |

## Inertia v3 Configuration & Responses

::: vertebra.framework.inertia.InertiaConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.InertiaResponse
    options:
      heading_level: 4
      members: false

::: vertebra.framework.inertia.InertiaMiddleware
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

::: vertebra.framework.ssr.SsrConfig
    options:
      heading_level: 4
      members: false

::: vertebra.framework.ssr.SsrResult
    options:
      heading_level: 4
      members: false

::: vertebra.framework.ssr.EmbeddedSsrClient
    options:
      heading_level: 3

::: vertebra.framework.ssr.SidecarSsrClient
    options:
      heading_level: 4
      members: false

## HTMX Request & Response Helpers (`vertebra.framework.htmx`)

::: vertebra.framework.htmx.HxSwap
    options:
      heading_level: 4
      members: false

::: vertebra.framework.htmx.HtmxRequest
    options:
      heading_level: 3

::: vertebra.framework.htmx.HtmxResponse
    options:
      heading_level: 3
