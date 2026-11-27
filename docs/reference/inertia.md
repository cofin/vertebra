# Inertia v3, QuickJS SSR & HTMX

Server-driven UI primitives in `vertebra.framework.inertia`, `vertebra.framework.ssr`, and `vertebra.framework.htmx` for Inertia v3 responses, pooled QuickJS or sidecar SSR, and HTMX helpers. See [Frontend, Inertia & SSR](../usage/frontend-and-ssr.md) for usage examples.

| Subsystem | Purpose |
| --- | --- |
| **Inertia v3 Core** | Negotiate `X-Inertia` JSON versus initial HTML responses, handle `409 Conflict` asset version changes, and support partial reloads. |
| **Inertia v3 Props** | Control deferred props, shallow and deep client merges, infinite scroll pagination, and lazy evaluation. |
| **QuickJS SSR** | Pre-render initial HTML inside the embedded QuickJS pool or an external Node or Bun sidecar. |
| **HTMX Helpers** | Read `HX-*` request headers and emit trigger, retarget, push-URL, and out-of-band swap responses. |

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

## QuickJS Server-Side Rendering

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

## HTMX Request & Response Helpers

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
