# Bundler, Inertia v3, QuickJS SSR & HTMX

Vertebra includes a built-in Rust JavaScript bundler, an embedded QuickJS server-side rendering pool, and server helpers for Inertia v3 and HTMX. See [TypeScript SDK & Framework Adapters](typescript-sdk.md) for `@vertebra/client` usage.

## Embedded JavaScript Bundler

Bundle TypeScript, JSX/TSX, and CSS assets directly from Python without running a Node.js build server:

```python
from vertebra._bundler import BundleConfig, Bundler

config = BundleConfig(
    entry="frontend/src/main.tsx",
    outdir="static/dist",
    minify=True,
    sourcemap=True,
)
result = Bundler().build(config)
```

Or run it from the CLI:

```bash
vtb js build
```

## Inertia v3

Drive React, Vue, or Svelte single-page apps from Python handlers with `@inertia`. Vertebra supports partial reloads, deferred props, shallow and deep merge props, once-evaluated props, scroll pagination, and optional props:

```python
from vertebra import App
from vertebra.framework.inertia import (
    DeferredProp,
    InertiaConfig,
    InertiaResponse,
    MergeProp,
    OnceProp,
    ScrollProp,
    inertia,
)
from vertebra.framework.ssr import SSRConfig

app = App()

config = InertiaConfig(
    enabled=True,
    version="1.0",
    ssr=SSRConfig(bundle_path="static/ssr/ssr.js"),
)


@app.get("/dashboard")
@inertia("Dashboard/Index")
def dashboard() -> dict[str, object]:
    return {
        "summary": {"active_users": 42},
        "nav": OnceProp(lambda: ["Home", "Orders", "Settings"]),
        "events": MergeProp([{"id": 1, "kind": "OrderCreated"}]),
        "feed": ScrollProp(items=[{"id": 1}], next_page=2),
        "metrics": DeferredProp(lambda: {"p99_ms": 1.4}, group="analytics"),
    }


page = InertiaResponse(
    component="Dashboard/Index",
    props={"summary": {"active_users": 42}},
)
```

## QuickJS Server-Side Rendering

Render JavaScript and TypeScript page components on the server using the embedded QuickJS worker pool, or point `SSRConfig` at an external HTTP rendering service:

```python
from vertebra.framework.ssr import SSRConfig, SyncEmbeddedSSRClient

ssr = SyncEmbeddedSSRClient.from_config(
    SSRConfig(bundle_path="static/ssr/ssr.js", pool_size=4)
)
rendered = ssr.render(
    {
        "component": "Dashboard/Index",
        "props": {"title": "Overview"},
        "url": "/dashboard",
        "version": "1.0",
    }
)
```

## HTMX Helpers

Inspect incoming `HX-*` headers with `HTMXRequest` and build partial HTML responses with `HTMXResponse`:

```python
from vertebra.framework.htmx import HTMXRequest, HTMXResponse

hx = HTMXRequest.from_headers({"HX-Request": "true", "HX-Target": "items-list"})
response = HTMXResponse(
    "<li>New item</li>",
    trigger="itemAdded",
    retarget="#items-list",
    reswap="beforeend",
)
```
