# Bundler, Inertia v3, QuickJS SSR & HTMX

Vertebra includes a built-in Rust JavaScript bundler (`vertebra-bundler`), an in-process QuickJS server-side rendering pool (`EmbeddedSSRClient`), and Inertia v3 and HTMX server helpers. See [TypeScript SDK & Framework Adapters](typescript-sdk.md) for `@vertebra/client` usage.

## Embedded JavaScript Bundler (`vertebra._bundler`)

Compile TypeScript, JSX/TSX, and CSS assets directly from Python without running a separate Node.js build server:

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

Or run from the CLI:

```bash
vtb js build
```

## Inertia v3 Protocol & Prop Types (`vertebra.framework.inertia`)

Build single-page React, Vue, or Svelte applications driven by Python handlers with `@inertia("Component")`, partial reloads, deferred props (`DeferredProp`), shallow and deep merge props (`MergeProp`, `DeepMergeProp`), once-evaluated props (`OnceProp`), scroll pagination props (`ScrollProp`), and on-demand props (`OptionalProp`):

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

## QuickJS Server-Side Rendering & Prerendering (`vertebra.framework.ssr`)

Render JavaScript and TypeScript page components on the server using the embedded QuickJS worker pool (`EmbeddedSSRClient.from_config`), an external HTTP runtime (`SidecarSSRClient`), or static build-time prerendering (`prerender_routes`):

```python
from vertebra.framework.ssr import EmbeddedSSRClient, SSRConfig

ssr = EmbeddedSSRClient.from_config(
    SSRConfig(bundle_path="static/ssr/ssr.js", workers=4)
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

## HTMX Request & Response Helpers (`vertebra.framework.htmx`)

Inspect incoming `HX-*` headers with `HTMXRequest` (`is_htmx`, `boosted`, `target`, `trigger`) and construct partial HTML responses with `HTMXResponse` (`trigger`, `retarget`, `reswap`, `push_url`):

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
