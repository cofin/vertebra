# Bundler, Inertia v3, QuickJS SSR & HTMX

Vertebra includes a built-in Rust JavaScript bundler, an embedded QuickJS server-side rendering pool, and server helpers for Inertia v3 and HTMX. See [TypeScript SDK & Framework Adapters](typescript-sdk.md) for `@vertebra/client` usage.

## Embedded JavaScript Bundler

Bundle TypeScript, JSX/TSX, and CSS assets directly from Python without running a Node.js build server:

```python
from vertebra.build import BundleConfig, Bundler

bundler = Bundler(
    BundleConfig(
        entry="frontend/src/main.tsx",
        outdir="static/dist",
        minify=True,
        sourcemap=True,
    )
)
result = bundler.build()
```

Or run it from the CLI:

```bash
vtb js build
```

## Inertia v3

Use a native handler to construct an Inertia page and return the negotiated
HTTP response. For a browser application, build your frontend entry and make
its assets available to the HTML shell. Map `Dashboard/Index` to your React,
Vue, or Svelte component in that entry.

```python
from vertebra import App, get
from vertebra._vertebra import Request
from vertebra.framework.inertia import (
    AsyncInertiaMiddleware,
    DeferredProp,
    InertiaConfig,
    MergeProp,
    OnceProp,
    ScrollProp,
    inertia,
)

middleware = AsyncInertiaMiddleware(
    InertiaConfig(enabled=True, version="1.0")
)


@get("/dashboard")
async def dashboard(request: Request) -> tuple[int, dict[str, str], bytes]:
    page = inertia(
        "Dashboard/Index",
        {
            "summary": {"active_users": 42},
            "nav": OnceProp(lambda: ["Home", "Orders", "Settings"]),
            "events": MergeProp(lambda: [{"id": 1, "kind": "OrderCreated"}]),
            "feed": ScrollProp(lambda: [{"id": 1}], next_page=2),
            "metrics": DeferredProp(lambda: {"p99_ms": 1.4}, group="analytics"),
        },
    )
    response = await middleware.process(request, page)
    return response.status, response.headers, response.body


app = App(route_handlers=[dashboard])
```

Send `X-Inertia: true` to receive the page as JSON. In a browser, configure
the asset manifest and root template for the initial HTML response. Inertia
page props travel with the page response. For server-pushed changes after
the page loads, see [Live State & Pub/Sub Channels](queues-and-realtime.md#livestate-pubsub-channels).

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
    html="<li>New item</li>",
    trigger="itemAdded",
    retarget="#items-list",
    reswap="beforeend",
)
```
