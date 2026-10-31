# Frontend, Inertia v3 & QuickJS SSR

Vertebra includes a built-in Rust JavaScript bundler (`vertebra-bundler`), an in-process QuickJS server-side rendering pool (`EmbeddedSsrClient`), Inertia v3 and HTMX server helpers, and the `@vertebra/client` TypeScript SDK.

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

Build single-page React, Vue, or Svelte applications driven by Python handlers. Vertebra supports the `@inertia("Component")` route decorator, partial reloads, deferred props (`DeferredProp`), shallow and deep merge props (`MergeProp`, `DeepMergeProp`), once-evaluated props (`OnceProp`), scroll pagination props (`ScrollProp`), and on-demand props (`OptionalProp`):

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
from vertebra.framework.ssr import SsrConfig

app = App()

config = InertiaConfig(
    enabled=True,
    version="1.0",
    ssr=SsrConfig(bundle_path="static/ssr/ssr.js"),
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

Render JavaScript and TypeScript page components on the server using the embedded QuickJS worker pool (`EmbeddedSsrClient.from_config`), an external HTTP runtime (`SidecarSsrClient`), or static build-time prerendering (`prerender_routes`):

```python
from vertebra.framework.ssr import EmbeddedSsrClient, SsrConfig

ssr = EmbeddedSsrClient.from_config(
    SsrConfig(bundle_path="static/ssr/ssr.js", workers=4)
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

Inspect incoming `HX-*` headers with `HtmxRequest` (`is_htmx`, `boosted`, `target`, `trigger`) and construct partial HTML responses with `HtmxResponse` (`trigger`, `retarget`, `reswap`, `push_url`):

```python
from vertebra.framework.htmx import HtmxRequest, HtmxResponse, HxSwap

hx = HtmxRequest.from_headers({"HX-Request": "true", "HX-Target": "items-list"})
response = HtmxResponse(
    "<li>New item</li>",
    trigger="itemAdded",
    retarget="#items-list",
    reswap=HxSwap.BEFOREEND,
)
```

## TypeScript Client SDK & Distribution (`@vertebra/client`)

### Wheel-Native Zero-NPM Distribution (Default)

Vertebra bundles `@vertebra/client` directly inside the Python wheel (`vertebra-core`). This eliminates the requirement to publish to or install from the external NPM registry, removing supply-chain attack vectors, package-name squatting, and multi-registry release desynchronization:

- **Browser & Vanilla ESM**: When running Vertebra's server (`vtb run` or `App`), the client bundle and all 15 subpath entrypoints are served natively under `/__vertebra/client/*` (e.g. `/__vertebra/client/index.js`, `/__vertebra/client/react/index.js`, `/__vertebra/client/live/index.js`). In vanilla HTML, import modules directly without Node or NPM:
  ```html
  <script type="module">
    import { HttpClient } from "/__vertebra/client/index.js";
    const client = new HttpClient({ baseURL: "/" });
  </script>
  ```
- **Type Generation & Linking (`vtb js`)**: Run `vtb js install` to link the packaged TypeScript declaration files (`.d.ts`) into `.vertebra/client` and generate `.vertebra/importmap.json` for IDE autocomplete and type checking.

### External Tooling & Vite Integration (Optional)

For teams with existing Node.js pipelines or custom build workflows, `@vertebra/client` preserves standard `package.json` subpath exports and full compatibility with Vite, Webpack, and external bundlers:

```typescript
import { HttpClient } from "@vertebra/client";
import { LiveStateClient } from "@vertebra/client/live";
import { initHtmx } from "@vertebra/client/htmx";

const client = new HttpClient({ baseURL: "http://localhost:8000" });
const live = new LiveStateClient(
  "counter",
  { count: 0 },
  { url: "ws://localhost:8000/ws/live" },
);

initHtmx({ headers: { "X-App-Client": "vertebra" } });
```

Framework hooks and providers (`@vertebra/client/react`, `/vue`, `/svelte`, `/angular`, `/vite`):

```tsx
import { ClientProvider, useQuery, useMutation } from "@vertebra/client/react";
import { vitePlugin } from "@vertebra/client/vite";
```
