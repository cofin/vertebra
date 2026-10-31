# Frontend, Inertia v3 & QuickJS SSR

Vertebra embeds a Rust-native JavaScript bundler (`vertebra-bundler` powered by Rolldown and Oxc) and an in-process QuickJS server-side rendering pool (`EmbeddedSsrClient`), paired with the `@vertebra/client` TypeScript SDK.

## Embedded Rolldown/Oxc Bundler

Compile TypeScript, JSX/TSX, and CSS assets directly from Python without spawning an external Node.js build server:

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
uv run vtb js build
```

## Inertia v3 Protocol (`vertebra.framework.inertia`)

Build single-page React, Vue, or Svelte applications driven by server-side handlers without manually building a separate REST layer. Vertebra supports Inertia v3 partial reloads, deferred props (`DeferredProp`), deep/shallow merge props (`MergeProp`), scroll pagination props (`ScrollProp`), and lazy props (`OptionalProp`):

```python
from vertebra.framework.inertia import (
    DeferredProp,
    InertiaConfig,
    InertiaResponse,
)
from vertebra.framework.ssr import SsrConfig

config = InertiaConfig(
    enabled=True,
    version="1.0",
    ssr=SsrConfig(bundle_path="static/ssr/ssr.js"),
)
page = InertiaResponse(
    component="Dashboard/Index",
    props={
        "summary": {"active_users": 42},
        "metrics": DeferredProp(
            lambda: {"p99_ms": 1.4},
            group="analytics",
        ),
    },
)
```

## Pooled QuickJS Server-Side Rendering (`vertebra.framework.ssr`)

Render JavaScript/TypeScript page components on the server using the Rust-managed QuickJS worker pool (`SsrConfig`, `EmbeddedSsrClient`, `SsrResult`) with zero local TCP loopback overhead:

```python
from vertebra.framework.ssr import EmbeddedSsrClient, SsrConfig

ssr = EmbeddedSsrClient(
    SsrConfig(bundle_path="static/ssr/ssr.js", pool_size=4)
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

## TypeScript Client SDK (`@vertebra/client`)

Generate end-to-end TypeScript route contracts with `vtb js generate-types` and consume them with `@vertebra/client`:

```typescript
import { HttpClient } from "@vertebra/client";
import { LiveStateClient } from "@vertebra/client/live";

const client = new HttpClient({ baseUrl: "http://localhost:8000" });
const live = new LiveStateClient({ url: "ws://localhost:8000/ws/live" });
```
