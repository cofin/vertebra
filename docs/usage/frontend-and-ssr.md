# Frontend, Inertia v3 & QuickJS SSR

Vertebra embeds a Rust-native JavaScript bundler (`vertebra-bundler` powered by Rolldown and Oxc) and an in-process QuickJS server-side rendering pool (`SsrClient`), paired with the `@vertebra/client` TypeScript SDK.

## Embedded Rolldown/Oxc Bundler

Compile TypeScript, JSX/TSX, and CSS assets directly from Python without spawning an external Node.js build server:

```python
from vertebra._bundler import Bundler

bundler = Bundler(
    entry_points=["frontend/src/main.tsx"],
    out_dir="static/dist",
    minify=True,
    sourcemap=True,
)
result = bundler.bundle()
```

Or run from the CLI:

```bash
uv run vtb js build
```

## Inertia v3 Protocol (`vertebra.framework.inertia`)

Build single-page React, Vue, or Svelte applications driven by server-side controllers without manually building a separate REST layer. Vertebra supports Inertia v3 partial reloads, deferred props (`DeferProp`), deep/shallow merge props (`MergeProp`), scroll pagination props (`ScrollProp`), and lazy props (`OptionalProp`):

```python
from vertebra.framework.inertia import DeferProp, InertiaConfig, InertiaResponse

config = InertiaConfig(version="1.0", ssr_enabled=True)
page = InertiaResponse(
    component="Dashboard/Index",
    props={
        "summary": {"active_users": 42},
        "metrics": DeferProp( lambda: {"p99_ms": 1.4}, group="analytics" ),
    },
    url="/dashboard",
)
```

## Pooled QuickJS Server-Side Rendering (`vertebra.framework.ssr`)

Render JavaScript/TypeScript page components on the server using the Rust-managed QuickJS worker pool (`SsrConfig`, `SsrClient`, `SsrResult`) with zero local TCP loopback overhead:

```python
from vertebra.framework.ssr import SsrClient, SsrConfig

ssr = SsrClient(SsrConfig(bundle_path="static/ssr/ssr.js", pool_size=4))
rendered = ssr.render(component="Dashboard/Index", props={"title": "Overview"})
```

## TypeScript Client SDK (`@vertebra/client`)

Generate end-to-end TypeScript route contracts with `vtb js generate-types` and consume them with `@vertebra/client`:

```typescript
import { HttpClient } from "@vertebra/client";
import { LiveStateClient } from "@vertebra/client/live";

const client = new HttpClient({ baseUrl: "http://localhost:8000" });
const live = new LiveStateClient({ url: "ws://localhost:8000/ws/live" });
```
