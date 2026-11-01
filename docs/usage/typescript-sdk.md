# TypeScript SDK & Framework Adapters (`@vertebra/client`)

Vertebra bundles `@vertebra/client` directly inside the Python wheel (`vertebra-core`) and serves all subpath entrypoints from `/__vertebra/client/*`, while also supporting external Vite/Webpack/Bun toolchains.

## Wheel-Native Zero-NPM Distribution (Default)

When running Vertebra's server (`vtb run` or `App`), the client bundle and all 15 subpath entrypoints are served natively under `/__vertebra/client/*` (such as `/__vertebra/client/index.js`, `/__vertebra/client/react/index.js`, and `/__vertebra/client/live/index.js`). In vanilla HTML, import modules directly without Node or NPM:

```html
<script type="module">
  import { HttpClient } from "/__vertebra/client/index.js";
  const client = new HttpClient({ baseURL: "/" });
</script>
```

Run `vtb js install` to link the packaged TypeScript declaration files (`.d.ts`) into `.vertebra/client` and generate `.vertebra/importmap.json` for IDE autocomplete and type checking:

```bash
vtb js install
vtb js generate-types app:app --output frontend/src/generated
```

## External Tooling & Vite Integration

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

## Framework Adapters (`react`, `vue`, `svelte`, `angular`, `vite`)

Import framework-specific hooks, stores, and Vite plugins from `@vertebra/client/<subpath>`:

```tsx
import { ClientProvider, useQuery, useMutation } from "@vertebra/client/react";
import { vitePlugin } from "@vertebra/client/vite";
```

See the [`@vertebra/client` API Reference](../reference/typescript-client.md) for all 15 subpath modules and exported classes.
