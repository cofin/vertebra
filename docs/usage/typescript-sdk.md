# TypeScript SDK & Framework Adapters

Vertebra packages `@vertebra/client` inside `vertebra-core` and serves its subpath entrypoints from `/__vertebra/client/*`. It also works with Vite, Webpack, and Bun toolchains.

## Built-In Browser Distribution

When running Vertebra, the server exposes `@vertebra/client` and its subpath modules under `/__vertebra/client/*` (for example `/__vertebra/client/index.js` or `/__vertebra/client/react/index.js`). Import them directly in HTML without Node or npm:

```html
<script type="module">
  import { HttpClient } from "/__vertebra/client/index.js";
  const client = new HttpClient({ baseURL: "/" });
</script>
```

Run `vtb js install` to link packaged TypeScript declarations into `.vertebra/client` and generate an import map for editor autocomplete and type checking:

```bash
vtb js install
vtb js generate-types app:app --output frontend/src/generated
```

## External Bundlers & Vite

For existing Node.js or Vite build pipelines, the package exports standard package.json subpaths:

```typescript
import { HttpClient } from "@vertebra/client";
import { initHtmx } from "@vertebra/client/htmx";

const client = new HttpClient({ baseURL: "http://localhost:8000" });

initHtmx({ headers: { "X-App-Client": "vertebra" } });
```

## Live State

`LiveStateClient` stores reactive state. Connect it to the application transport
that decodes server frames into `StateDeltaMessage` objects. Use the Python
`LiveState` class name when constructing the client so both sides derive the same
state ID. Preserve `stateId` and `timestampNs` as `bigint` values when decoding
MessagePack.

```typescript
import { LiveStateClient, type StateDeltaMessage } from "@vertebra/client/live";

export function bindCounter(
  output: HTMLOutputElement,
  subscribeToDeltas: (
    receive: (delta: StateDeltaMessage) => void,
  ) => () => void,
): () => void {
  const live = new LiveStateClient("CounterLive", { count: 0 });
  const render = ({ count }: { count: number }) => {
    output.value = String(count);
  };

  render(live.getSnapshot());
  const unsubscribe = live.subscribe(render);
  const disconnect = subscribeToDeltas((delta) => {
    live.applyServerDelta(delta);
  });

  return () => {
    disconnect();
    unsubscribe();
  };
}
```

Pass the transport's decoded-message subscription as `subscribeToDeltas` and
call the returned function when the UI unmounts. The optional `onSend` callback
receives locally produced deltas. Set the `connected` option for the initial
connection state and call `setConnected()` when it changes.

## Framework Adapters

Import framework-specific hooks, stores, and Vite plugins from `@vertebra/client/<framework>` (supports React, Vue, Svelte, Angular, and Vite):

```tsx
import { ClientProvider, useQuery, useMutation } from "@vertebra/client/react";
import { vitePlugin } from "@vertebra/client/vite";
```

See the [`@vertebra/client` API Reference](../reference/typescript-client.md) for the full list of subpath modules and exported classes.
