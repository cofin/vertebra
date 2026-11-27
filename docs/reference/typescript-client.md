# TypeScript Client SDK

`@vertebra/client` (`src/ts/vertebra-client/`) provides typed HTTP requests, retries, SSE and WebSocket streams, binary `LiveState` sync, Oracle APEX island bridges, Apache Arrow streaming, WebAssembly loading, MCP and A2A clients, and framework adapters.

## Distribution & Wheel Packaging

`@vertebra/client` ships inside the `vertebra-core` Python wheel (`src/py/vertebra/server/_js/bundle/`), so projects do not need npm to install or serve it:

- **Runtime Serving**: Vertebra serves ESM modules under `/__vertebra/client/*` (such as `/__vertebra/client/index.js` and `/__vertebra/client/react/index.js`) with cache headers and MIME types.
- **Local Type Linking**: `vtb js install` links the bundled `.d.ts` files into `.vertebra/client` and writes `.vertebra/importmap.json` for editor autocomplete without Node or npm.
- **External Bundlers**: The package keeps standard `package.json` subpath exports and ESM entry points for Vite, Webpack, or Node toolchains.

## Package Subpath Exports

`@vertebra/client` exposes tree-shakeable ESM entry points:

| Import Path | Description |
| --- | --- |
| `@vertebra/client` | Root barrel re-exporting HTTP, SSE, WebSocket, live state, APEX, Arrow, Wasm, HMR, store, telemetry, router, WebMCP, and agent clients. |
| `@vertebra/client/vite` | Vite plugin for asset bundling, SSR entrypoints, and typegen. |
| `@vertebra/client/react` | React context provider and typed query and mutation hooks. |
| `@vertebra/client/vue` | Vue 3 provider and reactive query and mutation composables. |
| `@vertebra/client/svelte` | Svelte context helpers and reactive query and mutation stores. |
| `@vertebra/client/angular` | Angular client service and query and mutation helpers. |
| `@vertebra/client/htmx` | HTMX request header and JSON payload helper. |
| `@vertebra/client/store` | Client-side reactive session store and default singleton. |
| `@vertebra/client/hmr` | Development hot-reload client and HMR wire protocol types. |
| `@vertebra/client/wasm` | Browser WebAssembly loader, options, and magic-byte constants. |
| `@vertebra/client/arrow` | Apache Arrow IPC stream client, options, and wire constants. |
| `@vertebra/client/live` | Binary live-state client, hybrid logical clock, offline delta queue, opcodes, and FNV-1a hashing. |
| `@vertebra/client/apex` | Oracle APEX region bridge, offline outbox, and island descriptors. |
| `@vertebra/client/telemetry` | W3C `traceparent` parser, generator, and header injector. |
| `@vertebra/client/router` | Type-safe HTTP and WebSocket route URL interpolation helpers. |
| `@vertebra/client/agent` | MCP, A2A, agent streaming, and workflow live-state clients. |
| `@vertebra/client/webmcp` | Browser WebMCP auto-registration and tool manifest types. |

## HTTP Client, Retries & Errors

```typescript
import {
  HttpClient,
  ClientError,
  NetworkError,
  TimeoutError,
  ServerError,
  ValidationError,
  throwParsedError,
  computeBackoffDelay,
  routeUrl,
  wsUrl,
  interpolateRoutePath,
  type ClientConfig,
  type RetryConfig,
  type RouteDefinition,
  type RouteRegistry,
  type RouteResponse,
  type RouteUrlOptions,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `HttpClient` | Class | Type-safe fetch client with automatic JSON serialization, timeout handling, and retry backoff (`baseURL`). |
| `ClientConfig` | Type | Base URL, default headers, timeout, and `RetryConfig` options for `HttpClient`. |
| `RetryConfig` | Type | Maximum attempts, base delay, jitter, and retryable HTTP status codes. |
| `RouteDefinition` | Type | Generated route descriptor containing HTTP `method` and `path` pattern. |
| `RouteRegistry` | Type | Immutable mapping of route names to `RouteDefinition` entries. |
| `RouteResponse` | Type | Typed response wrapper pairing HTTP status, headers, and decoded payload. |
| `RouteUrlOptions` | Type | Path parameters, query parameters, and base URL options for `routeUrl` and `wsUrl`. |
| `routeUrl` | Function | Resolves a named route or path template into a formatted HTTP URL with interpolated parameters and query string. |
| `wsUrl` | Function | Resolves a named route or path template into a `ws://` or `wss://` WebSocket URL. |
| `interpolateRoutePath` | Function | Interpolates `{param}` placeholders in a route template with URL-encoded parameter values. |
| `computeBackoffDelay` | Function | Computes exponential backoff delay with bounded jitter for a retry attempt. |
| `ClientError` | Class | Base error class carrying an optional HTTP `status` code. |
| `NetworkError` | Class | Raised when the underlying `fetch` call fails due to a transport error. |
| `TimeoutError` | Class | Raised when a request exceeds the configured timeout (`status = 408`). |
| `ServerError` | Class | Raised on `5xx` HTTP responses. |
| `ValidationError` | Class | Raised on `422 Unprocessable Entity` responses with structured `fieldErrors`. |
| `throwParsedError` | Function | Parses an error `Response` and throws the matching `ClientError` subclass. |

## Real-Time Transports

```typescript
import {
  SseClient,
  type SseClientOptions,
  type SseEventHandler,
  type SseMessageMeta,
  WebSocketClient,
  type WebSocketClientOptions,
  type PubSubWireFrame,
  HotReloadClient,
  initHotReload,
  type ServerMessage,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `SseClient` | Class | Reconnecting Server-Sent Events client with `Last-Event-ID` tracking. |
| `SseClientOptions` | Type | Connection URL, headers, and reconnection backoff settings for `SseClient`. |
| `SseEventHandler` | Type | Callback signature invoked for each parsed SSE event frame. |
| `SseMessageMeta` | Type | Event ID, event type, and retry metadata accompanying an SSE payload. |
| `WebSocketClient` | Class | Multiplexed WebSocket client supporting pub/sub channels and automatic reconnect. |
| `WebSocketClientOptions` | Type | WebSocket URL, protocols, heartbeat interval, and reconnect policy. |
| `PubSubWireFrame` | Type | Wire frame structure for topic subscribe, publish, and unsubscribe messages. |
| `HotReloadClient` | Class | Development HMR client that applies CSS updates and page reloads from `vertebra-bundler`. |
| `initHotReload` | Function | Bootstraps a singleton `HotReloadClient` in browser development mode. |
| `ServerMessage` | Type | Discriminated union of HMR build, reload, and error messages from the dev server. |

## Live State & Session Store

```typescript
import {
  LiveStateClient,
  type LiveStateListener,
  type StateDeltaMessage,
  StateOpcode,
  type StateOpcodeValue,
  HybridLogicalClock,
  OfflineDeltaQueue,
  fnv1aFieldId,
  fnv1aStateId,
  SessionStore,
  store,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `LiveStateClient` | Class | Synchronizes component state with `vertebra.framework.live.LiveState` over binary delta frames. |
| `LiveStateListener` | Type | Listener callback invoked whenever a state snapshot or delta patch is applied. |
| `StateDeltaMessage` | Type | Decoded state transition message containing `StateOpcode`, field hash, and HLC timestamp. |
| `StateOpcode` | Const | Numeric opcodes (`FieldSet`, `FieldDelete`, `ObjectReplace`, `SyncRequest`, `SyncResponse`, `ActionCall`, `ActionResult`, `BatchPatch`) for binary state deltas. |
| `StateOpcodeValue` | Type | Union of valid `StateOpcode` numeric values. |
| `HybridLogicalClock` | Class | Monotonic physical + logical timestamp generator for causal ordering of offline mutations. |
| `OfflineDeltaQueue` | Class | Persistent queue buffering local state mutations while disconnected. |
| `fnv1aFieldId` | Function | Computes a 32-bit FNV-1a hash for a state field name. |
| `fnv1aStateId` | Function | Computes a 64-bit FNV-1a hash for a state room identifier. |
| `SessionStore` | Class | Reactive key-value store backed by `sessionStorage` / `localStorage` with cross-tab sync. |
| `store` | Const | Default shared `SessionStore` instance. |

## Agent Protocols: MCP, WebMCP & A2A

```typescript
import {
  McpClient,
  type McpClientOptions,
  MCP_PROTOCOL_VERSION,
  encodeMcpHeaderValue,
  A2aClient,
  type A2aClientOptions,
  AgentStreamClient,
  type AgentStreamClientOptions,
  WorkflowLiveSubscriber,
  type WorkflowLiveStatePayload,
  registerWebMCPTools,
  type RegisterWebMCPOptions,
  type WebMCPManifest,
  type WebMCPRegistrationHandle,
  type WebMCPToolDescriptor,
  type BrowserModelContext,
  type BrowserModelContextTool,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `McpClient` | Class | Streamable HTTP client for invoking MCP tools, reading resources, and fetching prompts. |
| `McpClientOptions` | Type | Endpoint URL, session headers, and timeout options for `McpClient`. |
| `MCP_PROTOCOL_VERSION` | Const | Supported Model Context Protocol version constant. |
| `encodeMcpHeaderValue` | Function | Base64url-encodes structured metadata headers for MCP HTTP requests. |
| `A2aClient` | Class | Agent-to-Agent client for fetching `AgentCard` metadata and managing remote tasks. |
| `A2aClientOptions` | Type | Configuration options for `A2aClient`. |
| `AgentStreamClient` | Class | Streams token deltas, tool call events, and confirmation tickets from `AgentRuntime`. |
| `AgentStreamClientOptions` | Type | Connection and replay options for `AgentStreamClient`. |
| `WorkflowLiveSubscriber` | Class | Subscribes to step transitions emitted by `vertebra.agent.workflow.WorkflowEngine`. |
| `WorkflowLiveStatePayload` | Type | Typed state payload delivered to `WorkflowLiveSubscriber` listeners. |
| `registerWebMCPTools` | Function | Fetches `/.well-known/webmcp.json` and registers advertised tools with `document.modelContext` or `navigator.modelContext`. |
| `RegisterWebMCPOptions` | Type | Options (`manifestUrl`, `modelContext`, `fetchImpl`, `headers`, `signal`) for `registerWebMCPTools`. |
| `WebMCPRegistrationHandle` | Type | Handle returned by `registerWebMCPTools` containing `tools`, per-tool `errors`, and `dispose()`. |
| `WebMCPManifest` | Type | Discovery payload schema served at `/.well-known/webmcp.json`. |
| `WebMCPToolDescriptor` | Type | Tool descriptor schema with `name`, `title`, `description`, `inputSchema`, `outputSchema`, `endpoint`, and `requiresConfirmation`. |
| `BrowserModelContext` | Type | W3C WebMCP `document.modelContext` / `navigator.modelContext` browser interface (`registerTool`, `unregisterTool`). |
| `BrowserModelContextTool` | Type | Tool registration object passed to `modelContext.registerTool(tool, { signal })`. |

## Oracle APEX Island Bridge

```typescript
import {
  ApexRegion,
  type ApexRegionBridgeOptions,
  ApexOfflineOutbox,
  type ApexOfflineMutationEntry,
  type ApexOfflineOperation,
  type ApexGlobalNamespace,
  type ApexItemHandle,
  type IslandDescriptor,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `ApexRegion` | Class | Bridges an Oracle APEX region DOM container to a reactive Vertebra frontend island. |
| `ApexRegionBridgeOptions` | Type | Region ID, hydration mode, and page item binding options for `ApexRegion`. |
| `ApexOfflineOutbox` | Class | Queues and replays APEX page item and tabular mutations across offline/online transitions. |
| `ApexOfflineMutationEntry` | Type | Queued mutation record stored inside `ApexOfflineOutbox`. |
| `ApexOfflineOperation` | Type | Allowed mutation operation kinds (`insert`, `update`, `delete`, `action`). |
| `ApexGlobalNamespace` | Type | TypeScript interface describing the browser `window.apex` global API. |
| `ApexItemHandle` | Type | Typed wrapper around `apex.item(name)` get/set/change operations. |
| `IslandDescriptor` | Type | Metadata describing a mounted component island inside an APEX page. |

## Apache Arrow, WebAssembly & Telemetry

```typescript
import {
  ArrowStreamClient,
  type ArrowStreamOptions,
  ARROW_CONTINUATION_MARKER,
  ARROW_FILE_MAGIC,
  ARROW_STREAM_MIME,
  WASMClient,
  type WASMClientOptions,
  WASM_MAGIC_BYTES,
  TraceContext,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `ArrowStreamClient` | Class | Fetches binary Apache Arrow IPC streams (`fetchBytes(): Promise<Uint8Array>`) over HTTP. |
| `ArrowStreamOptions` | Type | `endpoint` URL and custom request `headers` options for `ArrowStreamClient`. |
| `ARROW_STREAM_MIME` | Const | MIME type constant (`application/vnd.apache.arrow.stream`). |
| `ARROW_FILE_MAGIC` | Const | 6-byte `ARROW1` magic header for Arrow IPC files. |
| `ARROW_CONTINUATION_MARKER` | Const | `0xFFFFFFFF` continuation marker for Arrow IPC message framing. |
| `WASMClient` | Class | Fetches, validates, compiles, and instantiates WebAssembly modules in the browser. |
| `WASMClientOptions` | Type | Import object, memory bounds, and caching options for `WASMClient`. |
| `WASM_MAGIC_BYTES` | Const | 4-byte `\0asm` binary magic header verified before compilation. |
| `TraceContext` | Class | W3C Trace Context (`traceparent`) parser, generator, and header injector. |

## Frontend Framework Adapters

| Subpath | Exports | Description |
| --- | --- | --- |
| `@vertebra/client/vite` | `vitePlugin`, `VertebraViteOptions` | Vite plugin configuring client/SSR entrypoints, hot reload, and automatic `vtb js generate-types` runs. |
| `@vertebra/client/react` | `ClientProvider`, `useClient`, `useQuery`, `useMutation` | React context provider and typed data-fetching and mutation hooks backed by `HttpClient`. |
| `@vertebra/client/vue` | `provideClient`, `useClient`, `useQuery`, `useMutation` | Vue 3 dependency injection helpers and reactive query/mutation composables. |
| `@vertebra/client/svelte` | `setClient`, `getClient`, `createQuery`, `createMutation` | Svelte context helpers and reactive query/mutation stores. |
| `@vertebra/client/angular` | `ClientService`, `injectQuery`, `injectMutation` | Angular injectable HTTP client wrapper and reactive query/mutation helpers. |
| `@vertebra/client/htmx` | `initHtmx`, `HtmxConfig` | Attaches custom headers and `Content-Type: application/json` mutation encoding to HTMX requests. |
