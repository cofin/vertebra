# `@vertebra/client` — TypeScript Client SDK Reference

Full-stack TypeScript SDK (`src/ts/vertebra-client/`) providing typed HTTP requests, retries, SSE and WebSocket streaming, binary `LiveState` synchronization, Oracle APEX island bridges, Apache Arrow streaming, WebAssembly execution, MCP/A2A agent clients, and framework adapters.

## Distribution Model: Zero-NPM Wheel-Native Packaging

`@vertebra/client` is packaged and distributed directly within the `vertebra-core` Python wheel (`src/py/vertebra/server/_js/bundle/`), eliminating the requirement to publish to or install from the external NPM registry:

- **Native Runtime Serving**: When running Vertebra applications (`vtb run` or `App`), the runtime serves all ESM modules dynamically under `/__vertebra/client/*` (e.g. `/__vertebra/client/index.js`, `/__vertebra/client/react/index.js`, `/__vertebra/client/live/index.js`) with proper MIME types and cached HTTP headers.
- **Type Linking (`vtb js`)**: `vtb js install` vendors and links the wheel's TypeScript declarations (`.d.ts`) into `.vertebra/client` and configures `.vertebra/importmap.json` for IDE autocomplete without Node or NPM.
- **Vite & External Bundler Compatibility**: The package preserves full `package.json` subpath exports and ESM semantics for projects using external Vite, Webpack, or Node.js toolchains.

## Package Subpath Exports (`package.json`)

`@vertebra/client` exposes tree-shakeable ESM entry points:

| Import Path | Description |
| --- | --- |
| `@vertebra/client` | Root barrel exporting core HTTP, SSE, WebSocket, LiveState, Apex, Arrow, Wasm, HMR, Store, Telemetry, Router, WebMCP, and Agent clients |
| `@vertebra/client/vite` | Vite plugin (`vitePlugin`) for Vertebra asset bundling, SSR entrypoints, and typegen |
| `@vertebra/client/react` | React context provider (`ClientProvider`) and typed HTTP query/mutation hooks (`useQuery`, `useMutation`) |
| `@vertebra/client/vue` | Vue 3 provider (`provideClient`) and typed query/mutation composables (`useQuery`, `useMutation`) |
| `@vertebra/client/svelte` | Svelte context helpers (`setClient`, `getClient`) and query/mutation stores (`createQuery`, `createMutation`) |
| `@vertebra/client/angular` | Angular client service (`ClientService`) and query/mutation helpers (`injectQuery`, `injectMutation`) |
| `@vertebra/client/htmx` | HTMX request header and JSON payload helper (`initHtmx`) |
| `@vertebra/client/store` | Client-side reactive `SessionStore` and `store` singleton |
| `@vertebra/client/hmr` | `HotReloadClient`, `initHotReload`, and `ServerMessage` HMR wire protocol |
| `@vertebra/client/wasm` | `WASMClient`, `WASMClientOptions`, and `WASM_MAGIC_BYTES` loader |
| `@vertebra/client/arrow` | `ArrowStreamClient`, `ArrowStreamOptions`, and Apache Arrow IPC stream constants |
| `@vertebra/client/live` | `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue`, `StateOpcode`, and FNV-1a hashing |
| `@vertebra/client/apex` | `ApexRegion`, `ApexOfflineOutbox`, and Oracle APEX island hydration descriptors |
| `@vertebra/client/telemetry` | W3C `TraceContext` (`traceparent`) parser, generator, and header injector |
| `@vertebra/client/router` | Type-safe URL and WebSocket route helpers (`routeUrl`, `wsUrl`, `interpolateRoutePath`, `RouteUrlOptions`) |
| `@vertebra/client/agent` | `McpClient`, `A2aClient`, `AgentStreamClient`, and `WorkflowLiveSubscriber` |
| `@vertebra/client/webmcp` | Browser WebMCP (`navigator.modelContext`) auto-registration (`registerWebMCPTools`, `WebMCPManifest`, `WebMCPToolDescriptor`) |

## HTTP Client, Retries & Error Hierarchy

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
  type RouteResponse,
  type RouteUrlOptions,
} from "@vertebra/client";
```

| Export | Kind | Description |
| --- | --- | --- |
| `HttpClient` | Class | Type-safe fetch client with automatic JSON serialization, timeout handling, and retry backoff (`baseURL`). |
| `ClientConfig` | Type | Base URL, default headers, timeout, and `RetryConfig` options for `HttpClient`. |
| `RetryConfig` | Type | Maximum attempts, base delay, jitter, and retryable HTTP status codes. |
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

## Real-Time Transports: SSE, WebSocket & HMR

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

## Reactive `LiveState` & Session Store

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

## Agent Protocols: MCP, WebMCP & A2A (`@vertebra/client/agent`, `@vertebra/client/webmcp`)

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

## Oracle APEX Island Bridge (`@vertebra/client/apex`)

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

## Apache Arrow, WebAssembly & W3C Telemetry

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

## Frontend Framework Adapters (`/vite`, `/react`, `/vue`, `/svelte`, `/angular`, `/htmx`)

| Subpath | Exports | Description |
| --- | --- | --- |
| `@vertebra/client/vite` | `vitePlugin`, `VertebraViteOptions` | Vite plugin configuring client/SSR entrypoints, hot reload, and automatic `vtb js generate-types` runs. |
| `@vertebra/client/react` | `ClientProvider`, `useClient`, `useQuery`, `useMutation` | React context provider and typed data-fetching and mutation hooks backed by `HttpClient`. |
| `@vertebra/client/vue` | `provideClient`, `useClient`, `useQuery`, `useMutation` | Vue 3 dependency injection helpers and reactive query/mutation composables. |
| `@vertebra/client/svelte` | `setClient`, `getClient`, `createQuery`, `createMutation` | Svelte context helpers and reactive query/mutation stores. |
| `@vertebra/client/angular` | `ClientService`, `injectQuery`, `injectMutation` | Angular injectable HTTP client wrapper and reactive query/mutation helpers. |
| `@vertebra/client/htmx` | `initHtmx`, `HtmxConfig` | Attaches custom headers and `Content-Type: application/json` mutation encoding to HTMX requests. |
