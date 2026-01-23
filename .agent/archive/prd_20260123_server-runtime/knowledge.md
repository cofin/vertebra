# Knowledge Capture: Server & Runtime - MCP, Health Checks, and Observability

## Patterns Discovered
- **Stream Mocking**: Mocking `Readable` and `Writable` streams is effective for testing stdio-based transports without spawning subprocesses.
- **Readline for JSON-RPC**: `node:readline` simplifies parsing line-delimited JSON-RPC messages from input streams.
- **TDD Verification**: Validating pre-existing code with new tests confirms implementation readiness without unnecessary rewrites.
- **Library Resolution in Tests**: Tests relying on shared libraries via FFI need explicit path handling or environment variables (`VERTEBRA_SHM_LIB`) that account for the CWD difference between test execution and runtime. Relative path resolution from `import.meta.dir` is most robust.
- **Singleton Registry**: Used a singleton pattern for `ProcedureRegistry` to allow decorators to register tools without explicit context passing. This simplifies the `@mcp_tool` API.
- **Type Extraction**: `TypeExtractor.from_hint` is powerful but limited for runtime classes (like Dataclasses) without AST context. Combining it with `dataclasses.fields` or `msgspec.json.schema` provides a robust fallback for complex types.
- **ContextVars for Side Channels**: Using `contextvars.ContextVar` to inject side-channel capabilities (like progress reporting) into functions keeps the function signature clean and decoupled from the transport mechanism.

## Lessons Learned
- **Decorator Metadata**: `functools.wraps` is essential but insufficient if the registry needs to store the wrapper itself. Ensuring consistency between the registered handler and the decorated function is key.
- **Schema Generation**: `inspect.Parameter` does not have `description`. Docstring parsing is required for parameter descriptions, which is separate from signature inspection.
- **Metrics Aggregation**: Aggregating metrics from multiple subsystems (Broker, Server, MCP) into a single Prometheus endpoint simplifies observability configuration but requires careful coordination of registry state.

## Decisions Made
- **Stdio Transport**: Implemented using `readline` for robustness against partial chunks and simplified line handling.
- **Decorator Implementation**: `functools.wraps` is critical for preserving metadata on decorated functions. The registry stores the *wrapper* function to ensure any middleware logic in the wrapper is executed when the tool is called via the registry.
- **Schema Generation Strategy**: Use `TypeExtractor` for standard types and `dataclasses` introspection for complex types to generate JSON Schema on the fly.
- **Unified Metrics**: Exposed a single `/__metrics__` endpoint managed by the Bun server to serve as the single source of truth for all system metrics.

## Reusable Components
- `StdioTransport`, `SSETransport`, `WebSocketTransport`: Generic MCP transport implementations.
- `ProcedureRegistry`: Generic registry pattern adaptable for other plugin systems.
- `MCPSchemaGenerator`: Can be reused to generate JSON Schemas for any Python function.
- `PrometheusFormatter`: A lightweight, dependency-free utility for generating Prometheus exposition format.

## Gotchas & Warnings
- **Test CWD**: Bun tests run with CWD as the project root or the directory where `bun test` was invoked. FFI library loading logic must be robust to this.
- **Decorator Registration**: When registering decorated functions, ensure you register the correct callable (wrapper vs original) depending on intended behavior.
- **Serialization**: Ensure consistent use of `vertebra.serializers` across all IPC boundaries to handle custom types like `UUID` and `datetime` correctly.