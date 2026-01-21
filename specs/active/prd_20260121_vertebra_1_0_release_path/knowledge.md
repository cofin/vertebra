# Knowledge Capture: Vertebra 1.0 Release Path

## Patterns Discovered
- **Portable Runtime Distribution:** Using `python-build-standalone` (PBS) allows us to ship a full Python environment without relying on the system Python. This decouples our "User Application Distribution" from the "Framework Distribution".
- **Asset Caching:** Downloading large artifacts (like PBS or the Engine) requires robust caching in `~/.cache/vertebra` or XDG equivalent to avoid re-downloading on every build.
- **Async CLI Commands:** Wrapping async logic (like downloading/bundling) with `asyncio.run()` inside synchronous `click` commands provides a clean bridge between the CLI interface and async core logic.
- **MCP Initialization:** The Model Context Protocol (MCP) requires a stateful `initialize` handshake before any other tools can be called. The server must reject other requests with error code `-32002` (Server not initialized) until this handshake completes.
- **Python-to-JSON Schema:** Initial implementation uses `inspect` to map basic Python types (`int`, `str`, `bool`) to JSON Schema. This works for simple tools but will need to be upgraded to use `msgspec.json.schema` or `pydantic` for complex nested objects.
- **Bun Integration Testing:** Using `Bun.serve` in unit tests allows for realistic testing of HTTP, SSE, and WebSocket transports without needing external mocks or complex setups.

## Lessons Learned
- **Async Downloading:** Using `httpx` with `stream()` is essential for large file downloads to keep memory usage low and provide progress feedback capabilities (even if minimal initially).
- **Test Mocking:** Mocking async iterators (like `response.aiter_bytes`) requires a specific pattern (async generator function) rather than a simple `MagicMock` return value.
- **Patching Modules vs Imports:** When testing code that uses `from module import Class`, you must patch `module.Class` where it is *imported*, not where it is *defined*, if the import happens at the top level. However, if the import is inside a function (to avoid circular deps or startup cost), you patch the source module path.

## Decisions Made
- **HTTP Client:** Chose `httpx` for downloading PBS releases because it supports async/await and streaming out of the box, aligning with our modern Python stack.
- **Profile Strategy:** Adopted a `modern` (x86-64-v3) vs `baseline` (x86-64) profile strategy to balance performance on new hardware with compatibility on legacy systems.
- **User App Distribution:** Implemented via `vertebra build --one-file` as the primary mechanism for users to package their apps, distinct from the framework's PyPI distribution.
- **MCP Method Mapping:** Explicitly mapped JSON-RPC methods (`tools/list`, `tools/call`) to internal handlers rather than dynamic dispatch, to strictly enforce the protocol structure.
- **Transport Separation:** SSE and WebSocket logic is separated into distinct classes (`SseTransport`, `WebSocketTransport`) rather than embedded in the server, making testing and maintenance easier.

## Reusable Components
- `vertebra.build.standalone.StandaloneManager`: Can be reused for any tool that needs to download and manage PBS versions.
- `vertebra.build.bundler.Bundler`: Generic orchestration for assembling Python applications with an embedded runtime.
- `McpServer`: A reusable TypeScript class for handling the core MCP protocol state and dispatch logic.
- `@mcp_tool`: Python decorator for easily exposing functions as MCP tools.
- `SseTransport` / `WebSocketTransport`: Generic transport implementations for the MCP protocol.

## Gotchas & Warnings
- **Platform Mapping:** ARM64 mappings on PBS don't always distinguish between `baseline` and `modern` in the same way x64 does. We currently map them to the same artifact, but this might change.
