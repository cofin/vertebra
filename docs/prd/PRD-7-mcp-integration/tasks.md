# PRD-7: MCP Integration - Implementation Tasks

**Status**: Planning Complete
**Created**: 2026-01-17
**Est. Duration**: 14-16 weeks

---

## Phase 1: MCP Server Core (Weeks 1-2)

- [ ] Define MCP server package layout in `src/js/src/mcp/`
- [ ] Implement JSON-RPC 2.0 request/response handling
- [ ] Implement stdio transport (baseline)
- [ ] Add structured logging + request IDs
- [ ] Add conformance tests for MCP core methods

## Phase 2: Tool/Resource Registry (Weeks 3-4)

- [ ] Implement `@vertebra.tool` + `@vertebra.resource` decorators (Python)
- [ ] Build registry (Python) with metadata extraction
- [ ] Add registry handshake over IPC
- [ ] Add tool/resource discovery endpoints on Bun server
- [ ] Unit tests for registration + discovery

## Phase 3: Schema Generation (Weeks 5-7)

- [ ] Generate MCP JSON Schema from Python type metadata
- [ ] Support `Annotated` metadata and docstrings for descriptions
- [ ] Add schema caching + invalidation
- [ ] Validate schemas against MCP spec
- [ ] Perf targets: schema build < 100ms (baseline)

## Phase 4: IPC Bridge Integration (Weeks 8-9)

- [ ] Implement IPC bridge for tool invocation (UDS on Unix; named pipe/TCP on Windows)
- [ ] Add request/response serialization (MessagePack or Protobuf)
- [ ] Add concurrency limits + backpressure
- [ ] Add IPC metrics (latency, errors, queue depth)
- [ ] Integration tests: MCP client → Bun → Python tool

## Phase 5: Advanced Transports (Weeks 10-11)

- [ ] Implement SSE transport
- [ ] Implement Streamable HTTP transport
- [ ] Add transport capability negotiation
- [ ] Add auth hooks (token, mTLS placeholders)
- [ ] Cross-platform CI tests for transports

## Phase 6: Progress, Logging, Streaming (Weeks 12-13)

- [ ] Implement progress events + logging streams
- [ ] Add streaming responses for long-running tools
- [ ] Add cancellation support
- [ ] Add backpressure handling for stream output

## Phase 7: Production Readiness (Weeks 14-16)

- [ ] Load tests (1000 tool calls/sec sustained)
- [ ] Benchmark IPC overhead (p95 < 5ms target)
- [ ] Compatibility matrix: Claude Desktop, Cursor, reference client
- [ ] Doc set: setup guide, transport matrix, troubleshooting
- [ ] Release checklist + versioned spec alignment

---

## Decision Gates

- **MCP Spec Lock**: confirm target spec revision before Phase 3
- **Serialization Format**: confirm MessagePack vs Protobuf by end of Phase 4
- **Optional C ABI**: if non-Python/JS tools are required, coordinate with PRD-1 Phase 6b

