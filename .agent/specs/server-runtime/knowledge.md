# Knowledge Capture: Server & Runtime - MCP, Health Checks, and Observability

## Patterns Discovered
- **Stream Mocking**: Mocking `Readable` and `Writable` streams is effective for testing stdio-based transports without spawning subprocesses.
- **Readline for JSON-RPC**: `node:readline` simplifies parsing line-delimited JSON-RPC messages from input streams.
- **TDD Verification**: Validating pre-existing code with new tests confirms implementation readiness without unnecessary rewrites.

## Lessons Learned
[What worked well, what didn't]

## Decisions Made
- **Stdio Transport**: Implemented using `readline` for robustness against partial chunks and simplified line handling.

## Reusable Components
- `StdioTransport`: Can be used for any line-based JSON protocol over streams.

## Gotchas & Warnings
[Pitfalls to avoid in similar work]
