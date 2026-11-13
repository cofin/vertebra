# Chapters: MCP Protocol Completeness (`mcp-protocol-completeness`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-guards-params-and-alias-cleanup`](ch01-guards-params-and-alias-cleanup/spec.md) | `completed` | `4` | Single-pass `Tool.authorize`, `Mcp-Param-*` validation, single-use expiring `ConfirmationGate`, and `MCPService` alias removal (`1f269a58`) |
| [`ch02-version-negotiation-and-sessions`](ch02-version-negotiation-and-sessions/spec.md) | `completed` | `3` | `SUPPORTED_PROTOCOL_VERSIONS`, `MCPSessionStore`, legacy `initialize`, `2025-03-26` batching, `DELETE /mcp`, and unified capabilities (`22708450`) |
| [`ch03-sse-streams-and-resource-subscriptions`](ch03-sse-streams-and-resource-subscriptions/spec.md) | `completed` | `4` | `McpHttpResponse.stream`, `GET /mcp` SSE adapter writers, list/log/progress notifications, `resources/subscribe`, and `subscriptions/listen` (`8c964cc3`) |
| [`ch04-skills-and-mcp-apps-core`](ch04-skills-and-mcp-apps-core/spec.md) | `completed` | `4` | `SkillCatalog` disclosure, `io.modelcontextprotocol/skills`, and SEP-1865 `io.modelcontextprotocol/ui` metadata & validation (`a38f6ef7`) |
| [`ch05-framework-mounts-and-litestar-parity`](ch05-framework-mounts-and-litestar-parity/spec.md) | `completed` | `3` | `AgentPlugin`, Starlette, Flask, and Django skill + UI link validation wiring, and `litestar-mcp>=0.20.0` dual-mode pin (`8c1a9f30`) |
| [`ch06-stdio-serve-bridge-and-gcp-auth`](ch06-stdio-serve-bridge-and-gcp-auth/spec.md) | `completed` | `3` | Incremental stdio + `vtb ai mcp serve` app discovery, dual-mode `bridge --audience` OIDC auth, and `IapIdentitySource` JWT verification (`7607f4d8`) |
| [`ch07-docs-and-okf-sync`](ch07-docs-and-okf-sync/spec.md) | `active` | `4` | Agent/CLI docs, changelog, `mcp-and-a2a-protocol-harness.md` (`<= 200L`), `mcp-limitations.md`, and Litestar/CLI/test OKF (`make docs && make review-check`) |
