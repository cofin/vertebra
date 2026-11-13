# Chapters: MCP Protocol Completeness (`mcp-protocol-completeness`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-guards-params-and-alias-cleanup`](ch01-guards-params-and-alias-cleanup/spec.md) | `completed` | `4` | Single-pass `Tool.authorize`, `Mcp-Param-*` validation, single-use expiring `ConfirmationGate`, and `MCPService` alias removal (`1f269a58`) |
| [`ch02-version-negotiation-and-sessions`](ch02-version-negotiation-and-sessions/spec.md) | `completed` | `3` | `SUPPORTED_PROTOCOL_VERSIONS`, `MCPSessionStore`, legacy `initialize`, `2025-03-26` batching, `DELETE /mcp`, and unified capabilities (`5f8fd173`) |
| [`ch03-sse-streams-and-resource-subscriptions`](ch03-sse-streams-and-resource-subscriptions/spec.md) | `active` | `4` | `McpHttpResponse.stream`, `GET /mcp` SSE adapter writers, list/log/progress notifications, `resources/subscribe`, and `subscriptions/listen` (`ruff` + `mypy` + `test_mcp_conformance.py`) |
| [`ch04-skills-and-mcp-apps-core`](ch04-skills-and-mcp-apps-core/spec.md) | `planned` | `4` | `SkillCatalog` disclosure, `io.modelcontextprotocol/skills`, and SEP-1865 `io.modelcontextprotocol/ui` metadata & validation (`ruff` + `mypy` + `test_mcp_conformance.py`) |
| [`ch05-framework-mounts-and-litestar-parity`](ch05-framework-mounts-and-litestar-parity/spec.md) | `planned` | `3` | `AgentPlugin`, Starlette, Flask, and Django skill + UI link validation wiring, and `litestar-mcp>=0.20.0` dual-mode pin (`uv lock --check --offline` + `mypy` + conformance pytest) |
| [`ch06-stdio-serve-bridge-and-gcp-auth`](ch06-stdio-serve-bridge-and-gcp-auth/spec.md) | `planned` | `3` | Incremental stdio + `vtb ai mcp serve` app discovery, dual-mode `bridge --audience` OIDC auth, and `IapIdentitySource` JWT verification (`uv lock --check --offline` + `mypy` + CLI/core pytest) |
| [`ch07-docs-and-okf-sync`](ch07-docs-and-okf-sync/spec.md) | `planned` | `4` | Agent/CLI docs, changelog, `mcp-and-a2a-protocol-harness.md` (`<= 200L`), `mcp-limitations.md`, and Litestar/CLI/test OKF (`make docs && make review-check`) |
