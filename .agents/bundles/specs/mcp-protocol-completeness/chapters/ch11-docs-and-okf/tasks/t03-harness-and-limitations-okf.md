---
type: Task
id: "mcp-protocol-completeness:ch11:t03"
chapter_id: ch11-docs-and-okf
title: Rewrite the harness OKF chapter and add the MCP limitations chapter
description: Rewrite the agent harness OKF chapter in present tense for the completed MCP surface, create domains/agent/mcp-limitations.md recording every caveat found during the flow, and add it to the domain index.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "mcp-protocol-completeness:ch11:t02"
read_set:
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/skills/review-docs-okf/references/okf-frontmatter.md
  - .agents/bundles/specs/mcp-protocol-completeness/spec.md
files:
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - .agents/bundles/knowledge/domains/agent/mcp-limitations.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
estimated_loc: 230
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: harness-okf
    resource: .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
    title: Agent harness OKF chapter
  - id: limitations-precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Limitations chapter format
  - id: okf-frontmatter
    resource: .agents/skills/review-docs-okf/references/okf-frontmatter.md
    title: OKF frontmatter rules
tags:
  - okf
  - limitations
---

# Task t03: Rewrite the Harness OKF Chapter and Add the MCP Limitations Chapter

## Objective & Scope Boundary

The knowledge base states the completed MCP behaviour and lists every caveat in one place [^harness-okf] [^limitations-precedent] [^okf-frontmatter]. Out of scope: CLI, Litestar, and test-matrix chapters (`t04`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`.
- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` (format precedent).
- `.agents/bundles/knowledge/domains/index.md` (`## Agent Protocols & Harness` list).
- `.agents/skills/review-docs-okf/references/okf-frontmatter.md`.
- `.agents/bundles/specs/mcp-protocol-completeness/spec.md` (contracts and edge matrix).

## Symbol & Signature Contracts

- Harness chapter: truth block (`<= 40` lines) and Section 1 rewritten for dual-mode transport (`SUPPORTED_PROTOCOL_VERSIONS`, mode rule, `MCPSessionStore`, `GET` SSE, `DELETE`, `subscriptions/listen`), context propagation (`AgentContext.from_*`, `MCPConfig.trust_proxy_identity` / `identity_source` with `IapIdentitySource`, `MtlsIdentitySource`, and `CallableIdentitySource`, headers ignored by default, `bind_agent_context`), single-pass guards (`Tool.authorize` / `Tool.call`, `-32004` / `-32005`), confirmation lifecycle (`consume_call_approval`, TTL, owner rule), skills extension, MCP Apps (`UI_EXTENSION`, `UI_MIME_TYPE`, `MCPCore.validate`), and Litestar delegation to `litestar-mcp>=0.20.0`; the component table rows for `MCPCore` and `AsgiMcpApp` drop the `405 on GET /mcp` and removed-method claims; `sources:` add `src/py/vertebra/agent/mcp/sessions.py`, `src/py/vertebra/agent/core/sse.py`, `src/py/vertebra/agent/core/guards.py`, and `src/py/vertebra/agent/mcp/bridge.py` with matching footnotes.
- New `mcp-limitations.md` (`type: Concept`, `status: stable`, `tags: [domains, agent, mcp, limitations]`, `generated`, `verified`, `sources`) lists, each with the owning symbol: per-process in-memory sessions and subscriptions (sticky routing needed for legacy clients across workers); buffering behind Vertebra's own ASGI mount until `server-runtime-correctness` streaming is present; anonymous callers can decide unowned confirmation tickets; Django static well-known bytes are rendered at attach time; `subscriptions/listen` and SEP-1865 field names follow `litestar-mcp` `0.20.0`; TypeScript `McpClient` stays on the stateless profile; `vtb ai mcp serve` for Django needs `--app` pointing at the registry returned by `attach_mcp_routes`; OIDC minting relies on the `gcp-embeddings-and-identity` API; the unwired Rust `mcp_bridge.rs` library is not part of the CLI path.
- `domains/index.md` adds one bullet linking `agent/mcp-limitations.md` under `## Agent Protocols & Harness` and updates the harness bullet text to mention dual-mode transport.

## Edge Cases & Error Semantics

- Relative links use the same depth as existing chapters (`../../../../../src/...`).
- No standalone `---` rules in bodies; no historical narrative.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation` through `make review-check` (frontmatter, links, privacy).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Stale claims | `git grep -n -E "405. on .GET /mcp.|removed .2024-11-05. methods" -- .agents/bundles/knowledge/domains/agent` | No matches |
| Index link | `git grep -n "mcp-limitations.md" -- .agents/bundles/knowledge/domains/index.md` | One match |
| Truth block size | Count lines between `<!-- truth: start -->` and `<!-- truth: end -->` in both chapters | `<= 40` each |
| Footnotes | Every footnote reference in each chapter has a matching `sources:` entry | Verified by `make review-check` |

## Verification

- **Baseline**: `make review-check` exits `0` before edits.
- **Final**: `make review-check && make docs`.

## Acceptance Criteria

- [ ] R17.3: harness chapter, limitations chapter, and index are present-tense and pass review gates; `[9/12 files documented]` after this task.

## Notes & Discoveries

- [2026-11-12 00:30] The repository records caveats in dedicated limitations chapters (`domains/storage/object-storage-limitations.md`).

[^harness-okf]: `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Agent harness OKF chapter.
[^limitations-precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations chapter format.
[^okf-frontmatter]: `.agents/skills/review-docs-okf/references/okf-frontmatter.md` — OKF frontmatter rules.
