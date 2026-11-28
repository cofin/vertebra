---
type: Task
id: t05
chapter_id: null
title: "Rewrite examples as applications"
description: "Teach real application and workflow entry points while keeping verification in the docs test harness."
status: stable
state: completed
priority: normal
verification_strategy: "Run the docs verification suite over app modules, workflow scripts, and published snippets"
worksheet_format: compact
depends_on: [t03, t04]
read_set:
  - docs/examples/index.md
  - docs/examples/background_queues.py
  - docs/examples/storage_and_db.py
  - docs/examples/agent_mcp_tools.py
  - docs/examples/embeddings_app.py
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/storage.md
  - docs/usage/server-and-workers.md
  - docs/usage/agent-protocols.md
  - src/py/tests/unit/docs/test_examples.py
  - src/py/tests/unit/agent/test_mcp_core_and_plugin.py
files:
  - docs/examples/index.md
  - docs/examples/background_queues.py
  - docs/examples/storage_and_db.py
  - docs/examples/agent_mcp_tools.py
  - docs/examples/embeddings_app.py
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/storage.md
  - docs/usage/server-and-workers.md
tests:
  - src/py/tests/unit/docs/test_examples.py
estimated_loc: 235
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
commit: da8fdf79c1da0b3e5f14421d11e8f677d3b9c061
sources:
  - id: docs-tests
    resource: src/py/tests/unit/docs/test_examples.py
    title: "Example discovery and execution"
  - id: example-index
    resource: docs/examples/index.md
    title: "Published runnable examples"
tags: [documentation, examples, testing]
---

# Rewrite examples as applications

## Objective & Scope Boundary

Implement [R5](../spec.md#r5-application-shaped-documentation-examples). Keep the example inventory and validate the same code readers use.

## Bounded Read Manifest

Read the example index, listed files, and current discovery, snippet, and execution tests. Match the queue, storage, MCP, and embedding calls against their implementation or feature tests.

## Contracts & Edge Cases

| Example | Reader workflow |
| --- | --- |
| Queues | Register a real task, enqueue input, and show the local worker result. Demonstrate resource cleanup and identify the in-memory backend as local to the process. |
| Storage and SQLite | Store an attachment and persist its key in a transaction. Retrieve and show the result. Put Arrow IPC and error semantics in the feature documentation or existing tests where they are exercised. |
| MCP | Register a tool with `AgentPlugin`, mount it on a native app, and show a real MCP client request to the documented endpoint. |
| Embeddings | Expose a vector-search controller through an importable app. Document a request and returned hits, the deterministic local embedder, and where to read about truncation and binary codecs. |
| Inline guide checks | Show digest metadata as a normal application result in Storage and report GIL status as diagnostic output in Server and Workers. |

- Put required imports, handlers, and app setup inside the named snippets. Put launch commands and expected outputs on the example page. Use `module:app` for web apps and `python -m` for the queue and storage workflows. Include the `__main__` launch guard inside the published workflow snippets so the copied script runs. Update the existing snippet checker to allow that guard for workflow modules.
- Update the existing structure and execution tests to accept exactly one intended public entry point per example: an importable app or a genuine workflow `main()`. Continue discovering every example, checking snippet markers and Python hygiene, and accounting for optional dependencies. Check for `ast.Assert` in example modules. Keep assertions in pytest and the explicit Testing guide.
- Exercise quickstart, inventory, vector search, and MCP endpoints through native app dispatch. Exercise the counter's native SSE response and consume its initial and updated frames. Run scripts and verify their application outcomes. Reuse existing tests for feature-level assertions and extend coverage where the removed demo assertions establish a genuine gap.
- The baseline SQLite Arrow demo needs named shared memory and fails under a sandbox that blocks it. Keep Arrow coverage in the appropriate SQLite tests. Run those tests in an environment that supports shared memory.

## Test Matrix

App modules import with their documented names. The harness verifies health, guard denial and success, vector search results, an MCP exchange, and SSE push through the exposed apps. Workflow scripts run to completion and report their results. The rendered examples have imports, complete snippet sections, and reproducible commands.

## Verification

Run the chapter gate, including `src/py/tests/unit/docs/test_examples.py`. Run the docs build and launch-command checks at finish.

## Acceptance Criteria

- All seven published examples model normal application or workflow usage.
- Tests verify the example outcomes without imposing a script entrypoint on web apps.
- Examples and inline usage snippets keep test assertions out of application code.

[^docs-tests]: `src/py/tests/unit/docs/test_examples.py` owns example validation.
[^example-index]: `docs/examples/index.md` is the public entry point for the examples.
