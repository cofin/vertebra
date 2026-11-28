---
type: Task
id: t06
chapter_id: null
title: "Run WebAssembly in the browser"
description: "Serve a browser page and a tiny WebAssembly module from a native Vertebra example app."
status: stable
state: completed
priority: normal
verification_strategy: "Verify the example routes and execute the module through a real browser"
worksheet_format: compact
depends_on: [t04, t05]
read_set:
  - docs/examples/index.md
  - docs/usage/typescript-sdk.md
  - src/ts/vertebra-client/src/wasm.ts
  - src/py/tests/unit/docs/test_examples.py
  - .agents/bundles/knowledge/domains/frontend/wasm.md
files:
  - docs/examples/__init__.py
  - docs/examples/wasm_in_browser.py
  - docs/examples/index.md
  - docs/usage/typescript-sdk.md
  - .agents/bundles/knowledge/domains/frontend/wasm.md
tests:
  - src/py/tests/unit/docs/test_examples.py
estimated_loc: 125
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
commit: 1b934b31942f637a62633a0b768ea2e1613b488b
sources:
  - id: wasm-client
    resource: src/ts/vertebra-client/src/wasm.ts
    title: "Browser WebAssembly loader"
tags: [webassembly, browser, documentation]
---

# Run WebAssembly in the browser

## Objective & Scope Boundary

Implement [R6](../spec.md#r6-browser-webassembly-example).

## Bounded Read Manifest

Read the existing SDK client and docs pages. Follow the native app and snippet shape from t05.

## Contracts & Edge Cases

Publish `app` with standalone routes. Use a small module exporting integer addition and document the equivalent WAT in the example catalog. The browser uses `WebAssembly.instantiateStreaming(fetch("/add.wasm"))`, checks input bounds for signed 32-bit integers, and writes outputs to a status element. Signed addition wraps to 32 bits. Show the matching SDK byte-loading call on the SDK page.

## Test Matrix

Check both native routes, the WebAssembly MIME type and header, and the browser's visible addition result. Check that a module load failure reports a readable error.

## Verification

Run the chapter gate. Start `vtb run docs.examples.wasm_in_browser:app` and verify in a browser. Use existing browser tooling when available.

## Acceptance Criteria

The example loads through its documented command and visibly executes a WebAssembly export on the client. It requires no Wasmtime extra, guest compiler, or browser build.

[^wasm-client]: `src/ts/vertebra-client/src/wasm.ts` supplies the optional Vertebra client path.
