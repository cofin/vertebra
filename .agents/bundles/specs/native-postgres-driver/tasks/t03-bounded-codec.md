---
type: Task
id: native-postgres-driver:t03
title: Bound startup framing and backend message decoding
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t02
files:
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/src/protocol.rs
tests:
- src/rs/vertebra-postgres/src/protocol.rs::tests::bounded_frames
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
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R3
---

# Bound startup framing and backend message decoding

## Objective

Bound startup framing and backend message decoding, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 230 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED duplex tests for bytewise/adjacent AuthenticationOk/ParameterStatus/Ready frames, EOF within header/body, signed/short lengths0..3, >16MiB before allocation, malformed payload/trailing bytes and maximum-length header without unbounded reserve. Colocated decoder tests are permitted for the private deep framing seam; t04 socket tests exercise the same reader through the real session.
2. Implement protocol.rs::read_message<R:AsyncRead+Unpin> with exact five-byte tag+length header. Validate length includes four-byte length word and lies4..=16777216 before reserve, read exactly remaining bytes, then postgres_protocol::message::backend::Message::parse on exactly this frame; reject parse None/unconsumed bytes. No bespoke duplicate backend parser, read_to_end or unbounded body allocations. Preserve message payload owned by upstream Message.
3. Implement startup(config) through upstream frontend::startup_message with protocol196608 and user/database/application_name/client_encoding=UTF8. Existing shared encode_startup_message omits UTF8 and is not extended or duplicated. Upstream frontend helpers also own Terminate/password/SASL/SSLRequest in later tasks. Unknown tags and unsupported protocol messages fail explicitly, leaving future extension decoders narrowly owned by their later task.
4. Keep protocol module crate-private; no test-only public API. Commit feat(postgres): bound wire message decoding.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib protocol::tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R3: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
