---
type: Task
id: storage-completeness:ch13:t02
chapter_id: ch13-azure-conditional-block-list
title: Commit Azure block list atomically with metadata and conditions
description: Commit Azure block list atomically with metadata and conditions
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch13:t01
read_set:
- src/rs/vertebra-storage/src/azure_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/azure_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/azure_publication.rs
estimated_loc: 200
plan_revision: 6
plan_commit: e3e344b5
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: src/rs/vertebra-storage/src/azure_publication.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Commit Azure block list atomically with metadata and conditions

## Objective & Scope Boundary

Commit Azure block list atomically with metadata and conditions. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/azure_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/cloud_request.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Complete using PutBlockList through CloudRequestClient with `<Uncommitted>` identifiers base64-encoded exactly once from raw SDK PartIds. Send If-None-Match:* for Create and If-Match expected for Match; NotMatch sends native If-None-Match:<forbidden ETag> directly; Azure supports that exact condition, so no HEAD/unconditional publication is needed. Carry content type, metadata attributes and tags in commit headers; preserve SDK-authorized conditional/header naming. Parse actual ETag/version response into PutResult. Map create conflict versus update precondition accurately; permission errors remain genuine errors. Abort/cancellation retain staged-block ownership according to provider cleanup capability and documented Azure uncommitted-block lifecycle, with no fabricated explicit delete of competing committed blob.

Use the concrete SDK credential injection and actual wrapper constructor owned by t01 for end-to-end acceptance. Fetch `store.credentials().get_credential()` immediately before every commit attempt, run public `AzureAuthorizer::new(credential.as_ref(), &account).authorize(&mut request)`, preserve `credential.sensitive_request()` semantics, then call the live `CloudRequestClient::execute`; no custom SAS URL/signature logic or cached Authorization. Real local HTTP responses distinguish genuine 403 permission denial from 412 conditional conflict. Error context includes only static provider/operation, status and safe recognized protocol code; never serialize the signed URI, SAS query, Authorization, credential Debug or untrusted echoed response body/source chain. The real SAS fixture's responder intentionally echoes the signed request target in its rejection body, so the test proves actual remote-error redaction rather than scanning an invented error string.

SDK `PartId.content_id` is raw block-ID text, not base64. Preserve that exact raw value in the ordered PartId vector throughout t01/t02. At the block-list wire boundary only, encode each raw identifier once with `base64::engine::general_purpose::STANDARD` and write `<Uncommitted>encoded-id</Uncommitted>`, matching the pinned SDK's newly uploaded-block contract. SDK block PUT already base64-encodes this same raw value in its `blockid` query. Never pre-encode stored PartIds, place raw IDs into XML, or base64 an already encoded wire token. Canonical HTTP assertions decode each captured blockid query and each ordered XML element once and compare the exact original raw SDK identifiers; this catches omitted and double encoding without asserting the random ID-generation algorithm.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Raw block identifier boundary | SDK returns raw PartId.content_id; responder captures PUT blockid query and ordered commit XML | One base64 decode at each wire boundary equals the exact raw PartId value; internal IDs stay raw and no omitted/double encoding can publish mismatched blocks. |
| Stale/block race | Competing write between upload blocks and PutBlockList | Conditional commit rejects; competitor data intact |
| Metadata atomicity | Commit content_type and owner attribute/tag | Head sees exact caller metadata on winning object |
| Returned result | Commit response ETag/version differ from earlier head | PutResult returns commit response values |
| Cancellation | Cancel before commit | No new target visible; safe cleanup/lifecycle evidence |

| Configured commit transport and refresh | Real wrapper from t01 with local endpoint, allow_http/header options and rotating SDK bearer provider; two explicitly authorized commit attempts | Captured actual block-list requests retain configured options and fresh token identity; no mocked wrapper/client and no implicit retry of an ambiguous completion. |
| SAS error redaction and classification | Real SDK SASToken provider; responder returns 403 with body echoing full signed target, then a separate 412 case | Permission error remains distinct from typed CAS conflict; exception/debug/source output excludes signed URI, SAS values, Authorization and echoed secrets while preserving provider, operation and status. |

## Verification

Run the owned inline wrapper tests as well as canonical engine regressions: `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib azure_publication`.

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The executable transport staging and provider acceptance contracts await independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/azure_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
