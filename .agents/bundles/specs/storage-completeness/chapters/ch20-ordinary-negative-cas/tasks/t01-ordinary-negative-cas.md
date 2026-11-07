---
type: Task
id: storage-completeness:ch20:t01
chapter_id: ch20-ordinary-negative-cas
title: Fence ordinary negative ETag writes through provider CAS
description: Preserve negative predicates during ordinary byte puts with bounded native CAS retries
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch10:t01
- storage-completeness:ch08:t03
- storage-completeness:ch11:t04
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/oci.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 230
plan_revision: 6
plan_commit: e3e344b5
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: Independent cold plan review before production edits.
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: src/rs/vertebra-storage/src/store.rs
  title: Ordinary conditional byte-put entry point
- id: oci
  resource: src/rs/vertebra-storage/src/oci.rs
  title: Owned OCI ordinary PutOptions implementation
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Pinned SDK condition and metadata contracts
tags:
- storage
- conditional
- repair
---

# Fence Ordinary Negative ETag Writes Through Provider CAS

## Objective & Scope Boundary

Implement ordinary `put_opts(if_none_match=<opaque ETag>)` as an atomic predicate at the actual mutation. Own only the listed three production files and canonical native tests. Existing byte puts already own their Bytes input, so bounded CAS attempts may clone its views. Streaming/multipart replay, new configuration APIs and unrelated provider protocols are outside this worksheet. [^implementation] [^oci] [^research]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — ObjectStoreEngine::put_opts and StoredObjectMeta.
- `src/rs/vertebra-storage/src/conditional.rs` — normalized selectors and Local publication dispatch.
- `src/rs/vertebra-storage/src/oci.rs` — real ordinary PUT modes, metadata and signing.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — canonical engine/provider HTTP fixtures.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — pinned SDK/API evidence.

## Symbol & Signature Contracts

`ObjectStoreEngine::put_opts` normalizes selectors through the ch10 `PublishCondition` boundary before effects. For ordinary `NotMatch(forbidden)` on supported native stores, call the private `put_not_match` helper with caller-owned `Bytes`, resolved path and complete `PutOptions`. The helper runs at most eight compare-and-write attempts: HEAD the current object; if its raw ETag equals `forbidden`, return typed `StorageError::Precondition` without a PUT; otherwise use `PutMode::Update(UpdateVersion { e_tag: existing.e_tag.clone(), version: existing.version.clone() })`. On HEAD NotFound use `PutMode::Create`. Never select Overwrite after a negative predicate, including missing-object and retry paths. If a provider snapshot has no usable ETag/version for its actual CAS primitive, fail visibly rather than inventing a guard.

Retry only an Update precondition failure or a Create already-exists/precondition race, then HEAD and re-evaluate the original logical predicate. Other permission, credential, transport, missing-update, decode and unsupported-capability errors propagate without fallback. On eight CAS failures return typed precondition conflict with an explicit retry-budget message, not a claim that the forbidden ETag matched. Each attempt clones only the existing `Bytes`/PutPayload view and required metadata; no producer replay, stream buffering or new public API. Preserve attributes/content_type on every attempt and return the exact successful provider PutResult ETag/version in StoredObjectMeta, without an unfenced HEAD.

Dispatch Local through the ch11 physical-parent OS-lock publication path: predicate and final ordinary publish share that guard, and the generic loop must never bypass it or revive the old NotImplemented→HEAD→unconditional-write fallback. Memory uses the real native InMemory Create/Update implementation. S3 and Azure ordinary SDK PutMode::Update supply If-Match using the snapshot ETag; configured endpoint, SDK credentials and transport remain intact. GCS negative writes use the captured generation in UpdateVersion as well as the canonical XML ETag; ch14:t01 separately repairs positive if_match and preserves XML/JSON receipt semantics. This shared task does not replace the GCS positive-predicate owner.

The owned OCI ordinary collaborator currently ignores PutOptions. Repair `OciObjectStore::put_opts` in `oci.rs` to honor Overwrite/Create/Update: Create sends `If-None-Match: *`; Update requires the captured ETag and sends `If-Match`; use the existing Cavage request signer/transport. Forward caller content type and metadata attributes on every request as Content-Type and opc-meta-<name> headers. Map a failed Create condition to AlreadyExists and failed Update to Precondition; preserve genuine permission and transport failures. Parse actual response ETag/opc-version-id. The shared negative loop uses this real OCI Update/Create primitive; it must not assume ignored options are atomic. No OCI multipart policy changes belong here.

Private helper signature: `async fn put_not_match(&self, location: &StorePath, data: Bytes, forbidden: &str, options: PutOptions) -> Result<PutResult, ObjectStoreError>` is a private ObjectStoreEngine method in store.rs and is called from put_opts after the Local guarded dispatch. conditional.rs owns the normalized selector/dispatch integration. `options.mode` is replaced per attempt while its metadata fields survive. The only public entry point remains `ObjectStoreEngine::put_opts(path, data, PutConditionalOptions)`.

Caller → seam → collaborator: ordinary native put_opts → normalized NotMatch dispatch and bounded put_not_match → real SDK/native conditional put or Local OS-guarded publisher. OCI uses its repaired real signed PUT primitive, not an assumed trait capability.

## Edge Cases & Error Semantics

Wildcard `if_none_match="*"` remains Create directly. Conflicting selectors reject before HEAD or mutation through the existing normalizer. Missing objects use Create, never an unconditional write. A false predicate and an exhausted CAS budget are typed precondition conflicts with distinct truthful context. Preserve permission, credentials, transport and unsupported errors; no message substring classification. Do not claim the streaming completion contract is repaired by an ordinary byte-put test.

## Public-Seam Test Blueprint

Tests invoke actual ObjectStoreEngine::put_opts. S3/Azure/OCI fixtures are real local HTTP responders enforcing conditions; only the external service is substituted. Do not mock the engine, manufacture a successful put, or replace native CAS with a Python/process lock.

| Case | Input | Observable result |
| --- | --- | --- |
| S3/Azure forbidden replacement | HEAD returns allowed ETag A; a deterministic responder barrier installs forbidden ETag F before PUT; caller if_none_match=F | Actual PUT is fenced with If-Match:A; service returns 412, fresh HEAD sees F, typed Precondition and competitor bytes F remain intact. No unconditional PUT is sent. |
| Missing-object race | HEAD 404; competitor creates F before guarded PUT | Request uses If-None-Match:*; Create conflict triggers predicate re-evaluation, preserving F and returning typed conflict. |
| Budget and genuine failure | Eight distinct allowed versions invalidate eight conditional PUTs; separate genuine403/network/unsupported failures | Exactly bounded CAS attempts then truthful typed budget conflict; non-CAS failures are returned immediately with original classification and no Overwrite fallback. |
| Memory and metadata | Real native Memory creates, matching/nonmatching updates and forbidden rejection; successful retry on service fixture with content_type and attributes | Exact payload and metadata survive; returned ETag/version are the successful conditional PUT receipt, with no post-publish HEAD that can report a successor. |
| Local dispatch | Existing ch11 independently configured/overlapping-root processes race an ordinary negative put and another writer | Predicate and publish use the same physical-parent OS guard; winning bytes preserved and SDK staging/lock artifacts remain hidden. No generic loop bypass. |
| GCS generation and OCI primitive | GCS HEAD reports XML ETag A/generation1729; OCI real signed responder requires Create/Update headers and metadata | GCS update carries generation1729 plus captured XML ETag; OCI sends actual If-Match or create wildcard and returns genuine response ETag/opc-version-id. Neither ignored options nor JSON ETag substitution can produce false success. |

## Verification

Record Red from the ordinary S3/Azure barrier regressions and OCI ignored-PutOptions regression before production edits, then Green for the same real seams and the full canonical module. The Local prerequisites already provide native process fixtures; keep all host SHM suites serialized.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`
- `git diff --check`

Record exact commands/statuses, provider request predicates, bounded attempt count, metadata and actual result receipts. Planning-only validation never closes this runtime acceptance.

## Acceptance Criteria

- [ ] Real S3/Azure ordinary negative-predicate races preserve the competing forbidden version without an unconditional request.
- [ ] All attempts preserve payload/metadata and the successful provider receipt; retries are CAS-only and bounded to eight.
- [ ] Local native guards, real Memory CAS, GCS generation and OCI actual ordinary header capabilities remain truthful.
- [ ] Only owned paths change, read_set <=6, production files <=3 and estimated implementation <=250 LOC.
- [ ] Signed source and exact verification evidence are recorded, followed by documentation sync and independent chapter reviews.

## Notes & Discoveries

This draft awaits independent cold review. Any executable contract mismatch routes through bounded `/flow:revise storage-completeness ch20-ordinary-negative-cas` before dependent production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Existing ordinary put entry point.
[^oci]: `src/rs/vertebra-storage/src/oci.rs` — Actual signed OCI ordinary backend.
[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK and provider condition evidence.
