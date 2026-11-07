# Chapters: Feature-Complete Storage

20 chapters and 58 tasks retain the complete storage scope.

* [ch01 — Chapter 01: Native Rust Storage Engine, GCP Auth Unification, and Fail-Closed Facade](ch01-native-engine-and-auth/spec.md) — [active] Add zero-copy streaming reads, incremental multipart uploads, conditional puts, paginated listing, rename, OCI PAR presigning, unified GCP token caches, and fail-closed construction across vertebra-storage and vertebra.storage.
* [ch02 — Chapter 02: SQLSpec, Advanced Alchemy, and Django Storage Contracts](ch02-orm-and-sql-storage-contracts/spec.md) — [active] Implement fail-closed SQLSpec ObjectStoreProtocol, Advanced Alchemy StorageBackend registration without monkeypatching, and @deconstructible Django Storage subclass over SyncObjectStore and AsyncObjectStore.
* [ch03 — Chapter 03: HTTP Range Streaming and Litestar Store/Static Integration](ch03-litestar-and-range-streaming/spec.md) — [planned] Implement RFC 9110 single-range 206/416 StreamingStorageResponse and Litestar AsyncObjectStore Store plus static-files filesystem adapter.
* [ch04 — Chapter 04: Strict Arrow SHM Allocation, Parquet Streaming, and OKF Sync](ch04-arrow-shm-and-docs/spec.md) — [planned] Enforce fail-loud named ShmRegion allocation, eliminate JSON pseudo-Arrow fallback in ArrowCursor.from_rows, wire streaming Arrow/Parquet ObjectStore helpers, and synchronize storage documentation and OKF chapters.
* [ch05 — Raw Object Identity and Typed Conditional Conflicts](ch05-key-identity-and-preconditions/spec.md) — [completed] Raw key identity, adapter qualification and lazy typed conflicts are verified; correctness and fresh quality reviews report no findings through `0bce1806`. Native publication acceptance remains separate.
* [ch06 — Django Collision Length and SQLSpec Signing Errors](ch06-adapter-correctness/spec.md) — [completed] Per-call filename limits, consumed-source recovery and configured signing errors are verified and reviewed through `115ab8f5`. Native multipart publication acceptance remains separate.
* [ch07 — Withdraw First-Class R2 Python APIs](ch07-remove-r2-python/spec.md) — [active] The first three withdrawal/staging tasks are signed; correctness review requires SQLSpec logical-R2 URI and configured native-store rejection repair before fresh reviews.
* [ch08 — Move OCI Chunk Ownership and Remove Native R2](ch08-remove-r2-native/spec.md) — [completed] OCI accumulation ownership, native R2 constructor/helper/stub withdrawal and dead-module deletion are signed through `c7868b02`; knowledge is synchronized. Correctness and fresh quality reviews remain pending.
* [ch09 — Synchronize Supported Providers and Native Guarantees](ch09-r2-docs-and-knowledge/spec.md) — [active] Synchronize Supported Providers and Native Guarantees
* [ch10 — Native Publication Boundary and Cleanup Ownership](ch10-publication-boundary-and-lifecycle/spec.md) — [planned] Real request transport is verified in library tests; ch12:t01 owns runtime activation. Revision-5 contracts await cold review.
* [ch11 — Local Cross-Process Conditional Publication](ch11-local-atomic-publication/spec.md) — [planned] Local Cross-Process Conditional Publication
* [ch12 — S3 Metadata Multipart and Conditional Completion](ch12-s3-conditional-multipart/spec.md) — [planned] Activate the staged transport with the first S3 initiation caller, then verify metadata, refreshed SDK credentials and conditional completion.
* [ch13 — Azure Conditional Multipart Block Commit](ch13-azure-conditional-block-list/spec.md) — [planned] Real SDK credential forms, block identity, conditional commit and SAS error redaction remain provider acceptance obligations.
* [ch14 — GCS Generation CAS and Staged Rewrite Publication](ch14-gcs-conditional-rewrite/spec.md) — [planned] GCS Generation CAS and Staged Rewrite Publication
* [ch15 — OCI Conditional Commit and Multipart Limits](ch15-oci-conditional-multipart/spec.md) — [planned] OCI Conditional Commit and Multipart Limits
* [ch16 — Native Acceptance and Original Chapter Reclosure](ch16-native-acceptance-and-reclosure/spec.md) — [planned] Native Acceptance and Original Chapter Reclosure
* [ch17 — Bounded Arrow and Parquet Encoding and Sync Storage](ch17-arrow-sync-codecs/spec.md) — [planned] Bounded Arrow and Parquet Encoding and Sync Storage
* [ch18 — Bounded PyArrow Worker and Native Async I/O Bridge](ch18-arrow-async-bridge/spec.md) — [planned] Bounded PyArrow Worker and Native Async I/O Bridge
* [ch19 — SQLSpec Lazy Arrow and Parquet Integration](ch19-sqlspec-arrow-streaming/spec.md) — [planned] SQLSpec Lazy Arrow and Parquet Integration

* [ch20 — Ordinary Byte-Put Negative Predicate CAS](ch20-ordinary-negative-cas/spec.md) — [planned] Fence shared ordinary negative ETag puts through native Create/Update, preserve Local/GCS dispatch truth, and repair actual OCI ordinary conditions before native acceptance.
