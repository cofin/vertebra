# Chapters: Feature-Complete Storage

* [ch01 — Chapter 01: Native Rust Storage Engine, GCP Auth Unification, and Fail-Closed Facade](ch01-native-engine-and-auth/spec.md) — [active] Add zero-copy streaming reads, incremental multipart uploads, conditional puts, paginated listing, rename, OCI PAR presigning, unified GCP token caches, and fail-closed construction across vertebra-storage and vertebra.storage.
* [ch02 — Chapter 02: SQLSpec, Advanced Alchemy, and Django Storage Contracts](ch02-orm-and-sql-storage-contracts/spec.md) — [active] Implement fail-closed SQLSpec ObjectStoreProtocol, Advanced Alchemy StorageBackend registration without monkeypatching, and @deconstructible Django Storage subclass over SyncObjectStore and AsyncObjectStore.
* [ch03 — Chapter 03: HTTP Range Streaming and Litestar Store/Static Integration](ch03-litestar-and-range-streaming/spec.md) — [planned] Implement RFC 9110 single-range 206/416 StreamingStorageResponse and Litestar AsyncObjectStore Store plus static-files filesystem adapter.
* [ch04 — Chapter 04: Strict Arrow SHM Allocation, Parquet Streaming, and OKF Sync](ch04-arrow-shm-and-docs/spec.md) — [planned] Enforce fail-loud named ShmRegion allocation, eliminate JSON pseudo-Arrow fallback in ArrowCursor.from_rows, wire streaming Arrow/Parquet ObjectStore helpers, and synchronize storage documentation and OKF chapters.
* [ch05 — Raw Object Identity and Typed Conditional Conflicts](ch05-key-identity-and-preconditions/spec.md) — [active] Raw Object Identity and Typed Conditional Conflicts
* [ch06 — Django Collision Length and SQLSpec Signing Errors](ch06-adapter-correctness/spec.md) — [planned] Django Collision Length and SQLSpec Signing Errors
* [ch07 — Withdraw First-Class R2 Python APIs](ch07-remove-r2-python/spec.md) — [planned] Withdraw First-Class R2 Python APIs
* [ch08 — Move OCI Chunk Ownership and Remove Native R2](ch08-remove-r2-native/spec.md) — [planned] Move OCI Chunk Ownership and Remove Native R2
* [ch09 — Synchronize Supported Providers and Native Guarantees](ch09-r2-docs-and-knowledge/spec.md) — [planned] Synchronize Supported Providers and Native Guarantees
* [ch10 — Native Publication Boundary and Cleanup Ownership](ch10-publication-boundary-and-lifecycle/spec.md) — [planned] Native Publication Boundary and Cleanup Ownership
* [ch11 — Local Cross-Process Conditional Publication](ch11-local-atomic-publication/spec.md) — [planned] Local Cross-Process Conditional Publication
* [ch12 — S3 Metadata Multipart and Conditional Completion](ch12-s3-conditional-multipart/spec.md) — [planned] S3 Metadata Multipart and Conditional Completion
* [ch13 — Azure Conditional Multipart Block Commit](ch13-azure-conditional-block-list/spec.md) — [planned] Azure Conditional Multipart Block Commit
* [ch14 — GCS Generation CAS and Staged Rewrite Publication](ch14-gcs-conditional-rewrite/spec.md) — [planned] GCS Generation CAS and Staged Rewrite Publication
* [ch15 — OCI Conditional Commit and Multipart Limits](ch15-oci-conditional-multipart/spec.md) — [planned] OCI Conditional Commit and Multipart Limits
* [ch16 — Native Acceptance and Original Chapter Reclosure](ch16-native-acceptance-and-reclosure/spec.md) — [planned] Native Acceptance and Original Chapter Reclosure
* [ch17 — Bounded Arrow and Parquet Encoding and Sync Storage](ch17-arrow-sync-codecs/spec.md) — [planned] Bounded Arrow and Parquet Encoding and Sync Storage
* [ch18 — Bounded PyArrow Worker and Native Async I/O Bridge](ch18-arrow-async-bridge/spec.md) — [planned] Bounded PyArrow Worker and Native Async I/O Bridge
* [ch19 — SQLSpec Lazy Arrow and Parquet Integration](ch19-sqlspec-arrow-streaming/spec.md) — [planned] SQLSpec Lazy Arrow and Parquet Integration
