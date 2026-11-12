# Chapters: Object Storage Integrity, Create-Only Copy, and Streamed Arrow Writes

`4` dependency-ordered chapters (`12` vertical-slice tasks) delivering create-only copy on S3-compatible stores, SHA-256 write receipts and verified reads, the SQLSpec direct Arrow write capability, storage quality fixes, a storage wheel isolation check, and complete storage documentation.

* [ch01 — Create-Only Copy and Quality](ch01-create-only-copy-and-quality/spec.md) — [completed] (`2` tasks) Configures copy-if-not-exists strategies for S3/B2 and R2 with mock-S3 tests, streams `put_multipart` parts, and removes the `memory://` sentinel from `ObjectStoreBackend`.
* [ch02 — Write Receipts and Verified Reads](ch02-write-receipts-and-verified-reads/spec.md) — [planned] (`4` tasks) Adds engine SHA-256 receipts and `expected_sha256` enforcement, verified reads, PyO3 bindings with `IntegrityError`, and the Python facade surface ([#99](https://github.com/cofin/vertebra/issues/99)).
* [ch03 — Arrow Streaming and Integrations](ch03-arrow-streaming-and-integrations/spec.md) — [planned] (`3` tasks) Declares the SQLSpec direct Arrow write capability with abort-safety and bounded-memory tests, forwards digests through SQLSpec, Advanced Alchemy, and Django storage, and adds the checksum benchmark ([#100](https://github.com/cofin/vertebra/issues/100)).
* [ch04 — Wheel Check, Docs, and OKF](ch04-wheel-check-docs-and-okf/spec.md) — [planned] (`3` tasks) Adds `make storage-wheel-check`, completes the limitations chapter, OKF, and user docs, and drafts the upstream SQLSpec hook and changelog.
