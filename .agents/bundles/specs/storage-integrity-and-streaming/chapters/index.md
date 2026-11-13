# Chapters: Object Storage Content-Integrity Receipts, Verified Reads, and Companion Wheel Isolation (`storage-integrity-and-streaming`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-integrity-receipts-and-verified-reads`](ch01-integrity-receipts-and-verified-reads/spec.md) | `completed` | `3` | Rust & Python CRC32C/MD5/SHA-256 content-integrity receipts on `ObjectMeta`/`PutResult`, abort-before-publish checks, verified streaming reads, and `vertebra.typing.IntegrityError` + `vertebra.storage` re-exports (`b8b25207`). |
| [`ch02-streaming-arrow-writes-and-integrations`](ch02-streaming-arrow-writes-and-integrations/spec.md) | `completed` | `3` | Streaming Arrow/Parquet multipart writes and digest forwarding across `vertebra.storage`, SQLSpec (`supports_direct_arrow_write`), Advanced Alchemy, and Django, plus `put_checksum` Criterion benchmark (`c9f54436`). |
| [`ch03-wheel-isolation-and-okf-sync`](ch03-wheel-isolation-and-okf-sync/spec.md) | `completed` | `3` | Parameterized `verify_storage_wheel.py` + `Makefile` / `release.yaml` isolation gates for `storage`, `bigquery`, `spanner`, and `bundler`, user docs/changelog sync, and `<= 200L` storage OKF + upstream SQLSpec note (`66e6913f`). |
