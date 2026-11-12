# Chapters: Google Cloud Embedding Transports and Audience-Bound Identity Tokens

`6` dependency-ordered chapters (`17` vertical-slice tasks) that add cached OIDC ID token minting and correct the Google embedding transports.

* [ch01 — ID Token Core](ch01-id-token-core/spec.md) — [planned] (`4` tasks) Adds `GcpIdTokenCache` to `vertebra-core` with service-account, metadata-server, impersonation (any audience), gcloud CLI, and `authorized_user` ADC (`client_id` audience) minting, per-audience single-flight caching, and the metadata-server step at the end of ambient ADC discovery.
* [ch02 — ID Token Python Surface](ch02-id-token-python-surface/spec.md) — [planned] (`2` tasks) Exposes `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` on `vertebra._vertebra` with stubs and `vertebra.storage` re-exports.
* [ch03 — Google Embedder Runtime](ch03-google-embedder-runtime/spec.md) — [planned] (`3` tasks) Introduces the shared `_GoogleEmbedder` authentication and error base, per-event-loop micro-batch queues, and explicit-cache Oracle credential rotation.
* [ch04 — Gemini Wire and Multimodal](ch04-gemini-wire-and-multimodal/spec.md) — [planned] (`2` tasks) Fixes the Gemini `Part.text` field number, adds inline-data and file-data part encoding, and sends real media from `GeminiEmbedder.embed_multimodal`.
* [ch05 — Vertex REST and Remote Identity](ch05-vertex-rest-and-remote-identity/spec.md) — [planned] (`3` tasks) Moves `VertexEmbedder` to Vertex AI REST `:predict` / `:embedContent`, adds Vertex multimodal embedding, and authenticates `RemoteEmbedder` with ID tokens.
* [ch06 — Docs, OKF, and Limitations](ch06-docs-okf-and-limitations/spec.md) — [planned] (`3` tasks) Updates user docs, OKF chapters, and the new embeddings and identity limitations record.
