---
type: Gotcha
title: Embeddings Limitations
description: Verified gaps in the Google cloud embedders, covering the Vertex AI wire target and Gemini multimodal handling, that callers and maintainers must account for.
status: stable
tags:
- agent
- embeddings
- gemini
- vertex-ai
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-sync
  at: "2026-11-12T01:15:00Z"
  commit: c8722c76
sources:
- id: vertex-embedder
  resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
  title: VertexEmbedder endpoint and BatchEmbedContents request path
- id: gemini-embedder
  resource: src/py/vertebra/embeddings/_google.py::GeminiEmbedder.embed_multimodal
  title: GeminiEmbedder multimodal descriptor conversion
---

# Embeddings Limitations

<!-- truth: start -->
- `VertexEmbedder` defaults its endpoint to `https://{location}-aiplatform.googleapis.com` but posts to the Generative Language method path `google.ai.generativelanguage.v1beta.GenerativeService/BatchEmbedContents`; it does not call a Vertex AI `PredictionService` method [^vertex-embedder].
- `GeminiEmbedder.embed_multimodal(items, *, task="document")` sends no image, audio, video, or PDF parts. Each `MultimodalPart` becomes the text string `"[<mime_type>:<uri or base64(data)>]<text>"`, and the batch is embedded through `embed_many` as plain text, so the returned vectors embed the descriptor string rather than the media [^gemini-embedder].
- Because inline data is base64-encoded into the text descriptor, large media payloads count against text token budgets (`max_batch_tokens`) instead of being sent as inline-data or file-data parts [^gemini-embedder].
<!-- truth: end -->

## Planned Fixes

The `gcp-embeddings-and-identity` flow owns the Google embedder runtime, the Gemini wire encoding with real inline-data and file-data parts for `embed_multimodal`, and the Vertex AI REST target.

Current embedder behavior is described in [Shared-Memory Vector Slabs, Zero-Weight Cloud & SQL Embeddings, and Local Broker](embeddings-and-vectors.md).

[^vertex-embedder]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — `VertexEmbedder` endpoint and `BatchEmbedContents` request path.
[^gemini-embedder]: `src/py/vertebra/embeddings/_google.py::GeminiEmbedder.embed_multimodal` — `GeminiEmbedder` multimodal descriptor conversion.
