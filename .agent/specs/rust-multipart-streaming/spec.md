# Rust Multipart Streaming

## Overview

High-performance `multipart/form-data` handling for file uploads, implemented natively in Rust to avoid buffering large payloads in Python memory.

## Context (Migration)
Formerly `native-multipart-fallback`. The previous Bun-based design relied on Bun buffering or piping. The Rust architecture allows for true streaming from the socket to disk/storage.

## Functional Requirements

### Must Have
- [ ] **Streaming Parser:** Use `mule` or `axum_extra` to parse multipart streams on the fly in Rust.
- [ ] **Direct-to-Storage:** Stream file parts directly to a temporary file (or S3 in the future) without passing chunks through Python.
- [ ] **Metadata Hand-off:** Pass only the file path/metadata to Python once the upload is complete.
- [ ] **Backpressure:** Ensure the Rust TCP stream respects backpressure from the disk writer.

## Technical Approach
- Rust `vertebra-server` detects `multipart/form-data`.
- Spawns a dedicated task to drain the body.
- Writes to `tempfile`.
- Sends `IPC_MULTIPART_COMPLETE` message to Python with file path.
