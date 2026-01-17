# Error Handling Patterns

## Rust Core

- `ShmError`: OS errors, invalid names/sizes, mapping failures.
- `ChannelError`: invalid layouts, full/empty states, payload limits, sync errors.
- `SyncError`: wait/wake unsupported or syscall failures.

All errors use `thiserror` and preserve the original OS error when available.

## Python

PyO3 bindings map Rust errors to `RuntimeError` by default. Input validation
errors (e.g., payload size) raise `ValueError`.

## JS/Bun

napi-rs bindings convert Rust errors into JS exceptions with a human-readable
message.
