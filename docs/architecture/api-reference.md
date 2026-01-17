# API Reference

## Rust

- Generated rustdoc: `src/rs/target/doc/vertebra_core/index.html`
- Key modules: `shm`, `channel`, `buffer`, `sync`

## Python (`vertebra._vertebra`)

- `ShmRegion`
  - `create_named(name, size)` / `open_named(name, size)` (Unix)
  - `read(offset, len)`, `write(offset, data)`
- `SpscRing`, `MpmcRing`
  - `send(data, flags)`, `recv(max_len)`
- `BufferPool`, `Buffer`
  - `acquire()`, `read()`, `write(data)`
- `BufferChannel`
  - `acquire()`, `send(buffer, len, flags)`, `recv()`

## JS/Bun (napi)

Bindings mirror the Python surface with `ShmRegion`, `SpscRing`, `MpmcRing`,
`BufferPool`, `BufferChannel`, and `PoolBuffer` classes.

## C ABI

Optional C ABI is intentionally not implemented unless required by the PRD-1
decision checklist.
