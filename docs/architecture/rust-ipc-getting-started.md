# Rust IPC Core Getting Started

## Build & Test

```bash
cd src/rs
cargo build
cargo test --workspace
cargo doc --workspace --no-deps
```

## Quick SPSC Example (Rust)

```rust
use std::sync::Arc;
use vertebra_core::buffer::RingLayout;
use vertebra_core::channel::SpscRing;
use vertebra_core::shm::ShmRegion;

let layout = RingLayout::new(256, 64);
let region = Arc::new(ShmRegion::create(layout.total_size)?);
let ring = unsafe { SpscRing::initialize(region, layout, 0, 0)? };

ring.send(b"ping", 0)?;
let mut out = [0u8; 32];
let len = ring.recv(&mut out)?;
assert_eq!(&out[..len], b"ping");
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Notes

- Use power-of-two slot counts for SPSC/MPMC.
- Slot size should accommodate payload + header; prefer cacheline alignment.
- For multi-process usage on Unix, use `ShmRegion::create_named/open_named` and `SpscRing::attach`.
