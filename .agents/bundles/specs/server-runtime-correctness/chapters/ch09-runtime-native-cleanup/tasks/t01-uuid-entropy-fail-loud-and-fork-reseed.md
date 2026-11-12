---
type: Task
id: server-runtime-correctness:ch09:t01
title: Fail Loud on Missing OS Entropy and Reseed UUID Pools After Fork
description: Replace fill_os_random's platform branches and time-seeded fallback with getrandom::fill that panics on failure, and discard each thread's EntropyPool buffer after fork through a pthread_atfork-driven generation counter.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch09-runtime-native-cleanup/spec.md
  - src/rs/vertebra-core/src/id.rs
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/Cargo.toml
  - .agents/skills/runtime-ipc/SKILL.md
files:
  - src/rs/vertebra-core/src/id.rs
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/Cargo.toml
tests:
  - src/rs/vertebra-core/src/id.rs
estimated_loc: 110
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:30:00Z"
sources:
  - id: id-rs
    resource: src/rs/vertebra-core/src/id.rs::fill_os_random
    title: Entropy source with time-seeded fallback
  - id: entropy-pool-rs
    resource: src/rs/vertebra-core/src/id.rs::EntropyPool
    title: Thread-local 256-byte entropy pool without fork detection
tags:
  - entropy
  - uuid
  - fork
---

# Task t01: Fail Loud on Missing OS Entropy and Reseed UUID Pools After Fork

## Objective & Scope Boundary

Make `fill_os_random` call `getrandom::fill` and panic when it fails, deleting the Linux `libc::getrandom` loop, the macOS `arc4random_buf` branch, the `/dev/urandom` read, and the `SystemTime ^ ptr` splitmix fallback [^id-rs]. Add fork detection so a child process never reuses bytes left in its parent's `THREAD_ENTROPY` pool [^entropy-pool-rs]. Out of scope: UUID formatting, `UUID7_STATE` monotonic ordering, and the nanoid alphabet.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-core/src/id.rs` — `EntropyPool`, `THREAD_ENTROPY`, `fill_os_random`, `uuid4_u128`, `uuid7_u128`, `nanoid`, `mod tests`.
- `src/rs/vertebra-core/Cargo.toml` — `[dependencies]` and `[target.'cfg(unix)'.dependencies] libc`.
- `src/rs/Cargo.toml` — `[workspace.dependencies]`; the lockfile already resolves `getrandom 0.3.4`.
- `.agents/skills/runtime-ipc/SKILL.md` — unsafe and atomics discipline.

## Symbol & Signature Contracts

```rust
// src/rs/Cargo.toml [workspace.dependencies]
getrandom = "0.3"

// src/rs/vertebra-core/src/id.rs
static FORK_GENERATION: AtomicU64 = AtomicU64::new(0);
static ATFORK_REGISTERED: std::sync::Once = std::sync::Once::new();

struct EntropyPool {
    buf: [u8; ENTROPY_POOL_BYTES],
    pos: usize,
    generation: u64,
}

fn fill_os_random(dest: &mut [u8]) {
    if let Err(err) = getrandom::fill(dest) {
        panic!("vertebra-core: OS entropy source unavailable: {err}");
    }
}

#[cfg(unix)]
extern "C" fn bump_fork_generation();
```

- `EntropyPool::new` calls `ATFORK_REGISTERED.call_once(|| unsafe { libc::pthread_atfork(None, None, Some(bump_fork_generation)) })` on Unix, with a `// SAFETY:` comment stating that the child handler only performs an atomic increment, which is async-signal-safe.
- `EntropyPool::fill_bytes` loads `FORK_GENERATION` with `Ordering::Relaxed`; when it differs from `self.generation`, it stores the new value and calls `refill()` before copying. `refill` records the current generation.
- `bump_fork_generation` performs `FORK_GENERATION.fetch_add(1, Ordering::Relaxed)`.
- A non-zero `pthread_atfork` return panics with `vertebra-core: pthread_atfork failed: <rc>`.
- Drop the now-unused `std::io::Read` and `std::time::{SystemTime, UNIX_EPOCH}` imports unless `uuid7_u128` still uses the time import.

## Edge Cases & Error Semantics

- `getrandom::fill` failure panics; no deterministic bytes are ever produced.
- A fork from a thread whose pool is partly consumed: the child's first draw refills.
- Threads created in the child start with a fresh pool whose `generation` is the current counter.
- Windows has no fork; `ATFORK_REGISTERED` and `bump_fork_generation` are `#[cfg(unix)]`, and `generation` stays `0`.
- A spurious generation bump only costs one extra refill.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/src/id.rs` `#[cfg(test)] mod tests`, dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Simulated fork | Draw one `uuid4_u128()`, snapshot the pool's remaining bytes, `FORK_GENERATION.fetch_add(1, Relaxed)`, draw again | The second UUID's 16 bytes differ from the first 16 bytes of the snapshot |
| Real fork (`#[cfg(unix)]`) | Draw one UUID in the parent, `libc::fork()`; child draws a UUID and writes it to a pipe then `_exit(0)`; parent draws its next UUID | Child UUID ≠ parent's next UUID; child exit status `0` |
| Version and variant | `uuid4_u128()` after a generation bump | Version nibble `4`, variant bits `10` |
| No fallback | Source inspection test: `include_str!("id.rs")` | Does not contain `SystemTime::now().duration_since` inside `fill_os_random` |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core id::tests -- --nocapture`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core id::tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings
  make rs-test
  ```
- Planned local commit: `fix(core): fail loud on missing entropy and reseed UUID pools after fork`

## Acceptance Criteria

- [ ] `fill_os_random` uses `getrandom::fill` and panics on failure with no time-seeded fallback (R20).
- [ ] Parent and forked child produce distinct UUIDs (R20).

## Notes & Discoveries

- The fail-loud panic is recorded as a runtime caveat in the limitations chapter by `ch10:t04`.

[^id-rs]: `src/rs/vertebra-core/src/id.rs::fill_os_random` — Entropy source with time-seeded fallback.
[^entropy-pool-rs]: `src/rs/vertebra-core/src/id.rs::EntropyPool` — Thread-local 256-byte entropy pool without fork detection.
