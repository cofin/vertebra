---
name: review-core
description: Use when auditing or executing code reviews across Vertebra to apply shared area manifests, checklist items, false-positive suppressions, test strength criteria, and finding ledger formats.
---

<!-- truth: start -->
- **Manifest**: Area routing, shards, and tool execution policies are defined strictly in `areas.toml`.
- **Checklist**: Stable IDs (`RT`, `HTTP`, `FFI`, `PY`, `TS`, `PERF`, `TEST`, `DOC`) with observable verification.
- **Evidence**: Findings require concrete code citations and reproducible test or compiler output.
- **Confidence**: `critical` and `important` findings require `Confidence: verified` before acceptance.
- **Suppression**: Respect all `FP-*` catalog entries and verify bidirectional skill parity.
<!-- truth: end -->

# Review Core (`review-core`)

The `review-core` skill defines the shared infrastructure, schemas, and grammar for all domain review skills and paired agents across Vertebra.

## Canonical References

All review operations derive from the six authoritative reference documents:

1. [`areas.toml`](references/areas.toml): Manifest declaring area prefixes, path globs, shards, preloads, and tool constraints.
2. [`checklist.md`](references/checklist.md): Repository-wide checklist with stable rule IDs and verification commands.
3. [`false-positive-suppressions.md`](references/false-positive-suppressions.md): Shared catalog of verified false-positive exceptions.
4. [`test-strength.md`](references/test-strength.md): Standards for strong observable tests versus weak mock-echo assertions.
5. [`ledger-format.md`](references/ledger-format.md): Machine-validated finding grammar, severity tiers, and status lifecycle.
6. [`suppression-playbook.md`](references/suppression-playbook.md): Triage protocol and authoring guide for new suppression entries.

## Severity Tiers

Reviewers categorize candidate findings into one of three strict tiers:

- **`critical`**: Undefined behavior, memory corruption, data races, deadlocks, missed wakeups, FFI/SHM lifetime errors, wire-protocol violations, or 3.14t free-threaded GIL re-enablement.
- **`important`**: Edge-case bugs, leaks (slots, file descriptors, memoryviews), unbounded buffers, measured performance drops, missing behavioral tests, or contract drift.
- **`minor`**: Formatting, symbol naming, documentation gaps, and non-blocking test hygiene issues.

## Reviewer Command Restrictions

Read-only reviewers must never run state-altering commands:
- Never run mutating build commands (`make dev`, `make install`, `maturin develop`).
- Never run tests that clear shared memory without locks (`make rs-test`).
- Never run auto-fixing linters or formatters (`make lint`, `cargo fmt` without `--check`).
- Never run git mutation commands (`git commit`, `git push`, `git reset`, `git checkout`).
- Use only targeted inspection commands (`cargo check --manifest-path src/rs/Cargo.toml`, `cargo test --manifest-path src/rs/Cargo.toml -p <crate> --test <name>`, `git diff`).
