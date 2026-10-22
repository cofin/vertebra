---
name: review-tests
description: Evaluates test strength, observable assertions, negative path coverage, concurrency barriers, and test suite hygiene across polyglot test suites.
---

<!-- truth: start -->
- **Observable**: Assertions verify observable byte sequences, headers, or state; no mock-echo checks (`TEST-03`).
- **Concurrency**: Real multi-threaded synchronization using barriers; no wall-clock sleeps (`TEST-04`).
- **Failures**: Defect fixes require a test that fails without the change (red/green proof) (`TEST-01`).
- **Hygiene**: Canonical test naming; no `test_*_fix.py`, `*remediation*`, or stage numbers (`TEST-02`).
- **Async**: Async tests use `@pytest.mark.anyio` without pytest-asyncio plugins (`TEST-07`).
- **Suppressions**: Respect `FP-TEST-01` in the shared catalog.
<!-- truth: end -->

# Tests Review (`review-tests`)

## Scope

- Paths: `[areas.tests]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `TEST-01`..`TEST-11` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Polyglot test matrix](../../bundles/knowledge/testing/polyglot-test-matrix.md)
- [Benchmarks and free threaded](../../bundles/knowledge/testing/benchmarks-and-free-threaded.md)
- [Test strength criteria](../review-core/references/test-strength.md)

## Review Process

1. **Failure verification**: Confirm that the test reliably fails without the bug fix applied (`TEST-01`).
2. **Canonical placement**: Ensure tests reside in canonical test files matching implementation module names (`TEST-02`).
3. **Contract assertions**: Reject assertions that merely check `is not None` or mock return echo (`TEST-03`).
4. **Concurrency synchronization**: Eliminate `time.sleep` or `thread::sleep` in favor of sync primitives (`TEST-04`).
5. **Fault path coverage**: Verify tests cover timeout, overflow, malformed payload, and disconnection paths (`TEST-05`).
6. **Free-threaded verification**: Ensure GIL-disabled execution is verified for extensions (`TEST-10`).

## Suppressions Index

- `FP-TEST-01`: Anyio test markers

## Focused Commands

```bash
uv run pytest src/py/tests/unit
cargo test --workspace --no-run
cd src/ts/vertebra-client && bun run test
```
