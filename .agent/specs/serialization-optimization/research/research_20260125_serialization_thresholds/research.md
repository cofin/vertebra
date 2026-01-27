# Research: Serialization Benchmarks & Arrow Thresholds

## Executive Summary

-   **Arrow Threshold:** The current threshold of **1000 rows** is actually **optimistic** given the current Python implementation. `msgspec` (MessagePack) is significantly faster at serialization (~5x faster at 1000 rows) because `encode_arrow` incurs a heavy Python-side cost to pivot row-oriented data to column-oriented arrays.
-   **Optimization Opportunity:** To lower the threshold and realize Arrow's zero-copy benefits for smaller payloads, we must optimize the row-to-column conversion. Moving this logic to **Rust** (via `vertebra-py`) would likely eliminate the bottleneck, potentially bringing the crossover point down to ~100 rows.
-   **CBOR2 vs MsgPack:** `cbor2` is **5x-10x slower** than `msgspec`. There is no performance justification for switching. `msgspec` remains the optimal choice for small/row-oriented payloads.
    -   *Verification:* We confirmed the benchmark utilized the `_cbor2` C extension. `msgspec`'s performance advantage is intrinsic.
-   **Arrow Adoption:** We should focus on optimizing the *producer* side (Python encoding) before expanding adoption to other small-payload areas.

## Benchmark Analysis

We benchmarked `msgspec` vs `cbor2` vs `vertebra.resources.arrow.encode_arrow` (nanoarrow) on a standard dataset (5 columns, mixed types).

| Rows | MsgPack (us) | CBOR2 (us) | Arrow (us) | Winner |
|------|--------------|------------|------------|--------|
| 10 | 3.77 | 69.01 | 689.97 | MsgPack |
| 100 | 21.12 | 138.45 | 566.86 | MsgPack |
| 500 | 32.11 | 533.77 | 562.62 | MsgPack |
| 1000 | 206.49 | 1650.21 | 984.33 | MsgPack |
| 5000 | 1087.75 | 11506.60 | 5681.16 | MsgPack |

**Key Findings:**
1.  **MsgPack Dominance:** `msgspec` is incredibly efficient. It encodes 5000 rows in just ~1ms.
2.  **Arrow Overhead:** Even at 5000 rows, Arrow encoding takes ~5.6ms (vs 1ms for MsgPack).
    -   *Bottleneck:* The Python list comprehension `{col: [row[i] for row in rows] ...}` in `encode_arrow`.
    -   *Implication:* While Arrow saves time on the *consumer* side (zero-copy read), the *producer* side penalty is currently high.

## Threshold Evaluation

The user suspected 1000 rows was too high. **Our data suggests the opposite for the Producer.**
-   At 1000 rows, you pay a ~780us penalty to use Arrow.
-   The consumer must save >780us in decoding time to break even.
-   For a Browser/WASM client, this trade-off is likely worth it (JS parsing is slow).
-   For Server-to-Server, `msgspec` might still win at 1000 rows.

## Post-Implementation Note (2026-01-25)

We attempted to implement the recommended optimization (Task 7.1 in Native App Framework) by porting the Arrow encoder to Rust via PyO3.

**Results:**
-   **Rust Naive Iteration:** ~174ms (vs 62ms for Python/nanoarrow).
-   **Rust Hybrid (Msgspec -> Rust):** ~302ms.
-   **Verdict:** The overhead of PyO3 object iteration (`get_item`) is too high. `nanoarrow`'s C-extension iteration is significantly faster for `list[tuple]` data.

**Updated Recommendation:**
Abandon the Rust-based row-to-column pivot for Python objects. Stick with `nanoarrow` and keep the Arrow threshold at **1000 rows**. Any lower would likely result in a performance regression compared to pure `msgspec`.

## CBOR2 Evaluation

The request to evaluate `cbor2` was likely motivated by standard compliance or feature parity (e.g. tags), but the performance cost is prohibitive for a high-performance framework like Vertebra.
-   **Speed:** ~10x slower than `msgspec`.
-   **Feature Parity:** `msgspec` supports all necessary types.
-   **Verdict:** Reject. Stick with `msgspec`.

## Expanded Arrow Usage

Given the high fixed cost of producing Arrow messages currently (~600us minimum), we should **avoid** using Arrow for:
-   Small control messages (keep using MsgPack).
-   Single-row lookups.
-   Config/Metadata.

We **should** use Arrow for:
-   **Bulk Metrics:** If `src/ts/src/broker/metrics.ts` aggregates thousands of data points, sending them as Arrow columns would be efficient (and easy to create in JS).
-   **Log Batches:** If we implement a centralized log collector.

