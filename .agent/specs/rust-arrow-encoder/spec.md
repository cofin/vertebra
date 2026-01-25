# Rust-Based Arrow Encoder (Optimization)

## Overview
This PRD defines the implementation of a high-performance Arrow IPC encoder written in Rust, integrated into `vertebra-py`. The goal is to replace the current Python-based row-to-column pivot logic, which is a significant bottleneck (5x slower than msgpack), with a zero-copy Rust implementation.

## Research Reference
- `../active/serialization-optimization/research/research_20260125_serialization_thresholds/research.md`

## Functional Requirements
### Must Have
- [ ] **Rust Encoder Function:** Implement `encode_arrow_rows` in `vertebra-py` (Rust) exposed to Python.
- [ ] **Input Support:**
    - [ ] List of Tuples (`list[tuple]`)
    - [ ] List of Dictionaries (`list[dict]`)
    - [ ] List of Objects (`Pydantic`, `msgspec.Struct`, `dataclasses`, `attrs`)
- [ ] **Schema Handling:** Accept an explicit schema definition (field names and types) from Python to avoid runtime introspection overhead.
- [ ] **Native Type Handling:**
    - [ ] Integers, Floats, Strings, Booleans, Bytes
    - [ ] `None` (Null handling)
    - [ ] **Datetime/Date/Time:** Use PyO3 `PyDateTime` C-API for zero-copy extraction.
- [ ] **Output:** Return a standard Arrow IPC stream (bytes) compatible with `nanoarrow` and `pyarrow`.

### Should Have
- [ ] **Batch Processing:** Support processing data in chunks to minimize memory pressure for large datasets.

## Non-Functional Requirements
- **Performance:**
    - Must be at least **5x faster** than the current Python implementation for 1000+ rows.
    - Encoding 5000 simple rows should take < 2ms (currently ~5.6ms).
- **Safety:** Use safe Rust code where possible; ensure no memory leaks or segfaults when handling malformed Python objects.

## Technical Approach
### Recommended Implementation
1.  **Python Layer (`vertebra.resources.arrow`):**
    - Reuse `vertebra.types.extractor` to resolve the schema of the input objects *once*.
    - Convert this schema into a lightweight "Field Map" (e.g., list of `(field_name, arrow_type_enum)`).
    - Pass the data list and the Field Map to the Rust function.

2.  **Rust Layer (`src/rs/vertebra-py/src/arrow_encoder.rs`):**
    - Use `pyo3` to iterate over the input Python list.
    - For each column in the schema:
        - Allocate a native Arrow builder (e.g., `arrow::array::Int64Builder`).
        - Iterate the rows, extracting the specific attribute/key using `PyAny::getattr` or `PyAny::get_item`.
        - Use `PyDateTime` bindings for efficient temporal type extraction.
    - Construct a `RecordBatch` from the built arrays.
    - Write to an IPC buffer using `arrow::ipc::writer::StreamWriter`.

### Files to Modify
- `src/rs/vertebra-py/Cargo.toml`: Add `arrow` crate (if not present).
- `src/rs/vertebra-py/src/lib.rs`: Register the new module.
- `src/rs/vertebra-py/src/arrow.rs` (New): Implementation of the encoder.
- `src/py/vertebra/resources/arrow.py`: Update `encode_arrow` to use the Rust implementation.

## Acceptance Criteria
- [ ] Benchmark confirms >5x speedup for 1000 row batch of mixed types.
- [ ] `pytest` suite passes, verifying correctness for all supported input types (dict, tuple, object).
- [ ] Dates and timestamps are correctly preserved across the bridge.
- [ ] Null values are correctly handled for optional fields.

## Out of Scope
- Full replacement of `nanoarrow` or `pyarrow` dependencies (we still use them for decoding/wrapper types).
- Complex nested types (Lists of Lists, deeply nested Structs) for the initial version (focus on tabular data).

## Dependencies
### Internal Dependencies
- `vertebra-py` (Rust extension)
- `vertebra.types.extractor` (Python schema resolution)

### External Dependencies
- `arrow` (Rust crate)
- `pyo3` (Rust crate)

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **Schema Mismatch:** Python objects don't match provided schema. | Medium | Low | Rust `extract` calls will fail cleanly returning `PyErr`. We must ensure good error messages. |
| **Memory Usage:** Building large Arrow batches in memory. | Low | Medium | Current implementation does this too. Rust's memory management is generally more efficient. |

### Recovery Strategy
**Rollback Trigger:** Segfaults in CI or severe performance regression.
**Rollback Steps:**
1. Revert changes to `src/py/vertebra/resources/arrow.py` to use the Python-only implementation.
2. Disable the Rust module in `lib.rs`.

## Open Questions
- None.
