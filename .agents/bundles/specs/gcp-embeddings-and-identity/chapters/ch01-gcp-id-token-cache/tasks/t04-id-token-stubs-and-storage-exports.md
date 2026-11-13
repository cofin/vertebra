---
type: Task
id: "gcp-embeddings-and-identity:ch01:t04"
flow_id: gcp-embeddings-and-identity
chapter_id: ch01-gcp-id-token-cache
title: "Add ID Token Stubs, Storage Re-Exports, and GCSConfig ADC Cleanup"
description: "Declare GcpIdTokenSnapshot, SyncGcpIdTokenCache, and AsyncGcpIdTokenCache in _vertebra.pyi, re-export them from vertebra.storage, and delete manual metadata-server heuristics in GCSConfig."
status: stable
state: completed
commit: a601dee4
last_verified_commit: a601dee4
last_verified_at: "2026-11-13T04:49:30Z"
verification_evidence: "stubtest vertebra._vertebra + ruff + mypy + pytest (19 passed, 2 skipped)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t03"
files:
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
read_set:
  - src/rs/vertebra-py/src/gcp_identity.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 110
tags:
  - gcp
  - identity
  - stubs
  - storage
sources:
  - id: py-stub
    resource: src/py/vertebra/_vertebra.pyi
    title: "Native type stub definitions checked by mypy.stubtest"
  - id: py-gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: "GCSConfig.to_token_cache_options and GCP cache imports"
  - id: storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: "Public vertebra.storage re-export surface"
---

# Task `t04`: Add ID Token Stubs, Storage Re-Exports, and `GCSConfig` ADC Cleanup

## Objective & Scope Boundary

Add exact `.pyi` stub declarations for `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` in `src/py/vertebra/_vertebra.pyi`, re-export all three from `vertebra.storage._store` and `vertebra.storage.__init__` (`__all__`), and remove the redundant `use_metadata_server` heuristic in `GCSConfig.to_token_cache_options` per [`../spec.md` §2](../spec.md) [^py-stub] [^py-gcs-config] [^storage-init]. Out of scope: cloud embedder consumers (`ch02`).

## Bounded Read Manifest

- `src/rs/vertebra-py/src/gcp_identity.rs` — PyO3 method signatures and defaults from `t03`.
- `src/py/vertebra/_vertebra.pyi` — `GcpTokenSnapshot`, `SyncGcpTokenCache`, `AsyncGcpTokenCache` stubs [^py-stub].
- `src/py/vertebra/storage/_store.py` — `GCSConfig.to_token_cache_options` and `_vertebra` imports [^py-gcs-config].
- `src/py/vertebra/storage/__init__.py` — `__all__` exports [^storage-init].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — ambient ADC precedence and export tests.

## Contracts & Edge Cases

- `_vertebra.pyi` signatures, keyword-only markers, defaults, and `RustDbTaskAwaitable[GcpIdTokenSnapshot]` return annotation match `vertebra-py/src/gcp_identity.rs` 1:1 so `python -m mypy.stubtest vertebra._vertebra` exits `0`.
- `vertebra.storage` exports `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` with object identity matching `vertebra._vertebra`.
- `GCSConfig.to_token_cache_options` relies on core ADC resolution order (`t02`) rather than injecting `use_metadata_server = "true"` when ambient ADC files or env vars may be present, while preserving `allow_ambient_adc=False` isolation when explicit credentials are configured.

## Test Matrix

- Add assertions in `src/py/tests/unit/storage/test_gcs_token_refresh.py`: `vertebra.storage.{GcpIdTokenSnapshot,SyncGcpIdTokenCache,AsyncGcpIdTokenCache}` identity and `__all__` membership; `GCSConfig` ambient ADC file precedence over metadata server and `allow_ambient_adc=False` isolation.

## Verification

```bash
uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync ruff check src/py/vertebra/_vertebra.pyi src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q
```

## Acceptance Criteria

- [x] `mypy.stubtest vertebra._vertebra` exits `0`, `vertebra.storage` re-exports all three ID token classes, and `GCSConfig` uses canonical core ADC order (`R3`, `R4`).
- [x] Zero regressions in the chapter's fast verification gate.

[^py-stub]: `src/py/vertebra/_vertebra.pyi` — Native type stub definitions checked by `mypy.stubtest`.
[^py-gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — `GCSConfig.to_token_cache_options` and GCP cache imports.
[^storage-init]: `src/py/vertebra/storage/__init__.py` — Public `vertebra.storage` re-export surface.
