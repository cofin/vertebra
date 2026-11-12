---
type: Task
id: "driver-conformance-ci:ch02:t03"
chapter_id: ch02-gcs-bearer-secret-rotation
title: Prove bearer rotation through live httpfs against a loopback GCS endpoint
description: Add a live DuckDB suite that loads httpfs, creates a gcs secret backed by SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider, reads gs:// objects from a loopback GCS endpoint, and asserts rotated Authorization headers and credential-rejection invalidation.
status: draft
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
  - "driver-conformance-ci:ch02:t02"
read_set:
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/tests/integration/db/test_oracle_native.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - .agents/bundles/specs/driver-conformance-ci/chapters/ch02-gcs-bearer-secret-rotation/spec.md
files: []
tests:
  - src/py/tests/integration/db/test_duckdb_native.py
estimated_loc: 220
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
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: providers
    resource: src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider
    title: Shipped GCS bearer secret providers (ch02 t01)
  - id: duckdb-config
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig
    title: DuckDB extensions, secrets, and HTTP metadata cache flags
  - id: oracle-suite
    resource: src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config
    title: Precedent for a gated per-engine live suite
  - id: mock-token
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::_MockTokenServer
    title: Precedent loopback OAuth2 token server
tags:
  - duckdb
  - httpfs
  - gcs
  - integration
---

# Task t03: Prove Bearer Rotation Through Live `httpfs` Against a Loopback GCS Endpoint

## Objective & Scope Boundary

Create `src/py/tests/integration/db/test_duckdb_native.py`, the DuckDB live suite following the `test_oracle_native.py` precedent [^oracle-suite]:

1. `pytestmark = [pytest.mark.driver_engine("duckdb")]`.
2. Module fixture `httpfs_gate` skipping with `"DuckDB httpfs acceptance requires VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1"` unless `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS == "1"`, after `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", install_package="vertebra-core[duckdb]", extra="duckdb")`.
3. Fixture `gcs_endpoint` starting a stdlib `http.server.ThreadingHTTPServer` on `127.0.0.1:0` that serves `POST /token` (OAuth2 `refresh_token` grant returning `{"access_token": "tok-<n>", "expires_in": 3600, "token_type": "Bearer"}` with `n` incrementing per call), and `HEAD`/`GET` on `/vtb-bucket/data.csv` returning `id,label\n1,alpha\n2,beta\n` (honoring `Range: bytes=a-b` with `206`), recording each `Authorization` header in order; requests whose header is in a `rejected` set return `401`.
4. Tests building `DuckDBConfig(extensions=("httpfs",), enable_http_metadata_cache=False, httpfs_connection_caching=False, secrets=(DuckDBSecretConfig(name="vtb_gcs", secret_type="gcs", value={"endpoint": f"127.0.0.1:{port}", "url_style": "path", "use_ssl": False}, value_provider=provider, scope="gs://vtb-bucket/"),))` with providers from `SyncGcsBearerSecretProvider.from_config(GCSConfig(bucket="vtb-bucket", application_credentials=str(adc)))` (ADC `authorized_user` file whose `token_uri` is `http://127.0.0.1:<port>/token`) [^providers] [^duckdb-config] [^mock-token].

Out of scope: production code changes (owned by `t01`/`t02`); if the live run shows the rejection text or endpoint parameters differ from the plan, mark this task `blocked` with the observed text and route the fix to `t02` via `/flow:revise`.

## Bounded Read Manifest

- `src/py/vertebra/storage/_secrets.py` — provider classes [^providers].
- `src/py/vertebra/db/adapters/duckdb/config.py` — `DuckDBConfig`, `DuckDBSecretConfig` [^duckdb-config].
- `src/py/tests/integration/db/test_oracle_native.py` — gate pattern [^oracle-suite].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — `_MockTokenServer` handler shape [^mock-token].
- Chapter spec acceptance scenarios.

## Symbol & Signature Contracts

```python
class _LoopbackGcsHandler(BaseHTTPRequestHandler):
    """Serve OAuth2 refresh grants and a CSV object while recording bearer headers."""

class LoopbackGcsEndpoint:
    """Threaded loopback GCS XML endpoint and OAuth2 token issuer for DuckDB httpfs acceptance."""

    def __init__(self) -> None: ...
    @property
    def port(self) -> int: ...
    @property
    def authorizations(self) -> list[str]: ...
    @property
    def rejected(self) -> set[str]: ...
    def close(self) -> None: ...
```

- Fixtures: `httpfs_gate() -> None`, `gcs_endpoint(httpfs_gate: None) -> Iterator[LoopbackGcsEndpoint]`, `adc_file(gcs_endpoint, tmp_path) -> Path`.
- No inline comments; Google docstrings; module-level imports; parametrize sync/async via `@pytest.mark.parametrize("mode", ["sync", "async"])` where both drivers apply.

## Edge Cases & Error Semantics

- Offline or sandboxed runs without the env var skip cleanly with zero network access.
- With the env var set but no network to install `httpfs`, driver construction raises (fail loud); the zero-skip gate never hides it.
- HTTP metadata caching and keep-alive are disabled so each read issues fresh requests.
- After `provider.invalidate(1)` the next statement re-mints `tok-2` before the read.
- A rejected token (`tok-1` added to `rejected`) makes the read raise a `DatabaseError` containing `(HTTP 401)`; the provider is invalidated and the next read succeeds with `tok-2`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_duckdb_native.py`. Dependency category: real embedded DuckDB plus real `httpfs` against a loopback remote endpoint.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Gate offline | `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS` unset | Every test skips with the exact reason; `gcs_endpoint` never starts |
| Sync read with bearer | `SyncDuckDBDriver(cfg).select("SELECT count(*) AS n FROM read_csv('gs://vtb-bucket/data.csv')")` | `[{"n": 2}]`; every recorded data-request header equals `"Bearer tok-1"` |
| Sync proactive rotation | `provider.invalidate(1)` then the same select | `[{"n": 2}]`; headers after the invalidate equal `"Bearer tok-2"` |
| Async read and rotation | `AsyncDuckDBDriver` with `AsyncGcsBearerSecretProvider`; select; `provider.invalidate(1)`; select | Both return `[{"n": 2}]`; headers `tok-1` then `tok-2` |
| Reactive invalidation | `gcs_endpoint.rejected.add("Bearer tok-1")` after the first successful read; select | Raises `DatabaseError` whose text contains `(HTTP 401)`; `provider.generation == 2` afterwards; next select returns `[{"n": 2}]` with `"Bearer tok-2"` |
| Secret is scoped | `SELECT name, type, scope FROM duckdb_secrets() WHERE name = 'vtb_gcs'` | `[{"name": "vtb_gcs", "type": "gcs", "scope": ["gs://vtb-bucket/"]}]` |

## Verification

- Offline: `env -u VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS uv run --no-sync pytest src/py/tests/integration/db/test_duckdb_native.py -q -rs` exits `0` with all cases skipped.
- Live (network to install `httpfs`): `make dev && VERTEBRA_CONFORMANCE_ENGINES=duckdb VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1 uv run --no-sync pytest src/py/tests/integration/db/test_duckdb_native.py src/py/tests/integration/db/test_driver_conformance.py -q -rs` exits `0` with zero skips.
- Lint: `uv run --no-sync ruff check src/py/tests/integration/db/test_duckdb_native.py && uv run --no-sync ruff format --check src/py/tests/integration/db/test_duckdb_native.py`.

## Acceptance Criteria

- [ ] Live `httpfs` reads carry the provider's bearer token and rotate on proactive invalidation in both drivers (R14).
- [ ] A `401` rejection invalidates the provider and the following read succeeds with a fresh token (R13, R14).
- [ ] The suite skips cleanly offline and is tagged `driver_engine("duckdb")` (R14).

## Notes & Discoveries

- [2026-11-12 00:30] DuckDB `httpfs` accepts `BEARER_TOKEN` on `TYPE gcs` secrets; the `ENDPOINT` / `URL_STYLE` / `USE_SSL` overrides follow the S3-family secret parameters. Record the exact request paths and rejection text observed in the limitations record (`ch05 / t04`).

[^providers]: `src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider` — Shipped GCS bearer secret providers.
[^duckdb-config]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig` — DuckDB extensions, secrets, and cache flags.
[^oracle-suite]: `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` — Gated live suite precedent.
[^mock-token]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::_MockTokenServer` — Loopback token server precedent.
