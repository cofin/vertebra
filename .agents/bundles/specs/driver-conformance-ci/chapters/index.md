# Chapters: Live Driver Conformance CI and DuckDB GCS Secret Rotation (`driver-conformance-ci`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-duckdb-gcs-secrets-and-oracle-notify`](ch01-duckdb-gcs-secrets-and-oracle-notify/spec.md) | `completed` | `3` | `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider`, `DuckDBSecretConfig` validation + `401`/`403` invalidation + startup `_client` cleanup, and Oracle AQ `wait_timeout_seconds` liveness (`cargo test -p vertebra-oracle notify:: && pytest test_gcs_token_refresh.py test_duckdb_driver.py`). |
| [`ch02-conformance-harness-and-live-suites`](ch02-conformance-harness-and-live-suites/spec.md) | `completed` | `4` | `ConformanceGate`, `ServiceVersionMatrix`, `mariadb` `EngineConformanceSpec` + dead-surface cleanup, and DuckDB, MySQL/MariaDB, SQL Server, and Oracle live suites (`pytest test_conformance_gate.py test_duckdb_native.py test_driver_conformance.py`). |
| [`ch03-make-lanes-bench-isolation-and-ci`](ch03-make-lanes-bench-isolation-and-ci/spec.md) | `completed` | `2` | Single-pass `Makefile` (`db-conformance`, `oracle-test`, `bench`-only PyPI `duckdb`) and `.github/workflows/ci.yml` `test-db-conformance` + `conformance-oracle` jobs (`pytest test_bench_infrastructure.py && make review-check`). |
| [`ch04-docs-and-okf-sync`](ch04-docs-and-okf-sync/spec.md) | `completed` | `4` | Storage/analytics/database user docs, changelog, `AGENTS.md`, testing/workflow OKF chapters, and in-place `database-driver-limitations.md` update (`make docs && make review-check`). |
