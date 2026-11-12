# Chapters: Live Driver Conformance CI and DuckDB GCS ADC Secret Rotation

`5` dependency-ordered chapters (`16` vertical-slice tasks) delivering live `pytest-databases` conformance lanes for the merged DuckDB, MySQL, MariaDB, SQL Server, and Oracle wheels plus shipped DuckDB GCS ADC secret rotation.

* [ch01 — Live Conformance Harness](ch01-live-conformance-harness/spec.md) — [planned] (`2` tasks) Adds the `VERTEBRA_CONFORMANCE_ENGINES` lane-selection and zero-skip gate, `driver_engine` tagging, versioned `pytest-databases` service fixtures, a MariaDB conformance engine, and removes dead conformance surface.
* [ch02 — GCS Bearer Secret Rotation](ch02-gcs-bearer-secret-rotation/spec.md) — [planned] (`3` tasks) Ships `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider`, hardens `DuckDBSecretConfig` and credential-rejection invalidation in the DuckDB drivers, and proves bearer rotation through live `httpfs` against a loopback GCS endpoint.
* [ch03 — Engine Live Suites](ch03-engine-live-suites/spec.md) — [planned] (`4` tasks) Adds live MySQL/MariaDB and SQL Server edge suites, tags the Oracle live suite for lane selection, and makes the Oracle AQ listener honor `wait_timeout_seconds` as its idle wait budget.
* [ch04 — Make and CI Lanes](ch04-make-and-ci-lanes/spec.md) — [planned] (`3` tasks) Adds `make db-conformance`, removes the `oracle-test` fallback branch, adds the `test-db-conformance` CI matrix job, and limits PyPI `duckdb` to `--group bench`.
* [ch05 — Docs and OKF](ch05-docs-and-okf/spec.md) — [planned] (`4` tasks) Synchronizes user docs, `AGENTS.md`, the changelog, OKF testing/workflow/data chapters, and a new driver conformance limitations record.
