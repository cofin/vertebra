# Vertebra Skills and Agents Index

This index maps every engineering domain and review area across Vertebra to its domain implementation skill, review skill, paired reviewer/specialist subagents, checklist items, false-positive catalog range, and canonical OKF v0.2 knowledge chapters.

## Foundation Skills

- [`review-core`](review-core/SKILL.md): Shared area manifest ([`areas.toml`](review-core/references/areas.toml)), checklist ([`checklist.md`](review-core/references/checklist.md)), false-positive suppressions ([`false-positive-suppressions.md`](review-core/references/false-positive-suppressions.md)), test strength criteria ([`test-strength.md`](review-core/references/test-strength.md)), and finding ledger grammar ([`ledger-format.md`](review-core/references/ledger-format.md)).

## Engineering Domain & Review Capability Matrix

| Area | Prefix | Engineering Skill | Review Skill | Reviewer | Specialist | Checklist | FP Range | Knowledge Chapter |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `runtime-ipc` | `RT` | [`runtime-ipc`](runtime-ipc/SKILL.md) | [`review-runtime-ipc`](review-runtime-ipc/SKILL.md) | [`runtime-ipc-reviewer`](../agents/runtime-ipc-reviewer.md) | [`runtime-ipc-specialist`](../agents/runtime-ipc-specialist.md) | `RT-01`..`RT-12` | `FP-RT-01`..`03` | [`lock-free-shm-and-sharding.md`](../bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md) |
| `http-ingress` | `HTTP` | [`http-ingress`](http-ingress/SKILL.md) | [`review-http-ingress`](review-http-ingress/SKILL.md) | [`http-ingress-reviewer`](../agents/http-ingress-reviewer.md) | [`http-ingress-specialist`](../agents/http-ingress-specialist.md) | `HTTP-01`..`HTTP-12` | `FP-HTTP-01` | [`ingress-and-routing.md`](../bundles/knowledge/architecture/http/ingress-and-routing.md) |
| `pyo3-bridge` | `FFI` | [`pyo3-bridge`](pyo3-bridge/SKILL.md) | [`review-pyo3-bridge`](review-pyo3-bridge/SKILL.md) | [`pyo3-bridge-reviewer`](../agents/pyo3-bridge-reviewer.md) | [`pyo3-bridge-specialist`](../agents/pyo3-bridge-specialist.md) | `FFI-01`..`FFI-10` | `FP-FFI-01`..`02` | [`pyo3-ffi-and-batch-dispatch.md`](../bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md) |
| `python-framework` | `PY` | [`python-framework`](python-framework/SKILL.md) | [`review-python-framework`](review-python-framework/SKILL.md) | [`python-framework-reviewer`](../agents/python-framework-reviewer.md) | [`python-framework-specialist`](../agents/python-framework-specialist.md) | `PY-01`..`PY-12` | `FP-PY-01`..`03` | [`module-and-class-layout.md`](../bundles/knowledge/patterns/python/module-and-class-layout.md) |
| `typescript-client` | `TS` | [`typescript-client`](typescript-client/SKILL.md) | [`review-typescript-client`](review-typescript-client/SKILL.md) | [`typescript-client-reviewer`](../agents/typescript-client-reviewer.md) | [`typescript-client-specialist`](../agents/typescript-client-specialist.md) | `TS-01`..`TS-08` | `FP-TS-01` | [`bundler-and-client-sdk.md`](../bundles/knowledge/domains/frontend/bundler-and-client-sdk.md) |
| `performance` | `PERF` | — | [`review-performance`](review-performance/SKILL.md) | [`performance-reviewer`](../agents/performance-reviewer.md) | [`performance-specialist`](../agents/performance-specialist.md) | `PERF-01`..`PERF-09` | `FP-PERF-01` | [`benchmarks-and-free-threaded.md`](../bundles/knowledge/testing/benchmarks-and-free-threaded.md) |
| `tests` | `TEST` | — | [`review-tests`](review-tests/SKILL.md) | [`tests-reviewer`](../agents/tests-reviewer.md) | [`tests-specialist`](../agents/tests-specialist.md) | `TEST-01`..`TEST-11` | `FP-TEST-01` | [`polyglot-test-matrix.md`](../bundles/knowledge/testing/polyglot-test-matrix.md) |
| `docs-okf` | `DOC` | — | [`review-docs-okf`](review-docs-okf/SKILL.md) | [`docs-okf-reviewer`](../agents/docs-okf-reviewer.md) | [`docs-okf-specialist`](../agents/docs-okf-specialist.md) | `DOC-01`..`DOC-09` | `FP-DOC-01` | [`workflow.md`](../bundles/knowledge/workflow.md) |
