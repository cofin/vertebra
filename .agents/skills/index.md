# Skills Index

Index of operational skills, review capability skills, paired agents, checklists, and knowledge links across Vertebra.

---

## Domain Review Areas

| Area | Prefix | Skill | Reviewer | Specialist | Checklist | FP Range | Knowledge Chapter |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `runtime-ipc` | `RT` | [review-runtime-ipc](review-runtime-ipc/SKILL.md) | [reviewer](../agents/runtime-ipc-reviewer.md) | [specialist](../agents/runtime-ipc-specialist.md) | [RT-01..12](review-core/references/checklist.md) | [FP-RT-01..03](review-core/references/false-positive-suppressions.md) | [Lock-free SHM](../bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md) |
| `http-ingress` | `HTTP` | [review-http-ingress](review-http-ingress/SKILL.md) | [reviewer](../agents/http-ingress-reviewer.md) | [specialist](../agents/http-ingress-specialist.md) | [HTTP-01..12](review-core/references/checklist.md) | [FP-HTTP-01](review-core/references/false-positive-suppressions.md) | [Ingress & routing](../bundles/knowledge/architecture/http/ingress-and-routing.md) |
| `pyo3-bridge` | `FFI` | [review-pyo3-bridge](review-pyo3-bridge/SKILL.md) | [reviewer](../agents/pyo3-bridge-reviewer.md) | [specialist](../agents/pyo3-bridge-specialist.md) | [FFI-01..10](review-core/references/checklist.md) | [FP-FFI-01..02](review-core/references/false-positive-suppressions.md) | [FFI discipline](../bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md) |
| `python-framework` | `PY` | [review-python-framework](review-python-framework/SKILL.md) | [reviewer](../agents/python-framework-reviewer.md) | [specialist](../agents/python-framework-specialist.md) | [PY-01..12](review-core/references/checklist.md) | [FP-PY-01..03](review-core/references/false-positive-suppressions.md) | [Module layout](../bundles/knowledge/patterns/python/module-and-class-layout.md) |
| `typescript-client` | `TS` | [review-typescript-client](review-typescript-client/SKILL.md) | [reviewer](../agents/typescript-client-reviewer.md) | [specialist](../agents/typescript-client-specialist.md) | [TS-01..08](review-core/references/checklist.md) | [FP-TS-01](review-core/references/false-positive-suppressions.md) | [Bundler & SDK](../bundles/knowledge/domains/frontend/bundler-and-client-sdk.md) |
| `performance` | `PERF` | [review-performance](review-performance/SKILL.md) | [reviewer](../agents/performance-reviewer.md) | [specialist](../agents/performance-specialist.md) | [PERF-01..09](review-core/references/checklist.md) | [FP-PERF-01](review-core/references/false-positive-suppressions.md) | [Benchmarks](../bundles/knowledge/testing/benchmarks-and-free-threaded.md) |
| `tests` | `TEST` | [review-tests](review-tests/SKILL.md) | [reviewer](../agents/tests-reviewer.md) | [specialist](../agents/tests-specialist.md) | [TEST-01..11](review-core/references/checklist.md) | [FP-TEST-01](review-core/references/false-positive-suppressions.md) | [Polyglot matrix](../bundles/knowledge/testing/polyglot-test-matrix.md) |
| `docs-okf` | `DOC` | [review-docs-okf](review-docs-okf/SKILL.md) | [reviewer](../agents/docs-okf-reviewer.md) | [specialist](../agents/docs-okf-specialist.md) | [DOC-01..09](review-core/references/checklist.md) | [FP-DOC-01](review-core/references/false-positive-suppressions.md) | [Workflow](../bundles/knowledge/workflow.md) |

---

## Workflow and Operational Skills

- [`flow-memory-keeper`](flow-memory-keeper/SKILL.md): Continuous OKF chapter curation, Flow closeout protocol, and durable pattern elevation.
- [`review-core`](review-core/SKILL.md): Foundation review infrastructure, finding grammar, and false-positive catalog.
