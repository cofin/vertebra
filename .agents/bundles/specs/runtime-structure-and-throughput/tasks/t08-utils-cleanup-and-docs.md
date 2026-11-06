---
type: Task
id: runtime-structure-and-throughput:t08
title: Remove pure-Python utils fallbacks, clean SQLSpec aliases, and sync docs/OKF
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- runtime-structure-and-throughput:t02
- runtime-structure-and-throughput:t03
- runtime-structure-and-throughput:t06
- runtime-structure-and-throughput:t07
files:
- src/py/vertebra/utils/text.py
- src/py/vertebra/utils/humanize.py
- src/py/vertebra/utils/uuids.py
- src/py/vertebra/utils/module_loader.py
- src/py/vertebra/extensions/sqlspec/bigquery.py
- src/py/vertebra/extensions/sqlspec/spanner.py
- docs/usage/embeddings-and-vectors.md
- .agents/bundles/knowledge/patterns/python/serialization-and-utils.md
- .agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md
- .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
tests:
- src/py/tests/unit/utils/test_text_and_humanize.py
- src/py/tests/unit/utils/test_utils_core.py
- src/py/tests/unit/test_utils_init.py
- src/py/tests/unit/extensions/test_sqlspec_bigquery.py
- src/py/tests/unit/extensions/test_sqlspec_spanner.py
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T06:35:00Z'
commit: null
tags:
- utils
- cleanup
- docs
requirements:
- R8
---

# Remove pure-Python utils fallbacks, clean SQLSpec aliases, and sync docs/OKF

## Objective

Delete `_py_*` pure-Python fallback tables and `_LATIN_FOLD_MAP` from `vertebra.utils.{text,humanize}`, simplify `vertebra.utils.uuids` onto `vertebra._vertebra` + stdlib without `_load_uuid_utils` test hooks, ensure `module_loader.import_optional` fast-paths `sys.modules` hits, remove redundant internal aliases in `extensions/sqlspec/{bigquery,spanner}.py`, and update docs and OKF chapters.

## Context

Read `../spec.md` and `.agents/bundles/knowledge/patterns/python/serialization-and-utils.md`. Keep all public `vertebra.utils.*` helper names and signatures unchanged and preserve `src/py/vertebra/utils/__init__.py` untouched.

## Steps

1. Update `test_text_and_humanize.py` and `test_utils_core.py` to test public `vertebra.utils.{text,humanize,uuids,module_loader}` behavior directly without reaching into deleted `_py_*` or `_load_uuid_utils` internals, adding tests for `import_optional` `sys.modules` fast-path hits and cached negative misses.
2. Bind `src/py/vertebra/utils/text.py` and `src/py/vertebra/utils/humanize.py` directly to `vertebra._vertebra` (removing `_LATIN_FOLD_MAP`, `_py_*` implementations, and cross-module private imports), simplify `src/py/vertebra/utils/uuids.py` to `vertebra._vertebra` + `uuid` stdlib (removing `_UuidBackend` / `_load_uuid_utils`), verify `sys.modules.get(name)` fast-path in `src/py/vertebra/utils/module_loader.py`, and remove redundant internal aliases in `src/py/vertebra/extensions/sqlspec/{bigquery,spanner}.py`.
3. Update `docs/usage/embeddings-and-vectors.md` and the three OKF chapters (`serialization-and-utils.md`, `hot-path-and-async-dispatch.md`, `embeddings-and-vectors.md`) to reflect the unified fastpath core, Rust HTTP/2 `VertexEmbedder`, and direct `vertebra._vertebra` utils bindings.
4. Run full flow verification (`make lint-all`, `make rs-test`, `make py-test`, `make ts-test`, `make docs`) and commit `refactor(utils,docs): remove pure-Python utils fallbacks and sync runtime documentation`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/utils src/py/tests/unit/test_utils_init.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q
make lint-all
make docs
```

## Acceptance Criteria

- [ ] R8: `utils/{text,humanize,uuids}.py` contain zero `_py_*` fallback duplicates or `_load_uuid_utils` test hooks while preserving every public `vertebra.utils.*` symbol and passing `make lint-all` and `make docs`.

## Notes & Discoveries

None.
