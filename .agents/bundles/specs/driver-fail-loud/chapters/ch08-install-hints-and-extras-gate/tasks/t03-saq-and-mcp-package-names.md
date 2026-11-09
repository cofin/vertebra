---
type: Task
id: "driver-fail-loud:ch08:t03"
chapter_id: ch08-install-hints-and-extras-gate
title: Replace sentence-valued package arguments in the litestar-saq and litestar-mcp plugin factories
description: Make create_saq_plugin and create_mcp_plugin pass litestar-saq and litestar-mcp as package= instead of a sentence, and pin the resulting MissingDependencyError package and install_package in the existing SAQ and MCP ecosystem tests.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-fail-loud:ch08:t01"
read_set:
  - src/py/vertebra/extensions/litestar/saq/plugin.py
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
files:
  - src/py/vertebra/extensions/litestar/saq/plugin.py
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py
estimated_loc: 30
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T00:00:00Z"
last_verified_commit: 616de0f2
verification_evidence: "pytest test_litestar_ecosystem_frontend_and_saq.py test_litestar_ecosystem_mcp.py passed"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T00:00:00Z"
commit: 616de0f2
sources:
  - id: saq-plugin
    resource: src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin
    title: litestar-saq plugin factory with a sentence-valued package argument
  - id: mcp-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: litestar-mcp plugin factory with a sentence-valued package argument
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: ensure_module forwarding package, extra, and install_package to MissingDependencyError
  - id: saq-tests
    resource: src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors
    title: Frontend and SAQ missing-dependency test with import_optional patched to None
  - id: mcp-tests
    resource: src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp
    title: MCP missing-dependency test with import_optional patched to None
tags:
  - packaging
  - install-hints
  - litestar
  - python
---

# Task t03: Package-Name Literals in the SAQ and MCP Plugin Factories

## Objective & Scope Boundary

Replace the sentence-valued `package=` literal in `create_saq_plugin` and `create_mcp_plugin` with the pip package name, so a missing integration renders `Optional dependency 'litestar-saq' is required but not installed.` instead of `Optional dependency 'litestar-saq is required for litestar saq integration' is required but not installed.`[^saq-plugin][^mcp-plugin][^module-loader] Pin the result through the factories themselves in the existing ecosystem tests, which today call `ensure_module` directly and never exercise the factory literals.[^saq-tests][^mcp-tests]

Depends on `t01`: the `install_package == "vertebra-core[saq]"` / `"vertebra-core[mcp]"` assertions rely on the `t01` default.

Out of scope: the module-level `install_package="litestar-saq"` / `install_package="litestar-mcp"` hints in the same files (they already name real packages); deleting the redundant function-level `ensure_module` guards; the `litestar-queues` literals and the AST gate (`t04`); removing `from __future__ import annotations` from either test module.

## Bounded Read Manifest

- `src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin` — first statement `ensure_module("litestar_saq", package="litestar-saq is required for litestar saq integration", extra="saq")`.[^saq-plugin]
- `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — first statement `ensure_module("litestar_mcp", package="litestar-mcp is required for litestar mcp integration", extra="mcp")`, spread over five lines.[^mcp-plugin]
- `src/py/vertebra/utils/module_loader.py::ensure_module`, `import_optional` — `ensure_module` resolves `import_optional` through the module global, so `monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)` reaches calls made inside the factories.[^module-loader]
- `src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors` — imports `create_saq_plugin`; patches `import_optional`.[^saq-tests]
- `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp` — imports `create_mcp_plugin`; patches `import_optional`.[^mcp-tests]

## Symbol & Signature Contracts

Production literals (signatures unchanged):

- `saq/plugin.py::create_saq_plugin(config: SAQConfig | None = None, *, queue_configs: Any = None, redis_url: str | None = None, **kwargs: Any) -> SAQPlugin` — first statement becomes `ensure_module("litestar_saq", package="litestar-saq", extra="saq")`.
- `mcp/plugin.py::create_mcp_plugin(config: MCPConfig | None = None, *, name: str = "Vertebra MCP API", enable_tasks: bool = True, registry: Any | None = None, expose_well_known: bool = True, well_known_path: str = "/.well-known/mcp.json", **kwargs: Any) -> LitestarMCP` — first statement becomes `ensure_module("litestar_mcp", package="litestar-mcp", extra="mcp")` on one line if it fits the formatter width, else as `ruff format` lays it out.

Test additions (existing tests extended, no new test functions):

- `test_missing_dependency_errors` appends, after the existing `saq_exc` block:

  ```python
  with pytest.raises(MissingDependencyError) as saq_plugin_exc:
      create_saq_plugin(redis_url="redis://localhost:6379")
  assert saq_plugin_exc.value.package == "litestar-saq"
  assert saq_plugin_exc.value.install_package == "vertebra-core[saq]"
  ```

- `test_missing_dependency_error_on_missing_mcp` appends:

  ```python
  with pytest.raises(MissingDependencyError) as plugin_exc:
      create_mcp_plugin(name="Vertebra Agent MCP")
  assert plugin_exc.value.package == "litestar-mcp"
  assert plugin_exc.value.install_package == "vertebra-core[mcp]"
  ```

## Edge Cases & Error Semantics

- `config=` supplied: the guard still runs first, so `create_saq_plugin(config=SAQConfig(...))` with `litestar_saq` absent raises the same error; the tests use the no-config path.
- Module-level import guards (`try: from litestar_saq import ... except ImportError as exc: raise MissingDependencyError(..., install_package="litestar-saq", extra="saq")`) fire at import time only; the tests import the modules before patching, so only the function-level guard is exercised.
- The message string uses `package`, the hint uses `install_package`; both are pinned so neither regresses to the sentence or to `vtb[...]`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py` and `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py` (in-process; `import_optional` patched to `lambda name: None` on `vertebra.utils.module_loader`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| SAQ factory, module absent | patch active; `create_saq_plugin(redis_url="redis://localhost:6379")` | `MissingDependencyError`; `package == "litestar-saq"`; `install_package == "vertebra-core[saq]"` |
| MCP factory, module absent | patch active; `create_mcp_plugin(name="Vertebra Agent MCP")` | `MissingDependencyError`; `package == "litestar-mcp"`; `install_package == "vertebra-core[mcp]"` |
| SAQ factory, module present | no patch; `create_saq_plugin(redis_url="redis://localhost:6379")` (`test_create_saq_plugin`) | returns an `SAQPlugin` (unchanged) |
| MCP factory, module present | no patch; `create_mcp_plugin(name="Vertebra Agent MCP")` (`test_create_mcp_plugin`) | returns a `LitestarMCP` (unchanged) |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp -q` fails on `package == "litestar-saq"` (actual `'litestar-saq is required for litestar saq integration'`) and `package == "litestar-mcp"`.
- Final Green + lint + typecheck: `uv run --no-sync ruff check src/py/vertebra/extensions/litestar/saq/plugin.py src/py/vertebra/extensions/litestar/mcp/plugin.py src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py && uv run --no-sync ruff format --check src/py/vertebra/extensions/litestar/saq/plugin.py src/py/vertebra/extensions/litestar/mcp/plugin.py src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py && uv run --no-sync mypy src/py/vertebra/extensions/litestar/saq src/py/vertebra/extensions/litestar/mcp && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q`

## Acceptance Criteria

- [x] `create_saq_plugin` and `create_mcp_plugin` pass `package="litestar-saq"` / `package="litestar-mcp"` (R10).
- [x] Both factories raise `MissingDependencyError` with `install_package == "vertebra-core[<extra>]"` when the module is absent, pinned in the existing ecosystem tests (R10).
- [x] Module-level `install_package="litestar-<x>"` hints are unchanged.
- [x] Zero inline `#` comments in touched files.

## Notes & Discoveries

- [2026-11-08 00:00] The existing missing-dependency tests call `ensure_module("litestar_saq", extra="saq")` and `ensure_module("litestar_mcp", extra="mcp")` directly, so they pass with or without the factory literal fix; the factory calls added here are the assertions that fail first.

[^saq-plugin]: `src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin` — litestar-saq plugin factory with a sentence-valued package argument.
[^mcp-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — litestar-mcp plugin factory with a sentence-valued package argument.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — ensure_module forwarding package, extra, and install_package to MissingDependencyError.
[^saq-tests]: `src/py/tests/unit/extensions/test_litestar_ecosystem_frontend_and_saq.py::test_missing_dependency_errors` — Frontend and SAQ missing-dependency test with import_optional patched to None.
[^mcp-tests]: `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py::test_missing_dependency_error_on_missing_mcp` — MCP missing-dependency test with import_optional patched to None.
