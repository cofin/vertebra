---
type: Task
id: "driver-fail-loud:ch07:t01"
chapter_id: ch07-install-hints-and-conformance
title: Default MissingDependencyError hints to vertebra-core extras and fix loader and Litestar plugin hints
description: Change the MissingDependencyError default install hint from vtb[extra] to vertebra-core[extra], drop undeclared numpy and pandas extras from the module loader, and drop the vertebra[mcp] override in VertebraPlugin.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/_typing.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/extensions/litestar/plugin.py
  - src/py/tests/unit/framework/test_typing.py
  - src/py/tests/unit/framework/test_framework_subdomains.py
files:
  - src/py/vertebra/_typing.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/extensions/litestar/plugin.py
tests:
  - src/py/tests/unit/framework/test_typing.py
  - src/py/tests/unit/framework/test_framework_subdomains.py
estimated_loc: 60
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint default
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_numpy
    title: ensure_numpy and ensure_pandas loader helpers
  - id: litestar-plugin
    resource: src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin
    title: VertebraPlugin MCP and A2A dependency errors
tags:
  - packaging
  - install-hints
  - python
---

# Task t01: Default `MissingDependencyError` Hints to `vertebra-core` Extras

## Objective & Scope Boundary

Make `MissingDependencyError` render `vertebra-core[<extra>]` as its default install target, make `ensure_numpy` / `ensure_pandas` name the `numpy` / `pandas` packages without the undeclared `numpy` / `pandas` extras, and make both `MissingDependencyError` raises in `VertebraPlugin.on_app_init` fall back to the default `vertebra-core[mcp]` hint instead of the nonexistent `vertebra[mcp]`.[^missing-dep][^module-loader][^litestar-plugin]

Out of scope: `ensure_pyarrow` (its `install_package="pyarrow"` and declared `arrow` extra stay); engine driver and WASM call sites (t02); sentence-valued `package=` literals and the extras gate (t03); removing `from __future__ import annotations` from `module_loader.py`.

## Bounded Read Manifest

- `src/py/vertebra/_typing.py::MissingDependencyError` — default `install_package` expression.[^missing-dep]
- `src/py/vertebra/utils/module_loader.py::ensure_module`, `ensure_numpy`, `ensure_pandas`.[^module-loader]
- `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin.on_app_init` — the `mcp_config` and `a2a_config` branches.[^litestar-plugin]
- `src/py/tests/unit/framework/test_typing.py::test_module_loader_caches_imported_modules_and_ensure_module` — canonical loader / error tests.
- `src/py/tests/unit/framework/test_framework_subdomains.py` — the test that patches `vertebra.extensions.litestar.plugin.import_optional` and expects `MissingDependencyError, match="litestar-mcp"`.

## Symbol & Signature Contracts

- `src/py/vertebra/_typing.py::MissingDependencyError.__init__(self, package: str, *, install_package: str | None = None, extra: str | None = None) -> None` — signature unchanged; body sets `self.install_package = install_package or (f"vertebra-core[{extra}]" if extra else package)`. `message`, `hint`, and `show` are unchanged.
- `src/py/vertebra/utils/module_loader.py::ensure_numpy() -> Any` — `return ensure_module("numpy")`.
- `src/py/vertebra/utils/module_loader.py::ensure_pandas() -> Any` — `return ensure_module("pandas")`.
- `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin.on_app_init` — both raises become `raise MissingDependencyError("litestar-mcp", extra="mcp")`.

## Edge Cases & Error Semantics

- `extra=""` is falsy: `install_package` falls back to `package`.
- `install_package` set together with `extra`: `install_package` wins (`MissingDependencyError("x", install_package="pkg", extra="sqlspec").install_package == "pkg"`).
- `ensure_module("vertebra_nonexistent_pkg_xyz", extra="custom")` yields `install_package == "vertebra-core[custom]"`; the loader does not validate extras (the t03 gate scans only `src/py/vertebra`).
- `ensure_numpy()` with `sys.modules["numpy"] = None` raises `MissingDependencyError` with `package == "numpy"` and `install_package == "numpy"`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/framework/test_typing.py` (in-process; extend the loader test and add `test_missing_dependency_error_install_hint_defaults` parametrized over the first three rows) and `src/py/tests/unit/framework/test_framework_subdomains.py` (tighten the existing `litestar-mcp` assertion with `exc_info`).
- Missing-module setup: `monkeypatch.setitem(sys.modules, "<name>", None)` followed by `reset_dependency_cache("<name>")`, as in `src/py/tests/unit/db/test_native_db_drivers.py::test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Extra default | `MissingDependencyError("x", extra="sqlspec")` | `install_package == "vertebra-core[sqlspec]"`; `hint == "Install it with: uv add vertebra-core[sqlspec]"`; `str(err) == "Optional dependency 'x' is required but not installed."` |
| Explicit override | `MissingDependencyError("x", install_package="pkg", extra="sqlspec")` | `install_package == "pkg"`; `hint == "Install it with: uv add pkg"` |
| Package fallback | `MissingDependencyError("x")` | `install_package == "x"` |
| Loader forwarding | `ensure_module("vertebra_nonexistent_pkg_xyz", extra="custom")` | raises; `exc.install_package == "vertebra-core[custom]"`; `"vtb[" not in exc.hint` |
| numpy absent | `sys.modules["numpy"] = None`; `ensure_numpy()` | raises; `exc.package == "numpy"`; `exc.install_package == "numpy"` |
| pandas absent | `sys.modules["pandas"] = None`; `ensure_pandas()` | raises; `exc.package == "pandas"`; `exc.install_package == "pandas"` |
| Litestar MCP missing | `import_optional` patched to `lambda _mod: None`; `VertebraPlugin(VertebraConfig(mcp_config={"name": "demo"})).on_app_init(fake_app_config)` | raises; `exc.package == "litestar-mcp"`; `exc.install_package == "vertebra-core[mcp]"` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/framework/test_framework_subdomains.py -q` fails on the `vertebra-core[sqlspec]`, `vertebra-core[custom]`, and `vertebra-core[mcp]` assertions.
- Final Green + lint + typecheck: `uv run --no-sync ruff check src/py/vertebra/_typing.py src/py/vertebra/utils/module_loader.py src/py/vertebra/extensions/litestar/plugin.py src/py/tests/unit/framework/test_typing.py src/py/tests/unit/framework/test_framework_subdomains.py && uv run --no-sync ruff format --check src/py/vertebra/_typing.py src/py/vertebra/utils/module_loader.py src/py/vertebra/extensions/litestar/plugin.py && uv run --no-sync mypy src/py/vertebra/_typing.py src/py/vertebra/utils/module_loader.py src/py/vertebra/extensions/litestar/plugin.py && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/extensions -q`

## Acceptance Criteria

- [ ] `MissingDependencyError` defaults `install_package` to `vertebra-core[<extra>]` and no `vtb[` string remains under `src/py/vertebra` (R10).
- [ ] `ensure_numpy` and `ensure_pandas` pass no `extra=` and name `numpy` / `pandas` (R10).
- [ ] `VertebraPlugin.on_app_init` raises with `install_package == "vertebra-core[mcp]"` (R10).
- [ ] Existing `"<extra>" in exc.install_package` assertions under `src/py/tests/unit/extensions` stay green.
- [ ] Zero inline `#` comments and no new `from __future__` imports in touched files.

## Notes & Discoveries

- [2026-11-07 00:00] `grep -rn "vtb\[" src/py` matches only `_typing.py::MissingDependencyError`; no test asserts the `vtb[` form.

[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint default.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_numpy` — ensure_numpy and ensure_pandas loader helpers.
[^litestar-plugin]: `src/py/vertebra/extensions/litestar/plugin.py::VertebraPlugin` — VertebraPlugin MCP and A2A dependency errors.
