---
type: Task
id: "driver-fail-loud:ch07:t03"
chapter_id: ch07-install-hints-and-conformance
title: Replace sentence-valued package arguments and add the optional-dependency AST gate
description: Replace sentence-valued package= literals in the litestar-queues, litestar-saq, and litestar-mcp integrations with package names, and add an AST unit gate that fails on undeclared extra= literals, whitespace in package or install_package literals, and bracketed distributions other than vertebra-core.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-fail-loud:ch07:t01"
  - "driver-fail-loud:ch07:t02"
read_set:
  - src/py/vertebra/extensions/litestar/queues/config.py
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/extensions/litestar/saq/plugin.py
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
files:
  - src/py/vertebra/extensions/litestar/queues/config.py
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/extensions/litestar/saq/plugin.py
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
estimated_loc: 160
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
  - id: queues-config
    resource: src/py/vertebra/extensions/litestar/queues/config.py::create_shm_queue_config
    title: litestar-queues config helpers
  - id: queues-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMExecutionBackend
    title: litestar-queues SHM queue and execution backends
  - id: saq-plugin
    resource: src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin
    title: litestar-saq plugin factory
  - id: mcp-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: litestar-mcp plugin factory
  - id: packaging-test
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py::test_optional_extension_pyproject_isolation_and_bundler_extra
    title: Packaging test module reading pyproject.toml optional-dependencies
  - id: pyproject
    resource: pyproject.toml
    title: Declared project optional-dependencies
tags:
  - packaging
  - install-hints
  - gate
  - python
---

# Task t03: Package-Name Literals and the Optional-Dependency AST Gate

## Objective & Scope Boundary

Replace the eight sentence-valued `package=` literals in the Litestar integrations with package names, and add a unit gate in `test_packaging_and_pgo.py` that parses every `src/py/vertebra/**/*.py` file and fails, naming `path:line::callee keyword=value`, when an `ensure_module` or `MissingDependencyError` call passes an undeclared `extra=` literal, a `package=`, `install_package=`, or positional package literal containing whitespace, or a bracketed Vertebra distribution (`vtb[...]`, `vertebra[...]`) other than `vertebra-core`.[^queues-config][^queues-backend][^saq-plugin][^mcp-plugin][^packaging-test][^pyproject]

The task owns four production files, one more than the three-file task bound. Reason: each change is a trivial one-token literal swap (eight lines in total) proven by one gate test; splitting the files across tasks would leave that gate red between them.

Out of scope: adding extras to `pyproject.toml`; changing explicit module-level `install_package="litestar-<x>"` hints; deleting the redundant function-level `ensure_module` guards; validating the bracketed extra inside `install_package` (ch01's `vertebra-core[oracle]` / `vertebra-core[mssql]` hints name extras the native flows declare later).

## Bounded Read Manifest

- `src/py/vertebra/extensions/litestar/queues/config.py::create_sqlspec_queue_config`, `create_shm_queue_config` — four sentence literals.[^queues-config]
- `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend.__init__`, `SHMExecutionBackend.__init__` — two sentence literals.[^queues-backend]
- `src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin` — one sentence literal.[^saq-plugin]
- `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — one sentence literal.[^mcp-plugin]
- `src/py/tests/unit/build/test_packaging_and_pgo.py::test_optional_extension_pyproject_isolation_and_bundler_extra` — `repo_root = Path(__file__).resolve().parents[5]` and `tomllib` pattern.[^packaging-test]
- `src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py::test_missing_dependency_error_on_missing_queues` — `import_optional` patch pattern.

Declared extras (from `pyproject.toml`, do not re-read): `sqlspec`, `alchemy`, `arrow`, `pydantic`, `attrs`, `polars`, `litestar`, `starlette`, `fastapi`, `flask`, `django`, `queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `saq`, `embeddings`, `bigquery`, `bundler`, `spanner`, `storage`, `rloop`, `all`.

## Symbol & Signature Contracts

Production literals:

- `queues/config.py` (three calls in `create_sqlspec_queue_config`, one in `create_shm_queue_config`) and `queues/backend.py` (`SHMQueueBackend.__init__`, `SHMExecutionBackend.__init__`): `package="litestar-queues"`, `extra="queues"` kept.
- `saq/plugin.py::create_saq_plugin`: `ensure_module("litestar_saq", package="litestar-saq", extra="saq")`.
- `mcp/plugin.py::create_mcp_plugin`: `ensure_module("litestar_mcp", package="litestar-mcp", extra="mcp")`.

Gate in `src/py/tests/unit/build/test_packaging_and_pgo.py` (add `import ast` and `import re`; private helpers sit after the tests that call them):

```python
_DEPENDENCY_CALLEES: frozenset[str] = frozenset({"ensure_module", "MissingDependencyError"})
_BRACKETED_DISTRIBUTION = re.compile(r"^(?P<dist>[A-Za-z0-9_.-]+)\[(?P<extra>[^\]]+)\]$")


def test_optional_dependency_literals_name_declared_extras() -> None:
    """Every optional-dependency literal under src/py/vertebra names a declared extra and a real package."""
    repo_root = Path(__file__).resolve().parents[5]
    pyproject_data = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    declared = frozenset(pyproject_data["project"]["optional-dependencies"])
    violations = _optional_dependency_violations(repo_root / "src" / "py" / "vertebra", declared, relative_to=repo_root)
    assert violations == []


def test_optional_dependency_gate_reports_each_malformed_literal(tmp_path: Path) -> None:
    """The gate flags undeclared extras, sentence packages, and non-vertebra-core distributions only."""


def _optional_dependency_violations(source_root: Path, declared: frozenset[str], *, relative_to: Path) -> list[str]:
    """Return malformed optional-dependency literals under ``source_root``.

    Args:
        source_root: Directory scanned recursively for ``*.py`` files.
        declared: Keys of ``[project.optional-dependencies]``.
        relative_to: Base directory for the reported paths.

    Returns:
        Sorted ``"<path>:<line>::<callee> <keyword>=<repr(value)>"`` entries, empty when every literal is valid.
    """


def _callee_name(func: ast.expr) -> str | None:
    """Return the trailing name of a call target (``ast.Name.id`` or ``ast.Attribute.attr``)."""
```

Rules applied to each `ast.Call` whose `_callee_name` is in `_DEPENDENCY_CALLEES`, considering only `ast.Constant` string values:

1. `extra=<s>` with `s not in declared` → violation `extra`.
2. `package=<s>`, `install_package=<s>`, or the first positional argument of `MissingDependencyError`, where `s` contains whitespace (`any(ch.isspace() for ch in s)`) → violation for that keyword (`package` for the positional argument). No `install_package=` literal under `src/py/vertebra` contains whitespace today; the rule keeps the chapter truth block's "no whitespace" invariant enforced for both keywords.
3. `package=<s>` or `install_package=<s>` matching `_BRACKETED_DISTRIBUTION` where `dist` is `"vtb"` or starts with `"vertebra"` and `dist != "vertebra-core"` → violation for that keyword. Third-party bracketed packages such as `psycopg[binary,pool]` are accepted.

Iterate `sorted(source_root.rglob("*.py"))`, parse with `ast.parse(text, filename=str(path))`, build entries as `f"{path.relative_to(relative_to).as_posix()}:{node.lineno}::{callee} {keyword}={value!r}"`, and return `sorted(entries)`.

## Edge Cases & Error Semantics

- Multi-line calls and attribute callees (`module_loader.ensure_module(...)`) are matched through the AST.
- Non-literal keywords (`extra=extra` in `module_loader.py::ensure_module`) are skipped.
- `extra=` on other callees (`logging` `extra={...}`, `print(..., extra=...)`) is ignored.
- `install_package="psycopg[binary,pool]"` matches the bracket pattern with `dist == "psycopg"` and is accepted by rule 3.
- `install_package="vertebra-core[oracle]"` is accepted (distribution check only).
- A file with a syntax error fails the test through `SyntaxError` from `ast.parse`; that is correct because `ruff` rejects it too.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/build/test_packaging_and_pgo.py` (gate plus self-test, in-process AST over the filesystem) and `src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py` (behavioral hint for `create_shm_queue_config` and `SHMExecutionBackend`, `import_optional` patched to `lambda name: None` on `vertebra.utils.module_loader`).
- Self-test fixture: write `tmp_path / "pkg" / "mod.py"` with exactly these eight lines:

  ```text
  ensure_module("wasmtime", extra="wasm")
  module_loader.ensure_module("numpy", package="numpy is required", extra="arrow")
  MissingDependencyError("litestar-mcp", install_package="vertebra[mcp]", extra="mcp")
  MissingDependencyError("x", extra="sqlspec")
  ensure_module("y", extra=extra_name, install_package="psycopg[binary,pool]")
  print("z", extra="wasm")
  MissingDependencyError("pandas is required")
  MissingDependencyError("redis", install_package="redis is required")
  ```

- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Self-test | fixture above, `declared=frozenset({"arrow", "mcp", "sqlspec"})`, `relative_to=tmp_path` | `["pkg/mod.py:1::ensure_module extra='wasm'", "pkg/mod.py:2::ensure_module package='numpy is required'", "pkg/mod.py:3::MissingDependencyError install_package='vertebra[mcp]'", "pkg/mod.py:7::MissingDependencyError package='pandas is required'", "pkg/mod.py:8::MissingDependencyError install_package='redis is required'"]` |
| Repository gate, before this task | repo after t01 and t02 | eight `package='litestar-<x> is required for litestar <x> integration'` entries across `queues/config.py` (4), `queues/backend.py` (2), `saq/plugin.py` (1), `mcp/plugin.py` (1) |
| Repository gate, after this task | repo | `[]` |
| SHM queue config hint | `import_optional` patched to `None`; `create_shm_queue_config()` | `MissingDependencyError`; `package == "litestar-queues"`; `install_package == "vertebra-core[queues]"` |
| SHM execution backend hint | same patch; `SHMExecutionBackend()` | `MissingDependencyError`; `package == "litestar-queues"`; `install_package == "vertebra-core[queues]"` |
| Mutation check | temporarily add `ensure_module("x", extra="postgres")` to any module under `src/py/vertebra` | gate fails naming that file and `extra='postgres'`; revert before committing |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py::test_optional_dependency_literals_name_declared_extras src/py/tests/unit/build/test_packaging_and_pgo.py::test_optional_dependency_gate_reports_each_malformed_literal src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py -q` fails with the eight sentence-package entries and the `package == "litestar-queues"` assertions.
- Final Green + lint + typecheck: `uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues/config.py src/py/vertebra/extensions/litestar/queues/backend.py src/py/vertebra/extensions/litestar/saq/plugin.py src/py/vertebra/extensions/litestar/mcp/plugin.py src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py && uv run --no-sync ruff format --check src/py/tests/unit/build/test_packaging_and_pgo.py && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues src/py/vertebra/extensions/litestar/saq src/py/vertebra/extensions/litestar/mcp && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/extensions -q`

## Acceptance Criteria

- [ ] No `package=` or `install_package=` literal under `src/py/vertebra` passed to `ensure_module` / `MissingDependencyError` contains whitespace (R10).
- [ ] `test_optional_dependency_literals_name_declared_extras` passes and fails when an undeclared `extra=` literal is introduced (R11).
- [ ] `test_optional_dependency_gate_reports_each_malformed_literal` pins the exact five-entry list, including the `install_package=` whitespace entry (R11).
- [ ] `pyproject.toml` is unchanged.
- [ ] Zero inline `#` comments in touched files.

## Notes & Discoveries

- [2026-11-07 00:00] Sentence-valued `package=` literals sit in `queues/config.py`, `queues/backend.py`, `saq/plugin.py`, and `mcp/plugin.py`; `import_optional` takes no `extra`, so the gate checks only `ensure_module` and `MissingDependencyError`.

[^queues-config]: `src/py/vertebra/extensions/litestar/queues/config.py::create_shm_queue_config` — litestar-queues config helpers.
[^queues-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMExecutionBackend` — litestar-queues SHM queue and execution backends.
[^saq-plugin]: `src/py/vertebra/extensions/litestar/saq/plugin.py::create_saq_plugin` — litestar-saq plugin factory.
[^mcp-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — litestar-mcp plugin factory.
[^packaging-test]: `src/py/tests/unit/build/test_packaging_and_pgo.py::test_optional_extension_pyproject_isolation_and_bundler_extra` — Packaging test module reading pyproject.toml optional-dependencies.
[^pyproject]: `pyproject.toml` — Declared project optional-dependencies.
