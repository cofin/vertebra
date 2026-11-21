---
type: Task
id: "native-cursor-data-path:cleanup:t01"
chapter_id: cleanup-and-checks
title: "Scope the shared-memory test cleanup by process role"
description: "An xdist worker removes only the shared-memory names the shm_name fixture handed out; the controller process runs the broad sweep."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
  - "src/py/tests/conftest.py:1-15"
  - "src/py/tests/conftest.py:43-56"
files:
  - src/py/tests/conftest.py
tests:
  - src/py/tests/unit/test_conftest_shm_cleanup.py
estimated_loc: 130
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: test-conftest
    resource: src/py/tests/conftest.py::_cleanup_shm
    title: "Autouse shared-memory cleanup fixture"
tags: [testing, shared-memory, bugfix]
---

# t01 — Scope the shared-memory test cleanup by process role

## Objective & Scope Boundary

Parallel test workers stop deleting each other's live shared-memory segments. Today `_cleanup_shm` unlinks `/dev/shm/test-*`, `/dev/shm/vtb-*`, and `/dev/shm/*vertebra*` after every test in every process.[^test-conftest]

In scope: `src/py/tests/conftest.py` and one new test file. Out of scope: tests that build their own `vtb-*` names, the `make rs-test` sweep, and any production code.

## Bounded Read Manifest

- `src/py/tests/conftest.py:1-15` for the imports.
- `src/py/tests/conftest.py:43-56` for `shm_name` and `_cleanup_shm`.

No other file is needed.

## Contracts & Edge Cases

Add these module-level names to `src/py/tests/conftest.py`. Imports added: `from collections.abc import Iterable, Mapping, Sequence`, `from pathlib import Path`, `from typing import Final`. Remove the `contextlib` import only if it has no other user.

```python
_SHM_ROOT: Final[Path] = Path("/dev/shm")
_BROAD_SHM_PATTERNS: Final[tuple[str, ...]] = ("test-*", "vtb-*", "*vertebra*")


def _is_xdist_worker(environ: Mapping[str, str] | None = None) -> bool: ...
def _post_test_patterns(issued: Sequence[str], *, is_worker: bool) -> tuple[str, ...]: ...
def _sweep_shm(root: Path, patterns: Iterable[str]) -> int: ...
```

- `_is_xdist_worker` returns `"PYTEST_XDIST_WORKER" in environ`. `environ=None` reads `os.environ`.
- `_post_test_patterns` returns `tuple(f"{glob.escape(name)}*" for name in issued)` when `is_worker` is true, and `_BROAD_SHM_PATTERNS` otherwise.
- `_sweep_shm` unlinks every entry under `root` that matches any pattern, suppresses `OSError` per entry, and returns the number of entries it removed. A missing `root` returns `0`.

Fixtures and hook:

```python
@pytest.fixture()
def _issued_shm_names() -> list[str]: ...

@pytest.fixture()
def shm_name(_issued_shm_names: list[str]) -> str: ...

@pytest.fixture(autouse=True)
def _cleanup_shm(_issued_shm_names: list[str]) -> Any: ...

def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None: ...
```

- `_issued_shm_names` returns a new empty list per test.
- `shm_name` builds `test-<uuid8>` as today, appends it to the list, and returns it.
- `_cleanup_shm` yields, then calls `_sweep_shm(_SHM_ROOT, _post_test_patterns(_issued_shm_names, is_worker=_is_xdist_worker()))`.
- `pytest_sessionfinish` calls `_sweep_shm(_SHM_ROOT, _BROAD_SHM_PATTERNS)` when `_is_xdist_worker()` is false, and does nothing in a worker.

Edge cases:

- A worker test that never requests `shm_name` sweeps nothing.
- A single-process run keeps today's behavior: the broad sweep after every test.
- A name with glob characters is escaped before the `*` suffix is added.
- An entry that vanishes between listing and unlinking is skipped without an error.
- Docstrings carry the explanation. No inline comments.

## Test Matrix

New file `src/py/tests/unit/test_conftest_shm_cleanup.py`. It imports the helpers with `from tests import conftest as tests_conftest` and uses `tmp_path` as the root, never `/dev/shm`.

| Test | Arrange | Assert |
| --- | --- | --- |
| `test_worker_sweep_keeps_segments_it_did_not_issue` | Files `test-aaaa1111`, `test-aaaa1111.lock`, `test-bbbb2222`, `vtb-live-1`, `my-vertebra-seg` in `tmp_path`; patterns from `_post_test_patterns(["test-aaaa1111"], is_worker=True)` | The two `test-aaaa1111*` files are gone; the other three remain; return value is `2` |
| `test_worker_without_issued_names_sweeps_nothing` | Same files; `issued=[]`, `is_worker=True` | Patterns are `()`; all files remain; return value is `0` |
| `test_controller_sweep_uses_broad_patterns` | Same files plus `unrelated`; `is_worker=False` | Patterns equal `_BROAD_SHM_PATTERNS`; only `unrelated` remains |
| `test_is_xdist_worker_reads_environment` | `{"PYTEST_XDIST_WORKER": "gw0"}` and `{}` | `True`, then `False` |
| `test_sweep_missing_root_returns_zero` | `tmp_path / "absent"` | Returns `0`, raises nothing |
| `test_shm_name_is_recorded_for_cleanup` | Request `shm_name` and `_issued_shm_names` | The list equals `[shm_name]` |

The first test fails on the current tree because the helpers do not exist.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/test_conftest_shm_cleanup.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/server/test_zero_copy_buffer.py -n 4 -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/server/test_zero_copy_buffer.py -q
uv run --no-sync ruff check src/py/tests/conftest.py src/py/tests/unit/test_conftest_shm_cleanup.py
uv run --no-sync mypy src/py/tests/conftest.py
```

## Acceptance Criteria

- [ ] The six tests pass.
- [ ] The four-worker run and the single-process run of the three shared-memory test files both pass.
- [ ] `_cleanup_shm` contains no pattern literal; both sweeps read `_BROAD_SHM_PATTERNS`.
- [ ] `ruff` and `mypy` report nothing for the two files.

[^test-conftest]: `src/py/tests/conftest.py::_cleanup_shm`
