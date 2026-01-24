# Knowledge Capture: Test Suite Reorganization and Refactoring

## Patterns Discovered
- **Pytest Plugins in Nested Conftests:** Defining `pytest_plugins` in a non-top-level `conftest.py` is deprecated and causes collection errors. The correct pattern is to import fixtures directly in the nested `conftest.py` or use the top-level configuration.
- **Git Ignore Collisions:** The directory name `build/` is commonly ignored. Putting tests in `src/py/tests/unit/build/` triggered git ignore rules. Tests for build logic might be better named `bundler` or `distribution` to avoid this, or require force-adding.

## Lessons Learned
- **Unique Test Module Names:** Renaming test files to be unique (e.g., `test_litestar_plugin.py` vs `test_plugin.py` in different folders) prevents pytest collection errors related to import file mismatches, especially when `__init__.py` usage varies.
- **Flattening Hierarchies:** Deeply nested test structures (like `tests/extensions/sqlspec/postgres/...`) hide complexity. Flattening them to `tests/unit/sqlspec/postgres` makes the structure more navigable.

## Decisions Made
- **Conftest Split:** Split the global `conftest.py` into root, `unit/`, and `integration/` to prevent unit tests from loading heavy integration fixtures (Docker containers).
- **Extensions Flattening:** Removed the `tests/extensions` directory and moved its contents up to `tests/unit` or `tests/integration` to align with user preference and reduce nesting.
- **Force Add Build Tests:** Decided to `git add -f` the tests in `src/py/tests/unit/build` rather than renaming the directory, to preserve the logical mapping to `vertebra.build`.

## Reusable Components
- The `src/py/tests/integration/helpers/database.py` module is a reusable pattern for Docker-based integration testing with `pytest-xdist` support.

## Gotchas & Warnings
- **Pre-existing Failures:** When reorganizing, ensure you identify pre-existing failures (like `test_supervisor_native.py` missing a binary export) so they aren't attributed to the refactor.
