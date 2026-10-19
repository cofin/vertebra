---
name: pattern-python-module-and-class-layout
description: Python 3.12+ module layout, PEP 562 lazy exports, class member ordering, zero inline comments, and PEP 695/604/585 typing.
sources:
  - src/py/vertebra/__init__.py
  - src/py/vertebra/framework/_app.py
  - src/py/vertebra/utils/module_loader.py
  - pyproject.toml
---

# Python Module Structure, Class Layout & PEP 695 Typing

All Python code under [`src/py/vertebra/`](../../../../../src/py/vertebra/__init__.py) and [`src/py/tests/`](../../../../../src/py/tests/conftest.py) targets Python 3.12+ (`3.12`, `3.13`, `3.14`, `3.14t`) under strict Ruff and Mypy gates configured in [`pyproject.toml`](../../../../../pyproject.toml).

## 1. Module Layout & PEP 562 Lazy Package Exports

Every module follows a fixed top-to-bottom layout: module docstring $\to$ grouped top-level imports (`stdlib`, third-party, local) $\to$ `if TYPE_CHECKING:` $\to$ `__all__` $\to$ public classes and functions $\to$ module-scoped `_...` helpers. Package `__init__.py` files use PEP 562 `__getattr__` lazy loading so `import vertebra` stays under 7 ms in-process:

<!-- truth: start -->
```python
def __getattr__(name: str) -> Any:
    """Lazy-load framework exports on first attribute access."""
    if name in _APP_EXPORTS:
        from vertebra.framework import _app

        value = getattr(_app, name)
        globals()[name] = value
        return value
    if name in _ROUTER_EXPORTS:
        from vertebra.framework import _router

        value = getattr(_router, name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
```
<!-- truth: end -->

## 2. Class Member Ordering & Zero Inline `#` Comments

Classes order members strictly as: class attributes / `__slots__` $\to$ `__init__` $\to$ `@property` getters $\to$ public methods $\to$ private `_...` helper methods. Private helpers used by a single class belong on that class as private methods rather than standalone module functions.

- **Zero Inline `#` Comments**: Never write inline `#` comments in Python implementation or test files. Document intent, invariants, and edge cases inside Google-style docstrings.
- **No Nested Imports**: Place all imports at module top level, or inside `if TYPE_CHECKING:` / [`module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py) (`ensure_packages`, `import_app`) when guarding optional third-party libraries.
- **PEP 695 / 604 / 585 Typing**: Use built-in generic collections (`dict`, `list`, `tuple`, `set`), `X | Y` unions, and PEP 695 `type` aliases and generic functions/methods (`def schedule[T]`) with zero `typing_extensions` imports.
