---
type: Pattern
title: "Python Module Structure, Class Layout & PEP 695 Typing"
description: "Python 3.12+ module layout, PEP 562 lazy root exports, msgspec.Struct value objects, class member ordering, zero inline comments, and PEP 695/604/585 typing."
status: stable
tags: [patterns, python, code-style, typing, conventions]
sources:
  - resource: src/py/vertebra/__init__.py
  - resource: src/py/vertebra/framework/_app.py
  - resource: src/py/vertebra/framework/_router.py
  - resource: src/py/vertebra/utils/module_loader.py
  - resource: src/py/vertebra/_typing.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: pyproject.toml
---

# Python Module Structure, Class Layout & PEP 695 Typing

All Python code under [`src/py/vertebra/`](../../../../../src/py/vertebra/__init__.py) and [`src/py/tests/`](../../../../../src/py/tests/conftest.py) targets Python 3.12+ (`3.12`, `3.13`, `3.14`, `3.14t`) under strict Ruff (`PLC0415` enforced) and Mypy gates configured in [`pyproject.toml`](../../../../../pyproject.toml).

## 1. Module Layout & PEP 562 Lazy Root Exports

Every module follows a fixed top-to-bottom layout: module docstring $\to$ grouped top-level imports (`stdlib`, third-party, local) $\to$ `if TYPE_CHECKING:` $\to$ `__all__` $\to$ public classes and functions $\to$ module-scoped `_...` helpers. The top-level [`vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py) uses PEP 562 `__getattr__` lazy loading so `import vertebra` does not import the framework, server, or native extensions until first attribute access (whereas subpackage `__init__.py` files such as `vertebra/framework/__init__.py` and `vertebra/build/__init__.py` re-export their public symbols eagerly):

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
- **Top-Level Imports (`PLC0415`) & Sanctioned Guards**: Place imports at module top level, or inside `if TYPE_CHECKING:` / [`module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py) (`import_optional`, `ensure_<pkg>()`, `import_app`). `PLC0415` is enabled repo-wide; per-file exemptions in `pyproject.toml` are restricted to PEP 562 `__getattr__` in `vertebra/__init__.py`, optional dependency probing in [`vertebra/_typing.py`](../../../../../src/py/vertebra/_typing.py), and documented runtime/circular-import boundaries.
- **Fail-Fast `UnsupportedBackendError`**: Unwired or unconfigured external backends raise [`UnsupportedBackendError`](../../../../../src/py/vertebra/_typing.py) (`RuntimeError` subclass) instead of returning silent placeholder data.
- **PEP 695 / 604 / 585 Typing**: Use built-in generic collections (`dict`, `list`, `tuple`, `set`), `X | Y` unions, and PEP 695 `type` aliases and generic functions/methods (`def schedule[T]` in [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)) with zero `typing_extensions` imports.

## 3. Value Objects (`msgspec.Struct`) & `__slots__`

- **`msgspec.Struct(..., frozen=True, gc=False)`**: Use frozen, GC-exempt `msgspec.Struct` definitions (with `array_like=True` on hot-path frames and route descriptors such as [`RouteDef`](../../../../../src/py/vertebra/framework/_router.py) and [`HandlerInfo`](../../../../../src/py/vertebra/framework/_app.py)) instead of `@dataclass` for configuration, metadata, spec, and wire frame value objects so CPython omits `PyGC_Head` cyclic GC tracking overhead.
- **`__slots__` on Stateful Classes**: Stateful service and runtime classes (`App`, `Router`, `InertiaMiddleware`, `JSONSerializer`) declare explicit `__slots__` to eliminate per-instance `__dict__` allocation.
