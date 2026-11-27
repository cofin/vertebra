---
type: Pattern
title: "Python Module Structure, Class Layout & PEP 695 Typing"
description: "Python 3.12+ module layout, PEP 810/562 lazy root exports, msgspec.Struct value objects, class member ordering, zero inline comments, and PEP 695/604/585 typing."
status: stable
tags:
  - patterns
  - python
  - code-style
  - typing
  - conventions
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-27T22:10:00Z"
    commit: f3b624a2
  - by: agent:flow-completion
    at: "2026-11-14T21:38:00Z"
    commit: c526639f
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-init-py
    resource: src/py/vertebra/__init__.py
    title: "Python implementation module (src/py/vertebra/__init__.py)"
  - id: src-framework-init-py
    resource: src/py/vertebra/framework/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/__init__.py)"
  - id: src-framework-app-py
    resource: src/py/vertebra/framework/_app.py
    title: "Python implementation module (src/py/vertebra/framework/_app.py)"
  - id: src-framework-router-py
    resource: src/py/vertebra/framework/_router.py
    title: "Python implementation module (src/py/vertebra/framework/_router.py)"
  - id: src-utils-module-loader-py
    resource: src/py/vertebra/utils/module_loader.py
    title: "Python implementation module (src/py/vertebra/utils/module_loader.py)"
  - id: src-typing-py
    resource: src/py/vertebra/typing.py
    title: "Python implementation module (src/py/vertebra/typing.py)"
  - id: src-typing-py-2
    resource: src/py/vertebra/_typing.py
    title: "Python implementation module (src/py/vertebra/_typing.py)"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Python implementation module (src/py/vertebra/server/_dispatch.py)"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
---

# Python Module Structure, Class Layout & PEP 695 Typing

<!-- truth: start -->
- Every Python module orders top-level sections as module docstring, grouped imports (`stdlib`, third-party, local), `if TYPE_CHECKING:`, `__all__`, public classes and functions, and private `_...` helpers, with zero inline `#` comments.
- `vertebra/__init__.py` and `vertebra/framework/__init__.py` declare PEP 810 `__lazy_modules__` paired with PEP 562 `__getattr__` lazy loading (`lazy_exports`) so root imports incur zero framework or extension import overhead.
- Public typing aliases, sentinels, protocols, exceptions, and `msgspec.Struct` bases (`BaseStruct`, `CamelizedBaseStruct`, `FrozenBaseStruct`, `FrozenCamelizedStruct`) resolve exclusively through `vertebra.typing` using PEP 695/604/585 syntax without `typing_extensions`.
- Immutable and acyclic `msgspec.Struct` value objects set `gc=False`, stateful service classes declare explicit `__slots__`, and public `msgspec.Struct` / `.pyi` fields include single-line PEP 257 docstrings.
<!-- truth: end -->

All Python code under [`src/py/vertebra/`](../../../../../src/py/vertebra/__init__.py)[^src-init-py] and [`src/py/tests/`](../../../../../src/py/tests/conftest.py) targets Python 3.12+ (`3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`) under strict Ruff, Mypy (`strict = true`), and Pyright gates configured in [`pyproject.toml`](../../../../../pyproject.toml)[^src-pyproject-toml].

## 1. Module Layout, PEP 810 `__lazy_modules__` & PEP 562 Lazy Root Exports

Every module follows a fixed top-to-bottom layout: module docstring $\to$ grouped top-level imports (`stdlib`, third-party, local) $\to$ `if TYPE_CHECKING:` $\to$ `__all__` $\to$ public classes and functions $\to$ module-scoped `_...` helpers. Both [`vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py) and [`vertebra/framework/__init__.py`](../../../../../src/py/vertebra/framework/__init__.py)[^src-framework-init-py] declare PEP 810 `__lazy_modules__` (honored natively by Python 3.15+ while remaining a harmless `list[str]` on 3.12–3.14), and [`vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py) pairs it with PEP 562 `__getattr__` lazy loading so `import vertebra` does not import the framework, server, or native extensions until first attribute access:

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

## 2. Class Member Ordering & Zero Inline `#` Comments

Classes order members strictly as: class attributes / `__slots__` $\to$ `__init__` $\to$ `@property` getters $\to$ public methods $\to$ private `_...` helper methods. Private helpers used by a single class belong on that class as private methods rather than standalone module functions.

- **Zero Inline `#` Comments**: Never write inline `#` comments in Python implementation or test files. Document intent, invariants, and edge cases inside Google-style docstrings.
- **Top-Level Imports, Public `vertebra.typing` Shim & `module_loader`**: Place imports at module top level, or inside `if TYPE_CHECKING:` / [`module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-utils-module-loader-py] (`import_optional`, `ensure_module`, `module_available`, `import_string`, `import_app`). `import_optional` resolves already-loaded modules directly from `sys.modules` (skipping initializing specs), while `module_available` caches `find_spec` booleans in `_dependency_cache` (cleared by `reset_dependency_cache()`) so repeated availability checks resolve in $O(1)$. Catch `Exception` (never `BaseException`) in operational sync/async handlers so `asyncio.CancelledError`, `KeyboardInterrupt`, and `SystemExit` propagate cleanly (reserving `BaseException` strictly for deterministic rollback/context cleanup), and keep blocking sync I/O out of `*_async` DSN parsers. Always import typing aliases, struct bases (`BaseStruct`, `CamelizedBaseStruct`, `FrozenBaseStruct`, `FrozenCamelizedStruct`, `Struct`), sentinels (`Empty`, `EmptyType`, `UNSET`), protocols, and exceptions (`VertebraError`, `MissingDependencyError`, `ImproperConfigurationError`, `IntegrityError`, `PreconditionError`, `UnsupportedBackendError`) from the public [`vertebra.typing`](../../../../../src/py/vertebra/typing.py)[^src-typing-py] shim — never from `vertebra._typing` outside `src/py/vertebra/typing.py`.
- **Fail-Fast `UnsupportedBackendError`**: Unwired or unconfigured external backends raise [`UnsupportedBackendError`](../../../../../src/py/vertebra/typing.py) (`VertebraError` subclass) instead of returning silent placeholder data.
- **PEP 695 / 604 / 585 Typing**: Use built-in generic collections (`dict`, `list`, `tuple`, `set`), `X | Y` unions, and PEP 695 `type` aliases and generic functions/methods (`def schedule[T]` in [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)) with zero `typing_extensions` imports.

## 3. Value Objects (`msgspec.Struct`), Base Struct Inheritance & `__slots__`

- **`gc=False` Discipline**: All immutable (`frozen=True`) and acyclic mutable `msgspec.Struct` classes set `gc=False` so CPython omits the 16-byte `PyGC_Head` header per instance and excludes instances from cyclic GC graph scans while preserving immediate reference-count deallocation. Keep `gc=True` whenever a struct can hold arbitrary nested user containers or cycles (such as Spanner `Mutation`/`MutationGroup` or MSSQL `_QueuedMSSQLOperation`) or requires `dict=True` / `weakref=True`.
- **Canonical Base Structs ([`vertebra.typing`](../../../../../src/py/vertebra/typing.py) / [`vertebra.utils.serializers`](../../../../../src/py/vertebra/utils/serializers/__init__.py))**:
  - [`BaseStruct`](../../../../../src/py/vertebra/typing.py) (`gc=False, omit_defaults=True, repr_omit_defaults=True`) and [`CamelizedBaseStruct`](../../../../../src/py/vertebra/typing.py) (`rename="camel"`) for mutable state objects.
  - [`FrozenBaseStruct`](../../../../../src/py/vertebra/typing.py) (`frozen=True, gc=False, omit_defaults=True, repr_omit_defaults=True`) and [`FrozenCamelizedStruct`](../../../../../src/py/vertebra/typing.py) (`rename="camel", frozen=True`) for immutable wire models (such as A2A 1.0 models in [`agent/a2a/models.py`](../../../../../src/py/vertebra/agent/a2a/models.py) and MCP `2026-07-28` models in [`agent/mcp/models.py`](../../../../../src/py/vertebra/agent/mcp/models.py)).
  - **Inheritance Rules**: At runtime, `msgspec`'s `__struct_config__` automatically propagates `frozen`, `gc`, `rename`, `omit_defaults`, `repr_omit_defaults`, `forbid_unknown_fields`, `cache_hash`, `array_like`, and `tag_field` to subclasses, but **`kw_only=True` applies only to fields declared in that specific class body**. Under `mypy --strict` (PEP 681 `dataclass_transform`), subclasses of a frozen base must still repeat `frozen=True` in their class header (`class AgentSkill(FrozenCamelizedStruct, frozen=True, kw_only=True):`), and frozen structs cannot inherit from a non-frozen base.
- **Option Selection Matrix**:
  - `frozen=True`: Default for all configs, specs, DTOs, and wire frames. If `__post_init__` computes a derived attribute (e.g., [`Provide`](../../../../../src/py/vertebra/framework/di.py)), use `msgspec.structs.force_setattr(self, name, value)`.
  - `cache_hash=True`: Enable on `frozen=True` value objects whose fields are strictly hashable (no `dict`, `list`, or `set` fields) and that are used as cache keys, route/parameter specs, or URI descriptors ([`BuildTarget`](../../../../../src/py/vertebra/build/packaging/targets.py), [`StatementPlan`](../../../../../src/py/vertebra/extensions/broker/transport.py), [`ParamSpec`](../../../../../src/py/vertebra/framework/_router.py)[^src-framework-router-py], [`HandlerInfo`](../../../../../src/py/vertebra/framework/_app.py)[^src-framework-app-py], [`TraceContext`](../../../../../src/py/vertebra/telemetry/otel.py), [`GenAiUsage`](../../../../../src/py/vertebra/telemetry/otel.py)).
  - `array_like=True`: Serializes the struct in JSON and MessagePack as a **positional array (`[f1, f2, ...]`)** rather than a keyed object (`{"f1": ...}`). It does not alter Python's in-memory C-struct layout. Use `array_like=True` **strictly** on internal binary/MessagePack/IPC ring-buffer frames where both producer and consumer share the exact `Struct` definition ([`ChannelMessage`](../../../../../src/py/vertebra/extensions/broker/transport.py), [`StateDeltaFrame`](../../../../../src/py/vertebra/framework/live.py), [`TaskEnvelope`](../../../../../src/py/vertebra/framework/queues/base.py), [`DeadLetterEntry`](../../../../../src/py/vertebra/framework/queues/base.py)). Never set `array_like=True` on structs serialized to HTTP/REST/SSE/OpenAPI JSON ([`GenAiUsage`](../../../../../src/py/vertebra/telemetry/otel.py), [`AgentStreamFrame`](../../../../../src/py/vertebra/agent/runtime.py), [`EmailReceipt`](../../../../../src/py/vertebra/framework/email/base.py)).
  - `forbid_unknown_fields=True`: Enable on strict inbound request/command envelopes ([`ChatRequest`](../../../../../src/py/vertebra/agent/controller.py), [`ConfirmationDecisionRequest`](../../../../../src/py/vertebra/agent/controller.py), [`JSONRPCRequest`](../../../../../src/py/vertebra/agent/core/jsonrpc.py)).
- **`__slots__` on Stateful Classes**: Stateful service and runtime classes (`App`, `Router`, `SyncInertiaMiddleware`, `AsyncInertiaMiddleware`, `JSONSerializer`) declare explicit `__slots__` to eliminate per-instance `__dict__` allocation.
- **PEP 257 Inline Field Docstrings on Public `msgspec.Struct` & `.pyi` Stub Classes**: Document every field of public `msgspec.Struct` classes and `.pyi` stub classes rendered in `docs/reference/` with a single-line PEP 257 string literal immediately below the field declaration (enforced by [`tools/docs/scan_public_docstrings.py`](../../../../../tools/docs/scan_public_docstrings.py) and [`tools/docs/verify_docs_artifacts.py`](../../../../../tools/docs/verify_docs_artifacts.py)). Custom Griffe templates in [`tools/docs/templates/python/material/`](../../../../../tools/docs/templates/python/material/docstring/attributes.html.jinja) filter out un-annotated `__init__` attributes lacking both `annotation` and `description` and render `Default: <code>...</code>` when `description` is omitted.

Provenance sources: [^src-typing-py-2], [^src-server-dispatch-py].

[^src-init-py]: `src/py/vertebra/__init__.py` — Python implementation module (src/py/vertebra/__init__.py).
[^src-framework-init-py]: `src/py/vertebra/framework/__init__.py` — Python implementation module (src/py/vertebra/framework/__init__.py).
[^src-framework-app-py]: `src/py/vertebra/framework/_app.py` — Python implementation module (src/py/vertebra/framework/_app.py).
[^src-framework-router-py]: `src/py/vertebra/framework/_router.py` — Python implementation module (src/py/vertebra/framework/_router.py).
[^src-utils-module-loader-py]: `src/py/vertebra/utils/module_loader.py` — Python implementation module (src/py/vertebra/utils/module_loader.py).
[^src-typing-py]: `src/py/vertebra/typing.py` — Python implementation module (src/py/vertebra/typing.py).
[^src-typing-py-2]: `src/py/vertebra/_typing.py` — Python implementation module (src/py/vertebra/_typing.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
