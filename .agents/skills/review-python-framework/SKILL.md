---
name: review-python-framework
description: Use when reviewing Python framework architecture, server runners, Litestar/Starlette/FastAPI/Flask/Django extensions, msgspec schemas, module layout, or serialization discipline.
---

<!-- truth: start -->
- **Comments**: Zero inline `#` comments in Python files; intent lives in Google docstrings (`PY-02`).
- **Typing**: Strict PEP 695/604/585 typing; strict mypy passes with zero ignores (`PY-03`).
- **Structs**: Config, metadata, and wire frames are `msgspec.Struct(..., frozen=True, gc=False)` (`PY-04`).
- **Serializers**: JSON serialization routes through `vertebra.utils.serializers` (`PY-05`).
- **Imports**: Top-level grouped imports; public `vertebra.typing` shim only (never `vertebra._typing`); cached `vertebra.utils.module_loader` (`import_optional`, `ensure_module`) instead of raw `importlib.import_module`; PEP 562 lazy loading in packages (`PY-01`, `PY-06`, `PY-08`).
- **Suppressions**: Respect `FP-PY-01`..`FP-PY-03` in the shared catalog.
<!-- truth: end -->

# Python Framework Review (`review-python-framework`)

## Scope

- Paths: `[areas.python-framework]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `PY-01`..`PY-12` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Module and class layout](../../bundles/knowledge/patterns/python/module-and-class-layout.md)
- [Serialization and utils](../../bundles/knowledge/patterns/python/serialization-and-utils.md)
- [Hot path and async dispatch](../../bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md)
- [Routing and app](../../bundles/knowledge/domains/framework/routing-and-app.md)
- [Subdomains](../../bundles/knowledge/domains/framework/subdomains.md)
- Code layout and serializers reference: [`references/layout-and-serializers.md`](references/layout-and-serializers.md)

## Review Process

1. **Inline comment audit**: Reject any inline `#` comments; require Google-style docstrings (`PY-02`).
2. **Type annotations**: Verify PEP 695 type parameters, PEP 604 unions (`T | None`), and built-in generics (`PY-03`).
3. **Data models**: Require `msgspec.Struct(frozen=True, gc=False)` on value objects and wire frames (`PY-04`).
4. **Serialization hygiene**: Ensure all JSON operations use `vertebra.utils.serializers` (`PY-05`).
5. **Import discipline**: Enforce top-level imports, public `vertebra.typing` shim usage (forbidding `vertebra._typing` outside `vertebra/typing.py`), cached `vertebra.utils.module_loader` (`import_optional`, `ensure_module`) instead of raw `importlib.import_module`, and lazy PEP 562 exports in `__init__.py` files (`PY-06`, `PY-08`).
6. **Class ordering**: Verify slots, `__init__`, properties, public methods, and private methods in sequence (`PY-09`).
7. **Hot path execution**: Prevent loop wakeups and signature inspection on per-request paths (`PY-10`).

## Suppressions Index

- `FP-PY-01`: Absence of inline comments in Python
- `FP-PY-02`: PEP 562 lazy attribute loading
- `FP-PY-03`: ASGI direct send optimization

## Focused Commands

```bash
uv run ruff check src/py/vertebra
uv run mypy src/py/vertebra
uv run pytest src/py/tests/unit
```
