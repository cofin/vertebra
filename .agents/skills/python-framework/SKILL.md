---
name: python-framework
description: Use when implementing or refactoring Python framework routing, controllers, procedures, DI, server runners, Litestar/Starlette/FastAPI/Flask/Django extensions, msgspec schemas, or serializers under src/py/vertebra.
---

<!-- truth: start -->
- **Zero Inline `#` Comments**: Never write inline `#` comments in Python files (`src/py/`, `tools/`, `examples/`); place all explanations in Google-style docstrings (`PY-02`).
- **Value Objects**: All config, metadata, and wire frame types use `msgspec.Struct(..., frozen=True, gc=False)` (`PY-04`).
- **Centralized JSON**: Route all JSON and arena encoding/decoding through `vertebra.utils.serializers` (`encode_json`, `encode_json_bytes`, `encode_json_into`, `encode_json_into_arena`, `encode_msgpack_into_arena`, `format_json`, `decode_json`) (`PY-05`).
- **Imports & Lazy Loading**: Place grouped imports at module top-level, import shared types/structs/exceptions from `vertebra.typing` (never `vertebra._typing`), use `vertebra.utils.module_loader` (`import_optional`, `ensure_module`), and expose heavy package exports via PEP 810 `__lazy_modules__` and PEP 562 `__getattr__` (`PY-01`, `PY-06`, `PY-08`).
- **Domain Naming & Layout**: Name classes after their domain role without `Vertebra*` or `Native*` prefixes (`PY-07`) and order class members as class attributes -> `__init__` -> `@property` -> public methods -> private `_` methods (`PY-01`).
- **Verify**: `uv run --no-sync ruff check src/py/ tools/ && uv run --no-sync mypy src/py/vertebra tools/`
<!-- truth: end -->

# Python Framework & Extensions (`python-framework`)

Implementation guide for the `vertebra` Python package (`src/py/vertebra/`, distributed on PyPI as `vertebra-core`), including `App`, `Controller`/`Router`/`Route`/`Procedure`, hierarchical dependency injection (`Provide`, `Inject`, `Container`), server runners, and framework extensions (`litestar`, `starlette`, `fastapi`, `flask`, `django`, `sqlspec`, `alchemy`, `sqlalchemy`, `apex`, `grpc`).

## Architecture & Domain References

- [Module and class layout](../../bundles/knowledge/patterns/python/module-and-class-layout.md)
- [Serialization and utils](../../bundles/knowledge/patterns/python/serialization-and-utils.md)
- [Hot path and async dispatch](../../bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md)
- [Routing and app](../../bundles/knowledge/domains/framework/routing-and-app.md)
- [Subdomains](../../bundles/knowledge/domains/framework/subdomains.md)
- [Worker loop and protocol adapters](../../bundles/knowledge/domains/server/worker-and-protocols.md)
- [Litestar extension](../../bundles/knowledge/domains/extensions/litestar.md)
- [SQLSpec, Advanced Alchemy, and dialects](../../bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)
- [MCP and A2A protocol harness](../../bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md)
- Routing, DI, and extensions reference: [`references/routing-di-and-extensions.md`](references/routing-di-and-extensions.md)
- Review checklist & suppressions: [`review-python-framework`](../review-python-framework/SKILL.md)

## Core Implementation Rules

1. **Module & Class Organization (`PY-01`, `PY-02`, `PY-07`)**:
   - Module layout: module docstring -> `from __future__ import annotations` -> grouped top-level imports (`stdlib`, third-party, `vertebra.*`, `if TYPE_CHECKING:`) -> `__all__` -> public classes/functions -> private `_` module helpers.
   - Consolidate single-caller helper functions onto their owning class as `@staticmethod`, `@classmethod`, or private `_` instance methods.
   - Zero inline `#` comments anywhere in Python source or test files, and zero backward-compatibility `X = Y` aliases.

2. **Schema & Serialization Discipline (`PY-04`, `PY-05`)**:
   - Define immutable configuration and wire types with `class Config(msgspec.Struct, frozen=True, gc=False):` (or `FrozenBaseStruct` from `vertebra.typing`).
   - Never call `json.dumps`, `json.loads`, `msgspec.json.encode`, or `msgspec.json.decode` directly outside `vertebra/utils/serializers/`.

3. **Pre-Compiled Hot-Path Dispatch (`PY-09`, `PY-10`)**:
   - Compile route parameter converters, DI dependency graphs (`DependencyGraph`), guards, and `msgspec` decoders once in `vertebra.server._dispatch.compile_handler_spec` at startup time rather than inspecting `inspect.signature` per request.

## Verification

```bash
uv run --no-sync ruff check src/py/ tools/
uv run --no-sync mypy src/py/vertebra tools/
GOOGLE_API_USE_CLIENT_CERTIFICATE=false uv run --no-sync pytest src/py/tests/unit -q
```
