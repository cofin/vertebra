---
type: Pattern
title: "Serialization & Python Utility Conventions"
description: "How `vertebra.utils` centralizes JSON and MessagePack encoding and decoding, schema dumping, async/sync bridging, free-threaded worker pools, optional imports, and config lookup."
status: stable
tags: [patterns, python, serialization, concurrency]
sources:
  - resource: src/py/vertebra/utils/serializers/__init__.py
  - resource: src/py/vertebra/utils/serializers/_json.py
  - resource: src/py/vertebra/utils/serializers/_msgpack.py
  - resource: src/py/vertebra/utils/serializers/_schema.py
  - resource: src/py/vertebra/utils/serializers/_numpy.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/py/vertebra/utils/portal.py
  - resource: src/py/vertebra/utils/module_loader.py
  - resource: src/py/vertebra/utils/config_tools.py
  - resource: src/py/vertebra/utils/uuids.py
  - resource: src/py/vertebra/utils/type_guards.py
  - resource: src/py/vertebra/db/pipeline.py
  - resource: src/py/vertebra/db/rls.py
  - resource: src/py/tests/unit/utils/test_sync_tools.py
---

# Serialization & Python Utility Conventions

[`vertebra.utils`](../../../../../src/py/vertebra/utils/__init__.py) is a namespace package (its `__init__.py` holds only a docstring) with no re-exports. Import from submodules. Module layout and lazy-import rules are covered in [module-and-class-layout](module-and-class-layout.md).

## 1. JSON & MessagePack Go Through `vertebra.utils.serializers`

Every JSON or MessagePack encode or decode in `src/py/vertebra` goes through [`vertebra.utils.serializers`](../../../../../src/py/vertebra/utils/serializers/__init__.py). The package uses `msgspec.json` behind a lazily created process-wide `JSONSerializer` (`get_default_serializer()` in `_json.py`) and `msgspec.msgpack` behind [`MsgpackCodec`](../../../../../src/py/vertebra/utils/serializers/_msgpack.py) (`encode`, `decode`, `encode_into`, `decode_typed`).

| Function | Behavior |
|---|---|
| `encode_json(data, *, as_bytes=False)` | Returns `str` (or `bytes` with `as_bytes=True`), typed with overloads. |
| `encode_json_bytes(data)` | Returns UTF-8 `bytes`. Use it on hot paths and before writing to sockets, queues, or stores. |
| `encode_json_into(data, buffer, offset=0)` | `msgspec` `Encoder.encode_into` into a caller-owned `bytearray`. It has no stdlib fallback. |
| `decode_json(data, *, decode_bytes=True)` | Accepts `str` or `bytes`. With `decode_bytes=False`, `bytes` input is returned unchanged. |
| `format_json(value, *, indent=2)` | Pretty-prints with `msgspec.json.format`. |
| `MsgpackCodec.encode(value)` / `MsgpackCodec.decode(payload, type=...)` | Centralized MessagePack encoding and decoding (`encode_msgpack`, `decode_msgpack`). |

`_normalize_supported_value` is the encoder `enc_hook`. It converts:

- `datetime` → ISO 8601. Naive values are treated as UTC, and `+00:00` becomes `Z`.
- `date` → ISO. `uuid.UUID` and `uuid_utils.UUID` → `str`.
- `Decimal` → `float`. This loses precision, so encode exact decimals as strings first.
- `Enum` → `.value`.
- Pydantic models, dataclasses, attrs instances, and `msgspec.Struct` → `dict`.
- NumPy arrays and scalars → lists and Python scalars.

Anything else raises `TypeError("unsupported JSON value: ...")`.

Fallbacks: when `msgspec` raises any other `TypeError` or a `ValueError`, `encode_bytes` retries with `json.dumps(default=_normalize_supported_value)`. `decode` retries with `json.loads`. The stdlib path is much slower, so a hot path that keeps hitting it is a bug.

`_numpy.py` also exports `numpy_array_enc_hook`, `numpy_array_dec_hook` (direct one-argument use or Litestar's `(target_type, value)` form), and `numpy_array_predicate`. Each hook returns the value unchanged when NumPy is not installed.

## 2. Base Structs & Schema Dumping (`_schema.py`, `typing.py`)

[`vertebra.typing`](../../../../../src/py/vertebra/typing.py) and [`vertebra.utils.serializers`](../../../../../src/py/vertebra/utils/serializers/__init__.py) export four canonical `msgspec.Struct` base classes, each providing `.to_dict()`:
- `BaseStruct(Struct, gc=False, omit_defaults=True, repr_omit_defaults=True)` and `CamelizedBaseStruct(BaseStruct, rename="camel")` for mutable state objects.
- `FrozenBaseStruct(Struct, frozen=True, gc=False, omit_defaults=True, repr_omit_defaults=True)` and `FrozenCamelizedStruct(FrozenBaseStruct, rename="camel", frozen=True)` for immutable wire models and configs.

`_dump_msgspec_fields(value)` in [`_schema.py`](../../../../../src/py/vertebra/utils/serializers/_schema.py) extracts `msgspec.Struct` fields into a plain dictionary when invoked through the serializer fallback hook.

## 3. Async/Sync Bridging (`sync_tools.py`, `portal.py`)

| Symbol | Behavior |
|---|---|
| `async_(fn, *, limiter=None)` | Wraps a blocking callable. On free-threaded builds (`sys._is_gil_enabled() is False`) it runs on `SyncPool.default()`; otherwise it uses `asyncio.to_thread`. An optional `CapacityLimiter` bounds concurrency. |
| `run_(async_fn)` | Blocking wrapper. Inside a running loop it uses the global portal. Otherwise it calls `asyncio.run`, installing `uvloop` first when available on non-Windows GIL builds. |
| `await_(async_fn, raise_sync_error=False)` | Blocking wrapper that routes through the portal, or uses `run_coroutine_threadsafe` when called from a non-task thread that owns the loop. With `raise_sync_error=True` it raises instead of falling back. |
| `ensure_async_`, `with_ensure_async_`, `get_next`, `NO_VALUE` | Adapt sync callables and context managers to async. `get_next` is `anext` with an optional default. |
| `SyncPool` | Lazily created `ThreadPoolExecutor` (`max_workers` default `2 × cpu_count`, thread prefix `vertebra-ft-sync`). `submit` / `submit_async` copy `contextvars`. The executor is rebuilt after `fork` (PID check). `SyncPool.default()` is a double-checked singleton. |
| `CapacityLimiter` | One `asyncio.Semaphore` per event loop (`WeakKeyDictionary`), reset after fork. Assigning `total_tokens` drops existing semaphores. |
| `LoopShardedResource[T]` | One resource per event loop from `factory` or `async_factory`. A factory set up with only `async_factory` must be read with `get_async()`. |
| `configure_event_loop_policy()`, `LoopDiagnostics`, `LoopMismatchError` | Event-loop policy installation (uvloop on non-Windows GIL builds unless `VERTEBRA_UVLOOP` disables it) and nested-loop/thread-affinity diagnostics in `sync_tools.py`. |
| [`PortalProvider` / `Portal` / `PortalManager` / `get_global_portal`](../../../../../src/py/vertebra/utils/portal.py) | A background thread running its own event loop. `PortalManager` is a singleton that restarts the provider after fork or if the thread died, and stops it at exit. |

Outside `vertebra.utils`, `vertebra.extensions.sqlalchemy.dialects` (`SyncPool`) imports `sync_tools`. Tests are in [`test_sync_tools.py`](../../../../../src/py/tests/unit/utils/test_sync_tools.py).

## 4. Optional Dependencies, Config, SQL Identifiers & IDs

- [`module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py): `import_app("module:attr")`, `import_string`, `import_optional` (caches imported `ModuleType` or `None` in `_module_cache` so repeated positive and negative lookups are $O(1)$ without re-running `importlib.import_module`), `ensure_module` (returns cached `ModuleType` or raises `MissingDependencyError`), `module_available`, `dependency_flag`, `module_to_os_path`, `ensure_{sqlspec,sqlalchemy,advanced_alchemy,uvloop,pyarrow,msgspec}` (raise `MissingDependencyError`), `reset_dependency_cache` (clears `_availability_cache`, `_module_cache`, and resets `OptionalDependency` flags).
- [`config_tools.py`](../../../../../src/py/vertebra/utils/config_tools.py): `get_env(key, default, type_hint)` (bool coercion from `1/true/yes/on` and `0/false/no/off`), `find_pyproject_toml`, `parse_pyproject_config` / `read_pyproject_section` (reads `[tool.vertebra]` with `[tool.vtb]` fallback), and `SingletonMeta` (thread-safe per-class singleton metaclass).
- [`pipeline.py`](../../../../../src/py/vertebra/db/pipeline.py) and [`rls.py`](../../../../../src/py/vertebra/db/rls.py): `_IDENT_RE` (`^[A-Za-z_][A-Za-z0-9_]*$`) and `_validate_identifier()` validate SQL, PL/SQL, CTE, and GUC identifiers before string interpolation.
- [`uuids.py`](../../../../../src/py/vertebra/utils/uuids.py): `uuid3/4/5/6/7` and `nanoid` use `uuid_utils` / `fastnanoid` when installed. `uuid6` falls back to `uuid4` with a warning.
- [`type_guards.py`](../../../../../src/py/vertebra/utils/type_guards.py): `is_dataclass_instance`, `is_pydantic_model`, `is_msgspec_struct`, `is_attrs_instance`, `dataclass_to_dict`.

<!-- truth: start -->
- No module under `src/py/vertebra` outside `utils/serializers/` calls `msgspec.json.encode`, `msgspec.json.decode`, `msgspec.msgpack.encode`, `msgspec.msgpack.decode`, `json.dumps`, `json.loads`, or `orjson`.
- `encode_json` and `encode_json_bytes` fall back to stdlib `json.dumps` for any `msgspec` `TypeError` other than "unsupported JSON value", and for any `ValueError`. `encode_json_into` has no fallback.
- `decode_json` falls back to `json.loads` on `msgspec` `TypeError` or `ValueError`.
- The JSON `enc_hook` encodes `Decimal` as `float` and naive `datetime` as UTC with a `Z` suffix.
- `BaseStruct`, `CamelizedBaseStruct`, `FrozenBaseStruct`, and `FrozenCamelizedStruct` configure `gc=False, omit_defaults=True, repr_omit_defaults=True` (with `frozen=True` on the frozen variants and `rename="camel"` on the camelized variants) and expose `.to_dict()` via `msgspec.structs.asdict`.
- `async_` dispatches to `SyncPool.default()` only when `sys._is_gil_enabled()` returns `False`. Otherwise it uses `asyncio.to_thread`.
- `SyncPool`, `CapacityLimiter`, and `PortalManager` rebuild their executor, semaphores, or provider when `os.getpid()` changes.
- `vertebra.utils.config_tools` reads `[tool.vertebra]` (falling back to `[tool.vtb]`) and `vertebra.telemetry.LoggingConfig` reads `[tool.vertebra.logging]`.
<!-- truth: end -->
