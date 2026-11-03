---
type: Pattern
title: "Serialization & Python Utility Conventions"
description: "How `vertebra.utils` centralizes JSON and MessagePack encoding and decoding, Rust-accelerated text/humanize/UUID primitives, type dispatch, schema hydration, async/sync bridging, free-threaded worker pools, optional imports, and config lookup."
status: stable
tags: [patterns, python, serialization, concurrency, text, humanize]
sources:
  - resource: src/rs/vertebra-core/src/text.rs
  - resource: src/rs/vertebra-core/src/id.rs
  - resource: src/rs/vertebra-py/src/text.rs
  - resource: src/py/vertebra/utils/__init__.py
  - resource: src/py/vertebra/utils/text.py
  - resource: src/py/vertebra/utils/humanize.py
  - resource: src/py/vertebra/utils/uuids.py
  - resource: src/py/vertebra/utils/dispatch.py
  - resource: src/py/vertebra/utils/schema.py
  - resource: src/py/vertebra/utils/type_converters.py
  - resource: src/py/vertebra/utils/type_guards.py
  - resource: src/py/vertebra/utils/config_tools.py
  - resource: src/py/vertebra/utils/module_loader.py
  - resource: src/py/vertebra/utils/deprecation.py
  - resource: src/py/vertebra/utils/correlation.py
  - resource: src/py/vertebra/utils/serializers/__init__.py
  - resource: src/py/vertebra/utils/serializers/_json.py
  - resource: src/py/vertebra/utils/serializers/_msgpack.py
  - resource: src/py/vertebra/utils/serializers/_schema.py
  - resource: src/py/vertebra/utils/serializers/_numpy.py
  - resource: src/py/vertebra/utils/serializers/_zero_copy.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/py/vertebra/utils/portal.py
  - resource: src/py/tests/unit/utils/test_text_and_humanize.py
  - resource: src/py/tests/unit/utils/test_dispatch_schema_converters.py
  - resource: src/py/tests/unit/utils/test_deprecation_and_correlation.py
  - resource: src/py/tests/unit/utils/test_utils_core.py
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

[`_zero_copy.py`](../../../../../src/py/vertebra/utils/serializers/_zero_copy.py) provides `ZeroCopyCodecRegistry` (`TypeAdapterPlan` classification across `msgspec_struct`, `pydantic`, `dataclass`, `attrs`, `typeddict`, `namedtuple`, `dict`, and `primitive`), thread-local `bytearray` scratch-arena encoders (`encode_json_into_arena`, `encode_msgpack_into_arena`), zero-copy gRPC Length-Prefixed-Message helpers (`encode_grpc_frame_into`, `decode_grpc_frame`), and `hydrate_rows_vectorcall` / `hydrate_one_vectorcall` / `orm_to_struct_vectorcall` wrappers that delegate to `vertebra._vertebra` when compiled and fall back cleanly to pure Python during documentation builds (`--only-group doc`).

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
| `SyncPool` | Lazily created `ThreadPoolExecutor` (`max_workers` default `2 × cpu_count`, thread prefix `vertebra-ft-sync`). `submit` / `submit_async` copy `contextvars`. The executor is rebuilt after `fork` (PID check). `SyncPool.default()` is a double-checked singleton. |
| `CapacityLimiter` | One `asyncio.Semaphore` per event loop (`WeakKeyDictionary`), reset after fork. Assigning `total_tokens` drops existing semaphores. |
| `LoopShardedResource[T]` | One resource per event loop from `factory` or `async_factory`. A factory set up with only `async_factory` must be read with `get_async()`. |
| `configure_event_loop_policy()`, `LoopDiagnostics`, `LoopMismatchError` | Event-loop policy installation (uvloop on non-Windows GIL builds unless `VERTEBRA_UVLOOP` disables it) and nested-loop/thread-affinity diagnostics in `sync_tools.py`. |
| [`PortalProvider` / `Portal` / `PortalManager` / `get_global_portal`](../../../../../src/py/vertebra/utils/portal.py) | A background thread running its own event loop. `PortalManager` is a singleton that restarts the provider after fork or if the thread died, and stops it at exit. |

Outside `vertebra.utils`, `vertebra.extensions.sqlalchemy.dialects` (`SyncPool`) imports `sync_tools`. Tests are in [`test_sync_tools.py`](../../../../../src/py/tests/unit/utils/test_sync_tools.py).

## 4. Rust-Accelerated Text, Humanize, UUIDs & Runtime Utilities (`vertebra.utils`)

- [`text.py`](../../../../../src/py/vertebra/utils/text.py) & [`vertebra-core/src/text.rs`](../../../../../src/rs/vertebra-core/src/text.rs): Single-pass, zero-regex Rust state machines exposed on `vertebra._vertebra` with pure-Python `--only-group doc` fallbacks for `snake_case`, `camelize`, `pascalize`, `kebabize`, `slugify`, `quote_identifier`, `quote_backtick_identifier`, `quote_bracket_identifier`, `quote_dialect_identifier`, `split_qualified_identifier`, `normalize_identifier`, `validate_identifier`, $O(1)$ `truncate_utf8_bytes` (`floor_char_boundary`), and `truncate`.
- [`humanize.py`](../../../../../src/py/vertebra/utils/humanize.py) & [`vertebra-core/src/text.rs`](../../../../../src/rs/vertebra-core/src/text.rs): Rust-backed human-friendly formatting, English noun inflection, and unit parsing (`humanize`, `titleize`, `pluralize`, `singularize`, `ordinalize`, `humanize_bytes`, `parse_bytes`, `humanize_duration`, `parse_duration`, `humanize_number`, `humanize_rate`).
- [`uuids.py`](../../../../../src/py/vertebra/utils/uuids.py) & [`vertebra-core/src/id.rs`](../../../../../src/rs/vertebra-core/src/id.rs): `uuid3`, `uuid4`, `uuid5`, `uuid6`, `uuid7`, `nanoid`, `uuid_from_string`, `uuid_from_bytes`, and `uuid_from_int`. `uuid4()`, `uuid7()` (lock-free `AtomicU64` monotonic millisecond + 12-bit sequence counter), and `nanoid()` dispatch to `_vertebra.{uuid4_u128,uuid7_u128,nanoid_generate}` by default while honoring monkeypatched `_load_uuid_utils()` in tests.
- [`dispatch.py`](../../../../../src/py/vertebra/utils/dispatch.py), [`schema.py`](../../../../../src/py/vertebra/utils/schema.py), [`type_converters.py`](../../../../../src/py/vertebra/utils/type_converters.py), and [`type_guards.py`](../../../../../src/py/vertebra/utils/type_guards.py): $O(1)$ MRO-cached `TypeDispatcher[T]`, `transform_dict_keys`, `to_schema`, `to_value_type`, `build_decimal_converter`, `build_nested_decimal_normalizer`, `build_uuid_coercions`, `should_json_encode_sequence`, `time_iso_convert`, and runtime type guards (`is_readable`, `is_async_readable`, `is_typed_dict`, `is_dict_row`, `is_mapping_like`, `is_iterable_parameters`, `is_attrs_schema`, `is_schema`, `is_schema_or_dict`, `is_schema_or_dict_sequence`, `get_msgspec_rename_config`, `extract_dataclass_fields`, `extract_dataclass_items`, `supports_close`).
- [`module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py) & [`config_tools.py`](../../../../../src/py/vertebra/utils/config_tools.py): `import_app`, `import_string`, `import_optional`, `import_optional_attr`, `resolve_optional_attr`, `module_to_os_path`, `ensure_module`, `ensure_{pyarrow,numpy,pandas}`, `lazy_exports`, `SingletonMeta`, `get_env`, `get_env_with_aliases`, `is_env_set`, `find_pyproject_toml`, `read_pyproject_section` / `parse_pyproject_config` (reading `[tool.vertebra]` with `[tool.vtb]` fallback), `get_config_val`, `get_config_val_with_aliases`, `normalize_connection_config`, `parse_mysql_dsn`, `parse_odbc_connection_string`, `resolve_config_sync`, and `resolve_config_async`.
- [`deprecation.py`](../../../../../src/py/vertebra/utils/deprecation.py) & [`correlation.py`](../../../../../src/py/vertebra/utils/correlation.py): PEP 695 `@deprecated` and `warn_deprecation` with external caller stack attribution, and `CorrelationContext` (`context()`, `extract_from_headers()`, `to_headers()`, `get_correlation_adapter()`).

<!-- truth: start -->
- No module under `src/py/vertebra` outside `utils/serializers/` calls `msgspec.json.encode`, `msgspec.json.decode`, `msgspec.msgpack.encode`, `msgspec.msgpack.decode`, `json.dumps`, `json.loads`, or `orjson`.
- `vertebra.utils.{text,humanize,uuids}` dispatch to compiled Rust functions in `vertebra._vertebra` (`vertebra-core::{text,id}`) when available and fall back cleanly to pure Python in `--only-group doc` environments by guarding `callable(fn) and not isinstance(fn, type)`.
- `BaseStruct`, `CamelizedBaseStruct`, `FrozenBaseStruct`, and `FrozenCamelizedStruct` configure `gc=False, omit_defaults=True, repr_omit_defaults=True` (with `frozen=True` on the frozen variants and `rename="camel"` on the camelized variants) and expose `.to_dict()` via `msgspec.structs.asdict`.
- `async_` dispatches to `SyncPool.default()` only when `sys._is_gil_enabled()` returns `False`, and `SyncPool`, `CapacityLimiter`, and `PortalManager` rebuild their executor, semaphores, or provider when `os.getpid()` changes.
- `vertebra.utils.config_tools` reads `[tool.vertebra]` (falling back to `[tool.vtb]`) and `vertebra.telemetry.LoggingConfig` reads `[tool.vertebra.logging]`.
<!-- truth: end -->
