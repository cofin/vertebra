---
type: Pattern
title: "Serialization & Python Utility Conventions"
description: "How `vertebra.utils` centralizes JSON and MessagePack encoding and decoding, Rust-accelerated text/humanize/UUID primitives, type dispatch, schema hydration, async/sync bridging, free-threaded worker pools, optional imports, and config lookup."
status: stable
tags:
  - patterns
  - python
  - serialization
  - concurrency
  - text
  - humanize
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
sources:
  - id: src-vertebra-core-src-text-rs
    resource: src/rs/vertebra-core/src/text.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/text.rs)"
  - id: src-vertebra-core-src-id-rs
    resource: src/rs/vertebra-core/src/id.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/id.rs)"
  - id: src-vertebra-py-src-text-rs
    resource: src/rs/vertebra-py/src/text.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/text.rs)"
  - id: src-utils-init-py
    resource: src/py/vertebra/utils/__init__.py
    title: "Python implementation module (src/py/vertebra/utils/__init__.py)"
  - id: src-utils-text-py
    resource: src/py/vertebra/utils/text.py
    title: "Python implementation module (src/py/vertebra/utils/text.py)"
  - id: src-utils-humanize-py
    resource: src/py/vertebra/utils/humanize.py
    title: "Python implementation module (src/py/vertebra/utils/humanize.py)"
  - id: src-utils-uuids-py
    resource: src/py/vertebra/utils/uuids.py
    title: "Python implementation module (src/py/vertebra/utils/uuids.py)"
  - id: src-utils-dispatch-py
    resource: src/py/vertebra/utils/dispatch.py
    title: "Python implementation module (src/py/vertebra/utils/dispatch.py)"
  - id: src-utils-schema-py
    resource: src/py/vertebra/utils/schema.py
    title: "Python implementation module (src/py/vertebra/utils/schema.py)"
  - id: src-utils-type-converters-py
    resource: src/py/vertebra/utils/type_converters.py
    title: "Python implementation module (src/py/vertebra/utils/type_converters.py)"
  - id: src-utils-type-guards-py
    resource: src/py/vertebra/utils/type_guards.py
    title: "Python implementation module (src/py/vertebra/utils/type_guards.py)"
  - id: src-utils-config-tools-py
    resource: src/py/vertebra/utils/config_tools.py
    title: "Python implementation module (src/py/vertebra/utils/config_tools.py)"
  - id: src-utils-module-loader-py
    resource: src/py/vertebra/utils/module_loader.py
    title: "Python implementation module (src/py/vertebra/utils/module_loader.py)"
  - id: src-utils-deprecation-py
    resource: src/py/vertebra/utils/deprecation.py
    title: "Python implementation module (src/py/vertebra/utils/deprecation.py)"
  - id: src-utils-correlation-py
    resource: src/py/vertebra/utils/correlation.py
    title: "Python implementation module (src/py/vertebra/utils/correlation.py)"
  - id: src-utils-serializers-init-py
    resource: src/py/vertebra/utils/serializers/__init__.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/__init__.py)"
  - id: src-utils-serializers-json-py
    resource: src/py/vertebra/utils/serializers/_json.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_json.py)"
  - id: src-utils-serializers-msgpack-py
    resource: src/py/vertebra/utils/serializers/_msgpack.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_msgpack.py)"
  - id: src-utils-serializers-schema-py
    resource: src/py/vertebra/utils/serializers/_schema.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_schema.py)"
  - id: src-utils-serializers-numpy-py
    resource: src/py/vertebra/utils/serializers/_numpy.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_numpy.py)"
  - id: src-utils-serializers-zero-copy-py
    resource: src/py/vertebra/utils/serializers/_zero_copy.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_zero_copy.py)"
  - id: src-utils-sync-tools-py
    resource: src/py/vertebra/utils/sync_tools.py
    title: "Python implementation module (src/py/vertebra/utils/sync_tools.py)"
  - id: src-utils-portal-py
    resource: src/py/vertebra/utils/portal.py
    title: "Python implementation module (src/py/vertebra/utils/portal.py)"
  - id: src-tests-unit-utils-test-sync-tools-py
    resource: src/py/tests/unit/utils/test_sync_tools.py
    title: "Python implementation module (src/py/tests/unit/utils/test_sync_tools.py)"
---

# Serialization & Python Utility Conventions

<!-- truth: start -->
- No module under `src/py/vertebra` outside `utils/serializers/` calls `msgspec.json.encode`, `msgspec.json.decode`, `msgspec.msgpack.encode`, `msgspec.msgpack.decode`, `json.dumps`, `json.loads`, or `orjson`.
- `vertebra.utils.{text,humanize,uuids}` bind directly to mandatory compiled Rust functions in `vertebra._vertebra` (`vertebra-core::{text,id}`) and stdlib `uuid` without pure-Python `_py_*` fallback tables or `_load_uuid_utils` test hooks, and `import_optional` fast-paths `sys.modules.get(module_name)` hits before probing `module_available`.
- `BaseStruct`, `CamelizedBaseStruct`, `FrozenBaseStruct`, and `FrozenCamelizedStruct` configure `gc=False, omit_defaults=True, repr_omit_defaults=True` (with `frozen=True` on the frozen variants and `rename="camel"` on the camelized variants) and expose `.to_dict()` via `msgspec.structs.asdict`.
- `async_` dispatches to `SyncPool.default()` only when `sys._is_gil_enabled()` returns `False`, and `SyncPool`, `CapacityLimiter`, and `PortalManager` rebuild their executor, semaphores, or provider when `os.getpid()` changes.
- `vertebra.utils.config_tools` reads `[tool.vertebra]` (falling back to `[tool.vtb]`) and `vertebra.telemetry.LoggingConfig` reads `[tool.vertebra.logging]`.
<!-- truth: end -->

[`vertebra.utils`](../../../../../src/py/vertebra/utils/__init__.py)[^src-utils-init-py] is a namespace package (its `__init__.py` holds only a docstring) with no re-exports. Import from submodules. Module layout and lazy-import rules are covered in [module-and-class-layout](module-and-class-layout.md).

## 1. JSON & MessagePack Go Through `vertebra.utils.serializers`

Every JSON or MessagePack encode or decode in `src/py/vertebra` goes through [`vertebra.utils.serializers`](../../../../../src/py/vertebra/utils/serializers/__init__.py)[^src-utils-serializers-init-py]. The package uses `msgspec.json` behind a lazily created process-wide `JSONSerializer` (`get_default_json_serializer()` in [`_json.py`](../../../../../src/py/vertebra/utils/serializers/_json.py)[^src-utils-serializers-json-py]) and `msgspec.msgpack` behind [`MsgPackSerializer`](../../../../../src/py/vertebra/utils/serializers/_msgpack.py)[^src-utils-serializers-msgpack-py] (`get_default_msgpack_serializer()`, `encode_msgpack_bytes`, `encode_msgpack_into`, `decode_msgpack`).

| Function | Behavior |
|---|---|
| `encode_json(data, *, as_bytes=False)` | Returns `str` (or `bytes` with `as_bytes=True`), typed with overloads. |
| `encode_json_bytes(data)` | Returns UTF-8 `bytes`. Use it on hot paths and before writing to sockets, queues, or stores. |
| `encode_json_into(data, buffer, offset=0)` | `msgspec` `Encoder.encode_into` into a caller-owned `bytearray` with zero stdlib fallback. |
| `decode_json(data, *, decode_bytes=True, type=Any)` | Accepts `str` or `bytes` and caches typed `Decoder(type=...)` instances. With `decode_bytes=False`, `bytes` input is returned unchanged. |
| `format_json(value, *, indent=2)` | Pretty-prints with `msgspec.json.format`. |
| `encode_msgpack_bytes(value)` / `encode_msgpack_into(value, buffer, offset=0)` / `decode_msgpack(raw, *, type=Any)` | Centralized MessagePack encoding and typed decoding via `MsgPackSerializer`. |

`_normalize_supported_value` is the encoder `enc_hook`: it converts `datetime` (naive treated as UTC, `+00:00` → `Z`) and `date` to ISO 8601, `uuid.UUID`/`uuid_utils.UUID` to `str`, `Decimal` to `float` (encode exact decimals as `str` first), `Enum` to `.value`, Pydantic/dataclass/attrs/`msgspec.Struct` instances to `dict`, and NumPy arrays/scalars to Python lists/scalars; unsupported types raise `TypeError("unsupported JSON value: ...")` directly without stdlib `json` fallback. `_numpy.py` exports `numpy_array_enc_hook`, `numpy_array_dec_hook`, and `numpy_array_predicate` (no-op when NumPy is absent).

[`_zero_copy.py`](../../../../../src/py/vertebra/utils/serializers/_zero_copy.py)[^src-utils-serializers-zero-copy-py] provides `ZeroCopyCodecRegistry` (`TypeAdapterPlan` classification across `msgspec_struct`, `pydantic`, `dataclass`, `attrs`, `typeddict`, `namedtuple`, `dict`, and `primitive`), thread-local `bytearray` scratch-arena encoders (`encode_json_into_arena`, `encode_msgpack_into_arena`), zero-copy gRPC Length-Prefixed-Message helpers (`encode_grpc_frame_bytes`, `encode_grpc_frame_into`, `decode_grpc_frame`, `decode_grpc_frames`), and `hydrate_rows_vectorcall` / `hydrate_one_vectorcall` / `orm_to_struct_vectorcall` wrappers that delegate to `vertebra._vertebra` when compiled and fall back cleanly to pure Python during documentation builds (`--only-group doc`).

## 2. Base Structs & Schema Dumping (`_schema.py`, `typing.py`)

[`vertebra.typing`](../../../../../src/py/vertebra/typing.py) and [`vertebra.utils.serializers`](../../../../../src/py/vertebra/utils/serializers/__init__.py) export four canonical `msgspec.Struct` base classes, each providing `.to_dict()`:
- `BaseStruct(Struct, gc=False, omit_defaults=True, repr_omit_defaults=True)` and `CamelizedBaseStruct(BaseStruct, rename="camel")` for mutable state objects.
- `FrozenBaseStruct(Struct, frozen=True, gc=False, omit_defaults=True, repr_omit_defaults=True)` and `FrozenCamelizedStruct(FrozenBaseStruct, rename="camel", frozen=True)` for immutable wire models and configs.

`_dump_msgspec_fields(value)` in [`_schema.py`](../../../../../src/py/vertebra/utils/serializers/_schema.py)[^src-utils-serializers-schema-py] extracts `msgspec.Struct` fields into a plain dictionary when invoked through the serializer `enc_hook`. CDC change-feed envelope decoding and shared-feed local fan-out rules are documented in [Query Pipeline, CDC & Multi-Tenancy](../../domains/data/query-pipeline-cdc-and-tenancy.md).

## 3. Async/Sync Bridging (`sync_tools.py`, `portal.py`)

| Symbol | Behavior |
|---|---|
| `async_(fn, *, limiter=None)` | Wraps a blocking callable. On free-threaded builds (`sys._is_gil_enabled() is False`) it runs on `SyncPool.default()`; otherwise it uses `asyncio.to_thread`. An optional `CapacityLimiter` bounds concurrency. |
| `run_(async_fn)` | Blocking wrapper. Inside a running loop it uses the global portal. Otherwise it calls `asyncio.run`, installing `uvloop` first when available on non-Windows GIL builds. |
| `SyncPool` | Lazily created `ThreadPoolExecutor` (`max_workers` default `2 × cpu_count`, thread prefix `vertebra-ft-sync`). `submit` / `submit_async` copy `contextvars`. The executor is rebuilt after `fork` (PID check). `SyncPool.default()` is a double-checked singleton. |
| `CapacityLimiter` | One `asyncio.Semaphore` per event loop (`WeakKeyDictionary`), reset after fork. Assigning `total_tokens` drops existing semaphores. |
| `SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]` | One resource per event loop from `factory` (`get()`) or async `factory` (`await get()`). |
| `configure_event_loop_policy()`, `LoopDiagnostics`, `LoopMismatchError` | Event-loop policy installation (uvloop on non-Windows GIL builds unless `VERTEBRA_UVLOOP` disables it) and nested-loop/thread-affinity diagnostics in `sync_tools.py`. |
| [`PortalProvider` / `Portal` / `PortalManager` / `get_global_portal`](../../../../../src/py/vertebra/utils/portal.py)[^src-utils-portal-py] | A background thread running its own event loop. `PortalManager` is a singleton that restarts the provider after fork or if the thread died, and stops it at exit. |

Outside `vertebra.utils`, `vertebra.extensions.sqlalchemy.dialects` (`SyncPool`) imports `sync_tools`. Tests are in [`test_sync_tools.py`](../../../../../src/py/tests/unit/utils/test_sync_tools.py)[^src-tests-unit-utils-test-sync-tools-py].

## 4. Rust-Accelerated Text, Humanize, UUIDs & Runtime Utilities (`vertebra.utils`)

- [`text.py`](../../../../../src/py/vertebra/utils/text.py)[^src-utils-text-py] & [`vertebra-core/src/text.rs`](../../../../../src/rs/vertebra-core/src/text.rs)[^src-vertebra-core-src-text-rs]: Single-pass, zero-regex Rust state machines exposed directly on mandatory `vertebra._vertebra` (with zero pure-Python `_py_*` fallback tables or `_LATIN_FOLD_MAP` duplicates) for `snake_case`, `camelize`, `pascalize`, `kebabize`, `slugify`, `quote_identifier`, `quote_backtick_identifier`, `quote_bracket_identifier`, `quote_dialect_identifier`, `split_qualified_identifier`, `normalize_identifier`, `validate_identifier`, $O(1)$ `truncate_utf8_bytes` (`floor_char_boundary`), and `truncate`.
- [`humanize.py`](../../../../../src/py/vertebra/utils/humanize.py)[^src-utils-humanize-py] & [`vertebra-core/src/text.rs`](../../../../../src/rs/vertebra-core/src/text.rs): Rust-backed human-friendly formatting, English noun inflection, and unit parsing bound directly to `vertebra._vertebra` (`humanize`, `titleize`, `pluralize`, `singularize`, `ordinalize`, `humanize_bytes`, `parse_bytes`, `humanize_duration`, `parse_duration`, `humanize_number`, `humanize_rate`).
- [`uuids.py`](../../../../../src/py/vertebra/utils/uuids.py)[^src-utils-uuids-py] & [`vertebra-core/src/id.rs`](../../../../../src/rs/vertebra-core/src/id.rs)[^src-vertebra-core-src-id-rs]: `uuid3`, `uuid4`, `uuid5`, `uuid6`, `uuid7`, `nanoid`, `uuid_from_string`, `uuid_from_bytes`, and `uuid_from_int`. `uuid4()`, `uuid7()` (lock-free `AtomicU64` monotonic millisecond + 12-bit sequence counter), and `nanoid()` bind directly to `_vertebra.{uuid4_u128,uuid7_u128,nanoid_generate}` while `uuid3`, `uuid5`, and `uuid6` delegate to stdlib `uuid` without `_UuidBackend` or `_load_uuid_utils` test hooks.
- [`dispatch.py`](../../../../../src/py/vertebra/utils/dispatch.py)[^src-utils-dispatch-py], [`schema.py`](../../../../../src/py/vertebra/utils/schema.py)[^src-utils-schema-py], [`type_converters.py`](../../../../../src/py/vertebra/utils/type_converters.py)[^src-utils-type-converters-py], and [`type_guards.py`](../../../../../src/py/vertebra/utils/type_guards.py)[^src-utils-type-guards-py]: $O(1)$ MRO-cached `TypeDispatcher[T]`, `transform_dict_keys`, `to_schema`, `to_value_type`, `build_decimal_converter`, `build_nested_decimal_normalizer`, `build_uuid_coercions`, `should_json_encode_sequence`, `time_iso_convert`, and runtime type guards (`is_readable`, `is_async_readable`, `is_typed_dict`, `is_dict_row`, `is_mapping_like`, `is_iterable_parameters`, `is_attrs_schema`, `is_schema`, `is_schema_or_dict`, `is_schema_or_dict_sequence`, `get_msgspec_rename_config`, `extract_dataclass_fields`, `extract_dataclass_items`, `supports_close`).
- [`module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-utils-module-loader-py] & [`config_tools.py`](../../../../../src/py/vertebra/utils/config_tools.py)[^src-utils-config-tools-py]: `import_app`, `import_string`, `import_optional` (fast-pathing `sys.modules.get(module_name)` hits and honoring `sys.modules[name] is None` before cached `module_available` probes), `import_optional_attr`, `resolve_optional_attr`, `module_to_os_path`, `ensure_module`, `ensure_{pyarrow,numpy,pandas}`, `lazy_exports`, `SingletonMeta`, `get_env`, `get_env_with_aliases`, `is_env_set`, `find_pyproject_toml`, `read_pyproject_section` / `parse_pyproject_config` (reading `[tool.vertebra]` with `[tool.vtb]` fallback), `get_config_val`, `get_config_val_with_aliases`, `normalize_connection_config`, `parse_mysql_dsn`, `parse_odbc_connection_string`, `resolve_config_sync`, and `resolve_config_async`.
- [`deprecation.py`](../../../../../src/py/vertebra/utils/deprecation.py)[^src-utils-deprecation-py] & [`correlation.py`](../../../../../src/py/vertebra/utils/correlation.py)[^src-utils-correlation-py]: PEP 695 `@deprecated` and `warn_deprecation` with external caller stack attribution, and `CorrelationContext` (`context()`, `extract_from_headers()`, `to_headers()`, `get_correlation_adapter()`).

Provenance sources: [^src-vertebra-py-src-text-rs], [^src-utils-serializers-numpy-py], [^src-utils-sync-tools-py].

[^src-vertebra-core-src-text-rs]: `src/rs/vertebra-core/src/text.rs` — Rust implementation module (src/rs/vertebra-core/src/text.rs).
[^src-vertebra-core-src-id-rs]: `src/rs/vertebra-core/src/id.rs` — Rust implementation module (src/rs/vertebra-core/src/id.rs).
[^src-vertebra-py-src-text-rs]: `src/rs/vertebra-py/src/text.rs` — Rust implementation module (src/rs/vertebra-py/src/text.rs).
[^src-utils-init-py]: `src/py/vertebra/utils/__init__.py` — Python implementation module (src/py/vertebra/utils/__init__.py).
[^src-utils-text-py]: `src/py/vertebra/utils/text.py` — Python implementation module (src/py/vertebra/utils/text.py).
[^src-utils-humanize-py]: `src/py/vertebra/utils/humanize.py` — Python implementation module (src/py/vertebra/utils/humanize.py).
[^src-utils-uuids-py]: `src/py/vertebra/utils/uuids.py` — Python implementation module (src/py/vertebra/utils/uuids.py).
[^src-utils-dispatch-py]: `src/py/vertebra/utils/dispatch.py` — Python implementation module (src/py/vertebra/utils/dispatch.py).
[^src-utils-schema-py]: `src/py/vertebra/utils/schema.py` — Python implementation module (src/py/vertebra/utils/schema.py).
[^src-utils-type-converters-py]: `src/py/vertebra/utils/type_converters.py` — Python implementation module (src/py/vertebra/utils/type_converters.py).
[^src-utils-type-guards-py]: `src/py/vertebra/utils/type_guards.py` — Python implementation module (src/py/vertebra/utils/type_guards.py).
[^src-utils-config-tools-py]: `src/py/vertebra/utils/config_tools.py` — Python implementation module (src/py/vertebra/utils/config_tools.py).
[^src-utils-module-loader-py]: `src/py/vertebra/utils/module_loader.py` — Python implementation module (src/py/vertebra/utils/module_loader.py).
[^src-utils-deprecation-py]: `src/py/vertebra/utils/deprecation.py` — Python implementation module (src/py/vertebra/utils/deprecation.py).
[^src-utils-correlation-py]: `src/py/vertebra/utils/correlation.py` — Python implementation module (src/py/vertebra/utils/correlation.py).
[^src-utils-serializers-init-py]: `src/py/vertebra/utils/serializers/__init__.py` — Python implementation module (src/py/vertebra/utils/serializers/__init__.py).
[^src-utils-serializers-json-py]: `src/py/vertebra/utils/serializers/_json.py` — Python implementation module (src/py/vertebra/utils/serializers/_json.py).
[^src-utils-serializers-msgpack-py]: `src/py/vertebra/utils/serializers/_msgpack.py` — Python implementation module (src/py/vertebra/utils/serializers/_msgpack.py).
[^src-utils-serializers-schema-py]: `src/py/vertebra/utils/serializers/_schema.py` — Python implementation module (src/py/vertebra/utils/serializers/_schema.py).
[^src-utils-serializers-numpy-py]: `src/py/vertebra/utils/serializers/_numpy.py` — Python implementation module (src/py/vertebra/utils/serializers/_numpy.py).
[^src-utils-serializers-zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Python implementation module (src/py/vertebra/utils/serializers/_zero_copy.py).
[^src-utils-sync-tools-py]: `src/py/vertebra/utils/sync_tools.py` — Python implementation module (src/py/vertebra/utils/sync_tools.py).
[^src-utils-portal-py]: `src/py/vertebra/utils/portal.py` — Python implementation module (src/py/vertebra/utils/portal.py).
[^src-tests-unit-utils-test-sync-tools-py]: `src/py/tests/unit/utils/test_sync_tools.py` — Python implementation module (src/py/tests/unit/utils/test_sync_tools.py).
