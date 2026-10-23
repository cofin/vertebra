# Python Code Layout and Serializer Rules

This guide provides concrete standards for Python code structure and data serialization across `vertebra`.

---

## Zero Inline Comments Rule

- No inline `#` comments may be committed in Python source files.
- Explain *why* an operation is designed in a specific way in the class or method Google-style docstring.
- Tooling directives such as `# type: ignore` or `# noqa` must not be added. Address the underlying typing or lint issue directly.

---

## Centralized Serializers

All JSON encoding and decoding must invoke functions from `vertebra.utils.serializers`:

- `encode_json(obj)`: Encodes Python struct or model to JSON string.
- `encode_json_bytes(obj)`: Encodes directly to bytes without intermediate string conversion.
- `encode_json_into(buffer, offset, obj)`: Serializes directly into memory slices.
- `decode_json(data, target_type)`: Decodes JSON bytes or strings with type validation.

Direct imports of `json` or `msgspec.json` outside `vertebra.utils.serializers` are forbidden.

---

## Public `vertebra.typing` Shim & Cached `module_loader` (`PY-08`)

- Always import typing aliases, base structs (`BaseStruct`, `CamelizedBaseStruct`, `FrozenBaseStruct`, `FrozenCamelizedStruct`, `Struct`), sentinels (`Empty`, `EmptyType`, `UNSET`), protocols (`DataclassProtocol`, `AttrsInstance`), exceptions (`VertebraError`, `MissingDependencyError`, `ImproperConfigurationError`, `UnsupportedBackendError`), and optional dependency flags from the public `vertebra.typing` shim.
- Importing from `vertebra._typing` outside `src/py/vertebra/typing.py` (and its dedicated unit test `src/py/tests/unit/framework/test_typing.py`) is forbidden.
- Always route optional or dynamic module loading through `vertebra.utils.module_loader` (`import_optional`, `ensure_module`, `module_available`, `import_string`, `import_app`), which caches imported `ModuleType` objects in `_module_cache`. Direct `importlib.import_module` calls outside `src/py/vertebra/utils/module_loader.py` and the standalone self-extractor stub `src/py/vertebra/build/packaging/_onefile_extract.py` are forbidden.
