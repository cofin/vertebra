"""WebAssembly (WASI 0.2 Component Model) sandbox policy, module configuration, and plugin host."""

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec

from vertebra.typing import ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    decode_msgpack,
    encode_json_into,
    encode_msgpack_into,
)

__all__ = (
    "WASMExecutionError",
    "WASMHttpEnvelope",
    "WASMHttpResponse",
    "WASMModuleConfig",
    "WASMPluginHost",
    "WASMSandboxPolicy",
    "WASM_MAGIC_HEADER",
)

WASM_MAGIC_HEADER: bytes = b"\x00asm\x01\x00\x00\x00"
_wasmtime = import_optional("wasmtime")


class WASMExecutionError(RuntimeError):
    """Raised when a WebAssembly guest module traps or exceeds its fuel/memory budget."""


class WASMHttpEnvelope(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Compact array-like WASI HTTP request envelope for zero-copy Wasm guest dispatch."""

    method: str
    path: str
    headers: list[tuple[str, str]] = msgspec.field(default_factory=list)
    body: bytes = b""


class WASMHttpResponse(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Compact array-like WASI HTTP response envelope returned by Wasm guest handlers."""

    status: int = 200
    headers: list[tuple[str, str]] = msgspec.field(default_factory=list)
    body: bytes = b""


class WASMSandboxPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Capability-based WASI 0.2 sandbox policy for untrusted Wasm plugins and agent tools."""

    max_memory_bytes: int = 64 * 1024 * 1024
    max_fuel: int = 10_000_000
    epoch_timeout_ms: int = 500
    allow_network: bool = False
    allowed_preopen_dirs: tuple[str, ...] = ()
    env_vars: dict[str, str] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.max_memory_bytes <= 0:
            msg = "max_memory_bytes must be > 0"
            raise ValueError(msg)
        if self.max_fuel <= 0:
            msg = "max_fuel must be > 0"
            raise ValueError(msg)
        if self.epoch_timeout_ms <= 0:
            msg = "epoch_timeout_ms must be > 0"
            raise ValueError(msg)


class WASMModuleConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for a registered WebAssembly guest module."""

    name: str
    wasm_source: str | Path | bytes
    policy: WASMSandboxPolicy = msgspec.field(default_factory=WASMSandboxPolicy)

    def __post_init__(self) -> None:
        if isinstance(self.wasm_source, str):
            msgspec.structs.force_setattr(self, "wasm_source", Path(self.wasm_source))

    @staticmethod
    def validate_binary(wasm_bytes: bytes) -> bool:
        """Return True if ``wasm_bytes`` begins with the canonical 8-byte WebAssembly binary header."""
        return len(wasm_bytes) >= 8 and wasm_bytes[:8] == WASM_MAGIC_HEADER

    def load_and_validate(self) -> bytes:
        """Load the Wasm binary from disk or memory and validate its 8-byte magic header."""
        if isinstance(self.wasm_source, Path):
            raw = self.wasm_source.read_bytes()
        elif isinstance(self.wasm_source, str):
            raw = Path(self.wasm_source).read_bytes()
        else:
            raw = self.wasm_source
        if not self.validate_binary(raw):
            msg = f"Invalid WebAssembly binary header for module {self.name!r}"
            raise ValueError(msg)
        return raw


def _has_runtime_attr(target: Any, attr: str, *, instantiate_if_missing: bool = False) -> bool:
    """Return True if ``target`` (or a zero-arg instance when requested) exposes ``attr``."""
    if target is None:
        return False
    if hasattr(target, attr):
        return True
    if instantiate_if_missing and callable(target):
        try:
            return hasattr(target(), attr)
        except Exception:
            return False
    return False


def _validate_policy_support(policy: WASMSandboxPolicy, *, exports_mode: bool = False) -> None:
    """Fail closed with ImproperConfigurationError when the active runtime cannot enforce policy fields."""
    wasi_cls = None if _wasmtime is None else getattr(_wasmtime, "WasiConfig", None)
    linker_cls = None if _wasmtime is None else getattr(_wasmtime, "Linker", None)
    store_cls = None if _wasmtime is None else getattr(_wasmtime, "Store", None)
    config_cls = None if _wasmtime is None else getattr(_wasmtime, "Config", None)
    engine_cls = None if _wasmtime is None else getattr(_wasmtime, "Engine", None)

    has_wasi_link = _has_runtime_attr(linker_cls, "define_wasi") and _has_runtime_attr(store_cls, "set_wasi")

    if policy.allow_network and (
        exports_mode or not _has_runtime_attr(wasi_cls, "inherit_network") or not has_wasi_link
    ):
        msg = "Active WASM runtime cannot enforce WASMSandboxPolicy.allow_network=True"
        raise ImproperConfigurationError(msg)

    if policy.allowed_preopen_dirs and (
        exports_mode or not _has_runtime_attr(wasi_cls, "preopen_dir") or not has_wasi_link
    ):
        msg = "Active WASM runtime cannot enforce WASMSandboxPolicy.allowed_preopen_dirs"
        raise ImproperConfigurationError(msg)

    if policy.env_vars and (exports_mode or not _has_runtime_attr(wasi_cls, "set_env") or not has_wasi_link):
        msg = "Active WASM runtime cannot enforce WASMSandboxPolicy.env_vars"
        raise ImproperConfigurationError(msg)

    if policy.epoch_timeout_ms != 500 and (
        exports_mode
        or not _has_runtime_attr(config_cls, "epoch_interruption", instantiate_if_missing=True)
        or not _has_runtime_attr(engine_cls, "increment_epoch")
        or not _has_runtime_attr(store_cls, "set_epoch_deadline")
    ):
        msg = "Active WASM runtime cannot enforce custom WASMSandboxPolicy.epoch_timeout_ms"
        raise ImproperConfigurationError(msg)


class WASMPluginHost:
    """Host registry and sandboxed executor for WASI 0.2 WebAssembly guest modules."""

    __slots__ = ("_compiled_modules", "_default_policy", "_host_exports", "_modules")

    def __init__(self, policy: WASMSandboxPolicy | None = None) -> None:
        self._default_policy = policy if policy is not None else WASMSandboxPolicy()
        if _wasmtime is not None:
            _validate_policy_support(self._default_policy, exports_mode=False)
        self._modules: dict[str, WASMModuleConfig] = {}
        self._host_exports: dict[tuple[str, str], Callable[..., Any]] = {}
        self._compiled_modules: dict[str, tuple[Any, Any]] = {}

    @property
    def registered_modules(self) -> tuple[str, ...]:
        """Return the names of all registered Wasm modules."""
        return tuple(self._modules.keys())

    def register_module(
        self,
        config: WASMModuleConfig,
        *,
        exports: dict[str, Callable[..., Any]] | None = None,
    ) -> None:
        """Validate and register a WebAssembly module and its exported host/guest bindings."""
        raw = config.load_and_validate()
        if exports is not None:
            _validate_policy_support(config.policy, exports_mode=True)
            self._modules[config.name] = config
            for export_name, fn in exports.items():
                self._host_exports[(config.name, export_name)] = fn
            return

        if _wasmtime is None:
            raise MissingDependencyError("wasmtime", extra="wasmtime")

        _validate_policy_support(config.policy, exports_mode=False)
        wt_cfg = _wasmtime.Config()
        wt_cfg.consume_fuel = True
        if hasattr(wt_cfg, "epoch_interruption"):
            wt_cfg.epoch_interruption = True
        engine = _wasmtime.Engine(wt_cfg)
        try:
            module = _wasmtime.Module(engine, raw)
        except Exception as exc:
            msg = f"Failed to compile Wasm module {config.name!r}: {exc}"
            raise WASMExecutionError(msg) from exc
        self._modules[config.name] = config
        self._compiled_modules[config.name] = (engine, module)

    def get_module(self, name: str) -> WASMModuleConfig:
        """Retrieve a registered WASMModuleConfig by name."""
        if name not in self._modules:
            msg = f"Wasm module {name!r} is not registered"
            raise KeyError(msg)
        return self._modules[name]

    def call_export(
        self,
        module_name: str,
        export_name: str,
        *args: Any,
        fuel_cost: int = 100,
    ) -> Any:
        """Invoke an exported function under the module's configured fuel and memory budget."""
        cfg = self.get_module(module_name)
        fn = self._host_exports.get((module_name, export_name))
        if fn is not None:
            if fuel_cost > cfg.policy.max_fuel:
                msg = f"Wasm module {module_name!r} exceeded max_fuel budget ({fuel_cost} > {cfg.policy.max_fuel})"
                raise WASMExecutionError(msg)
            return fn(*args)

        compiled = self._compiled_modules.get(module_name)
        if compiled is None or _wasmtime is None:
            msg = f"Export {export_name!r} not found in Wasm module {module_name!r}"
            raise WASMExecutionError(msg)

        engine, module = compiled
        timer: threading.Timer | None = None
        try:
            store = _wasmtime.Store(engine)
            store.set_fuel(cfg.policy.max_fuel)
            store.set_limits(memory_size=cfg.policy.max_memory_bytes)
            if hasattr(store, "set_epoch_deadline") and hasattr(engine, "increment_epoch"):
                store.set_epoch_deadline(1)
                timer = threading.Timer(cfg.policy.epoch_timeout_ms / 1000.0, engine.increment_epoch)
                timer.daemon = True
                timer.start()

            needs_wasi = bool(cfg.policy.allowed_preopen_dirs or cfg.policy.env_vars or cfg.policy.allow_network)
            if needs_wasi:
                wasi_cfg = _wasmtime.WasiConfig()
                if hasattr(wasi_cfg, "inherit_network"):
                    wasi_cfg.inherit_network(cfg.policy.allow_network)
                for env_key, env_val in cfg.policy.env_vars.items():
                    wasi_cfg.set_env(env_key, env_val)
                for preopen_dir in cfg.policy.allowed_preopen_dirs:
                    wasi_cfg.preopen_dir(preopen_dir, preopen_dir)
                store.set_wasi(wasi_cfg)
                linker = _wasmtime.Linker(engine)
                linker.define_wasi()
                instance = linker.instantiate(store, module)
            else:
                instance = _wasmtime.Instance(store, module, [])

            exports_map = instance.exports(store)
            if export_name not in exports_map:
                msg = f"Export {export_name!r} not found in Wasm module {module_name!r}"
                raise WASMExecutionError(msg)
            export_fn = exports_map[export_name]
            return export_fn(store, *args)
        except WASMExecutionError:
            raise
        except Exception as exc:
            msg = f"Wasm execution failed in {module_name!r}.{export_name!r}: {exc}"
            raise WASMExecutionError(msg) from exc
        finally:
            if timer is not None:
                timer.cancel()

    def call_json(
        self,
        module_name: str,
        export_name: str,
        payload: Any,
        *,
        return_type: Any = Any,
        fuel_cost: int = 100,
    ) -> Any:
        """Encode payload into a reusable buffer via msgspec.json.Encoder.encode_into and decode the guest response."""
        buf = bytearray(256)
        encode_json_into(payload, buf, 0)
        mv = memoryview(buf)
        cfg = self.get_module(module_name)
        if len(mv) > cfg.policy.max_memory_bytes:
            msg = f"Wasm payload size {len(mv)} exceeds max_memory_bytes ({cfg.policy.max_memory_bytes})"
            raise WASMExecutionError(msg)

        fn = self._host_exports.get((module_name, export_name))
        if fn is not None:
            if fuel_cost > cfg.policy.max_fuel:
                msg = f"Wasm module {module_name!r} exceeded max_fuel budget ({fuel_cost} > {cfg.policy.max_fuel})"
                raise WASMExecutionError(msg)
            raw_out = fn(mv)
            if isinstance(raw_out, bytes | bytearray | memoryview | str):
                if isinstance(raw_out, str):
                    return decode_json(raw_out, type=return_type)
                plan = ZeroCopyCodecRegistry.compile(return_type)
                return plan.decode_buffer(raw_out, format="json")
            if return_type is not Any and not isinstance(raw_out, return_type):
                return msgspec.convert(raw_out, type=return_type)
            return raw_out

        raw_res = self.call_export(module_name, export_name, bytes(mv), fuel_cost=fuel_cost)
        if isinstance(raw_res, bytes | bytearray | memoryview):
            plan = ZeroCopyCodecRegistry.compile(return_type)
            return plan.decode_buffer(raw_res, format="json")
        return decode_json(str(raw_res), type=return_type)

    def call_msgpack(
        self,
        module_name: str,
        export_name: str,
        payload: Any,
        *,
        return_type: Any = Any,
        fuel_cost: int = 100,
    ) -> Any:
        """Encode payload into a reusable buffer via msgspec.msgpack.Encoder.encode_into and decode the guest response."""
        buf = bytearray(256)
        encode_msgpack_into(payload, buf, 0)
        mv = memoryview(buf)
        cfg = self.get_module(module_name)
        if len(mv) > cfg.policy.max_memory_bytes:
            msg = f"Wasm payload size {len(mv)} exceeds max_memory_bytes ({cfg.policy.max_memory_bytes})"
            raise WASMExecutionError(msg)

        fn = self._host_exports.get((module_name, export_name))
        if fn is not None:
            if fuel_cost > cfg.policy.max_fuel:
                msg = f"Wasm module {module_name!r} exceeded max_fuel budget ({fuel_cost} > {cfg.policy.max_fuel})"
                raise WASMExecutionError(msg)
            raw_out = fn(mv)
            if isinstance(raw_out, bytes | bytearray | memoryview):
                plan = ZeroCopyCodecRegistry.compile(return_type)
                return plan.decode_buffer(raw_out, format="msgpack")
            if return_type is not Any and not isinstance(raw_out, return_type):
                return msgspec.convert(raw_out, type=return_type)
            return raw_out

        raw_res = self.call_export(module_name, export_name, bytes(mv), fuel_cost=fuel_cost)
        if isinstance(raw_res, bytes | bytearray | memoryview):
            plan = ZeroCopyCodecRegistry.compile(return_type)
            return plan.decode_buffer(raw_res, format="msgpack")
        return decode_msgpack(bytes(raw_res), type=return_type)

    def dispatch_http(
        self,
        module_name: str,
        request: Any,
        *,
        export_name: str = "handle_http",
        fuel_cost: int = 100,
    ) -> WASMHttpResponse:
        """Dispatch an HTTP Request to a Wasm guest handler using a compact array_like MsgPack envelope."""
        method = str(getattr(request, "method", "GET"))
        path = str(getattr(request, "path", "/"))
        headers_obj = getattr(request, "headers", None)
        hdrs: list[tuple[str, str]] = (
            list(headers_obj.items()) if headers_obj is not None and hasattr(headers_obj, "items") else []
        )
        bv = getattr(request, "body_view", None)
        body_bytes = bytes(bv) if bv is not None else (getattr(request, "body", b"") or b"")
        envelope = WASMHttpEnvelope(method=method, path=path, headers=hdrs, body=body_bytes)
        res: WASMHttpResponse = self.call_msgpack(
            module_name,
            export_name,
            envelope,
            return_type=WASMHttpResponse,
            fuel_cost=fuel_cost,
        )
        return res
