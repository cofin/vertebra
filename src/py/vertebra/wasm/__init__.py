"""WebAssembly (WASI 0.2 Component Model) sandbox policy, module configuration, and plugin host."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional

__all__ = (
    "WASM_MAGIC_HEADER",
    "WasmExecutionError",
    "WasmModuleConfig",
    "WasmPluginHost",
    "WasmSandboxPolicy",
)

WASM_MAGIC_HEADER: bytes = b"\x00asm\x01\x00\x00\x00"
_wasmtime = import_optional("wasmtime")


class WasmExecutionError(RuntimeError):
    """Raised when a WebAssembly guest module traps or exceeds its fuel/memory budget."""


class WasmSandboxPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
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


class WasmModuleConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for a registered WebAssembly guest module."""

    name: str
    wasm_source: str | Path | bytes
    policy: WasmSandboxPolicy = msgspec.field(default_factory=WasmSandboxPolicy)

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


class WasmPluginHost:
    """Host registry and sandboxed executor for WASI 0.2 WebAssembly guest modules."""

    __slots__ = ("_compiled_modules", "_host_exports", "_modules")

    def __init__(self) -> None:
        self._modules: dict[str, WasmModuleConfig] = {}
        self._host_exports: dict[tuple[str, str], Callable[..., Any]] = {}
        self._compiled_modules: dict[str, tuple[Any, Any]] = {}

    @property
    def registered_modules(self) -> tuple[str, ...]:
        """Return the names of all registered Wasm modules."""
        return tuple(self._modules.keys())

    def register_module(
        self,
        config: WasmModuleConfig,
        *,
        exports: dict[str, Callable[..., Any]] | None = None,
    ) -> None:
        """Validate and register a WebAssembly module and its exported host/guest bindings."""
        raw = config.load_and_validate()
        if exports is not None:
            self._modules[config.name] = config
            for export_name, fn in exports.items():
                self._host_exports[(config.name, export_name)] = fn
            return

        if _wasmtime is None:
            raise MissingDependencyError("wasmtime", install_package="vertebra[wasm]", extra="wasm")

        wt_cfg = _wasmtime.Config()
        wt_cfg.consume_fuel = True
        engine = _wasmtime.Engine(wt_cfg)
        try:
            module = _wasmtime.Module(engine, raw)
        except Exception as exc:
            msg = f"Failed to compile Wasm module {config.name!r}: {exc}"
            raise WasmExecutionError(msg) from exc
        self._modules[config.name] = config
        self._compiled_modules[config.name] = (engine, module)

    def get_module(self, name: str) -> WasmModuleConfig:
        """Retrieve a registered WasmModuleConfig by name."""
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
                raise WasmExecutionError(msg)
            return fn(*args)

        compiled = self._compiled_modules.get(module_name)
        if compiled is None or _wasmtime is None:
            msg = f"Export {export_name!r} not found in Wasm module {module_name!r}"
            raise WasmExecutionError(msg)

        engine, module = compiled
        try:
            store = _wasmtime.Store(engine)
            store.set_fuel(cfg.policy.max_fuel)
            store.set_limits(memory_size=cfg.policy.max_memory_bytes)
            instance = _wasmtime.Instance(store, module, [])
            exports_map = instance.exports(store)
            if export_name not in exports_map:
                msg = f"Export {export_name!r} not found in Wasm module {module_name!r}"
                raise WasmExecutionError(msg)
            export_fn = exports_map[export_name]
            return export_fn(store, *args)
        except WasmExecutionError:
            raise
        except Exception as exc:
            msg = f"Wasm execution failed in {module_name!r}.{export_name!r}: {exc}"
            raise WasmExecutionError(msg) from exc
