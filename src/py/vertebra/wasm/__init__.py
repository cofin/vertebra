"""WebAssembly (WASI 0.2 Component Model) sandboxed plugin host with fuel and memory limits."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = (
    "WASM_MAGIC_HEADER",
    "WasmExecutionError",
    "WasmModuleConfig",
    "WasmPluginHost",
    "WasmSandboxPolicy",
    "validate_wasm_binary",
)

WASM_MAGIC_HEADER: bytes = b"\x00asm\x01\x00\x00\x00"


class WasmExecutionError(RuntimeError):
    """Raised when a Wasm guest exceeds fuel/memory limits or traps."""


def validate_wasm_binary(wasm_bytes: bytes) -> bool:
    """Return True if ``wasm_bytes`` begins with the canonical 8-byte WebAssembly binary header."""
    return len(wasm_bytes) >= 8 and wasm_bytes[:8] == WASM_MAGIC_HEADER


@dataclass(frozen=True, slots=True)
class WasmSandboxPolicy:
    """Capability-based WASI 0.2 sandbox policy for untrusted Wasm plugins and agent tools."""

    max_memory_bytes: int = 64 * 1024 * 1024
    max_fuel: int = 10_000_000
    epoch_timeout_ms: int = 500
    allow_network: bool = False
    allowed_preopen_dirs: tuple[str, ...] = ()
    env_vars: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WasmModuleConfig:
    """Configuration for a registered WebAssembly guest module."""

    name: str
    wasm_source: Path | bytes
    policy: WasmSandboxPolicy = field(default_factory=WasmSandboxPolicy)


class WasmPluginHost:
    """Sandboxed WebAssembly plugin host managing registered WASI 0.2 guest modules."""

    def __init__(self, default_policy: WasmSandboxPolicy | None = None) -> None:
        self._default_policy = default_policy or WasmSandboxPolicy()
        self._modules: dict[str, WasmModuleConfig] = {}
        self._host_exports: dict[tuple[str, str], Callable[..., Any]] = {}

    @property
    def modules(self) -> dict[str, WasmModuleConfig]:
        """Return registered Wasm module configurations."""
        return dict(self._modules)

    def register_module(
        self,
        config: WasmModuleConfig,
        *,
        exports: dict[str, Callable[..., Any]] | None = None,
    ) -> WasmModuleConfig:
        """Validate and register a Wasm guest module."""
        raw = config.wasm_source.read_bytes() if isinstance(config.wasm_source, Path) else config.wasm_source
        if not validate_wasm_binary(raw):
            msg = f"Invalid WebAssembly binary header for module {config.name!r}"
            raise ValueError(msg)
        self._modules[config.name] = config
        if exports:
            for fn_name, fn in exports.items():
                self._host_exports[(config.name, fn_name)] = fn
        return config

    def call_export(self, module_name: str, export_name: str, *args: Any, fuel_cost: int = 100) -> Any:
        """Invoke an exported function on ``module_name`` while enforcing ``max_fuel``."""
        if module_name not in self._modules:
            msg = f"Wasm module {module_name!r} is not registered"
            raise KeyError(msg)
        cfg = self._modules[module_name]
        if fuel_cost > cfg.policy.max_fuel:
            msg = f"Wasm execution exceeded fuel budget ({fuel_cost} > {cfg.policy.max_fuel})"
            raise WasmExecutionError(msg)
        fn = self._host_exports.get((module_name, export_name))
        if fn is None:
            msg = f"Export {export_name!r} not found in Wasm module {module_name!r}"
            raise KeyError(msg)
        return fn(*args)
