"""Unified build orchestrator, multi-target codegen, and packaging for Vertebra."""

from typing import TYPE_CHECKING

from vertebra.utils.module_loading import lazy_exports

if TYPE_CHECKING:
    from vertebra.build import codegen, packaging
    from vertebra.build._config import BuildConfig, JsConfig
    from vertebra.build._orchestrator import BuildOrchestrator

__all__ = (
    "BuildConfig",
    "BuildOrchestrator",
    "JsConfig",
    "codegen",
    "packaging",
)

_EXPORTS: dict[str, str] = {
    "BuildConfig": "vertebra.build._config",
    "BuildOrchestrator": "vertebra.build._orchestrator",
    "JsConfig": "vertebra.build._config",
    "codegen": "vertebra.build.codegen",
    "packaging": "vertebra.build.packaging",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS)
