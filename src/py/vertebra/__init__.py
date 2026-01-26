"""Vertebra - Python application backbone with Rust IPC primitives.

Core Exports:
    Vertebra - Application container for broker-managed plugins
    ProviderPlugin - Protocol for broker plugins

Procedures:
    procedure - Decorator for registering type-bridged handlers

Serialization:
    Serializer - Protocol for serialization backends
    MsgspecSerializer - msgspec-backed JSON serializer
    PydanticSerializer - Pydantic-backed JSON serializer
    detect_serializer - Auto-detect serializer from type hints
    get_serializer - Get a serializer instance by name
    register_serializer - Register a custom serializer

For IPC and typing, see:
    vertebra.typing - Type aliases and dependency flags
    vertebra.ipc - IPC channel wrappers
    vertebra.serializers - JSON/MessagePack codecs
    vertebra.exceptions - Exception hierarchy
"""

from vertebra.procedures import (
    MsgspecSerializer,
    PydanticSerializer,
    Serializer,
    detect_serializer,
    get_serializer,
    procedure,
    register_serializer,
)

__all__ = (
    "App",
    "MsgspecSerializer",
    "ProviderPlugin",
    "PydanticSerializer",
    # Serialization
    "Serializer",
    # State
    "ShadowState",
    # Core
    "Vertebra",
    "Vertex",
    # Version
    "__version__",
    "detect_serializer",
    "get_serializer",
    # Procedures
    "procedure",
    "register_serializer",
)

__version__ = "0.1.0"


def __getattr__(name: str) -> object:
    if name in {"App", "Vertex", "ProviderPlugin", "Vertebra"}:
        from vertebra.core.entrypoint import ProviderPlugin, Vertebra
        from vertebra.core.lifecycle import App, Vertex

        return {"App": App, "Vertex": Vertex, "ProviderPlugin": ProviderPlugin, "Vertebra": Vertebra}[name]
    if name == "ShadowState":
        from vertebra.state.shadow import ShadowState

        return ShadowState
    msg = f"module 'vertebra' has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> "list[str]":
    return sorted(set(__all__))
