"""Vertebra - Python application backbone with Rust IPC primitives.

Core Exports:
    Vertebra - Application container for broker-managed plugins
    ProviderPlugin - Protocol for broker plugins
    Vertex - Native application class
    App - Alias for Vertex

Procedures:
    procedure - Decorator for registering type-bridged handlers

Serialization:
    Serializer - Protocol for serialization backends
    MsgspecSerializer - msgspec-backed JSON serializer
    PydanticSerializer - Pydantic-backed JSON serializer
    detect_serializer - Auto-detect serializer from type hints
    get_serializer - Get a serializer instance by name
    register_serializer - Register a custom serializer

State:
    ShadowState - Shadow state for tracking remote state

For IPC and typing, see:
    vertebra.typing - Type aliases and dependency flags
    vertebra.messaging.transport - IPC channel wrappers
    vertebra.common.serializers - JSON/MessagePack codecs
    vertebra.common.exceptions - Exception hierarchy
"""

from vertebra.core.entrypoint import ProviderPlugin, Vertebra
from vertebra.core.lifecycle import App, Vertex
from vertebra.core.procedures import (
    MsgspecSerializer,
    PydanticSerializer,
    Serializer,
    detect_serializer,
    get_serializer,
    procedure,
    register_serializer,
)
from vertebra.state.shadow import ShadowState

__all__ = (
    "App",
    "MsgspecSerializer",
    "ProviderPlugin",
    "PydanticSerializer",
    "Serializer",
    "ShadowState",
    "Vertebra",
    "Vertex",
    "__version__",
    "detect_serializer",
    "get_serializer",
    "procedure",
    "register_serializer",
)

__version__ = "0.1.0"
