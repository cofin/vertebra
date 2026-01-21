"""Vertebra core package."""

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
    "MsgspecSerializer",
    "ProviderPlugin",
    "PydanticSerializer",
    "Serializer",
    "Vertebra",
    "__version__",
    "detect_serializer",
    "get_serializer",
    "procedure",
    "register_serializer",
)

__version__ = "0.1.0"


def __getattr__(name: str) -> object:
    if name in {"ProviderPlugin", "Vertebra"}:
        from vertebra.main import ProviderPlugin, Vertebra

        return {"ProviderPlugin": ProviderPlugin, "Vertebra": Vertebra}[name]
    msg = f"module 'vertebra' has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> "list[str]":
    return sorted(set(__all__))
