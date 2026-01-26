"""Vertebra exception hierarchy.

Base Exceptions:
    VertebraError - Base for all Vertebra exceptions
    ConfigurationError - Configuration/setup issues

Dependency Exceptions:
    MissingDependencyError - Optional dependency not installed

IPC/Communication Exceptions:
    IPCError - Base for IPC-related errors
    ChannelError - Channel operation failures
    SerializationError - Serialization/deserialization failures
    BrokerError - Broker communication errors

Resource Exceptions:
    ResourceError - Base for resource management errors
    PoolError - Connection pool errors
    StorageError - Storage/filesystem errors

Type Generation Exceptions:
    TypeGenerationError - TypeScript/type generation errors
"""


class VertebraError(Exception):
    """Base exception for all Vertebra errors."""


class ConfigurationError(VertebraError):
    """Raised for configuration or setup issues."""


class MissingDependencyError(VertebraError, ImportError):
    """Raised when an optional dependency is required but missing."""

    def __init__(self, package: str, install_package: str | None = None) -> None:
        self.package = package
        self.install_package = install_package
        super().__init__(self.__str__())

    def __str__(self) -> str:
        install = self.install_package or self.package
        return f"Missing dependency '{self.package}'. Install with 'pip install {install}' to enable this feature."


# ============================================================================
# IPC Exceptions
# ============================================================================


def _format_ipc_context(message: str, procedure_id: str | None, stage: str | None) -> str:
    """Format error message with optional IPC context."""
    context: list[str] = []
    if stage:
        context.append(f"stage={stage}")
    if procedure_id:
        context.append(f"procedure_id={procedure_id}")
    if not context:
        return message
    return f"{message} ({', '.join(context)})"


class IPCError(VertebraError):
    """Base exception for IPC-related errors."""


class ChannelError(IPCError):
    """Raised when channel operations fail."""

    def __init__(self, message: str, *, procedure_id: str | None = None, stage: str | None = None) -> None:
        super().__init__(_format_ipc_context(message, procedure_id, stage))
        self.procedure_id = procedure_id
        self.stage = stage


class SerializationError(IPCError):
    """Raised when serialization or deserialization fails."""

    def __init__(self, message: str, *, procedure_id: str | None = None, stage: str | None = None) -> None:
        super().__init__(_format_ipc_context(message, procedure_id, stage))
        self.procedure_id = procedure_id
        self.stage = stage


class BrokerError(IPCError):
    """Raised for broker communication errors."""


# ============================================================================
# Resource Exceptions
# ============================================================================


class ResourceError(VertebraError):
    """Base exception for resource management errors."""


class PoolError(ResourceError):
    """Raised when pool operations fail."""


class StorageError(ResourceError):
    """Raised for storage/filesystem errors."""


# ============================================================================
# Type Generation Exceptions
# ============================================================================


class TypeGenerationError(VertebraError):
    """Raised when TypeScript/type generation fails."""


__all__ = (
    "BrokerError",
    "ChannelError",
    "ConfigurationError",
    # IPC
    "IPCError",
    # Dependencies
    "MissingDependencyError",
    "PoolError",
    # Resources
    "ResourceError",
    "SerializationError",
    "StorageError",
    # Type generation
    "TypeGenerationError",
    # Base
    "VertebraError",
)
