"""Shared error types for optional dependencies."""


class MissingDependencyError(ImportError):
    """Raised when an optional dependency is required but missing."""

    def __init__(self, package: str, install_package: str | None = None) -> None:
        self.package = package
        self.install_package = install_package
        super().__init__(self.__str__())

    def __str__(self) -> str:
        install = self.install_package or self.package
        return (
            f"Missing dependency '{self.package}'. "
            f"Install with 'pip install {install}' to enable this feature."
        )


__all__ = ("MissingDependencyError",)
