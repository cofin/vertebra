"""Metadata for the Vertebra package."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, metadata, version

__all__ = ("__project__", "__version__")

try:
    __version__ = version("vertebra")
    _metadata = metadata("vertebra")
    __project__ = _metadata["Name"]
except PackageNotFoundError:
    __version__ = "0.0.1"
    __project__ = "Vertebra"
finally:
    del version, PackageNotFoundError, metadata
