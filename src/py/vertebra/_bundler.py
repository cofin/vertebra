"""Re-export the native bundler extension as ``vertebra._bundler``."""

import _bundler as _native_bundler
from _bundler import BuildResult, BundleConfig, Bundler, DevConfig, OutputFile

GIL_USED: bool = getattr(_native_bundler, "GIL_USED", False)

__all__ = (
    "BuildResult",
    "BundleConfig",
    "Bundler",
    "DevConfig",
    "GIL_USED",
    "OutputFile",
)
