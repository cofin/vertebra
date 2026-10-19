"""Re-export the native bundler extension as ``vertebra._bundler``."""

from _bundler import BuildResult, BundleConfig, DevConfig, OutputFile
from _bundler import RolldownBundler as Bundler

__all__ = (
    "BuildResult",
    "BundleConfig",
    "Bundler",
    "DevConfig",
    "OutputFile",
    "RolldownBundler",
)

RolldownBundler = Bundler
