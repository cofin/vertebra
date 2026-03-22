# Re-export the native bundler module so it's accessible as vertebra._bundler.
# The native extension is built separately via maturin from vertebra-bundler crate.
from _bundler import *  # noqa: F401, F403
from _bundler import BuildResult, BundleConfig, DevConfig, OutputFile, RolldownBundler

__all__ = ["RolldownBundler", "BundleConfig", "DevConfig", "BuildResult", "OutputFile"]
