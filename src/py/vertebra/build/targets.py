"""Re-export shim for vertebra.build.packaging.targets."""

from vertebra.build.packaging.targets import (
    KNOWN_TARGETS,
    PBS_BASE_URL,
    PBS_RELEASE_TAG,
    BuildTargetTuple,
    resolve_build_target,
)

__all__ = (
    "KNOWN_TARGETS",
    "PBS_BASE_URL",
    "PBS_RELEASE_TAG",
    "BuildTargetTuple",
    "resolve_build_target",
)
