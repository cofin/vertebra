"""Standalone and onefile packaging subpackage for Vertebra."""

from vertebra.build.packaging.onefile import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    OnefileBuilder,
)
from vertebra.build.packaging.standalone import StandaloneBuilder
from vertebra.build.packaging.targets import (
    KNOWN_TARGETS,
    PBS_BASE_URL,
    PBS_RELEASE_TAG,
    BuildTargetTuple,
    resolve_build_target,
)

__all__ = (
    "KNOWN_TARGETS",
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "PBS_BASE_URL",
    "PBS_RELEASE_TAG",
    "BuildTargetTuple",
    "OnefileBuilder",
    "StandaloneBuilder",
    "resolve_build_target",
)
