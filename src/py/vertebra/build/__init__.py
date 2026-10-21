"""Single-binary standalone and onefile packaging suite for Vertebra."""

from vertebra.build.onefile import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    OnefileBuilder,
)
from vertebra.build.standalone import StandaloneBuilder
from vertebra.build.targets import (
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
