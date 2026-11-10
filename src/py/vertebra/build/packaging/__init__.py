"""Standalone directory and single-file executable packaging for production Vertebra deployments.

Builds relocatable application bundles independent of the project's package manager: the
builders stage sources and lockfiles from the project tree as-is, so projects managed with
pip/venv, uv, pdm, poetry, or conda package the same way.
"""

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
    SUPPORTED_PYTHON_VERSIONS,
    BuildTarget,
)

__all__ = (
    "KNOWN_TARGETS",
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "PBS_BASE_URL",
    "PBS_RELEASE_TAG",
    "SUPPORTED_PYTHON_VERSIONS",
    "BuildTarget",
    "OnefileBuilder",
    "StandaloneBuilder",
)
