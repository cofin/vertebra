"""Test configuration and fixtures for Vertebra."""

from __future__ import annotations

import os

# Set test environment before any other imports.
os.environ.update(
    {
        "VERTEBRA_DEBUG": "true",
        "VERTEBRA_DEV_MODE": "true",
        "VERTEBRA_LOG_LEVEL": "40",
    }
)
