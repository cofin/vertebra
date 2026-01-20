"""SQLite broker adapter core helpers."""

from collections.abc import Mapping

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig

from vertebra.extensions.sqlspec._core import build_broker_profile
from vertebra.extensions.sqlspec._core import build_statement_config as _build_statement_config

__all__ = (
    "apply_driver_features",
    "build_profile",
    "build_statement_config",
    "default_statement_config",
    "driver_profile",
)


def build_profile() -> DriverParameterProfile:
    """Create the broker-backed SQLite parameter profile."""
    return build_broker_profile("VertebraSqlite", default_style=ParameterStyle.QMARK, dialect="sqlite")


driver_profile = build_profile()


def build_statement_config() -> StatementConfig:
    """Construct the SQLite statement configuration."""
    return _build_statement_config(driver_profile, "sqlite")


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: StatementConfig, driver_features: Mapping[str, object] | None
) -> tuple[StatementConfig, dict[str, object]]:
    """Normalize driver features and return the updated config + features."""
    return statement_config, dict(driver_features or {})
