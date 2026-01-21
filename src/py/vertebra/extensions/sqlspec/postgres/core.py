"""PostgreSQL broker adapter core helpers."""

from collections.abc import Mapping
from typing import Any

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig

from vertebra._internal.typing import MSGSPEC_INSTALLED
from vertebra.extensions.sqlspec._core import build_broker_profile
from vertebra.extensions.sqlspec._core import build_statement_config as _build_statement_config

__all__ = (
    "apply_driver_features",
    "build_profile",
    "build_statement_config",
    "default_statement_config",
    "driver_profile",
    "register_json_codecs",
)


def build_profile() -> DriverParameterProfile:
    """Create the broker-backed PostgreSQL parameter profile."""
    return build_broker_profile("VertebraPostgres", default_style=ParameterStyle.NUMERIC, dialect="postgres")


driver_profile = build_profile()


def build_statement_config() -> StatementConfig:
    """Construct the PostgreSQL statement configuration."""
    return _build_statement_config(driver_profile, "postgres")


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: StatementConfig, driver_features: Mapping[str, object] | None
) -> tuple[StatementConfig, dict[str, object]]:
    """Normalize driver features and return the updated config + features."""
    return statement_config, dict(driver_features or {})


async def register_json_codecs(connection: Any, encoder: Any = None, decoder: Any = None) -> None:
    """Register JSON codecs on an asyncpg connection."""
    if not MSGSPEC_INSTALLED:
        return

    import msgspec

    def _default_encoder(obj: Any) -> str:
        return msgspec.json.encode(obj).decode("utf-8")

    def _default_decoder(data: str) -> Any:
        return msgspec.json.decode(data.encode("utf-8"))

    await connection.set_type_codec(
        "json",
        encoder=encoder or _default_encoder,
        decoder=decoder or _default_decoder,
        schema="pg_catalog",
        format="text",
    )
    await connection.set_type_codec(
        "jsonb",
        encoder=encoder or _default_encoder,
        decoder=decoder or _default_decoder,
        schema="pg_catalog",
        format="text",
    )
