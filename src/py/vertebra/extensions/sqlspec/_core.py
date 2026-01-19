"""Broker-backed SQLSpec parameter profiles."""

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile


def build_broker_profile(name: str, default_style: ParameterStyle, dialect: str) -> "DriverParameterProfile":
    return DriverParameterProfile(
        name=name,
        default_style=default_style,
        supported_styles={default_style},
        default_execution_style=default_style,
        supported_execution_styles={default_style},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        default_dialect=dialect,
    )


def build_statement_config(profile: "DriverParameterProfile", dialect: str) -> "StatementConfig":
    return build_statement_config_from_profile(profile, statement_overrides={"dialect": dialect})


__all__ = ("build_broker_profile", "build_statement_config")
