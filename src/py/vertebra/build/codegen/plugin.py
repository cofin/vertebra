"""Codegen extension plugin for Vertebra and Litestar applications."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from vertebra.build.codegen._metadata import RouteMetadata
from vertebra.build.codegen.openapi import OpenAPIGenerator
from vertebra.build.codegen.procedures import (
    ProcedureClientGenerator,
    ProcedureRegistry,
    get_procedure_registry,
)
from vertebra.build.codegen.protobuf import ProtobufGenerator
from vertebra.build.codegen.valibot import ValibotGenerator
from vertebra.build.codegen.zod import ZodGenerator

__all__ = (
    "CODEGEN_STATE_KEY",
    "CodegenConfig",
    "CodegenPlugin",
    "VertebraCodegenPlugin",
)

CODEGEN_STATE_KEY: str = "codegen"


@dataclass(frozen=True, slots=True)
class CodegenConfig:
    """Configuration controlling multi-target schema and RPC client emission."""

    output_dir: Path = Path("src/generated")
    enable_valibot: bool = True
    enable_zod: bool = True
    enable_openapi: bool = True
    enable_protobuf: bool = False
    enable_procedures: bool = True
    openapi_title: str = "Vertebra API"
    openapi_version: str = "1.0.0"
    protobuf_package: str = "vertebra.v1"
    protobuf_service: str = "VertebraService"


@dataclass(slots=True)
class CodegenPlugin:
    """Plugin orchestrating Valibot, Zod, OpenAPI 3.1.0, Protobuf v3, and @procedure output."""

    config: CodegenConfig = field(default_factory=CodegenConfig)
    registry: ProcedureRegistry = field(default_factory=get_procedure_registry)

    def install_state(self, state_mapping: dict[str, Any]) -> CodegenConfig:
        """Register the CodegenConfig in application state."""
        state_mapping[CODEGEN_STATE_KEY] = self.config
        return self.config

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol hook registering codegen state."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            target_state.update(holder)
        return app_config

    def emit_all(self, routes: list[RouteMetadata], *, output_dir: Path | None = None) -> dict[str, Path]:
        """Write all enabled schema targets to output_dir and return a map of target to file path."""
        out = output_dir or self.config.output_dir
        out.mkdir(parents=True, exist_ok=True)
        written: dict[str, Path] = {}

        if self.config.enable_valibot:
            valibot_path = out / "schemas.valibot.ts"
            valibot_path.write_text(ValibotGenerator().generate(routes))
            written["valibot"] = valibot_path

        if self.config.enable_zod:
            zod_path = out / "schemas.zod.ts"
            zod_path.write_text(ZodGenerator().generate(routes))
            written["zod"] = zod_path

        if self.config.enable_openapi:
            openapi_path = out / "openapi.json"
            openapi_path.write_text(
                OpenAPIGenerator().generate_json(
                    routes,
                    title=self.config.openapi_title,
                    version=self.config.openapi_version,
                )
            )
            written["openapi"] = openapi_path

        if self.config.enable_protobuf:
            proto_path = out / "service.proto"
            proto_path.write_text(
                ProtobufGenerator(
                    package=self.config.protobuf_package,
                    service_name=self.config.protobuf_service,
                ).generate(routes)
            )
            written["protobuf"] = proto_path

        if self.config.enable_procedures:
            proc_path = out / "procedures.client.ts"
            proc_path.write_text(ProcedureClientGenerator(self.registry).generate_typescript_client())
            written["procedures"] = proc_path

        return written


VertebraCodegenPlugin = CodegenPlugin
