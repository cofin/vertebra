"""Import map generation from JavaScript dependency configuration."""

from collections.abc import Sequence
from pathlib import Path

import msgspec

from vertebra.build._config import JsConfig
from vertebra.utils.serializers import encode_json_bytes

__all__ = ("ImportMapGenerator",)


class ImportMapGenerator:
    """Generate and persist V8-compatible JavaScript import maps."""

    @staticmethod
    def parse_spec(spec: str) -> tuple[str, str]:
        """Parse a package specifier into ``(name, version)``."""
        if spec.startswith("@"):
            rest = spec[1:]
            after_slash = rest.split("/", 1)[-1]
            if "@" in after_slash:
                idx = spec.rindex("@")
                return spec[:idx], spec[idx + 1 :]
            return spec, ""

        name, _, version = spec.partition("@")
        return name, version

    @staticmethod
    def generate(
        js_config: JsConfig | Sequence[str],
        *,
        include_dev: bool = False,
    ) -> dict[str, dict[str, str]]:
        """Generate a V8-compatible import map from *js_config* or package specifiers."""
        imports: dict[str, str] = {}

        if isinstance(js_config, JsConfig):
            all_specs = list(js_config.dependencies)
            if include_dev:
                all_specs.extend(js_config.dev_dependencies)
        else:
            all_specs = list(js_config)

        for spec in all_specs:
            name, version = ImportMapGenerator.parse_spec(spec)
            if version:
                imports[name] = f"npm:{name}@{version}"
            else:
                imports[name] = f"npm:{name}"

        return {"imports": imports}

    @staticmethod
    def write(
        js_config: JsConfig | Sequence[str],
        output_dir: Path,
        *,
        include_dev: bool = False,
    ) -> Path:
        """Generate and write ``import_map.json`` to *output_dir*."""
        import_map = ImportMapGenerator.generate(js_config, include_dev=include_dev)

        if output_dir.suffix == ".json":
            output_dir.parent.mkdir(parents=True, exist_ok=True)
            output_path = output_dir
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "import_map.json"
        formatted = msgspec.json.format(encode_json_bytes(import_map), indent=2).decode("utf-8")
        output_path.write_text(formatted + "\n")
        return output_path
