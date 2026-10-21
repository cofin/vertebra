"""Import map generation from JavaScript dependency configuration."""

from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.utils.serializers._json import encode_json

if TYPE_CHECKING:
    from pathlib import Path

    from vertebra.build._config import JsConfig

__all__ = ("generate_import_map", "parse_package_spec", "write_import_map")


def parse_package_spec(spec: str) -> tuple[str, str]:
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


def generate_import_map(js_config: "JsConfig", *, include_dev: bool = False) -> dict[str, Any]:
    """Generate a V8-compatible import map from *js_config*."""
    imports: dict[str, str] = {}

    all_specs = list(js_config.dependencies)
    if include_dev:
        all_specs.extend(js_config.dev_dependencies)

    for spec in all_specs:
        name, version = parse_package_spec(spec)
        if version:
            imports[name] = f"npm:{name}@{version}"
        else:
            imports[name] = f"npm:{name}"

    return {"imports": imports}


def write_import_map(js_config: "JsConfig", output_dir: "Path", *, include_dev: bool = False) -> "Path":
    """Generate and write ``import_map.json`` to *output_dir*."""
    import_map = generate_import_map(js_config, include_dev=include_dev)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "import_map.json"
    formatted = msgspec.json.format(encode_json(import_map, as_bytes=True), indent=2).decode("utf-8")
    output_path.write_text(formatted + "\n")
    return output_path
