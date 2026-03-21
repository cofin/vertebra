"""Import map generation from JavaScript dependency configuration.

Generates V8-compatible import maps from :class:`JsConfig` declarations
in ``[tool.vertebra.js]``.
"""

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from vertebra.builder._config import JsConfig


# ---------------------------------------------------------------------------
# Package spec parser
# ---------------------------------------------------------------------------


def parse_package_spec(spec: str) -> tuple[str, str]:
    """Parse a package specifier into ``(name, version)``.

    Handles both plain and scoped packages::

        "react@^18"              -> ("react", "^18")
        "lodash"                 -> ("lodash", "")
        "@tanstack/router@^1"   -> ("@tanstack/router", "^1")
        "@types/react"           -> ("@types/react", "")

    Returns:
        A ``(name, version)`` tuple.  *version* is an empty string when
        no version constraint is present.
    """
    if spec.startswith("@"):
        # Scoped package -- the first ``@`` is part of the scope, so we
        # need to find the ``@`` that separates name from version (if any).
        rest = spec[1:]  # strip leading @
        after_slash = rest.split("/", 1)[-1]
        if "@" in after_slash:
            idx = spec.rindex("@")
            return spec[:idx], spec[idx + 1 :]
        return spec, ""

    # Unscoped: name@version or just name
    name, _, version = spec.partition("@")
    return name, version


# ---------------------------------------------------------------------------
# Import map generation
# ---------------------------------------------------------------------------


def generate_import_map(js_config: "JsConfig", *, include_dev: bool = False) -> dict[str, Any]:
    """Generate a V8-compatible import map from *js_config*.

    Each package spec like ``"react@^19"`` becomes::

        "react" -> "npm:react@^19"

    When *include_dev* is ``True``, development-only packages are also
    included in the output.

    Args:
        js_config: JavaScript dependency configuration.
        include_dev: Whether to include dev dependencies.

    Returns:
        An import map dict with an ``"imports"`` key.
    """
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
    """Generate and write ``import_map.json`` to *output_dir*.

    Creates the directory if it does not exist.

    Args:
        js_config: JavaScript dependency configuration.
        output_dir: Directory to write the import map into.
        include_dev: Whether to include dev dependencies.

    Returns:
        The path to the written file.
    """
    import_map = generate_import_map(js_config, include_dev=include_dev)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "import_map.json"
    output_path.write_text(json.dumps(import_map, indent=2) + "\n")
    return output_path


__all__ = ("generate_import_map", "parse_package_spec", "write_import_map")
