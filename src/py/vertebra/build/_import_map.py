"""Import map generation and wheel-native @vertebra/client synchronization."""

import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from vertebra.build._config import JsConfig
from vertebra.utils.serializers import decode_json, format_json

__all__ = ("ImportMapGenerator", "load_client_subpaths")

_FALLBACK_CLIENT_SUBPATHS: tuple[tuple[str, str, str], ...] = (
    (".", "./index.js", "./index.d.ts"),
    ("./vite", "./vite/index.js", "./vite/index.d.ts"),
    ("./react", "./react/index.js", "./react/index.d.ts"),
    ("./vue", "./vue/index.js", "./vue/index.d.ts"),
    ("./svelte", "./svelte/index.js", "./svelte/index.d.ts"),
    ("./angular", "./angular/index.js", "./angular/index.d.ts"),
    ("./htmx", "./htmx/index.js", "./htmx/index.d.ts"),
    ("./store", "./store/index.js", "./store/index.d.ts"),
    ("./hmr", "./hmr/index.js", "./hmr/index.d.ts"),
    ("./wasm", "./wasm.js", "./wasm.d.ts"),
    ("./arrow", "./arrow.js", "./arrow.d.ts"),
    ("./live", "./live/index.js", "./live/index.d.ts"),
    ("./apex", "./apex/index.js", "./apex/index.d.ts"),
    ("./telemetry", "./telemetry.js", "./telemetry.d.ts"),
    ("./router", "./router.js", "./router.d.ts"),
    ("./webmcp", "./webmcp.js", "./webmcp.d.ts"),
    ("./agent", "./agent/index.js", "./agent/index.d.ts"),
)


def load_client_subpaths(package_json_path: Path | None = None) -> tuple[tuple[str, str, str], ...]:
    """Derive ``(subpath, js_rel, types_rel)`` tuples from ``@vertebra/client`` ``package.json`` ``exports``."""
    candidates: tuple[Path, ...]
    if package_json_path is not None:
        candidates = (package_json_path,)
    else:
        candidates = (
            Path(__file__).resolve().parents[3] / "ts" / "vertebra-client" / "package.json",
            Path(__file__).resolve().parents[1] / "server" / "_js" / "bundle" / "package.json",
        )
    pkg_file = next((p for p in candidates if p.is_file()), None)
    if pkg_file is None:
        return _FALLBACK_CLIENT_SUBPATHS

    raw = decode_json(pkg_file.read_text(encoding="utf-8"))
    exports_obj = raw.get("exports") if isinstance(raw, dict) else None
    if not isinstance(exports_obj, dict):
        return _FALLBACK_CLIENT_SUBPATHS

    entries: list[tuple[str, str, str]] = []
    for subpath, target in exports_obj.items():
        if not isinstance(target, dict):
            continue
        import_path = str(target.get("import", ""))
        types_path = str(target.get("types", ""))
        if not import_path or not types_path:
            continue
        norm_import = "./" + import_path.removeprefix("./").removeprefix("dist/")
        norm_types = "./" + types_path.removeprefix("./").removeprefix("dist/")
        entries.append((str(subpath), norm_import, norm_types))

    return tuple(entries) if entries else _FALLBACK_CLIENT_SUBPATHS


_CLIENT_SUBPATHS: tuple[tuple[str, str, str], ...] = load_client_subpaths()


class ImportMapGenerator:
    """Generate V8-compatible JavaScript import maps and extract the wheel-bundled @vertebra/client SDK."""

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
        include_client: bool = False,
    ) -> dict[str, dict[str, str]]:
        """Generate a V8-compatible import map from *js_config* or package specifiers."""
        imports: dict[str, str] = {}

        if include_client:
            imports["@vertebra/client"] = "/__vertebra/client/index.js"
            imports["@vertebra/client/"] = "/__vertebra/client/"

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
        include_client: bool = False,
    ) -> Path:
        """Generate and write ``import_map.json`` to *output_dir*."""
        import_map = ImportMapGenerator.generate(
            js_config,
            include_dev=include_dev,
            include_client=include_client,
        )

        if output_dir.suffix == ".json":
            output_dir.parent.mkdir(parents=True, exist_ok=True)
            output_path = output_dir
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "import_map.json"
        formatted = format_json(import_map, indent=2)
        output_path.write_text(formatted + "\n", encoding="utf-8")
        return output_path

    @staticmethod
    def sync_wheel_client(output_dir: Path) -> Path | None:
        """Copy the wheel-packaged ``@vertebra/client`` ESM bundle and ``.d.ts`` declarations into *output_dir*."""
        bundle_dir = Path(__file__).resolve().parents[1] / "server" / "_js" / "bundle"
        ts_src_dir = Path(__file__).resolve().parents[3] / "ts" / "vertebra-client" / "src"
        if not bundle_dir.is_dir() and not ts_src_dir.is_dir():
            return None

        target_dir = output_dir / "client"
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        if bundle_dir.is_dir():
            shutil.copytree(bundle_dir, target_dir)
        else:
            target_dir.mkdir(parents=True, exist_ok=True)
            for _subpath, js_rel, types_rel in _CLIENT_SUBPATHS:
                rel_clean = js_rel.removeprefix("./")
                stem = rel_clean.removesuffix(".js")
                src_candidates = (
                    ts_src_dir / f"{stem}.ts",
                    ts_src_dir / f"{stem}.tsx",
                    ts_src_dir / stem / "index.ts",
                    ts_src_dir / stem / "index.tsx",
                )
                source_text = next(
                    (c.read_text(encoding="utf-8") for c in src_candidates if c.is_file()), "export {};\n"
                )
                js_target = target_dir / rel_clean
                dts_target = target_dir / types_rel.removeprefix("./")
                js_target.parent.mkdir(parents=True, exist_ok=True)
                dts_target.parent.mkdir(parents=True, exist_ok=True)
                js_target.write_text(source_text, encoding="utf-8")
                dts_target.write_text(source_text, encoding="utf-8")

        exports_map: dict[str, dict[str, str]] = {
            subpath: {"types": types_rel, "import": js_rel} for subpath, js_rel, types_rel in _CLIENT_SUBPATHS
        }
        pkg_manifest: dict[str, Any] = {
            "name": "@vertebra/client",
            "version": "0.1.0",
            "private": True,
            "description": "Wheel-packaged TypeScript client SDK for Vertebra",
            "type": "module",
            "main": "./index.js",
            "module": "./index.js",
            "types": "./index.d.ts",
            "exports": exports_map,
        }
        (target_dir / "package.json").write_text(format_json(pkg_manifest, indent=2) + "\n", encoding="utf-8")
        return target_dir
