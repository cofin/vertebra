"""Tests for import map generation."""

import importlib.metadata
from pathlib import Path

import pytest

from vertebra import __version__ as vertebra_version
from vertebra.build import JsConfig
from vertebra.build._import_map import (
    _CLIENT_SUBPATHS,
    ImportMapGenerator,
    _resolve_client_version,
    load_client_subpaths,
)
from vertebra.utils.serializers import decode_json


def test_parse_package_spec_simple_package() -> None:
    assert ImportMapGenerator.parse_spec("react@^19") == ("react", "^19")


def test_parse_package_spec_scoped_package() -> None:
    assert ImportMapGenerator.parse_spec("@tanstack/router@^1") == ("@tanstack/router", "^1")


def test_parse_package_spec_no_version() -> None:
    assert ImportMapGenerator.parse_spec("lodash") == ("lodash", "")


def test_parse_package_spec_scoped_no_version() -> None:
    assert ImportMapGenerator.parse_spec("@types/react") == ("@types/react", "")


def test_parse_package_spec_exact_version() -> None:
    assert ImportMapGenerator.parse_spec("react@19.0.0") == ("react", "19.0.0")


def test_parse_package_spec_tilde_version() -> None:
    assert ImportMapGenerator.parse_spec("react@~18.2") == ("react", "~18.2")


def test_generate_import_map_basic() -> None:
    js = JsConfig(dependencies=("react@^19",))
    result = ImportMapGenerator.generate(js)
    assert result["imports"]["react"] == "npm:react@^19"


def test_generate_import_map_scoped_dependency() -> None:
    js = JsConfig(dependencies=("@tanstack/router@^1",))
    result = ImportMapGenerator.generate(js)
    assert result["imports"]["@tanstack/router"] == "npm:@tanstack/router@^1"


def test_generate_import_map_no_version_dependency() -> None:
    js = JsConfig(dependencies=("lodash",))
    result = ImportMapGenerator.generate(js)
    assert result["imports"]["lodash"] == "npm:lodash"


def test_generate_import_map_empty_config() -> None:
    js = JsConfig()
    result = ImportMapGenerator.generate(js)
    assert result == {"imports": {}}


def test_generate_import_map_dev_excluded_by_default() -> None:
    js = JsConfig(dependencies=("react@^19",), dev_dependencies=("typescript@^5",))
    result = ImportMapGenerator.generate(js)
    assert "react" in result["imports"]
    assert "typescript" not in result["imports"]


def test_generate_import_map_include_dev() -> None:
    js = JsConfig(dependencies=("react@^19",), dev_dependencies=("typescript@^5",))
    result = ImportMapGenerator.generate(js, include_dev=True)
    assert "react" in result["imports"]
    assert "typescript" in result["imports"]
    assert result["imports"]["typescript"] == "npm:typescript@^5"


def test_generate_import_map_multiple_dependencies() -> None:
    js = JsConfig(dependencies=("react@^19", "react-dom@^19", "@tanstack/router@^1"))
    result = ImportMapGenerator.generate(js)
    assert len(result["imports"]) == 3
    assert result["imports"]["react"] == "npm:react@^19"
    assert result["imports"]["react-dom"] == "npm:react-dom@^19"
    assert result["imports"]["@tanstack/router"] == "npm:@tanstack/router@^1"


def test_write_import_map_writes_file(tmp_path: Path) -> None:
    js = JsConfig(dependencies=("react@^19",))
    path = ImportMapGenerator.write(js, tmp_path)
    assert path.exists()
    assert path.name == "import_map.json"
    data = decode_json(path.read_text())
    assert "imports" in data
    assert data["imports"]["react"] == "npm:react@^19"


def test_write_import_map_creates_directory(tmp_path: Path) -> None:
    out = tmp_path / "nested" / "dir"
    js = JsConfig(dependencies=("lodash",))
    path = ImportMapGenerator.write(js, out)
    assert path.exists()
    assert out.exists()


def test_write_import_map_include_dev_in_output(tmp_path: Path) -> None:
    js = JsConfig(dependencies=("react@^19",), dev_dependencies=("typescript@^5",))
    path = ImportMapGenerator.write(js, tmp_path, include_dev=True)
    data = decode_json(path.read_text())
    assert "typescript" in data["imports"]


def test_write_import_map_overwrites_existing(tmp_path: Path) -> None:
    js1 = JsConfig(dependencies=("react@^18",))
    ImportMapGenerator.write(js1, tmp_path)

    js2 = JsConfig(dependencies=("react@^19",))
    path = ImportMapGenerator.write(js2, tmp_path)
    data = decode_json(path.read_text())
    assert data["imports"]["react"] == "npm:react@^19"


def test_generate_import_map_include_client_and_sync_wheel_bundle(tmp_path: Path) -> None:
    """ImportMapGenerator maps @vertebra/client and extracts the wheel-packaged bundle."""
    js = JsConfig()
    mapped = ImportMapGenerator.generate(js, include_client=True)
    assert mapped["imports"]["@vertebra/client"] == "/__vertebra/client/index.js"
    assert mapped["imports"]["@vertebra/client/"] == "/__vertebra/client/"

    ts_pkg = Path(__file__).resolve().parents[4] / "ts" / "vertebra-client" / "package.json"
    expected_version = decode_json(ts_pkg.read_text(encoding="utf-8"))["version"]

    client_dir = ImportMapGenerator.sync_wheel_client(tmp_path)
    assert client_dir is not None
    assert (client_dir / "index.js").is_file()
    assert (client_dir / "index.d.ts").is_file()
    pkg = decode_json((client_dir / "package.json").read_text(encoding="utf-8"))
    assert pkg["name"] == "@vertebra/client"
    assert pkg["version"] == expected_version
    assert pkg["version"] != "0.1.0"
    assert "./react" in pkg["exports"]


def test_resolve_client_version_fallback_when_package_json_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_resolve_client_version falls back to installed distribution metadata or vertebra.__version__."""
    missing = (tmp_path / "missing_a" / "package.json", tmp_path / "missing_b" / "package.json")
    monkeypatch.setattr("vertebra.build._import_map._client_package_json_candidates", lambda: missing)
    monkeypatch.setattr("importlib.metadata.version", lambda _dist: "9.8.7")
    assert _resolve_client_version() == "9.8.7"

    def _raise_not_found(_dist: str) -> str:
        raise importlib.metadata.PackageNotFoundError(_dist)

    monkeypatch.setattr("importlib.metadata.version", _raise_not_found)
    assert _resolve_client_version() == vertebra_version


def test_client_subpaths_derived_dynamically_from_package_json(tmp_path: Path) -> None:
    """ImportMapGenerator.load_client_subpaths derives subpaths directly from package.json exports."""
    ts_pkg = Path(__file__).resolve().parents[4] / "ts" / "vertebra-client" / "package.json"
    raw_exports = decode_json(ts_pkg.read_text(encoding="utf-8"))["exports"]
    assert [sub for sub, _, _ in _CLIENT_SUBPATHS] == list(raw_exports.keys())

    custom_pkg = tmp_path / "package.json"
    custom_pkg.write_text(
        '{"name":"@vertebra/client","exports":{".":{"types":"./dist/index.d.ts","import":"./dist/index.js"},"./custom":{"types":"./dist/custom/index.d.ts","import":"./dist/custom/index.js"}}}',
        encoding="utf-8",
    )
    loaded = load_client_subpaths(custom_pkg)
    assert loaded == (
        (".", "./index.js", "./index.d.ts"),
        ("./custom", "./custom/index.js", "./custom/index.d.ts"),
    )
