"""Tests for import map generation."""

from pathlib import Path

from vertebra.build import ImportMapGenerator, JsConfig
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
