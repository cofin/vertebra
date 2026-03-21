"""Tests for import map generation."""

import json

from vertebra.builder._config import JsConfig
from vertebra.builder._import_map import generate_import_map, parse_package_spec, write_import_map


class TestParsePackageSpec:
    def test_simple_package(self) -> None:
        assert parse_package_spec("react@^19") == ("react", "^19")

    def test_scoped_package(self) -> None:
        assert parse_package_spec("@tanstack/router@^1") == ("@tanstack/router", "^1")

    def test_no_version(self) -> None:
        assert parse_package_spec("lodash") == ("lodash", "")

    def test_scoped_no_version(self) -> None:
        assert parse_package_spec("@types/react") == ("@types/react", "")

    def test_exact_version(self) -> None:
        assert parse_package_spec("react@19.0.0") == ("react", "19.0.0")

    def test_tilde_version(self) -> None:
        assert parse_package_spec("react@~18.2") == ("react", "~18.2")


class TestGenerateImportMap:
    def test_basic(self) -> None:
        js = JsConfig(dependencies=("react@^19",))
        result = generate_import_map(js)
        assert result["imports"]["react"] == "npm:react@^19"

    def test_scoped_dependency(self) -> None:
        js = JsConfig(dependencies=("@tanstack/router@^1",))
        result = generate_import_map(js)
        assert result["imports"]["@tanstack/router"] == "npm:@tanstack/router@^1"

    def test_no_version_dependency(self) -> None:
        js = JsConfig(dependencies=("lodash",))
        result = generate_import_map(js)
        assert result["imports"]["lodash"] == "npm:lodash"

    def test_empty_config(self) -> None:
        js = JsConfig()
        result = generate_import_map(js)
        assert result == {"imports": {}}

    def test_dev_excluded_by_default(self) -> None:
        js = JsConfig(dependencies=("react@^19",), dev_dependencies=("typescript@^5",))
        result = generate_import_map(js)
        assert "react" in result["imports"]
        assert "typescript" not in result["imports"]

    def test_include_dev(self) -> None:
        js = JsConfig(dependencies=("react@^19",), dev_dependencies=("typescript@^5",))
        result = generate_import_map(js, include_dev=True)
        assert "react" in result["imports"]
        assert "typescript" in result["imports"]
        assert result["imports"]["typescript"] == "npm:typescript@^5"

    def test_multiple_dependencies(self) -> None:
        js = JsConfig(dependencies=("react@^19", "react-dom@^19", "@tanstack/router@^1"))
        result = generate_import_map(js)
        assert len(result["imports"]) == 3
        assert result["imports"]["react"] == "npm:react@^19"
        assert result["imports"]["react-dom"] == "npm:react-dom@^19"
        assert result["imports"]["@tanstack/router"] == "npm:@tanstack/router@^1"


class TestWriteImportMap:
    def test_writes_file(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        js = JsConfig(dependencies=("react@^19",))
        path = write_import_map(js, tmp_path)
        assert path.exists()
        assert path.name == "import_map.json"
        data = json.loads(path.read_text())
        assert "imports" in data
        assert data["imports"]["react"] == "npm:react@^19"

    def test_creates_directory(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        out = tmp_path / "nested" / "dir"
        js = JsConfig(dependencies=("lodash",))
        path = write_import_map(js, out)
        assert path.exists()
        assert out.exists()

    def test_include_dev_in_output(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        js = JsConfig(dependencies=("react@^19",), dev_dependencies=("typescript@^5",))
        path = write_import_map(js, tmp_path, include_dev=True)
        data = json.loads(path.read_text())
        assert "typescript" in data["imports"]

    def test_overwrites_existing(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        js1 = JsConfig(dependencies=("react@^18",))
        write_import_map(js1, tmp_path)

        js2 = JsConfig(dependencies=("react@^19",))
        path = write_import_map(js2, tmp_path)
        data = json.loads(path.read_text())
        assert data["imports"]["react"] == "npm:react@^19"
