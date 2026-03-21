"""Tests for JsConfig and BuildConfig JS section parsing."""

from vertebra.builder._config import BuildConfig, JsConfig


class TestJsConfigDefaults:
    def test_dependencies_default_empty(self) -> None:
        cfg = JsConfig()
        assert cfg.dependencies == ()

    def test_dev_dependencies_default_empty(self) -> None:
        cfg = JsConfig()
        assert cfg.dev_dependencies == ()


class TestBuildConfigDefaults:
    def test_bundler_default_native(self) -> None:
        cfg = BuildConfig()
        assert cfg.bundler == "native"

    def test_dev_port_default_none(self) -> None:
        cfg = BuildConfig()
        assert cfg.dev_port is None

    def test_js_default_empty(self) -> None:
        cfg = BuildConfig()
        assert cfg.js.dependencies == ()
        assert cfg.js.dev_dependencies == ()


class TestBuildConfigLoad:
    def test_loads_js_section(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            """\
[tool.vertebra]
bundler = "vite"

[tool.vertebra.js]
dependencies = ["react@^19", "@tanstack/router@^1"]
dev-dependencies = ["typescript@^5"]
"""
        )
        cfg = BuildConfig.load(pyproject)
        assert cfg.bundler == "vite"
        assert cfg.js.dependencies == ("react@^19", "@tanstack/router@^1")
        assert cfg.js.dev_dependencies == ("typescript@^5",)

    def test_no_js_section(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            """\
[tool.vertebra]
bundler = "native"
"""
        )
        cfg = BuildConfig.load(pyproject)
        assert cfg.js.dependencies == ()
        assert cfg.js.dev_dependencies == ()

    def test_no_vertebra_section(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            """\
[project]
name = "myapp"
"""
        )
        cfg = BuildConfig.load(pyproject)
        assert cfg.bundler == "native"
        assert cfg.js.dependencies == ()

    def test_missing_file(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        cfg = BuildConfig.load(tmp_path / "nonexistent.toml")
        assert cfg.bundler == "native"
        assert cfg.js.dependencies == ()

    def test_hyphenated_keys(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            """\
[tool.vertebra.js]
dependencies = ["react@^19"]
dev-dependencies = ["vitest@^1", "@testing-library/react@^14"]
"""
        )
        cfg = BuildConfig.load(pyproject)
        assert cfg.js.dev_dependencies == ("vitest@^1", "@testing-library/react@^14")

    def test_dev_port_parsed(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            """\
[tool.vertebra]
dev-port = 3000
"""
        )
        cfg = BuildConfig.load(pyproject)
        assert cfg.dev_port == 3000
