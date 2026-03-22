"""Tests for JsConfig and BuildConfig JS section parsing."""

from vertebra.builder._config import BuildConfig, JsConfig


# ---------------------------------------------------------------------------
# JsConfigDefaults
# ---------------------------------------------------------------------------


def test_js_config_defaults_dependencies_default_empty() -> None:
    cfg = JsConfig()
    assert cfg.dependencies == ()


def test_js_config_defaults_dev_dependencies_default_empty() -> None:
    cfg = JsConfig()
    assert cfg.dev_dependencies == ()


# ---------------------------------------------------------------------------
# BuildConfigDefaults
# ---------------------------------------------------------------------------


def test_build_config_defaults_bundler_default_native() -> None:
    cfg = BuildConfig()
    assert cfg.bundler == "native"


def test_build_config_defaults_dev_port_default_none() -> None:
    cfg = BuildConfig()
    assert cfg.dev_port is None


def test_build_config_defaults_js_default_empty() -> None:
    cfg = BuildConfig()
    assert cfg.js.dependencies == ()
    assert cfg.js.dev_dependencies == ()


# ---------------------------------------------------------------------------
# BuildConfigLoad
# ---------------------------------------------------------------------------


def test_build_config_load_loads_js_section(tmp_path) -> None:  # type: ignore[no-untyped-def]
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


def test_build_config_load_no_js_section(tmp_path) -> None:  # type: ignore[no-untyped-def]
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


def test_build_config_load_no_vertebra_section(tmp_path) -> None:  # type: ignore[no-untyped-def]
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


def test_build_config_load_missing_file(tmp_path) -> None:  # type: ignore[no-untyped-def]
    cfg = BuildConfig.load(tmp_path / "nonexistent.toml")
    assert cfg.bundler == "native"
    assert cfg.js.dependencies == ()


def test_build_config_load_hyphenated_keys(tmp_path) -> None:  # type: ignore[no-untyped-def]
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


def test_build_config_load_dev_port_parsed(tmp_path) -> None:  # type: ignore[no-untyped-def]
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """\
[tool.vertebra]
dev-port = 3000
"""
    )
    cfg = BuildConfig.load(pyproject)
    assert cfg.dev_port == 3000
