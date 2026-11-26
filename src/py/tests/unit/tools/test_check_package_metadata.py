"""Unit tests for the PyPI package metadata check."""

from pathlib import Path
from typing import Any

import pytest
from tools.scripts.check_package_metadata import REQUIRED_URLS, find_metadata_problems, main

_URLS = "".join(f'{key} = "https://example.com/{key.lower()}"\n' for key in REQUIRED_URLS)


def _write_package(
    package_dir: Path,
    *,
    readme_field: str = 'readme = "README.md"\n',
    readme_text: str | None = "# demo\n",
    urls: str = _URLS,
    classifiers: str = 'classifiers = ["Programming Language :: Rust"]\n',
) -> Path:
    """Write a synthetic package ``pyproject.toml`` and README under ``package_dir``."""
    package_dir.mkdir(parents=True, exist_ok=True)
    if readme_text is not None:
        (package_dir / "README.md").write_text(readme_text, encoding="utf-8")
    pyproject = package_dir / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "demo"\nversion = "0.1.0"\ndescription = "Demo package."\n'
        f"{readme_field}{classifiers}\n[project.urls]\n{urls}",
        encoding="utf-8",
    )
    return pyproject


def test_complete_metadata_has_no_problems(tmp_path: Path) -> None:
    """Verify a package with every required field reports no problems."""
    assert find_metadata_problems(_write_package(tmp_path)) == []


def test_readme_table_form_is_accepted(tmp_path: Path) -> None:
    """Verify the PEP 621 table form of ``readme`` resolves its ``file`` key."""
    pyproject = _write_package(
        tmp_path, readme_field='readme = { file = "README.md", content-type = "text/markdown" }\n'
    )
    assert find_metadata_problems(pyproject) == []


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"readme_field": ""}, "missing [project] readme file"),
        ({"readme_text": None}, "readme 'README.md' does not exist"),
        ({"readme_text": "  \n"}, "readme 'README.md' is empty"),
        ({"classifiers": ""}, "missing [project] classifiers"),
        ({"urls": _URLS.replace("Issues", "Bugs")}, "missing [project.urls] Issues"),
        ({"urls": _URLS.replace("https://example.com/source", "http://example.com/source")}, "Source is not an https"),
    ],
)
def test_incomplete_metadata_is_reported(tmp_path: Path, overrides: dict[str, Any], expected: str) -> None:
    """Verify each missing or invalid field produces a matching problem."""
    problems = find_metadata_problems(_write_package(tmp_path, **overrides))
    assert any(expected in problem for problem in problems), problems


def test_main_reports_failures_with_relative_paths(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify the CLI exits non-zero and prefixes problems with repo-relative paths."""
    _write_package(tmp_path)
    _write_package(tmp_path / "src" / "rs" / "vertebra-demo", readme_field="")
    assert main(["--repo-root", str(tmp_path)]) == 1
    assert "src/rs/vertebra-demo/pyproject.toml: missing [project] readme file" in capsys.readouterr().err
