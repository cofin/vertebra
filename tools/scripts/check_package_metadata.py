"""Verify every published package declares the metadata PyPI renders on its project page."""

import argparse
import sys
import tomllib
from collections.abc import Sequence
from pathlib import Path

REQUIRED_URLS: tuple[str, ...] = ("Homepage", "Documentation", "Source", "Issues", "Changelog")


def iter_package_pyprojects(repo_root: Path) -> list[Path]:
    """Return the root ``pyproject.toml`` and every native extension ``pyproject.toml``.

    Args:
        repo_root: Repository root containing ``pyproject.toml`` and ``src/rs``.

    Returns:
        Sorted list of package manifest paths, root first.
    """
    return [repo_root / "pyproject.toml", *sorted((repo_root / "src" / "rs").glob("vertebra-*/pyproject.toml"))]


def find_metadata_problems(pyproject_path: Path) -> list[str]:
    """Return human-readable problems with one package's PyPI-facing metadata.

    Args:
        pyproject_path: Path to a package ``pyproject.toml``.

    Returns:
        Problem descriptions, empty when the package metadata is complete.
    """
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8")).get("project", {})
    problems: list[str] = []
    if not str(project.get("description", "")).strip():
        problems.append("missing [project] description")
    if not project.get("classifiers"):
        problems.append("missing [project] classifiers")

    readme = project.get("readme")
    readme_file = readme.get("file") if isinstance(readme, dict) else readme
    if not isinstance(readme_file, str) or not readme_file:
        problems.append("missing [project] readme file")
    else:
        readme_path = pyproject_path.parent / readme_file
        if not readme_path.is_file():
            problems.append(f"readme {readme_file!r} does not exist")
        elif not readme_path.read_text(encoding="utf-8").strip():
            problems.append(f"readme {readme_file!r} is empty")

    urls = project.get("urls", {})
    problems.extend(f"missing [project.urls] {key}" for key in REQUIRED_URLS if key not in urls)
    problems.extend(
        f"[project.urls] {key} is not an https URL" for key, url in urls.items() if not str(url).startswith("https://")
    )
    return problems


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint verifying README and project URL metadata for every published package."""
    parser = argparse.ArgumentParser(description="Verify PyPI-facing metadata for every published package.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Repository root containing pyproject.toml and src/rs",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    repo_root = args.repo_root.resolve()
    failures = 0
    for pyproject_path in iter_package_pyprojects(repo_root):
        for problem in find_metadata_problems(pyproject_path):
            sys.stderr.write(f"{pyproject_path.relative_to(repo_root)}: {problem}\n")
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
