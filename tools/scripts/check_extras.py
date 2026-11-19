"""Verify bidirectional parity between optional imports in Python sources and pyproject.toml extras."""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_IMPORT_HELPER_CALLEES: frozenset[str] = frozenset(
    {
        "dependency_flag",
        "ensure_module",
        "import_optional",
        "import_optional_attr",
    }
)

_MODULE_TO_EXTRA: tuple[tuple[str, str], ...] = (
    ("google.cloud.sqlalchemy_spanner", "sqlalchemy-gcp"),
    ("sqlalchemy_bigquery", "sqlalchemy-gcp"),
    ("google.genai", "google-genai"),
    ("google.adk", "adk"),
    ("duckdb_engine", "sqlalchemy-duckdb"),
    ("django_spanner", "django-spanner"),
    ("flask_caching", "flask"),
    ("advanced_alchemy", "alchemy"),
    ("sqlalchemy", "alchemy"),
    ("fastembed", "embeddings"),
    ("opentelemetry", "opentelemetry"),
    ("pyarrow", "arrow"),
    ("jwt", "iap"),
)

_DIST_SPLIT_RE = re.compile(r"[<>=!~;\[\s@]")


@dataclass(frozen=True)
class _OptionalUsage:
    """Single AST-discovered optional dependency usage."""

    rel_path: str
    lineno: int
    module_name: str | None = None
    explicit_extra: str | None = None
    explicit_package: str | None = None


def _normalize_dist_name(name: str) -> str:
    """Normalize a PEP 508 requirement or distribution name to lowercase hyphenated form."""
    base = _DIST_SPLIT_RE.split(name.strip(), maxsplit=1)[0].strip()
    return re.sub(r"[-_.]+", "-", base).lower()


def _callee_name(func: ast.expr) -> str | None:
    """Return the trailing identifier for an AST call target."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _str_constant(node: ast.expr | None) -> str | None:
    """Return the string value when an AST node is a string literal."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _is_type_checking_guard(test: ast.expr) -> bool:
    """Return True if an if-condition checks ``TYPE_CHECKING``."""
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    if isinstance(test, ast.Attribute):
        return test.attr == "TYPE_CHECKING"
    return False


def _catches_import_error(node: ast.Try) -> bool:
    """Return True if any handler on ``node`` catches ImportError or ModuleNotFoundError."""
    target_names = {"ImportError", "ModuleNotFoundError"}
    for handler in node.handlers:
        exc_type = handler.type
        if isinstance(exc_type, ast.Name) and exc_type.id in target_names:
            return True
        if isinstance(exc_type, ast.Tuple) and any(
            isinstance(elt, ast.Name) and elt.id in target_names for elt in exc_type.elts
        ):
            return True
    return False


def _is_importlib_import_module(func: ast.expr) -> bool:
    """Return True when ``func`` represents ``importlib.import_module``."""
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "import_module"
        and isinstance(func.value, ast.Name)
        and func.value.id == "importlib"
    )


class _OptionalImportVisitor(ast.NodeVisitor):
    """AST visitor collecting optional imports and extra keywords outside TYPE_CHECKING."""

    def __init__(self, rel_path: str) -> None:
        self._rel_path = rel_path
        self.usages: list[_OptionalUsage] = []

    def visit_If(self, node: ast.If) -> None:
        """Skip ``if TYPE_CHECKING:`` branches while inspecting runtime branches."""
        if _is_type_checking_guard(node.test):
            for stmt in node.orelse:
                self.visit(stmt)
            return
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:
        """Collect imports inside ``try ... except ImportError`` blocks."""
        if _catches_import_error(node):
            for stmt in node.body:
                self._collect_try_imports(stmt)
        self.generic_visit(node)

    def _collect_try_imports(self, node: ast.AST) -> None:
        """Walk statements inside a guarded ``try`` body for import nodes."""
        if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
            for stmt in node.orelse:
                self._collect_try_imports(stmt)
            return
        if isinstance(node, ast.Import):
            for alias in node.names:
                self.usages.append(
                    _OptionalUsage(
                        rel_path=self._rel_path,
                        lineno=node.lineno,
                        module_name=alias.name,
                    )
                )
            return
        if isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module is not None:
                self.usages.append(
                    _OptionalUsage(
                        rel_path=self._rel_path,
                        lineno=node.lineno,
                        module_name=node.module,
                    )
                )
            return
        for child in ast.iter_child_nodes(node):
            self._collect_try_imports(child)

    def visit_Call(self, node: ast.Call) -> None:
        """Collect optional loader calls, ``importlib.import_module``, and ``LoopBackendSpec``."""
        callee = _callee_name(node.func)
        kw_map = {kw.arg: _str_constant(kw.value) for kw in node.keywords if kw.arg is not None}
        explicit_extra = kw_map.get("extra")

        if callee in _IMPORT_HELPER_CALLEES:
            module_name = _str_constant(node.args[0]) if node.args else None
            if module_name is None:
                module_name = kw_map.get("module_name")
            if module_name is not None or explicit_extra is not None:
                self.usages.append(
                    _OptionalUsage(
                        rel_path=self._rel_path,
                        lineno=node.lineno,
                        module_name=module_name,
                        explicit_extra=explicit_extra,
                        explicit_package=kw_map.get("package"),
                    )
                )
        elif _is_importlib_import_module(node.func):
            module_name = _str_constant(node.args[0]) if node.args else None
            if module_name is not None:
                self.usages.append(
                    _OptionalUsage(
                        rel_path=self._rel_path,
                        lineno=node.lineno,
                        module_name=module_name,
                    )
                )
        elif callee == "MissingDependencyError":
            pkg_val = _str_constant(node.args[0]) if node.args else kw_map.get("package")
            if explicit_extra is not None or pkg_val is not None:
                self.usages.append(
                    _OptionalUsage(
                        rel_path=self._rel_path,
                        lineno=node.lineno,
                        explicit_extra=explicit_extra,
                        explicit_package=pkg_val,
                    )
                )
        elif callee == "LoopBackendSpec":
            module_name = kw_map.get("module_name")
            if module_name is None and len(node.args) >= 2:
                module_name = _str_constant(node.args[1])
            if module_name is not None:
                self.usages.append(
                    _OptionalUsage(
                        rel_path=self._rel_path,
                        lineno=node.lineno,
                        module_name=module_name,
                    )
                )
        elif explicit_extra is not None:
            self.usages.append(
                _OptionalUsage(
                    rel_path=self._rel_path,
                    lineno=node.lineno,
                    explicit_extra=explicit_extra,
                )
            )

        self.generic_visit(node)


def _resolve_module_target(module_name: str) -> tuple[str | None, str | None] | None:
    """Resolve a module path to ``(canonical_extra, normalized_dist)`` or ``None`` if internal/stdlib."""
    if module_name == "vertebra" or module_name.startswith("vertebra."):
        if not module_name.startswith("vertebra._"):
            return None
        sub = module_name.split(".", 2)[1]
        slug = sub.lstrip("_")
        if slug == "vertebra":
            return None
        return slug, f"vertebra-{slug}"

    top_level = module_name.split(".", 1)[0]
    if top_level in sys.stdlib_module_names:
        return None

    for prefix, mapped_extra in _MODULE_TO_EXTRA:
        if module_name == prefix or module_name.startswith(f"{prefix}."):
            return mapped_extra, None

    return None, _normalize_dist_name(top_level)


def _resolve_package_target(package_name: str) -> tuple[str | None, str | None] | None:
    """Resolve an explicit package string to ``(canonical_extra, normalized_dist)`` or ``None`` if core."""
    if package_name == "vertebra-core" or package_name.startswith("vertebra._vertebra"):
        return None
    if package_name.startswith("vertebra._"):
        sub = package_name.split(".", 2)[1]
        slug = sub.lstrip("_")
        return slug, f"vertebra-{slug}"
    return None, _normalize_dist_name(package_name)


def find_extras_mismatches(repo_root: Path) -> list[str]:
    """Compare optional imports under ``src/py/vertebra`` against ``pyproject.toml`` extras.

    Args:
        repo_root: Repository root containing ``pyproject.toml`` and ``src/py/vertebra``.

    Returns:
        Sorted list of human-readable parity mismatch descriptions.
    """
    pyproject_path = repo_root / "pyproject.toml"
    if not pyproject_path.is_file():
        return [f"Missing pyproject.toml at {pyproject_path}"]

    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = data.get("project", {})
    core_deps: set[str] = {
        _normalize_dist_name(dep) for dep in project.get("dependencies", []) if isinstance(dep, str) and dep.strip()
    }
    raw_optional = project.get("optional-dependencies", {})
    declared_extras: dict[str, set[str]] = {}
    package_to_extras: dict[str, set[str]] = {}
    for extra_name, deps in raw_optional.items():
        if not isinstance(deps, list):
            continue
        pkg_set = {_normalize_dist_name(dep) for dep in deps if isinstance(dep, str) and dep.strip()}
        declared_extras[extra_name] = pkg_set
        for pkg in pkg_set:
            package_to_extras.setdefault(pkg, set()).add(extra_name)

    source_root = repo_root / "src" / "py" / "vertebra"
    if not source_root.is_dir():
        return [f"Missing source directory at {source_root}"]

    mismatches: list[str] = []
    used_extras: set[str] = set()

    for py_path in sorted(source_root.rglob("*.py")):
        rel_path = py_path.relative_to(repo_root).as_posix()
        tree = ast.parse(py_path.read_text(encoding="utf-8"), filename=str(py_path))
        visitor = _OptionalImportVisitor(rel_path)
        visitor.visit(tree)

        for usage in visitor.usages:
            if usage.explicit_extra is not None:
                if usage.explicit_extra not in declared_extras:
                    mismatches.append(f"{usage.rel_path}:{usage.lineno}: undeclared extra {usage.explicit_extra!r}")
                else:
                    used_extras.add(usage.explicit_extra)
                continue

            resolved: tuple[str | None, str | None] | None = None
            label = ""
            if usage.module_name is not None:
                resolved = _resolve_module_target(usage.module_name)
                label = usage.module_name
            elif usage.explicit_package is not None:
                resolved = _resolve_package_target(usage.explicit_package)
                label = usage.explicit_package

            if resolved is None:
                continue

            mapped_extra, dist_name = resolved
            if mapped_extra is not None:
                if mapped_extra not in declared_extras:
                    mismatches.append(
                        f"{usage.rel_path}:{usage.lineno}: optional import {label!r} requires undeclared extra {mapped_extra!r}"
                    )
                else:
                    used_extras.add(mapped_extra)
                continue

            if dist_name is not None:
                if dist_name in core_deps:
                    continue
                if dist_name in package_to_extras:
                    used_extras.update(package_to_extras[dist_name])
                else:
                    mismatches.append(
                        f"{usage.rel_path}:{usage.lineno}: optional import {label!r} (package {dist_name!r}) is missing from pyproject.toml"
                    )

    for extra_name in sorted(declared_extras):
        if extra_name not in used_extras:
            mismatches.append(f"pyproject.toml: optional extra {extra_name!r} has no importers under src/py/vertebra")

    return sorted(set(mismatches))


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint verifying bidirectional pyproject extras parity."""
    parser = argparse.ArgumentParser(description="Verify bidirectional pyproject.toml optional extras parity.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Repository root containing pyproject.toml and src/py/vertebra",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    mismatches = find_extras_mismatches(args.repo_root.resolve())
    if mismatches:
        for item in mismatches:
            sys.stderr.write(f"{item}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
