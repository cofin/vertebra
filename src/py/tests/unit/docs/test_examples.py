"""Verification suite for standalone zero-comment documentation examples."""

import ast
import asyncio
import importlib
import importlib.util
import inspect
import io
import re
import sys
import textwrap
import tokenize
from pathlib import Path
from types import ModuleType

import pytest
from tools.docs.generate_cli_docs import ensure_native_stub_module

from vertebra.typing import MissingDependencyError

__all__ = (
    "test_example_packages_define_docstring_and_dunder_all",
    "test_examples_contain_zero_inline_comments",
    "test_examples_define_standalone_structure",
    "test_examples_define_valid_snippet_sections",
    "test_examples_directory_inventory",
    "test_examples_do_not_reference_pytest_tmp_path",
    "test_markdown_code_blocks_and_snippets_fit_viewport_width",
    "test_standalone_example_executes",
)

REPO_ROOT = Path(__file__).resolve().parents[5]
EXAMPLES_DIR = REPO_ROOT / "docs" / "examples"
DIRECTIVE_PREFIXES: tuple[str, ...] = (
    "#!",
    "# type:",
    "# pyright:",
    "# mypy:",
    "# noqa",
    "# ruff:",
    "# pragma:",
    "# fmt:",
    "# isort:",
    "# nosec",
    "# cov:",
    "# --8<--",
)


def _discover_python_files() -> tuple[list[Path], list[Path], list[Path]]:
    """Discover all Python files, __init__.py files, and runnable example modules under docs/examples."""
    all_files = sorted(EXAMPLES_DIR.rglob("*.py")) if EXAMPLES_DIR.is_dir() else []
    init_files = [path for path in all_files if path.name == "__init__.py"]
    example_modules = [path for path in all_files if path.name != "__init__.py"]
    return all_files, init_files, example_modules


def test_examples_directory_inventory() -> None:
    """Ensure the documentation examples tree is discovered and non-empty."""
    _, init_files, example_modules = _discover_python_files()
    assert len(init_files) >= 1
    assert len(example_modules) >= 6


def test_example_packages_define_docstring_and_dunder_all() -> None:
    """Verify every package __init__.py under docs/examples defines a docstring and __all__."""
    _, init_files, _ = _discover_python_files()
    failures: list[str] = []
    for path in init_files:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if not ast.get_docstring(tree):
            failures.append(f"{rel}: missing module docstring")
        has_all = any(
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
            for node in tree.body
        )
        if not has_all:
            failures.append(f"{rel}: missing __all__")
    assert not failures, "\n".join(failures)


def test_examples_contain_zero_inline_comments() -> None:
    """Verify zero non-directive inline # comments exist across docs/examples."""
    all_files, _, _ = _discover_python_files()
    violations: list[str] = []
    for path in all_files:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        source = path.read_text(encoding="utf-8")
        violations.extend(
            f"{rel}:{token.start[0]}: {token.string.strip()}"
            for token in tokenize.generate_tokens(io.StringIO(source).readline)
            if token.type == tokenize.COMMENT and not _is_directive_comment(token.string)
        )
    assert not violations, f"Found {len(violations)} inline comment(s):\n" + "\n".join(violations[:30])


def test_examples_do_not_reference_pytest_tmp_path() -> None:
    """Verify no documentation snippet references pytest's tmp_path fixture."""
    _, _, example_modules = _discover_python_files()
    violations: list[str] = []
    for path in example_modules:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id == "tmp_path":
                violations.append(f"{rel}:{node.lineno}: ast.Name('tmp_path')")
            elif isinstance(node, ast.arg) and node.arg == "tmp_path":
                violations.append(f"{rel}:{node.lineno}: ast.arg('tmp_path')")
    assert not violations, f"Found tmp_path leaks in {len(violations)} location(s):\n" + "\n".join(violations)


def test_examples_define_standalone_structure() -> None:
    """Verify every example defines a docstring, top-level imports, __all__, and main()."""
    _, _, example_modules = _discover_python_files()
    violations: list[str] = []
    for path in example_modules:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        source = path.read_text(encoding="utf-8")
        compile(source, str(path), "exec")
        tree = ast.parse(source, filename=str(path))
        if not ast.get_docstring(tree):
            violations.append(f"{rel}: missing module docstring")

        has_all = any(
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
            for node in tree.body
        )
        if not has_all:
            violations.append(f"{rel}: missing __all__")

        main_defs = [
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "main"
        ]
        if not main_defs:
            violations.append(f"{rel}: missing top-level def main() entrypoint")
            continue

        for func_node in main_defs:
            violations.extend(
                f"{rel}:{child.lineno}: nested import inside main()"
                for child in ast.walk(func_node)
                if isinstance(child, (ast.Import, ast.ImportFrom))
            )

    assert not violations, f"Found {len(violations)} structural issue(s):\n" + "\n".join(violations[:40])


def test_examples_define_valid_snippet_sections() -> None:
    """Verify all example modules define balanced, dedented # --8<-- [start:example] sections."""
    _, _, example_modules = _discover_python_files()
    violations: list[str] = []
    for path in example_modules:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()

        start_count = sum(1 for line in lines if line.strip() == "# --8<-- [start:example]")
        end_count = sum(1 for line in lines if line.strip() == "# --8<-- [end:example]")

        if start_count != 1:
            violations.append(f"{rel}: expected exactly 1 '# --8<-- [start:example]', found {start_count}")
            continue
        if end_count != 1:
            violations.append(f"{rel}: expected exactly 1 '# --8<-- [end:example]', found {end_count}")
            continue

        start_idx = next(i for i, line in enumerate(lines) if line.strip() == "# --8<-- [start:example]")
        end_idx = next(i for i, line in enumerate(lines) if line.strip() == "# --8<-- [end:example]")

        if start_idx >= end_idx:
            violations.append(
                f"{rel}: [start:example] at line {start_idx + 1} appears after [end:example] at line {end_idx + 1}"
            )
            continue

        section_lines = lines[start_idx + 1 : end_idx]
        dedented = textwrap.dedent("\n".join(section_lines)).splitlines()
        if not any(line.strip() for line in dedented):
            violations.append(f"{rel}: snippet section is empty")
            continue

        for offset, line in enumerate(dedented, start=start_idx + 2):
            if len(line) > 120:
                violations.append(f"{rel}:{offset}: dedented line exceeds 120 chars ({len(line)}): {line[:60]}...")
            if "__all__" in line:
                violations.append(f"{rel}:{offset}: snippet section leaks __all__")
            if 'if __name__ == "__main__":' in line:
                violations.append(f"{rel}:{offset}: snippet section leaks __main__ block")

    assert not violations, f"Snippet section violations ({len(violations)}):\n" + "\n".join(violations[:30])


def test_markdown_code_blocks_and_snippets_fit_viewport_width() -> None:
    """Verify Markdown code blocks, snippet directives, and inline Python blocks."""
    docs_dir = REPO_ROOT / "docs"
    snippet_pattern = re.compile(r"^--8<--\s+[\"']([^\"']+)[\"']$")
    hl_lines_pattern = re.compile(r"hl_lines\s*=")
    fence_pattern = re.compile(r"^```")

    violations: list[str] = []
    markdown_files = sorted([REPO_ROOT / "README.md", *docs_dir.rglob("*.md")])

    for md_path in markdown_files:
        if not md_path.is_file():
            continue
        rel = md_path.relative_to(REPO_ROOT).as_posix()
        lines = md_path.read_text(encoding="utf-8").splitlines()
        in_fence = False
        fence_lang = ""
        fence_start_line = 0
        fence_lines: list[tuple[int, str]] = []

        for line_num, line in enumerate(lines, start=1):
            stripped = line.strip()
            if fence_pattern.match(stripped):
                if in_fence:
                    if fence_lang.startswith("```python") and not rel.startswith("docs/changelog/"):
                        _validate_python_fence(rel, fence_start_line, fence_lines, snippet_pattern, violations)
                    in_fence = False
                    fence_lines = []
                else:
                    in_fence = True
                    fence_lang = stripped
                    fence_start_line = line_num
                    fence_lines = []
                    if hl_lines_pattern.search(stripped):
                        violations.append(f"{rel}:{line_num}: stale hl_lines on code block: {stripped}")
                continue

            if in_fence:
                fence_lines.append((line_num, line))
                if "mermaid" not in fence_lang and len(line) > 84:
                    violations.append(
                        f"{rel}:{line_num}: fenced code line exceeds 84 chars ({len(line)}): {line[:60]}..."
                    )
                if ("--8" in stripped or "8<--" in stripped) and not snippet_pattern.match(stripped):
                    violations.append(f"{rel}:{line_num}: malformed snippet directive: {stripped}")
                match = snippet_pattern.match(stripped)
                if match:
                    target = match.group(1)
                    if ":" not in target:
                        violations.append(
                            f"{rel}:{line_num}: whole-file snippet include without named section: {target}"
                        )
                    else:
                        file_part, section_name = target.split(":", 1)
                        target_path = REPO_ROOT / file_part
                        if not target_path.is_file():
                            violations.append(f"{rel}:{line_num}: missing snippet file: {file_part}")
                        else:
                            target_text = target_path.read_text(encoding="utf-8")
                            if f"# --8<-- [start:{section_name}]" not in target_text:
                                violations.append(
                                    f"{rel}:{line_num}: missing start marker [start:{section_name}] in {file_part}"
                                )
                            if f"# --8<-- [end:{section_name}]" not in target_text:
                                violations.append(
                                    f"{rel}:{line_num}: missing end marker [end:{section_name}] in {file_part}"
                                )
            elif stripped.startswith(("--8<--", "--8 < --")) and not snippet_pattern.match(stripped):
                violations.append(f"{rel}:{line_num}: malformed top-level snippet directive: {stripped}")

    assert not violations, f"Markdown code block violations ({len(violations)}):\n" + "\n".join(violations[:30])


@pytest.mark.parametrize(
    "example_path",
    _discover_python_files()[2],
    ids=lambda p: p.relative_to(EXAMPLES_DIR).as_posix(),
)
def test_standalone_example_executes(example_path: Path) -> None:
    """Import each documentation example and execute main()."""
    rel = example_path.relative_to(EXAMPLES_DIR).as_posix()
    try:
        module = _load_example_module(example_path)
    except (ModuleNotFoundError, MissingDependencyError) as exc:
        pytest.skip(f"Optional dependency not installed for {rel}: {exc}")

    assert hasattr(module, "main"), f"{rel} must expose main()"
    entrypoint = module.main
    assert callable(entrypoint)

    outcome = entrypoint()
    if inspect.iscoroutine(outcome):
        asyncio.run(outcome)


def _validate_python_fence(
    rel: str,
    fence_start_line: int,
    fence_lines: list[tuple[int, str]],
    snippet_pattern: re.Pattern[str],
    violations: list[str],
) -> None:
    """Validate syntax and comment hygiene of an inline Python code block."""
    if not fence_lines:
        return
    if any(snippet_pattern.match(line.strip()) or "--8" in line for _, line in fence_lines):
        return
    dedented = textwrap.dedent("\n".join(line for _, line in fence_lines))
    if "..." in dedented:
        return
    try:
        tree = ast.parse(dedented, filename=f"{rel}:{fence_start_line}")
    except SyntaxError as exc:
        violations.append(f"{rel}:{fence_start_line}: invalid Python syntax in code block: {exc.msg}")
        return
    _validate_vertebra_imports(tree=tree, rel=rel, fence_start_line=fence_start_line, violations=violations)
    for token in tokenize.generate_tokens(io.StringIO(dedented).readline):
        if token.type == tokenize.COMMENT and not _is_directive_comment(token.string):
            line_no = fence_start_line + token.start[0]
            violations.append(f"{rel}:{line_no}: inline comment in Python code block: {token.string.strip()}")


def _validate_vertebra_imports(
    *,
    tree: ast.AST,
    rel: str,
    fence_start_line: int,
    violations: list[str],
) -> None:
    """Verify that every vertebra.* module and symbol imported in a Markdown fence exists."""
    ensure_native_stub_module()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "vertebra" or alias.name.startswith("vertebra."):
                    try:
                        importlib.import_module(alias.name)
                    except Exception as exc:
                        line_no = fence_start_line + getattr(node, "lineno", 1)
                        violations.append(f"{rel}:{line_no}: unresolvable import '{alias.name}': {exc}")
        elif (
            isinstance(node, ast.ImportFrom)
            and node.module is not None
            and (node.module == "vertebra" or node.module.startswith("vertebra."))
        ):
            try:
                mod = importlib.import_module(node.module)
            except Exception as exc:
                line_no = fence_start_line + getattr(node, "lineno", 1)
                violations.append(f"{rel}:{line_no}: unresolvable module '{node.module}': {exc}")
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                has_attr = hasattr(mod, alias.name)
                if not has_attr:
                    submod_name = f"{node.module}.{alias.name}"
                    try:
                        importlib.import_module(submod_name)
                        has_attr = True
                    except Exception:
                        has_attr = False
                if not has_attr:
                    line_no = fence_start_line + getattr(node, "lineno", 1)
                    violations.append(f"{rel}:{line_no}: symbol '{alias.name}' does not exist in '{node.module}'")


def _is_directive_comment(comment_text: str) -> bool:
    """Return whether comment_text is a recognized linter, type-checker, or snippet directive."""
    lowered = comment_text.strip().lower()
    return any(lowered.startswith(prefix) for prefix in DIRECTIVE_PREFIXES)


def _load_example_module(path: Path) -> ModuleType:
    """Load an example module by file path under its canonical docs.examples dotted name."""
    ensure_native_stub_module()
    rel_parts = path.relative_to(REPO_ROOT).with_suffix("").parts
    module_name = ".".join(rel_parts)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        msg = f"Unable to create module spec for {path}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
