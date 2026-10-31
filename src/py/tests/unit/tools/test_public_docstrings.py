"""Behavioral unit tests for AST public docstring leak, overload, TypedDict, and Struct scanners."""

from pathlib import Path

import pytest
from tools.docs.scan_public_docstrings import main as scan_main
from tools.docs.scan_public_docstrings import (
    scan_overload_docstrings,
    scan_public_docstring_leaks,
    scan_undocumented_typeddicts,
)

__all__ = (
    "test_scan_overload_docstrings_flags_documented_overloads_in_py_and_pyi",
    "test_scan_public_docstring_leaks_detects_compiler_and_layout_leaks",
    "test_scan_public_docstrings_cli_main",
    "test_scan_undocumented_typeddicts_and_duplicate_struct_attributes",
    "test_vertebra_public_docstrings_have_zero_leaks_and_zero_overload_docs",
)


def test_scan_public_docstring_leaks_detects_compiler_and_layout_leaks(tmp_path: Path) -> None:
    """Verify scan_public_docstring_leaks catches compiler and layout leaks in public symbols."""
    module_file = tmp_path / "sample.py"
    module_file.write_text(
        '"""Module docstring mentioning mypyc compilation and ABI boundary notes."""\n\n'
        "class CleanDriver:\n"
        '    """Execute SQL statements synchronously."""\n\n'
        "    def run(self) -> None:\n"
        '        """Run a statement."""\n\n'
        "    def _internal(self) -> None:\n"
        '        """Internal helper avoiding mypyc segfault."""\n\n'
        "class LeakyBase:\n"
        '    """Driver base avoiding cross-trait attribute access and Private/internal methods."""\n\n'
        "    async def handle_errors(self) -> None:\n"
        '        """Defer raising to avoid ABI boundary violations."""\n\n'
        "def leaky_helper() -> None:\n"
        '    """Keep signature introspectable under mypyc."""\n\n'
        "class CrashPool:\n"
        '    """Prevent segmentation fault crashes."""\n\n'
        "def segfault_fn() -> None:\n"
        '    """Avoid a segfault during lookup."""\n\n'
        "class LeakySchema:\n"
        '    """Public schema with field docstrings."""\n\n'
        "    leaky_field: str\n"
        '    """Avoid mypyc ABI boundary issue."""\n'
        "    _private_field: str\n"
        '    """Internal mypyc note."""\n\n'
        "class _PrivateHelper:\n"
        '    """Private class mentioning mypyc ABI boundary."""\n',
        encoding="utf-8",
    )

    private_typing = tmp_path / "_typing.py"
    private_typing.write_text(
        'class SampleContext:\n    """Excluded from mypyc compilation to avoid ABI boundary issues."""\n',
        encoding="utf-8",
    )

    violations = scan_public_docstring_leaks([tmp_path])
    messages = [item.format() for item in violations]

    assert len(violations) == 6
    assert any(
        "LeakyBase" in msg and "cross-trait attribute" in msg.lower() and "private/internal methods" in msg.lower()
        for msg in messages
    )
    assert any("LeakyBase.handle_errors" in msg and "abi boundary" in msg.lower() for msg in messages)
    assert any("leaky_helper" in msg and "mypyc" in msg.lower() for msg in messages)
    assert any("CrashPool" in msg and "segmentation fault" in msg.lower() for msg in messages)
    assert any("segfault_fn" in msg and "segfault" in msg.lower() for msg in messages)
    assert any("LeakySchema.leaky_field" in msg and "mypyc" in msg.lower() for msg in messages)
    assert not any("LeakySchema._private_field" in msg for msg in messages)
    assert not any("SampleContext" in msg for msg in messages)
    assert not any("_PrivateHelper" in msg for msg in messages)
    assert not any("_internal" in msg for msg in messages)


def test_scan_overload_docstrings_flags_documented_overloads_in_py_and_pyi(tmp_path: Path) -> None:
    """Verify scan_overload_docstrings detects documented @overload stubs in .py and .pyi files."""
    module_file = tmp_path / "overloads.py"
    module_file.write_text(
        "import typing\n"
        "from typing import overload\n\n"
        "@overload\n"
        "def fetch(x: int) -> int: ...\n\n"
        "@overload\n"
        "def fetch(x: str) -> str:\n"
        '    """Documented overload stub."""\n'
        "    ...\n\n"
        "def fetch(x: int | str) -> int | str:\n"
        '    """Fetch value."""\n'
        "    return x\n",
        encoding="utf-8",
    )
    stub_file = tmp_path / "_native.pyi"
    stub_file.write_text(
        "import typing\n\n"
        "class Client:\n"
        "    @typing.overload\n"
        "    async def query(self, sql: str) -> list[dict[str, object]]:\n"
        '        """Documented method overload."""\n'
        "        ...\n",
        encoding="utf-8",
    )

    violations = scan_overload_docstrings([module_file, stub_file])
    symbols = {item.symbol for item in violations}
    assert symbols == {"fetch", "Client.query"}


def test_scan_undocumented_typeddicts_and_duplicate_struct_attributes(tmp_path: Path) -> None:
    """Verify scan_undocumented_typeddicts checks TypedDict fields and duplicate Struct Attributes headers."""
    module_file = tmp_path / "configs.py"
    module_file.write_text(
        "from typing import NotRequired, TypedDict\n"
        "import msgspec\n\n"
        "class CleanConfig(TypedDict):\n"
        '    """Connection configuration."""\n\n'
        "    host: str\n"
        '    """Database hostname."""\n'
        "    port: NotRequired[int]\n"
        '    """Database TCP port."""\n\n'
        "class _SharedBaseParams(TypedDict):\n"
        '    """Shared connection base."""\n\n'
        "    dsn: str\n\n"
        "class PublicDerivedParams(_SharedBaseParams):\n"
        '    """Derived connection parameters."""\n\n'
        "    timeout: NotRequired[float]\n\n"
        "class CleanStruct(msgspec.Struct):\n"
        '    """Clean msgspec struct."""\n\n'
        "    host: str = '127.0.0.1'\n\n"
        "class DuplicateStructAttributes(msgspec.Struct):\n"
        '    """Struct with duplicate Attributes block.\n\n'
        "    Attributes:\n"
        "        enable_cache: Enable caching.\n"
        '    """\n\n'
        "    enable_cache: bool = True\n",
        encoding="utf-8",
    )

    violations = scan_undocumented_typeddicts([module_file])
    by_symbol = {item.symbol: item for item in violations}

    assert "CleanConfig" not in by_symbol
    assert "CleanStruct" not in by_symbol
    assert "_SharedBaseParams" in by_symbol
    assert by_symbol["_SharedBaseParams"].missing_fields == ("dsn",)
    assert "PublicDerivedParams" in by_symbol
    assert by_symbol["PublicDerivedParams"].missing_fields == ("timeout",)
    assert "DuplicateStructAttributes" in by_symbol
    assert by_symbol["DuplicateStructAttributes"].has_duplicate_class_attributes is True


def test_scan_public_docstrings_cli_main(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify CLI main returns 0 on clean modules and 1 on violations."""
    clean_file = tmp_path / "clean.py"
    clean_file.write_text(
        "from typing import TypedDict\n\n"
        "class ItemConfig(TypedDict):\n"
        '    """Item configuration."""\n\n'
        "    name: str\n"
        '    """Item identifier."""\n',
        encoding="utf-8",
    )

    exit_ok = scan_main(["--check-leaks", "--check-overloads", "--check-typeddicts", str(clean_file)])
    assert exit_ok == 0

    bad_file = tmp_path / "bad.py"
    bad_file.write_text('def helper() -> None:\n    """Avoid mypyc ABI boundary failure."""\n', encoding="utf-8")
    exit_fail = scan_main(["--check-leaks", str(bad_file)])
    captured = capsys.readouterr()
    assert exit_fail == 1
    assert "helper" in captured.err


def test_vertebra_public_docstrings_have_zero_leaks_and_zero_overload_docs() -> None:
    """Verify src/py/vertebra has zero docstring leaks, overload docstrings, or schema violations."""
    vertebra_root = Path(__file__).resolve().parents[3] / "vertebra"
    assert scan_public_docstring_leaks([vertebra_root]) == []
    assert scan_overload_docstrings([vertebra_root]) == []
    assert scan_undocumented_typeddicts([vertebra_root]) == []
