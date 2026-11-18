"""Tests for the typing layer (_typing.py and typing.py)."""

import ast
import importlib.util
import pathlib
import subprocess
import sys
import tomllib
from typing import Any

import msgspec
import pytest

import vertebra._typing as internal_typing
import vertebra.db as vertebra_db
import vertebra.storage as vertebra_storage
from vertebra import typing as vertebra_typing
from vertebra._typing import HandlerID, LifecycleHook, OptionalDependency, RawHeaders
from vertebra._typing import IntegrityError as InternalIntegrityError
from vertebra._typing import ToolPolicyError as InternalPolicyError
from vertebra._typing import ToolProvider as InternalProvider
from vertebra.typing import (
    HandlerID as PublicHandlerID,
)
from vertebra.typing import (
    IntegrityError,
    PreconditionError,
    Struct,
    ToolPolicyError,
    ToolProvider,
    VertebraError,
)
from vertebra.typing import (
    LifecycleHook as PublicLifecycleHook,
)
from vertebra.typing import (
    RawHeaders as PublicRawHeaders,
)
from vertebra.utils import module_loader
from vertebra.utils.module_loader import (
    MissingDependencyError,
    ensure_module,
    ensure_numpy,
    ensure_pandas,
    import_optional,
    reset_dependency_cache,
)


def test_internal_typing_handler_id_is_int() -> None:
    assert HandlerID is int


def test_internal_typing_raw_headers_alias() -> None:
    assert RawHeaders == list[tuple[str, str]]


def test_internal_typing_lifecycle_hook_is_callable() -> None:
    assert LifecycleHook is not None


def test_public_typing_facade_reexports_from_internal() -> None:
    assert PublicHandlerID is int
    assert PublicRawHeaders == list[tuple[str, str]]
    assert PublicLifecycleHook is not None


def test_public_typing_facade_struct_available() -> None:
    """msgspec.Struct should be available since msgspec is installed."""
    assert Struct is msgspec.Struct


def test_public_typing_facade_missing_optional_raises() -> None:
    """Accessing a type for an uninstalled package should raise."""
    if importlib.util.find_spec("pyarrow") is not None:
        pytest.skip("pyarrow is installed")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        _ = vertebra_typing.Table


def test_public_typing_facade_unknown_attr_raises() -> None:
    with pytest.raises(AttributeError, match="no attribute"):
        _ = vertebra_typing.NonExistentType


def test_optional_dependency_canonical_name() -> None:
    flag = OptionalDependency("msgspec")
    assert bool(flag) is True


@pytest.mark.parametrize(
    "name",
    [
        "UnsetType",
        "StructStub",
        "AttrsInstanceStub",
        "NumpyArrayStub",
        "MSGSPEC_INSTALLED",
        "ORJSON_INSTALLED",
    ],
)
def test_trimmed_typing_surface_removes_dead_stubs(name: str) -> None:
    """Verify dead typing stubs and flags are removed from internal typing."""
    assert not hasattr(internal_typing, name)


def test_lazy_vertebra_import_and_native_server_import() -> None:
    """Bare `import vertebra` must not import heavy optional packages or server circular chains."""
    code = (
        "import sys; "
        "import vertebra; "
        "heavy = [m for m in ('pydantic', 'attrs', 'attr', 'numpy', 'orjson', 'rich', "
        "'vertebra.framework._app', 'vertebra.framework._router') if m in sys.modules]; "
        "assert not heavy, f'Unexpected eager imports: {heavy}'; "
        "import vertebra.server._dispatch; "
        "assert vertebra.App is not None"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr


@pytest.mark.parametrize(
    "attr",
    [
        "BaseStruct",
        "CamelizedBaseStruct",
        "FrozenBaseStruct",
        "FrozenCamelizedStruct",
        "Struct",
        "VertebraError",
        "MissingDependencyError",
        "ImproperConfigurationError",
        "IntegrityError",
        "PreconditionError",
        "UnsupportedBackendError",
        "Empty",
        "EmptyType",
        "UNSET",
        "OptionalDependency",
        "module_available",
        "reset_dependency_cache",
    ],
)
def test_public_typing_facade_exports_structs_exceptions_and_sentinels(attr: str) -> None:
    """Verify vertebra.typing exports the full public typing, struct, sentinel, and exception surface."""
    assert hasattr(vertebra_typing, attr), f"Missing {attr} on vertebra.typing"


@pytest.mark.parametrize(
    ("kwargs", "expected_install", "expected_hint"),
    [
        (
            {"extra": "sqlspec"},
            "vertebra-core[sqlspec]",
            "Install it with: uv add vertebra-core[sqlspec]",
        ),
        (
            {"install_package": "pkg", "extra": "sqlspec"},
            "pkg",
            "Install it with: uv add pkg",
        ),
        (
            {},
            "x",
            "Install it with: uv add x",
        ),
    ],
)
def test_missing_dependency_error_install_hint_defaults(
    kwargs: dict[str, str],
    expected_install: str,
    expected_hint: str,
) -> None:
    """MissingDependencyError defaults install_package to vertebra-core[extra] unless overridden."""
    err = MissingDependencyError("x", **kwargs)
    assert err.install_package == expected_install
    assert err.hint == expected_hint
    assert str(err) == "Optional dependency 'x' is required but not installed."


def test_module_loader_caches_imported_modules_and_ensure_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify import_optional resolves directly via sys.modules and _module_cache is removed."""
    reset_dependency_cache()
    try:
        assert not hasattr(vertebra_typing, "_module_cache")
        assert not hasattr(internal_typing, "_module_cache")
        assert not hasattr(module_loader, "_module_cache")

        mod1 = import_optional("json")
        assert mod1 is not None
        assert mod1 is sys.modules["json"]
        assert import_optional("json") is mod1
        assert ensure_module("json") is mod1

        monkeypatch.setitem(sys.modules, "json", None)
        assert import_optional("json") is None
        monkeypatch.undo()

        assert import_optional("vertebra_nonexistent_pkg_xyz") is None
        with pytest.raises(MissingDependencyError, match="vertebra_nonexistent_pkg_xyz") as exc_info:
            ensure_module("vertebra_nonexistent_pkg_xyz", extra="custom")
        assert exc_info.value.install_package == "vertebra-core[custom]"
        assert "vtb[" not in exc_info.value.hint

        reset_dependency_cache()
        assert import_optional("json") is mod1
    finally:
        reset_dependency_cache()


@pytest.mark.parametrize(
    ("pkg_name", "loader_fn"),
    [
        ("numpy", ensure_numpy),
        ("pandas", ensure_pandas),
    ],
)
def test_optional_array_loaders_raise_missing_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    pkg_name: str,
    loader_fn: Any,
) -> None:
    """Optional numpy and pandas loaders report their package and extra install hint when absent."""
    monkeypatch.setitem(sys.modules, pkg_name, None)
    reset_dependency_cache(pkg_name)
    try:
        with pytest.raises(MissingDependencyError) as pkg_exc:
            loader_fn()
        assert pkg_exc.value.package == pkg_name
        assert pkg_exc.value.install_package == f"vertebra-core[{pkg_name}]"
    finally:
        reset_dependency_cache(pkg_name)


def test_tool_provider_contract_and_policy_error_exports() -> None:
    """Neutral tool contracts share identity and accept structural providers."""

    class Provider:
        def operations(self) -> dict[str, Any]:
            return {"read": self.read}

        def confirmation_reason(self, operation_name: str, arguments: Any) -> None:
            return None

        async def read(self) -> int:
            return 1

    assert ToolProvider is InternalProvider
    assert ToolPolicyError is InternalPolicyError
    assert isinstance(Provider(), ToolProvider)
    error = ToolPolicyError("denied")
    assert isinstance(error, VertebraError)
    assert isinstance(error, PermissionError)


def test_public_typing_imports_exceptions_and_sentinels_without_msgspec() -> None:
    """Verify vertebra.typing imports exceptions and sentinels when msgspec is absent."""
    code = (
        "import importlib.abc, sys\n"
        "class BlockMsgspec(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, fullname, path, target=None):\n"
        "        if fullname == 'msgspec' or fullname.startswith('msgspec.'):\n"
        "            raise ModuleNotFoundError(\"No module named 'msgspec'\")\n"
        "        return None\n"
        "blocker = BlockMsgspec()\n"
        "sys.meta_path.insert(0, blocker)\n"
        "from vertebra.typing import Empty, EmptyType, IntegrityError, PreconditionError, VertebraError\n"
        "assert issubclass(PreconditionError, VertebraError)\n"
        "assert issubclass(IntegrityError, VertebraError)\n"
        "assert issubclass(IntegrityError, OSError)\n"
        "assert Empty is EmptyType.Empty\n"
        "assert 'msgspec' not in sys.modules\n"
        "sys.meta_path.remove(blocker)\n"
        "from vertebra.typing import BaseStruct, CamelizedBaseStruct, FrozenBaseStruct, FrozenCamelizedStruct, Struct\n"
        "assert issubclass(BaseStruct, Struct)\n"
        "assert issubclass(CamelizedBaseStruct, BaseStruct)\n"
        "assert issubclass(FrozenBaseStruct, Struct)\n"
        "assert issubclass(FrozenCamelizedStruct, FrozenBaseStruct)\n"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr


def test_storage_integrity_error_hierarchy_and_separation_from_db() -> None:
    """Verify storage IntegrityError shares identity across typing and storage without colliding with vertebra.db."""
    assert IntegrityError is InternalIntegrityError
    assert IntegrityError is vertebra_storage.IntegrityError
    assert "IntegrityError" in vertebra_typing.__all__
    assert "IntegrityError" in vertebra_storage.__all__
    assert issubclass(IntegrityError, VertebraError)
    assert issubclass(IntegrityError, OSError)
    assert issubclass(PreconditionError, VertebraError)
    assert issubclass(PreconditionError, OSError)
    integrity_cls: type[BaseException] = IntegrityError
    assert integrity_cls is not PreconditionError
    assert integrity_cls is not vertebra_db.IntegrityError
    assert not issubclass(IntegrityError, vertebra_db.DatabaseError)
    assert issubclass(vertebra_db.IntegrityError, vertebra_db.DatabaseError)
    assert not issubclass(vertebra_db.IntegrityError, OSError)

    err = IntegrityError("sha256 digest mismatch")
    assert isinstance(err, VertebraError)
    assert isinstance(err, OSError)
    assert not isinstance(err, vertebra_db.IntegrityError)


_CHAPTER_EXTENSION_HOSTS: tuple[str, ...] = ("litestar", "starlette", "fastapi", "flask", "django")
_CHAPTER_OWNED_SUBTREES: tuple[str, ...] = (
    "src/py/vertebra/extensions/litestar",
    "src/py/vertebra/extensions/starlette",
    "src/py/vertebra/extensions/fastapi",
    "src/py/vertebra/extensions/flask",
    "src/py/vertebra/extensions/django",
    "src/py/vertebra/extensions/grpc",
    "src/py/vertebra/extensions/apex",
    "src/py/vertebra/framework",
    "src/py/vertebra/agent",
    "src/py/vertebra/embeddings",
    "src/py/vertebra/server/worker.py",
    "src/py/vertebra/server/_offload.py",
    "src/py/vertebra/server/_routing.py",
    "src/py/vertebra/server/_fastpath_core.py",
)


def _iter_chapter_python_files() -> list[pathlib.Path]:
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    files: list[pathlib.Path] = []
    for rel in _CHAPTER_OWNED_SUBTREES:
        target = repo_root / rel
        if target.is_file():
            files.append(target)
        elif target.is_dir():
            files.extend(sorted(target.rglob("*.py")))
    assert files, f"Expected chapter Python files under {repo_root}"
    return files


@pytest.mark.parametrize("extension_name", _CHAPTER_EXTENSION_HOSTS)
def test_framework_extension_imports_isolated_from_siblings_and_alchemy(extension_name: str) -> None:
    """Each framework extension imports when sibling extensions, alchemy, and unrelated hosts are blocked."""
    siblings = [name for name in _CHAPTER_EXTENSION_HOSTS if name != extension_name]
    blocked_modules = [f"vertebra.extensions.{name}" for name in siblings] + [
        "vertebra.extensions.alchemy",
        *siblings,
    ]
    code = (
        "import importlib, sys\n"
        f"blocked = {blocked_modules!r}\n"
        "for mod in blocked:\n"
        "    sys.modules[mod] = None\n"
        f"mod = importlib.import_module('vertebra.extensions.{extension_name}')\n"
        "assert mod is not None\n"
        f"unexpected = [m for m in blocked if sys.modules.get(m) is not None]\n"
        "assert not unexpected, f'Unexpectedly imported blocked modules: {unexpected}'\n"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr


def test_chapter_extension_modules_have_no_cross_extension_imports() -> None:
    """No framework extension imports another framework extension or vertebra.extensions.alchemy."""
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    violations: list[str] = []
    for ext in _CHAPTER_EXTENSION_HOSTS:
        forbidden = {f"vertebra.extensions.{other}" for other in _CHAPTER_EXTENSION_HOSTS if other != ext} | {
            "vertebra.extensions.alchemy"
        }
        ext_dir = repo_root / "src/py/vertebra/extensions" / ext
        py_files = sorted(ext_dir.rglob("*.py"))
        assert py_files, f"Expected extension files under {ext_dir}"
        for py_file in py_files:
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if any(alias.name == f or alias.name.startswith(f"{f}.") for f in forbidden):
                            violations.append(f"{py_file.relative_to(repo_root)}:{node.lineno}: {ast.unparse(node)}")
                elif (
                    isinstance(node, ast.ImportFrom)
                    and node.module
                    and any(node.module == f or node.module.startswith(f"{f}.") for f in forbidden)
                ):
                    violations.append(f"{py_file.relative_to(repo_root)}:{node.lineno}: {ast.unparse(node)}")
    assert violations == []


def test_chapter_modules_use_centralized_schema_components() -> None:
    """No chapter module calls msgspec.json.schema_components directly outside utils/serializers/_schema.py."""
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    violations: list[str] = []
    for py_file in _iter_chapter_python_files():
        rel = py_file.relative_to(repo_root)
        if str(rel) == "src/py/vertebra/utils/serializers/_schema.py":
            continue
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "schema_components":
                rendered = ast.unparse(node)
                if rendered.endswith("msgspec.json.schema_components"):
                    violations.append(f"{rel}:{node.lineno}: {rendered}")
    assert violations == []


def test_chapter_modules_have_no_disallowed_function_scoped_imports() -> None:
    """Chapter modules contain no function-scoped runtime imports outside __getattr__."""
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    violations: list[str] = []
    for py_file in _iter_chapter_python_files():
        rel = py_file.relative_to(repo_root)
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name == "__getattr__":
                    continue
                for sub in ast.walk(node):
                    if isinstance(sub, (ast.Import, ast.ImportFrom)):
                        violations.append(f"{rel}:{sub.lineno} ({node.name}): {ast.unparse(sub)}")
    assert violations == []


def test_pyproject_typing_and_coverage_policy() -> None:
    """Verify pyright pin, strict mypy overrides, and explicit coverage omissions in pyproject.toml."""
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))

    lint_deps = pyproject["dependency-groups"]["lint"]
    assert "pyright==1.1.381" in lint_deps

    pyright_cfg = pyproject["tool"]["pyright"]
    assert pyright_cfg["pythonVersion"] == "3.12"
    assert pyright_cfg["include"] == ["src/py/vertebra"]
    assert pyright_cfg["exclude"] == ["**/__pycache__", "src/py/vertebra/server/_js/bundle"]
    assert pyright_cfg["typeCheckingMode"] == "standard"
    assert pyright_cfg["reportMissingImports"] == "error"
    assert pyright_cfg["reportMissingTypeStubs"] == "none"
    assert pyright_cfg["reportUnusedImport"] == "error"
    assert pyright_cfg["reportUnusedVariable"] == "warning"

    overrides = pyproject["tool"]["mypy"]["overrides"]
    assert not any(override.get("disallow_subclassing_any") is False for override in overrides)
    override_modules = {mod for override in overrides for mod in override.get("module", [])}
    assert "google.cloud.*" not in override_modules
    assert "dogpile" not in override_modules
    assert "dogpile.*" not in override_modules
    assert "google.cloud.sqlalchemy_spanner" in override_modules
    assert "google.cloud.sqlalchemy_spanner.*" in override_modules

    coverage_cfg = pyproject["tool"]["coverage"]
    assert "ignore_errors" not in coverage_cfg["report"]
    expected_omit = ["src/py/vertebra/server/_js/**", "src/py/tests/**"]
    assert coverage_cfg["run"]["source"] == ["src/py/vertebra"]
    assert coverage_cfg["run"]["omit"] == expected_omit
    assert coverage_cfg["report"]["omit"] == expected_omit
