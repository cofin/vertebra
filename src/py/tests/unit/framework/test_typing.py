"""Tests for the typing layer (_typing.py and typing.py)."""

import importlib.util
import subprocess
import sys
from typing import Any

import msgspec
import pytest

import vertebra._typing as internal_typing
from vertebra import typing as vertebra_typing
from vertebra._typing import HandlerID, LifecycleHook, OptionalDependency, RawHeaders
from vertebra._typing import ToolPolicyError as InternalPolicyError
from vertebra._typing import ToolProvider as InternalProvider
from vertebra.typing import (
    HandlerID as PublicHandlerID,
)
from vertebra.typing import (
    LifecycleHook as PublicLifecycleHook,
)
from vertebra.typing import (
    RawHeaders as PublicRawHeaders,
)
from vertebra.typing import (
    Struct,
    ToolPolicyError,
    ToolProvider,
    VertebraError,
)
from vertebra.utils.module_loader import (
    MissingDependencyError,
    _module_cache,
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


def test_trimmed_typing_surface_removes_dead_stubs() -> None:
    """Verify dead typing stubs and flags are removed from internal typing."""
    for name in (
        "UnsetType",
        "StructStub",
        "AttrsInstanceStub",
        "NumpyArrayStub",
        "MSGSPEC_INSTALLED",
        "ORJSON_INSTALLED",
    ):
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


def test_public_typing_facade_exports_structs_exceptions_and_sentinels() -> None:
    """Verify vertebra.typing exports the full public typing, struct, sentinel, and exception surface."""
    for attr in (
        "BaseStruct",
        "CamelizedBaseStruct",
        "FrozenBaseStruct",
        "FrozenCamelizedStruct",
        "Struct",
        "VertebraError",
        "MissingDependencyError",
        "ImproperConfigurationError",
        "UnsupportedBackendError",
        "Empty",
        "EmptyType",
        "UNSET",
        "OptionalDependency",
        "module_available",
        "reset_dependency_cache",
    ):
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
    """Verify import_optional and ensure_module cache imported ModuleType objects."""
    reset_dependency_cache()
    assert "json" not in _module_cache

    mod1 = import_optional("json")
    assert mod1 is not None
    assert _module_cache.get("json") is mod1
    assert import_optional("json") is mod1
    assert ensure_module("json") is mod1

    assert import_optional("vertebra_nonexistent_pkg_xyz") is None
    with pytest.raises(MissingDependencyError, match="vertebra_nonexistent_pkg_xyz") as exc_info:
        ensure_module("vertebra_nonexistent_pkg_xyz", extra="custom")
    assert exc_info.value.install_package == "vertebra-core[custom]"
    assert "vtb[" not in exc_info.value.hint

    for pkg_name, loader_fn in (("numpy", ensure_numpy), ("pandas", ensure_pandas)):
        monkeypatch.setitem(sys.modules, pkg_name, None)
        reset_dependency_cache(pkg_name)
        with pytest.raises(MissingDependencyError) as pkg_exc:
            loader_fn()
        assert pkg_exc.value.package == pkg_name
        assert pkg_exc.value.install_package == pkg_name

    reset_dependency_cache()
    assert "json" not in _module_cache


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
        "from vertebra.typing import Empty, EmptyType, PreconditionError, VertebraError\n"
        "assert issubclass(PreconditionError, VertebraError)\n"
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
