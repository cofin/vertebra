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


def test_module_loader_caches_imported_modules_and_ensure_module() -> None:
    """Verify import_optional and ensure_module cache imported ModuleType objects."""
    reset_dependency_cache()
    assert "json" not in _module_cache

    mod1 = import_optional("json")
    assert mod1 is not None
    assert _module_cache.get("json") is mod1
    assert import_optional("json") is mod1
    assert ensure_module("json") is mod1

    assert import_optional("vertebra_nonexistent_pkg_xyz") is None
    with pytest.raises(MissingDependencyError, match="vertebra_nonexistent_pkg_xyz"):
        ensure_module("vertebra_nonexistent_pkg_xyz", extra="custom")

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
