"""Tests for the typing layer (_typing.py and typing.py)."""

import importlib.util
import subprocess
import sys

import msgspec
import pytest

import vertebra._typing as internal_typing
from vertebra import typing as vertebra_typing
from vertebra._typing import HandlerID, LifecycleHook, OptionalDependency, RawHeaders
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
)
from vertebra.utils.module_loader import MissingDependencyError


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
