"""Tests for the typing layer (_typing.py and typing.py)."""

import pytest


def test_internal_typing_handler_id_is_int():
    from vertebra._typing import HandlerID

    assert HandlerID is int


def test_internal_typing_raw_headers_alias():
    from vertebra._typing import RawHeaders

    assert RawHeaders == list[tuple[str, str]]


def test_internal_typing_lifecycle_hook_is_callable():
    from vertebra._typing import LifecycleHook

    assert LifecycleHook is not None


def test_public_typing_facade_reexports_from_internal():
    from vertebra.typing import HandlerID, LifecycleHook, RawHeaders

    assert HandlerID is int
    assert RawHeaders == list[tuple[str, str]]
    assert LifecycleHook is not None


def test_public_typing_facade_struct_available():
    """msgspec.Struct should be available since msgspec is installed."""
    import msgspec

    from vertebra.typing import Struct

    assert Struct is msgspec.Struct


def test_public_typing_facade_missing_optional_raises():
    """Accessing a type for an uninstalled package should raise."""
    try:
        import pyarrow  # noqa: F401

        pytest.skip("pyarrow is installed")
    except ImportError:
        pass

    from vertebra.utils.module_loader import MissingDependencyError

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        from vertebra import typing

        typing.Table  # noqa: B018


def test_public_typing_facade_unknown_attr_raises():
    with pytest.raises(AttributeError, match="no attribute"):
        from vertebra import typing

        typing.NonExistentType  # noqa: B018


def test_optional_dependency_canonical_name_and_alias():
    from vertebra._typing import OptionalDependency, OptionalDependencyFlag

    assert OptionalDependency is OptionalDependencyFlag


def test_lazy_vertebra_import_and_native_server_import():
    """Bare `import vertebra` must not import heavy optional packages or server circular chains."""
    import subprocess
    import sys

    code = (
        "import sys; "
        "import vertebra; "
        "heavy = [m for m in ('pydantic', 'attrs', 'attr', 'numpy', 'orjson', 'rich', "
        "'vertebra.framework._app', 'vertebra.framework._router') if m in sys.modules]; "
        "assert not heavy, f'Unexpected eager imports: {heavy}'; "
        "import vertebra.server._native; "
        "assert vertebra.App is vertebra.VertebraApp"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr
