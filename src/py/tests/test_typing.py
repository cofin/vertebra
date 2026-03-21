"""Tests for the typing layer (_typing.py and typing.py)."""

import pytest


class TestInternalTyping:
    def test_handler_id_is_int(self):
        from vertebra._typing import HandlerID

        assert HandlerID is int

    def test_raw_headers_alias(self):
        from vertebra._typing import RawHeaders

        # RawHeaders should be list[tuple[str, str]]
        assert RawHeaders == list[tuple[str, str]]

    def test_lifecycle_hook_is_callable(self):
        from vertebra._typing import LifecycleHook

        assert LifecycleHook is not None


class TestPublicTypingFacade:
    def test_reexports_from_internal(self):
        from vertebra.typing import HandlerID, LifecycleHook, RawHeaders

        assert HandlerID is int
        assert RawHeaders == list[tuple[str, str]]
        assert LifecycleHook is not None

    def test_struct_available(self):
        """msgspec.Struct should be available since msgspec is installed."""
        import msgspec

        from vertebra.typing import Struct

        assert Struct is msgspec.Struct

    def test_missing_optional_raises(self):
        """Accessing a type for an uninstalled package should raise."""
        # pyarrow is likely not installed in the test env
        try:
            import pyarrow  # noqa: F401

            pytest.skip("pyarrow is installed")
        except ImportError:
            pass

        from vertebra.utils.module_loader import MissingDependencyError

        with pytest.raises(MissingDependencyError, match="pyarrow"):
            from vertebra import typing

            typing.Table  # noqa: B018

    def test_unknown_attr_raises(self):
        with pytest.raises(AttributeError, match="no attribute"):
            from vertebra import typing

            typing.NonExistentType  # noqa: B018
