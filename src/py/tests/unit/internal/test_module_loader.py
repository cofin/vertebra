"""Tests for the module_loader optional dependency utilities."""

from unittest.mock import patch

import pytest

from vertebra.exceptions import MissingDependencyError
from vertebra.utils.module_loader import (
    OptionalDependencyFlag,
    dependency_flag,
    ensure_dependency,
    ensure_pyarrow,
    module_available,
    reset_dependency_cache,
)


class TestModuleAvailable:
    def test_installed_module_returns_true(self) -> None:
        """Test that installed modules return True."""
        reset_dependency_cache()
        assert module_available("os") is True
        assert module_available("sys") is True
        assert module_available("pathlib") is True

    def test_missing_module_returns_false(self) -> None:
        """Test that missing modules return False."""
        reset_dependency_cache()
        assert module_available("nonexistent_module_xyz") is False
        assert module_available("fake_package_123") is False

    def test_result_is_cached(self) -> None:
        """Test that results are cached after first check."""
        reset_dependency_cache()
        # First call computes the result
        result1 = module_available("os")
        # Second call should return cached result
        result2 = module_available("os")
        assert result1 == result2 is True

    def test_cache_can_be_reset(self) -> None:
        """Test that reset_dependency_cache clears the cache."""
        reset_dependency_cache()
        module_available("os")
        reset_dependency_cache()
        # After reset, the cache should be empty
        # We verify this indirectly by checking the function still works
        assert module_available("os") is True


class TestOptionalDependencyFlag:
    def test_flag_evaluates_lazily(self) -> None:
        """Test that flag only evaluates when used in boolean context."""
        reset_dependency_cache()
        flag = OptionalDependencyFlag("os")
        # Should not have evaluated yet - just creates the flag
        assert isinstance(flag, OptionalDependencyFlag)
        # Now evaluate
        assert bool(flag) is True

    def test_flag_for_missing_module(self) -> None:
        """Test that flag returns False for missing modules."""
        reset_dependency_cache()
        flag = OptionalDependencyFlag("nonexistent_module_xyz")
        assert bool(flag) is False

    def test_repr(self) -> None:
        """Test the flag repr."""
        flag = OptionalDependencyFlag("pyarrow")
        assert repr(flag) == "OptionalDependencyFlag('pyarrow')"


class TestDependencyFlag:
    def test_creates_flag_instance(self) -> None:
        """Test that dependency_flag returns OptionalDependencyFlag."""
        reset_dependency_cache()
        flag = dependency_flag("os")
        assert isinstance(flag, OptionalDependencyFlag)
        assert flag.module_name == "os"

    def test_flag_works_in_if_statement(self) -> None:
        """Test that flags work correctly in if statements."""
        reset_dependency_cache()
        OS_INSTALLED = dependency_flag("os")  # noqa: N806
        FAKE_INSTALLED = dependency_flag("nonexistent_xyz")  # noqa: N806

        os_result = bool(OS_INSTALLED)

        fake_result = bool(FAKE_INSTALLED)

        assert os_result is True
        assert fake_result is False


class TestEnsureDependency:
    def test_returns_module_when_installed(self) -> None:
        """Test that ensure_dependency returns the module when installed."""
        reset_dependency_cache()
        os_module = ensure_dependency("os")
        assert os_module.__name__ == "os"

    def test_raises_for_missing_module(self) -> None:
        """Test that ensure_dependency raises MissingDependencyError."""
        reset_dependency_cache()
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("nonexistent_module_xyz")

        assert "nonexistent_module_xyz" in str(exc_info.value)

    def test_uses_custom_install_package(self) -> None:
        """Test that custom install_package is used in error message."""
        reset_dependency_cache()
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("nonexistent", install_package="my-package")

        error_msg = str(exc_info.value)
        assert "vertebra[my-package]" in error_msg


class TestEnsurePyarrow:
    def test_returns_pyarrow_when_installed(self) -> None:
        """Test that ensure_pyarrow returns pyarrow module."""
        reset_dependency_cache()
        # pyarrow is installed in test environment
        pa = ensure_pyarrow()
        assert pa.__name__ == "pyarrow"

    def test_raises_with_install_instructions_when_missing(self) -> None:
        """Test that ensure_pyarrow raises with clear install instructions."""
        reset_dependency_cache()
        # Mock find_spec to return None for pyarrow
        with patch("vertebra.utils.module_loader.find_spec", return_value=None):
            reset_dependency_cache()  # Clear cache so mock is used
            with pytest.raises(MissingDependencyError) as exc_info:
                ensure_pyarrow()

        error_msg = str(exc_info.value)
        assert "vertebra[arrow]" in error_msg
