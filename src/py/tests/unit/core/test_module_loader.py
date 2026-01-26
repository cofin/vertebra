"""Tests for vertebra.utils.module_loader optional dependency utilities."""

from unittest import mock

import pytest

from vertebra.common.exceptions import MissingDependencyError
from vertebra.utils.module_loader import (
    OptionalDependencyFlag,
    dependency_flag,
    ensure_dependency,
    module_available,
    reset_dependency_cache,
)


class TestModuleAvailable:
    """Tests for module_available function."""

    def setup_method(self) -> None:
        """Reset cache before each test."""
        reset_dependency_cache()

    def test_returns_true_for_installed_module(self) -> None:
        """module_available returns True for installed modules."""
        assert module_available("json") is True

    def test_returns_false_for_missing_module(self) -> None:
        """module_available returns False for missing modules."""
        assert module_available("definitely_not_a_real_module_12345") is False

    def test_caches_result(self) -> None:
        """module_available caches the result."""
        # First call
        result1 = module_available("json")
        # Second call should use cache
        result2 = module_available("json")
        assert result1 == result2

    def test_handles_import_error_gracefully(self) -> None:
        """module_available handles ImportError gracefully."""
        with mock.patch("vertebra.utils.module_loader.find_spec", side_effect=ModuleNotFoundError):
            reset_dependency_cache()
            assert module_available("some_module") is False


class TestDependencyFlag:
    """Tests for dependency_flag function."""

    def setup_method(self) -> None:
        """Reset cache before each test."""
        reset_dependency_cache()

    def test_returns_optional_dependency_flag(self) -> None:
        """dependency_flag returns an OptionalDependencyFlag instance."""
        flag = dependency_flag("json")
        assert isinstance(flag, OptionalDependencyFlag)

    def test_returns_true_for_installed_module(self) -> None:
        """dependency_flag returns True for installed modules."""
        flag = dependency_flag("json")  # stdlib, always available
        assert bool(flag) is True

    def test_returns_false_for_missing_module(self) -> None:
        """dependency_flag returns False for missing modules."""
        flag = dependency_flag("definitely_not_a_real_module_12345")
        assert bool(flag) is False

    def test_lazy_evaluation(self) -> None:
        """dependency_flag doesn't check availability until used in boolean context."""
        # Creating the flag should not raise or check anything
        flag = dependency_flag("any_module_name_here")
        # The type should be OptionalDependencyFlag
        assert isinstance(flag, OptionalDependencyFlag)
        # Module name is stored
        assert flag.module_name == "any_module_name_here"

    def test_repr(self) -> None:
        """dependency_flag has a useful repr."""
        flag = dependency_flag("json")
        assert "json" in repr(flag)
        assert "OptionalDependencyFlag" in repr(flag)


class TestEnsureDependency:
    """Tests for ensure_dependency function."""

    def setup_method(self) -> None:
        """Reset cache before each test."""
        reset_dependency_cache()

    def test_raises_missing_dependency_error_for_missing_module(self) -> None:
        """ensure_dependency raises MissingDependencyError for missing modules."""
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("nonexistent_module_12345")

        error = exc_info.value
        assert "nonexistent_module_12345" in str(error)

    def test_includes_custom_install_package(self) -> None:
        """ensure_dependency uses custom install package in error message."""
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("fake_module", install_package="vertebra[fake]")

        assert "vertebra[fake]" in str(exc_info.value)

    def test_returns_module_when_available(self) -> None:
        """ensure_dependency returns the module when it's installed."""
        # json is always available in Python stdlib
        import json

        result = ensure_dependency("json")
        assert result is json

    def test_vertebra_install_package_format(self) -> None:
        """ensure_dependency formats vertebra extras correctly."""
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("fake_module", install_package="fake")

        # Should format as vertebra[fake]
        assert "vertebra[fake]" in str(exc_info.value)


class TestResetDependencyCache:
    """Tests for reset_dependency_cache function."""

    def test_clears_cache(self) -> None:
        """reset_dependency_cache clears the module availability cache."""
        # Populate the cache
        module_available("json")

        # Clear it
        reset_dependency_cache()

        # The cache should be empty (we can't directly check, but we can verify
        # that subsequent calls still work correctly)
        assert module_available("json") is True


class TestIntegration:
    """Integration tests for dependency utilities."""

    def setup_method(self) -> None:
        """Reset cache before each test."""
        reset_dependency_cache()

    def test_dependency_flag_used_in_conditional(self) -> None:
        """dependency_flag can be used in if statements."""
        flag = dependency_flag("json")

        if flag:
            import json

            assert json is not None
        else:
            pytest.fail("json should be available")

    def test_ensure_dependency_then_import(self) -> None:
        """ensure_dependency validates before import."""
        # This pattern should work
        ensure_dependency("json")
        import json

        assert json.dumps([1, 2, 3]) == "[1, 2, 3]"

    def test_missing_dependency_error_is_informative(self) -> None:
        """MissingDependencyError provides actionable information."""
        with pytest.raises(MissingDependencyError) as exc_info:
            ensure_dependency("litestar_fake_12345", install_package="vertebra[litestar]")

        error_message = str(exc_info.value)
        # Should mention the package
        assert "litestar_fake_12345" in error_message
        # Should provide install instructions
        assert "pip install" in error_message.lower() or "vertebra[litestar]" in error_message
