"""Tests for banner hooks module."""

from __future__ import annotations

from vertebra.cli.banner.hooks import BannerSection, HookRegistry, banner_section, get_registry


class TestBannerSection:
    """Test BannerSection dataclass."""

    def test_default_values(self) -> None:
        section = BannerSection(name="Test", callback=dict)
        assert section.name == "Test"
        assert section.order == 100
        assert section.mode == "all"

    def test_custom_values(self) -> None:
        section = BannerSection(name="Custom", callback=dict, order=50, mode="dev")
        assert section.name == "Custom"
        assert section.order == 50
        assert section.mode == "dev"


class TestHookRegistry:
    """Test HookRegistry class."""

    def test_register_adds_section(self) -> None:
        registry = HookRegistry()
        registry.register("Test", lambda: {"Key": "Value"})
        sections = registry.collect()
        assert len(sections) == 1
        assert sections[0][0] == "Test"

    def test_register_maintains_order(self) -> None:
        registry = HookRegistry()
        registry.register("B", lambda: {"b": "2"}, order=20)
        registry.register("A", lambda: {"a": "1"}, order=10)
        registry.register("C", lambda: {"c": "3"}, order=30)

        sections = registry.collect()
        names = [s[0] for s in sections]
        assert names == ["A", "B", "C"]

    def test_collect_filters_by_mode(self) -> None:
        registry = HookRegistry()
        registry.register("All", lambda: {"a": "1"}, mode="all")
        registry.register("Dev", lambda: {"b": "2"}, mode="dev")
        registry.register("Serve", lambda: {"c": "3"}, mode="serve")

        # Serve mode should get "All" and "Serve"
        serve_sections = registry.collect(mode="serve")
        names = [s[0] for s in serve_sections]
        assert "All" in names
        assert "Serve" in names
        assert "Dev" not in names

        # Dev mode should get "All" and "Dev"
        dev_sections = registry.collect(mode="dev")
        names = [s[0] for s in dev_sections]
        assert "All" in names
        assert "Dev" in names
        assert "Serve" not in names

    def test_collect_handles_callback_errors(self) -> None:
        registry = HookRegistry()

        def broken_callback() -> dict[str, str]:
            msg = "Intentional error"
            raise RuntimeError(msg)

        registry.register("Broken", broken_callback)
        sections = registry.collect()

        assert len(sections) == 1
        assert sections[0][0] == "Broken"
        assert "Error" in sections[0][1]

    def test_collect_excludes_empty_results(self) -> None:
        registry = HookRegistry()
        registry.register("Empty", dict)
        registry.register("NonEmpty", lambda: {"Key": "Value"})

        sections = registry.collect()
        names = [s[0] for s in sections]

        assert "Empty" not in names
        assert "NonEmpty" in names

    def test_clear_removes_all_sections(self) -> None:
        registry = HookRegistry()
        registry.register("A", lambda: {"a": "1"})
        registry.register("B", lambda: {"b": "2"})

        registry.clear()
        sections = registry.collect()
        assert len(sections) == 0


class TestBannerSectionDecorator:
    """Test the banner_section decorator."""

    def test_registers_function(self) -> None:
        # Get fresh registry and clear it
        registry = get_registry()
        registry.clear()

        @banner_section("Test Section")
        def my_section() -> dict[str, str]:
            return {"Key": "Value"}

        sections = registry.collect()
        assert len(sections) == 1
        assert sections[0][0] == "Test Section"
        assert sections[0][1] == {"Key": "Value"}

    def test_preserves_function(self) -> None:
        registry = get_registry()
        registry.clear()

        @banner_section("Another")
        def my_func() -> dict[str, str]:
            return {"X": "Y"}

        # Function should still be callable
        result = my_func()
        assert result == {"X": "Y"}

    def test_accepts_order_parameter(self) -> None:
        registry = get_registry()
        registry.clear()

        @banner_section("Ordered", order=25)
        def ordered_section() -> dict[str, str]:
            return {"A": "B"}

        # Verify it was registered
        sections = registry.collect()
        assert len(sections) == 1

    def test_accepts_mode_parameter(self) -> None:
        registry = get_registry()
        registry.clear()

        @banner_section("DevOnly", mode="dev")
        def dev_section() -> dict[str, str]:
            return {"D": "E"}

        # Should not appear in serve mode
        serve_sections = registry.collect(mode="serve")
        assert len(serve_sections) == 0

        # Should appear in dev mode
        dev_sections = registry.collect(mode="dev")
        assert len(dev_sections) == 1


class TestGetRegistry:
    """Test the global registry accessor."""

    def test_returns_registry_instance(self) -> None:
        registry = get_registry()
        assert isinstance(registry, HookRegistry)

    def test_returns_same_instance(self) -> None:
        registry1 = get_registry()
        registry2 = get_registry()
        assert registry1 is registry2
