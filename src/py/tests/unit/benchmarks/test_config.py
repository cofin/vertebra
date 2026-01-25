"""Unit tests for benchmarks.servers.config module."""

import pytest
from benchmarks.servers.config import (
    ALL_SERVERS,
    CATEGORY_1_SERVERS,
    CATEGORY_2_RUNTIMES,
    CATEGORY_3_FRAMEWORKS,
    BenchmarkCategory,
    InterfaceType,
    ServerConfig,
    get_available_servers,
    get_servers_by_category,
)


@pytest.mark.unit
def test_benchmark_category_enum() -> None:
    """Test BenchmarkCategory enum values."""
    assert BenchmarkCategory.SERVER.value == "server"
    assert BenchmarkCategory.RUNTIME.value == "runtime"
    assert BenchmarkCategory.FRAMEWORK.value == "framework"


@pytest.mark.unit
def test_interface_type_enum() -> None:
    """Test InterfaceType enum values."""
    assert InterfaceType.ASGI.value == "asgi"
    assert InterfaceType.NATIVE.value == "native"


@pytest.mark.unit
def test_server_config_dataclass() -> None:
    """Test ServerConfig dataclass creation."""
    config = ServerConfig(
        name="test_server",
        display_name="Test Server",
        category=BenchmarkCategory.SERVER,
        command=("uvicorn", "app:app"),
        port=8000,
        interface=InterfaceType.ASGI,
        startup_delay=2.0,
        requires_install=("uvicorn",),
        env={"ENV_VAR": "value"},
        notes="Test notes",
    )

    assert config.name == "test_server"
    assert config.display_name == "Test Server"
    assert config.category == BenchmarkCategory.SERVER
    assert config.command == ("uvicorn", "app:app")
    assert config.port == 8000
    assert config.interface == InterfaceType.ASGI
    assert config.startup_delay == 2.0
    assert config.requires_install == ("uvicorn",)
    assert config.env == {"ENV_VAR": "value"}
    assert config.notes == "Test notes"


@pytest.mark.unit
def test_server_config_frozen() -> None:
    """Test that ServerConfig is frozen (immutable)."""
    config = ServerConfig(
        name="test_server",
        display_name="Test Server",
        category=BenchmarkCategory.SERVER,
        command=("uvicorn", "app:app"),
        port=8000,
    )

    with pytest.raises(AttributeError):
        config.name = "changed"  # type: ignore[misc]


@pytest.mark.unit
def test_category_1_servers_structure() -> None:
    """Test Category 1 (ASGI Servers) structure."""
    assert len(CATEGORY_1_SERVERS) > 0

    # Check expected servers
    expected_servers = ["uvicorn_h11", "uvicorn_httptools", "granian"]
    for server_name in expected_servers:
        assert server_name in CATEGORY_1_SERVERS

    # All should be SERVER category and ASGI interface
    for config in CATEGORY_1_SERVERS.values():
        assert config.category == BenchmarkCategory.SERVER
        assert config.interface == InterfaceType.ASGI
        assert config.port > 0
        assert len(config.command) > 0


@pytest.mark.unit
def test_category_2_runtimes_structure() -> None:
    """Test Category 2 (Integrated Runtimes) structure."""
    assert len(CATEGORY_2_RUNTIMES) > 0

    # Check expected runtimes
    expected_runtimes = [
        "vertebra",
        "vertebra_asgi_litestar",
        "vertebra_native_litestar",
        "vertebra_fastapi",
        "sanic_standalone",
        "robyn",
    ]
    for runtime_name in expected_runtimes:
        assert runtime_name in CATEGORY_2_RUNTIMES

    # All should be RUNTIME category
    for config in CATEGORY_2_RUNTIMES.values():
        assert config.category == BenchmarkCategory.RUNTIME
        assert config.port > 0
        assert len(config.command) > 0


@pytest.mark.unit
def test_category_3_frameworks_structure() -> None:
    """Test Category 3 (Frameworks) structure."""
    assert len(CATEGORY_3_FRAMEWORKS) > 0

    # Check expected frameworks
    expected_frameworks = [
        "raw_asgi",
        "starlette",
        "fastapi_uvicorn",
        "fastapi_granian",
        "blacksheep",
        "litestar_uvicorn",
        "litestar_granian",
        "sanic_asgi",
    ]
    for framework_name in expected_frameworks:
        assert framework_name in CATEGORY_3_FRAMEWORKS

    # All should be FRAMEWORK category and ASGI interface
    for config in CATEGORY_3_FRAMEWORKS.values():
        assert config.category == BenchmarkCategory.FRAMEWORK
        assert config.interface == InterfaceType.ASGI
        assert config.port > 0
        assert len(config.command) > 0


@pytest.mark.unit
def test_all_servers_combined() -> None:
    """Test that ALL_SERVERS combines all categories."""
    total_count = len(CATEGORY_1_SERVERS) + len(CATEGORY_2_RUNTIMES) + len(CATEGORY_3_FRAMEWORKS)

    assert len(ALL_SERVERS) == total_count

    # Check that all category servers are present
    for name in CATEGORY_1_SERVERS:
        assert name in ALL_SERVERS
    for name in CATEGORY_2_RUNTIMES:
        assert name in ALL_SERVERS
    for name in CATEGORY_3_FRAMEWORKS:
        assert name in ALL_SERVERS


@pytest.mark.unit
def test_unique_ports() -> None:
    """Test that all servers have unique ports."""
    ports = [config.port for config in ALL_SERVERS.values()]
    assert len(ports) == len(set(ports)), "Duplicate ports found"


@pytest.mark.unit
def test_uvicorn_h11_config() -> None:
    """Test Uvicorn (h11) server configuration."""
    config = CATEGORY_1_SERVERS["uvicorn_h11"]

    assert config.name == "uvicorn_h11"
    assert config.display_name == "Uvicorn (h11)"
    assert config.category == BenchmarkCategory.SERVER
    assert config.port == 8002
    assert "uvicorn" in config.command
    assert "--http" in config.command
    assert "h11" in config.command


@pytest.mark.unit
def test_uvicorn_httptools_config() -> None:
    """Test Uvicorn (httptools) server configuration."""
    config = CATEGORY_1_SERVERS["uvicorn_httptools"]

    assert config.name == "uvicorn_httptools"
    assert config.display_name == "Uvicorn (httptools)"
    assert config.category == BenchmarkCategory.SERVER
    assert config.port == 8003
    assert "httptools" in config.command


@pytest.mark.unit
def test_granian_config() -> None:
    """Test Granian server configuration."""
    config = CATEGORY_1_SERVERS["granian"]

    assert config.name == "granian"
    assert config.display_name == "Granian (ASGI)"
    assert config.category == BenchmarkCategory.SERVER
    assert config.port == 8004
    assert "granian" in config.command


@pytest.mark.unit
def test_vertebra_config() -> None:
    """Test Vertebra runtime configuration."""
    config = CATEGORY_2_RUNTIMES["vertebra"]

    assert config.name == "vertebra"
    assert config.display_name == "Vertebra"
    assert config.category == BenchmarkCategory.RUNTIME
    assert config.port == 8001
    assert "vertebra" in config.command
    assert "serve" in config.command


@pytest.mark.unit
def test_robyn_config() -> None:
    """Test Robyn runtime configuration."""
    config = CATEGORY_2_RUNTIMES["robyn"]

    assert config.name == "robyn"
    assert config.interface == InterfaceType.NATIVE
    assert config.category == BenchmarkCategory.RUNTIME


@pytest.mark.unit
def test_raw_asgi_config() -> None:
    """Test Raw ASGI framework configuration."""
    config = CATEGORY_3_FRAMEWORKS["raw_asgi"]

    assert config.name == "raw_asgi"
    assert config.display_name == "Raw ASGI"
    assert config.category == BenchmarkCategory.FRAMEWORK
    assert config.port == 8020


@pytest.mark.unit
def test_fastapi_config() -> None:
    """Test FastAPI framework configuration."""
    config = CATEGORY_3_FRAMEWORKS["fastapi_uvicorn"]

    assert config.name == "fastapi_uvicorn"
    assert config.display_name == "FastAPI (Uvicorn)"
    assert config.category == BenchmarkCategory.FRAMEWORK
    assert "uvicorn" in config.command


@pytest.mark.unit
def test_get_servers_by_category_server() -> None:
    """Test get_servers_by_category() for SERVER category."""
    servers = get_servers_by_category(BenchmarkCategory.SERVER)

    assert len(servers) > 0
    assert all(config.category == BenchmarkCategory.SERVER for config in servers.values())
    assert servers == CATEGORY_1_SERVERS


@pytest.mark.unit
def test_get_servers_by_category_runtime() -> None:
    """Test get_servers_by_category() for RUNTIME category."""
    servers = get_servers_by_category(BenchmarkCategory.RUNTIME)

    assert len(servers) > 0
    assert all(config.category == BenchmarkCategory.RUNTIME for config in servers.values())
    assert servers == CATEGORY_2_RUNTIMES


@pytest.mark.unit
def test_get_servers_by_category_framework() -> None:
    """Test get_servers_by_category() for FRAMEWORK category."""
    servers = get_servers_by_category(BenchmarkCategory.FRAMEWORK)

    assert len(servers) > 0
    assert all(config.category == BenchmarkCategory.FRAMEWORK for config in servers.values())
    assert servers == CATEGORY_3_FRAMEWORKS


@pytest.mark.unit
def test_get_available_servers_all() -> None:
    """Test get_available_servers() returns all servers when no filter."""
    servers = get_available_servers()

    assert len(servers) == len(ALL_SERVERS)
    assert servers == ALL_SERVERS


@pytest.mark.unit
def test_get_available_servers_filtered() -> None:
    """Test get_available_servers() with name filter."""
    servers = get_available_servers(names=["uvicorn_h11", "granian"])

    assert len(servers) == 2
    assert "uvicorn_h11" in servers
    assert "granian" in servers
    assert servers["uvicorn_h11"] == ALL_SERVERS["uvicorn_h11"]
    assert servers["granian"] == ALL_SERVERS["granian"]


@pytest.mark.unit
def test_get_available_servers_invalid_names() -> None:
    """Test get_available_servers() filters out invalid names."""
    servers = get_available_servers(names=["uvicorn_h11", "invalid_server", "granian"])

    # Should only return valid servers
    assert len(servers) == 2
    assert "uvicorn_h11" in servers
    assert "granian" in servers
    assert "invalid_server" not in servers


@pytest.mark.unit
def test_get_available_servers_empty() -> None:
    """Test get_available_servers() with empty names list."""
    servers = get_available_servers(names=[])

    assert len(servers) == 0


@pytest.mark.unit
def test_get_available_servers_single() -> None:
    """Test get_available_servers() with single server name."""
    servers = get_available_servers(names=["vertebra"])

    assert len(servers) == 1
    assert "vertebra" in servers
    assert servers["vertebra"].display_name == "Vertebra"


@pytest.mark.unit
def test_server_requires_install() -> None:
    """Test that servers have requires_install information."""
    uvicorn_config = CATEGORY_1_SERVERS["uvicorn_h11"]
    assert len(uvicorn_config.requires_install) > 0
    assert "uvicorn" in uvicorn_config.requires_install[0]

    granian_config = CATEGORY_1_SERVERS["granian"]
    assert len(granian_config.requires_install) > 0
    assert "granian" in granian_config.requires_install[0]


@pytest.mark.unit
def test_server_startup_delays() -> None:
    """Test that servers have reasonable startup delays."""
    for config in ALL_SERVERS.values():
        assert config.startup_delay > 0
        assert config.startup_delay <= 5.0  # No more than 5 seconds


@pytest.mark.unit
def test_server_notes_present() -> None:
    """Test that servers have descriptive notes."""
    for config in ALL_SERVERS.values():
        # Notes can be empty string, but should be a string
        assert isinstance(config.notes, str)


@pytest.mark.unit
def test_category_1_all_asgi_interface() -> None:
    """Test that all Category 1 servers use ASGI interface."""
    for config in CATEGORY_1_SERVERS.values():
        assert config.interface == InterfaceType.ASGI


@pytest.mark.unit
def test_category_3_all_asgi_interface() -> None:
    """Test that all Category 3 frameworks use ASGI interface."""
    for config in CATEGORY_3_FRAMEWORKS.values():
        assert config.interface == InterfaceType.ASGI


@pytest.mark.unit
def test_native_interfaces() -> None:
    """Test that native interface servers are properly marked."""
    native_servers = [config for config in ALL_SERVERS.values() if config.interface == InterfaceType.NATIVE]

    # Should have at least Robyn and Sanic standalone as native
    native_names = [config.name for config in native_servers]
    assert "robyn" in native_names
    assert "sanic_standalone" in native_names
