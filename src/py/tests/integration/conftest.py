import pytest
from tests.integration.helpers.database import (
    container_manager,
    postgres_service,
    mysql_service,
    redis_service,
    postgres_url,
    mysql_url,
    redis_url,
    pytest_sessionfinish,
)

def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Group tests by database type for pytest-xdist parallel execution."""
    for item in items:
        fixturenames = getattr(item, "fixturenames", [])
        if item.get_closest_marker("postgres") or "postgres_service" in fixturenames:
            item.add_marker(pytest.mark.xdist_group(name="postgres"))
        if item.get_closest_marker("mysql") or "mysql_service" in fixturenames:
            item.add_marker(pytest.mark.xdist_group(name="mysql"))
        if item.get_closest_marker("redis") or "redis_service" in fixturenames:
            item.add_marker(pytest.mark.xdist_group(name="redis"))