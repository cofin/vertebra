from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_config() -> MagicMock:
    config = MagicMock()
    config.extension_config = {}
    return config


def test_mysql_event_queue_store_column_types(mock_config) -> None:
    from vertebra.extensions.sqlspec.mysql.events.store import MysqlEventQueueStore

    store = MysqlEventQueueStore(mock_config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "DATETIME(6)"


def test_mysql_event_queue_store_timestamp_default(mock_config) -> None:
    from vertebra.extensions.sqlspec.mysql.events.store import MysqlEventQueueStore

    store = MysqlEventQueueStore(mock_config)
    assert store._timestamp_default() == "CURRENT_TIMESTAMP(6)"
