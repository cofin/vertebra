from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.sqlite.events.store import SqliteEventQueueStore


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.extension_config = {}
    return config


def test_sqlite_event_queue_store_column_types(mock_config):
    from vertebra.extensions.sqlspec.sqlite.events.store import SqliteEventQueueStore

    store = SqliteEventQueueStore(mock_config)
    payload_type, metadata_type, timestamp_type = store._column_types()
    
    assert payload_type == "TEXT"
    assert metadata_type == "TEXT"
    assert timestamp_type == "TIMESTAMP"
