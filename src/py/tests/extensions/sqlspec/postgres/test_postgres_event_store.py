from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.postgres.events.store import PostgresEventQueueStore


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.extension_config = {}
    return config


def test_postgres_event_queue_store_column_types(mock_config):
    from vertebra.extensions.sqlspec.postgres.events.store import PostgresEventQueueStore

    store = PostgresEventQueueStore(mock_config)
    payload_type, metadata_type, timestamp_type = store._column_types()
    
    assert payload_type == "JSONB"
    assert metadata_type == "JSONB"
    assert timestamp_type == "TIMESTAMPTZ"
