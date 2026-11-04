"""Transactional object storage and SQLite database driver example."""

from advanced_alchemy.types.file_object import FileObject

from vertebra.db import DatabaseRegistry, NotFoundError, provide_driver
from vertebra.db.adapters.sqlite import SQLiteConfig
from vertebra.extensions.alchemy.storage import ObjectStoreBackend
from vertebra.storage import SyncObjectStore

__all__ = ("main",)


# --8<-- [start:example]
def main() -> None:
    """Save model file attachments and execute transactional SQLite queries."""
    store = SyncObjectStore("memory://")
    backend = ObjectStoreBackend(store=store, prefix="reports")

    file_obj = FileObject(backend=backend, filename="q1.json")
    saved = backend.save_object(file_obj, b'{"quarter": "Q1"}')
    assert saved.size == 17
    assert backend.get_content("q1.json") == b'{"quarter": "Q1"}'

    registry = DatabaseRegistry()
    db = provide_driver(SQLiteConfig(path=":memory:"), sync=True, registry=registry)
    try:
        db.execute("CREATE TABLE reports (id INTEGER PRIMARY KEY, key TEXT NOT NULL)")
        file_obj2 = FileObject(backend=backend, filename="q2.json")
        backend.save_object(file_obj2, b'{"quarter": "Q2"}')
        with db.transaction():
            inserted = db.execute("INSERT INTO reports (key) VALUES (?)", ("reports/q2.json",))
            assert inserted == 1
        row = db.select_one("SELECT key FROM reports WHERE id = ?", (1,))
        assert row == {"key": "reports/q2.json"}
        assert db.select_one_or_none("SELECT key FROM reports WHERE id = ?", (99,)) is None
        with db.select_to_arrow("SELECT id, key FROM reports") as cursor:
            assert cursor.row_count == 1
        try:
            db.select_one("SELECT key FROM reports WHERE id = ?", (99,))
        except NotFoundError as exc:
            assert isinstance(exc, ValueError)
        assert backend.get_content("q2.json") == b'{"quarter": "Q2"}'
    finally:
        db.close()


# --8<-- [end:example]


if __name__ == "__main__":
    main()
