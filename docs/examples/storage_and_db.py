"""Transactional object storage and SQLite database driver example."""

from advanced_alchemy.types.file_object import FileObject

from vertebra.db.sqlite import SQLiteConfig, SyncSQLiteDriver
from vertebra.extensions.alchemy.storage import ObjectStoreBackend
from vertebra.storage import SyncObjectStore

__all__ = ("main",)


# --8<-- [start:example]
def main() -> None:
    """Save model file attachments and execute SQLite queries."""
    store = SyncObjectStore("memory://")
    backend = ObjectStoreBackend(store=store, prefix="reports")

    file_obj = FileObject(backend=backend, filename="q1.json")
    saved = backend.save_object(file_obj, b'{"quarter": "Q1"}')
    assert saved.size == 17
    assert backend.get_content("q1.json") == b'{"quarter": "Q1"}'

    db = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    try:
        db.execute("CREATE TABLE reports (id INTEGER PRIMARY KEY, key TEXT NOT NULL)")
        file_obj2 = FileObject(backend=backend, filename="q2.json")
        backend.save_object(file_obj2, b'{"quarter": "Q2"}')
        db.execute("INSERT INTO reports (key) VALUES (?)", "reports/q2.json")
        db.commit()
        rows = db.select("SELECT key FROM reports")
        assert rows == [{"key": "reports/q2.json"}]
        assert backend.get_content("q2.json") == b'{"quarter": "Q2"}'
    finally:
        db.close()


# --8<-- [end:example]


if __name__ == "__main__":
    main()
