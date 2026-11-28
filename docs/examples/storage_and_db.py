"""Store a report attachment and record its object key in SQLite."""

__all__ = ("main",)

# --8<-- [start:example]
from vertebra.db import DatabaseRegistry, provide_driver
from vertebra.db.adapters.sqlite import SQLiteConfig
from vertebra.storage import SyncObjectStore


def main() -> None:
    """Save a report and retrieve it using the recorded attachment key."""
    store = SyncObjectStore("memory://")
    key = "reports/q1.json"
    store.put(key, b'{"quarter": "Q1"}')

    registry = DatabaseRegistry()
    db = provide_driver(SQLiteConfig(path=":memory:"), sync=True, registry=registry)
    try:
        db.execute("CREATE TABLE reports (id INTEGER PRIMARY KEY, object_key TEXT)")
        with db.transaction():
            db.execute("INSERT INTO reports (id, object_key) VALUES (?, ?)", (1, key))
        report = db.select_one("SELECT object_key FROM reports WHERE id = ?", (1,))
        print(store.get(str(report["object_key"])).decode())
    finally:
        db.close()


if __name__ == "__main__":
    main()

# --8<-- [end:example]
