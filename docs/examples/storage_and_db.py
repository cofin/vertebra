"""Transactional object storage staging and SQLite database driver example."""

from vertebra.db.sqlite import SqliteConfig, SyncSqliteDriver
from vertebra.storage import ObjectStore, UploadTracker

__all__ = ("main",)


# --8<-- [start:example]
def main() -> None:
    """Stage cloud uploads alongside SQLite queries and verify rollback cleanup."""
    store = ObjectStore.from_memory(namespace="docs-example")
    tracker = UploadTracker(default_store=store)

    tracker.stage_upload("reports/q1.json", b'{"quarter": "Q1"}')
    assert tracker.staged_count == 1
    flushed = tracker.before_flush()
    assert len(flushed) == 1
    assert store.exists("reports/q1.json")

    reclaimed = tracker.after_rollback()
    assert reclaimed == 1
    assert not store.exists("reports/q1.json")

    db = SyncSqliteDriver(SqliteConfig(path=":memory:"))
    try:
        db.execute("CREATE TABLE reports (id INTEGER PRIMARY KEY, key TEXT NOT NULL)")
        tracker.stage_upload("reports/q2.json", b'{"quarter": "Q2"}')
        tracker.before_flush()
        db.execute("INSERT INTO reports (key) VALUES (?)", "reports/q2.json")
        db.commit()
        tracker.after_commit()
        rows = db.select("SELECT key FROM reports")
        assert rows == [{"key": "reports/q2.json"}]
        assert store.get("reports/q2.json") == b'{"quarter": "Q2"}'
    finally:
        db.close()


# --8<-- [end:example]


if __name__ == "__main__":
    main()
