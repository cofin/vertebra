"""SQLite event queue store implementing DDL generation over Vertebra native drivers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, NotRequired

from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    apply_pragmas_async,
    apply_pragmas_sync,
    extension_pragma_statements,
)

if TYPE_CHECKING:
    from sqlspec.migrations.schema import DropStatement, SchemaEnsureResult, SchemaTarget

__all__ = ("SQLiteEventQueueStore", "SQLiteEventsConfig")


class SQLiteEventsConfig(EventsConfig):
    """SQLite-specific events configuration extending `EventsConfig` with PRAGMA options."""

    pragma_profile: NotRequired[bool]
    """Apply the built-in SQLite extension PRAGMA profile when preparing schemas."""

    pragma_overrides: NotRequired[dict[str, str | int | bool]]
    """Validated SQLite PRAGMA overrides applied after any optional profile."""


class SQLiteEventQueueStore(BaseEventQueueStore[Any]):
    """SQLite event queue store storing JSON payloads as `TEXT` with `TIMESTAMP` columns."""

    __slots__ = ("_pragma_statements",)

    adapter_name = "vertebra_sqlite"
    dialect: ClassVar[str] = "sqlite"
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset(
        {
            "pragma_overrides",
            "pragma_profile",
        }
    )

    def __init__(self, config: Any) -> None:
        """Initialize the SQLite event queue store from an adapter configuration."""
        super().__init__(config)
        self._pragma_statements = extension_pragma_statements(config, "events")

    def prepare_schema_sync(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs before queue DDL execution."""
        apply_pragmas_sync(driver, self._pragma_statements)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs asynchronously before queue DDL execution."""
        await apply_pragmas_async(driver, self._pragma_statements)

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the SQLite event queue table."""
        return super().get_schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Apply configured PRAGMAs and create any missing SQLite companion queue index."""
        if self._pragma_statements:
            apply_pragmas_sync(driver, self._pragma_statements)
        super().reconcile_companion_objects_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Apply configured PRAGMAs and create any missing SQLite companion queue index asynchronously."""
        if self._pragma_statements:
            await apply_pragmas_async(driver, self._pragma_statements)
        await super().reconcile_companion_objects_async(driver)

    def reconcile_schema_sync(self, driver: Any) -> SchemaEnsureResult:
        """Apply configured PRAGMAs and reconcile SQLite queue schema synchronously."""
        self.prepare_schema_sync(driver)
        return super().reconcile_schema_sync(driver)

    async def reconcile_schema_async(self, driver: Any) -> SchemaEnsureResult:
        """Apply configured PRAGMAs and reconcile SQLite queue schema asynchronously."""
        await self.prepare_schema_async(driver)
        return await super().reconcile_schema_async(driver)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return SQLite downgrade drop statements for the event queue table."""
        return super().downgrade_drops()

    def _column_types(self) -> tuple[str, str, str]:
        """Return `(payload_type, metadata_type, timestamp_type)` for SQLite DDL."""
        return "TEXT", "TEXT", "TIMESTAMP"
