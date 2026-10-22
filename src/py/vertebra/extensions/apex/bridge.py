"""Vertebra + Oracle APEX reactive islands, offline HLC sync, and federated DataFusion queries."""

from __future__ import annotations

import html
import re
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Literal

import msgspec

from vertebra.db.datafusion import DataFusionEngine, TableRegistration
from vertebra.extensions.apex.session import (
    ApexSessionContext,
    OraclePoolLike,
    OracleSessionManager,
)
from vertebra.framework.live import HybridLogicalClock, LiveState
from vertebra.utils.serializers import encode_json

__all__ = (
    "ApexFederatedQuery",
    "ApexFederatedSource",
    "ApexIslandRegistry",
    "ApexIslandSpec",
    "ApexOfflineMutation",
    "ApexOfflineSyncEngine",
    "ApexOfflineSyncResult",
)

_APEX_ITEM_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")
_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")


class ApexIslandSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Specification for mounting a reactive Vertebra island inside an Oracle APEX region."""

    component_name: str
    region_static_id: str
    live_channel: str | None = None
    bound_page_items: tuple[str, ...] = ()
    props: dict[str, Any] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _SAFE_IDENT_RE.match(self.component_name):
            msg = f"Invalid APEX island component_name: {self.component_name!r}"
            raise ValueError(msg)
        if not _SAFE_IDENT_RE.match(self.region_static_id):
            msg = f"Invalid APEX region_static_id: {self.region_static_id!r}"
            raise ValueError(msg)
        for item_name in self.bound_page_items:
            if not _APEX_ITEM_RE.match(item_name):
                msg = f"Invalid bound APEX page item identifier: {item_name!r}"
                raise ValueError(msg)


class ApexIslandRegistry:
    """Renders ``<vertebra-island>`` mounts and synchronizes APEX page items with ``LiveState``."""

    __slots__ = ("_islands",)

    def __init__(self) -> None:
        self._islands: dict[str, ApexIslandSpec] = {}

    @property
    def islands(self) -> dict[str, ApexIslandSpec]:
        """Return registered island specifications keyed by ``region_static_id``."""
        return dict(self._islands)

    def register(self, spec: ApexIslandSpec) -> ApexIslandSpec:
        """Register an ``ApexIslandSpec`` for an APEX region static ID."""
        self._islands[spec.region_static_id] = spec
        return spec

    def render_island_html(
        self,
        region_static_id: str,
        context: ApexSessionContext | None = None,
        *,
        extra_props: dict[str, Any] | None = None,
    ) -> str:
        """Render a ``<vertebra-island>`` HTML element hydrated with bound APEX page items."""
        spec = self._islands.get(region_static_id)
        if spec is None:
            msg = f"Unknown APEX island region_static_id: {region_static_id!r}"
            raise KeyError(msg)
        merged_props: dict[str, Any] = dict(spec.props)
        if context is not None:
            bound_values = {
                item: context.page_items[item] for item in spec.bound_page_items if item in context.page_items
            }
            if bound_values:
                merged_props["pageItems"] = bound_values
            merged_props["apexSession"] = {
                "appId": context.app_id,
                "pageId": context.page_id,
                "sessionId": context.session_id,
                "appUser": context.app_user,
            }
        if extra_props:
            merged_props.update(extra_props)
        props_json = html.escape(encode_json(merged_props), quote=True)
        attrs: list[str] = [
            f'data-component="{html.escape(spec.component_name, quote=True)}"',
            f'data-region-id="{html.escape(spec.region_static_id, quote=True)}"',
            f'data-props="{props_json}"',
        ]
        if spec.live_channel is not None:
            attrs.append(f'data-live-channel="{html.escape(spec.live_channel, quote=True)}"')
        if spec.bound_page_items:
            bound_csv = html.escape(",".join(spec.bound_page_items), quote=True)
            attrs.append(f'data-bound-items="{bound_csv}"')
        return f"<vertebra-island {' '.join(attrs)}></vertebra-island>"

    @staticmethod
    def sync_context_to_live_state(
        context: ApexSessionContext,
        state: LiveState,
        *,
        item_to_field_map: dict[str, str] | None = None,
    ) -> list[str]:
        """Push ``ApexSessionContext.page_items`` into a ``LiveState`` instance."""
        updated: list[str] = []
        mapping = item_to_field_map or {item: item.lower() for item in context.page_items}
        for item_name, field_name in mapping.items():
            if item_name in context.page_items:
                setattr(state, field_name, context.page_items[item_name])
                updated.append(field_name)
        return updated

    @staticmethod
    def sync_live_state_to_context(
        state: LiveState,
        context: ApexSessionContext,
        *,
        field_to_item_map: dict[str, str],
    ) -> ApexSessionContext:
        """Project ``LiveState`` fields back into an updated ``ApexSessionContext``."""
        snap = state.snapshot()
        merged_items = dict(context.page_items)
        for field_name, item_name in field_to_item_map.items():
            if not _APEX_ITEM_RE.match(item_name):
                msg = f"Invalid APEX page item identifier: {item_name!r}"
                raise ValueError(msg)
            if field_name in snap and snap[field_name] is not None:
                merged_items[item_name] = str(snap[field_name])
        return msgspec.structs.replace(context, page_items=merged_items)


class ApexOfflineMutation(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Client mutation queued while an Oracle APEX browser client was offline."""

    mutation_id: str
    region_id: str
    operation: Literal["set_item", "row_upsert", "row_delete", "duality_patch"]
    hlc_timestamp: int
    item_name: str | None = None
    value: Any = None
    etag: str | None = None

    def __post_init__(self) -> None:
        if self.operation == "set_item" and (not self.item_name or not _APEX_ITEM_RE.match(self.item_name)):
            msg = f"Invalid APEX page item identifier for set_item: {self.item_name!r}"
            raise ValueError(msg)


class ApexOfflineSyncResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Outcome of synchronizing an offline batch of Oracle APEX client mutations."""

    applied_ids: tuple[str, ...]
    conflict_ids: tuple[str, ...]
    server_hlc: int
    updated_context: ApexSessionContext


class ApexOfflineSyncEngine:
    """Reconciles offline Oracle APEX mutations using Hybrid Logical Clocks and ETag checks."""

    __slots__ = ("_clock", "_etags", "_item_watermarks", "_row_handler")

    def __init__(
        self,
        *,
        row_handler: Callable[[ApexSessionContext, ApexOfflineMutation], Awaitable[bool]] | None = None,
    ) -> None:
        self._clock = HybridLogicalClock()
        self._item_watermarks: dict[str, int] = {}
        self._etags: dict[str, str] = {}
        self._row_handler = row_handler

    @property
    def current_hlc(self) -> int:
        """Return the current packed 64-bit Hybrid Logical Clock value."""
        return self._clock.pack()

    def register_etag(self, resource_key: str, etag: str) -> None:
        """Record the current server ETag for a region row or 26ai Duality View document."""
        self._etags[resource_key] = etag

    def record_server_item_write(self, item_name: str, hlc_timestamp: int | None = None) -> int:
        """Record a server-side write watermark for ``item_name``."""
        ts = self._clock.tick() if hlc_timestamp is None else self._clock.receive(hlc_timestamp)
        self._item_watermarks[item_name] = ts
        return ts

    async def sync_batch(
        self,
        context: ApexSessionContext,
        mutations: Sequence[ApexOfflineMutation],
        *,
        pool: OraclePoolLike | None = None,
        live_state: LiveState | None = None,
    ) -> ApexOfflineSyncResult:
        """Replay offline APEX mutations in HLC order, persists state to Oracle, and returns sync status."""
        ordered = sorted(mutations, key=lambda m: m.hlc_timestamp)
        applied: list[str] = []
        conflicts: list[str] = []
        updated_items = dict(context.page_items)

        for mutation in ordered:
            self._clock.receive(mutation.hlc_timestamp)
            if mutation.etag is not None:
                resource_key = f"{mutation.region_id}:{mutation.item_name or mutation.mutation_id}"
                expected_etag = self._etags.get(resource_key)
                if expected_etag is not None and expected_etag != mutation.etag:
                    conflicts.append(mutation.mutation_id)
                    continue

            if mutation.operation == "set_item" and mutation.item_name is not None:
                watermark = self._item_watermarks.get(mutation.item_name, 0)
                if mutation.hlc_timestamp < watermark:
                    conflicts.append(mutation.mutation_id)
                    continue
                self._item_watermarks[mutation.item_name] = mutation.hlc_timestamp
                str_val = "" if mutation.value is None else str(mutation.value)
                updated_items[mutation.item_name] = str_val
                if live_state is not None:
                    setattr(live_state, mutation.item_name.lower(), str_val)
                applied.append(mutation.mutation_id)
            else:
                if self._row_handler is not None:
                    ok = await self._row_handler(context, mutation)
                    if not ok:
                        conflicts.append(mutation.mutation_id)
                        continue
                applied.append(mutation.mutation_id)

        new_context = msgspec.structs.replace(context, page_items=updated_items)
        if pool is not None and applied:
            async with OracleSessionManager.acquire_scoped(
                pool,
                new_context,
                module="VertebraAPEX",
                action="apex.offline_sync",
            ):
                pass
        return ApexOfflineSyncResult(
            applied_ids=tuple(applied),
            conflict_ids=tuple(conflicts),
            server_hlc=self._clock.tick(),
            updated_context=new_context,
        )


class ApexFederatedSource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """External or Oracle dataset registered for federated queries alongside an APEX session."""

    alias: str
    uri: str
    format: Literal["parquet", "vortex", "arrow", "postgres", "oracle"] = "parquet"

    def __post_init__(self) -> None:
        if not _APEX_ITEM_RE.match(self.alias):
            msg = f"Invalid federated table alias: {self.alias!r}"
            raise ValueError(msg)


class ApexFederatedQuery:
    """Federates Oracle APEX session and ORDS datasets with Arrow/Parquet/Postgres in DataFusion."""

    __slots__ = ("_engine",)

    def __init__(self, engine: DataFusionEngine | None = None) -> None:
        self._engine = engine or DataFusionEngine()

    @property
    def engine(self) -> DataFusionEngine:
        """Return the underlying ``DataFusionEngine``."""
        return self._engine

    def register_source(self, source: ApexFederatedSource) -> TableRegistration:
        """Register an Oracle, Postgres, Parquet, or Arrow source in the federated engine."""
        reg = TableRegistration(name=source.alias, uri=source.uri, format=source.format)
        return self._engine.register_table(reg)

    def build_federated_plan(
        self,
        context: ApexSessionContext,
        sql: str,
    ) -> dict[str, Any]:
        """Compile a federated execution plan enriched with ``ApexSessionContext`` bindings."""
        plan = self._engine.explain_plan(sql)
        session_binds: dict[str, Any] = {
            "APP_ID": context.app_id,
            "APP_PAGE_ID": context.page_id,
            "APP_SESSION": context.session_id,
            "APP_USER": context.app_user,
            **context.page_items,
        }
        return {
            **plan,
            "apex_session": {
                "app_id": context.app_id,
                "page_id": context.page_id,
                "session_id": context.session_id,
                "app_user": context.app_user,
            },
            "session_binds": session_binds,
        }
