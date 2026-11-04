"""BrokerPlugin integration for Vertebra applications."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.extensions.broker.transport import (
    AsyncInMemoryBrokerTransport,
    AsyncSHMBrokerTransport,
    BrokerConfig,
    SyncInMemoryBrokerTransport,
    SyncSHMBrokerTransport,
)
from vertebra.telemetry.otel import DbPoolMetricSample

if TYPE_CHECKING:
    from vertebra.server.platform import ZPagesRegistry
    from vertebra.telemetry.otel import DbPoolRecorder


class BrokerPlugin:
    """Plugin managing shared database connection broker and statement cache."""

    def __init__(
        self,
        config: BrokerConfig | None = None,
        use_shm: bool = True,
        zpages: ZPagesRegistry | None = None,
        otel_recorder: DbPoolRecorder | None = None,
        name: str = "default",
        async_mode: bool = False,
    ) -> None:
        self.config = config or BrokerConfig()
        self.use_shm = use_shm
        self.name = name
        self.zpages = zpages
        self.otel_recorder = otel_recorder
        self.async_mode = async_mode

        self._transport: (
            SyncSHMBrokerTransport
            | AsyncSHMBrokerTransport
            | SyncInMemoryBrokerTransport
            | AsyncInMemoryBrokerTransport
        )
        if self.use_shm:
            self._transport = (
                AsyncSHMBrokerTransport(self.config) if self.async_mode else SyncSHMBrokerTransport(self.config)
            )
        else:
            self._transport = (
                AsyncInMemoryBrokerTransport(self.config)
                if self.async_mode
                else SyncInMemoryBrokerTransport(self.config)
            )

        if self.zpages is not None:
            self._register_zpages(self.zpages)

    @property
    def transport(
        self,
    ) -> SyncSHMBrokerTransport | AsyncSHMBrokerTransport | SyncInMemoryBrokerTransport | AsyncInMemoryBrokerTransport:
        """Return the active broker transport."""
        return self._transport

    def install_state(
        self, state_mapping: dict[str, Any]
    ) -> SyncSHMBrokerTransport | AsyncSHMBrokerTransport | SyncInMemoryBrokerTransport | AsyncInMemoryBrokerTransport:
        """Register the broker transport and metrics snapshot in state_mapping."""
        state_mapping[self.config.state_key] = self._transport
        state_mapping[self.config.metrics_state_key] = self._transport.metrics()
        return self._transport

    def snapshot_metrics(self) -> DbPoolMetricSample:
        """Create a point-in-time OpenTelemetry metric sample and record it."""
        m = self._transport.metrics()
        sample = DbPoolMetricSample(
            pool_name=self.name,
            active_leases=m.active_leases,
            primary_in_use=m.primary_in_use,
            overflow_in_use=m.overflow_in_use,
            max_capacity=self.config.pool_size + self.config.max_overflow,
            waiting_threads=m.waiting_threads,
            total_timeouts=m.total_timeouts,
            total_reclaimed=m.total_reclaimed,
            statement_cache_hits=m.statement_cache_hits,
            statement_cache_misses=m.statement_cache_misses,
        )
        if self.otel_recorder is not None:
            self.otel_recorder.record_sample(sample)
        return sample

    def on_app_init(self, app: Any) -> None:
        """Hook called during application initialization."""
        target_state = getattr(app, "state", None)
        if isinstance(target_state, dict):
            target_state["db_broker"] = self._transport
            target_state["db_broker_plugin"] = self
        elif target_state is not None:
            target_state.db_broker = self._transport
            target_state.db_broker_plugin = self
            if self.zpages is None and hasattr(target_state, "zpages"):
                zpages = getattr(target_state, "zpages", None)
                if zpages is not None:
                    self._register_zpages(zpages)

        dependencies = getattr(app, "dependencies", None)
        if isinstance(dependencies, dict):
            transport = self._transport

            def provide_broker() -> Any:
                return transport

            def provide_broker_metrics() -> Any:
                return transport.metrics()

            dependencies.setdefault(self.config.state_key, provide_broker)
            dependencies.setdefault(self.config.metrics_state_key, provide_broker_metrics)

    def _register_zpages(self, zpages: ZPagesRegistry) -> None:
        """Register broker telemetry snapshot provider with ZPagesRegistry."""

        def provider() -> dict[str, object]:
            m = self._transport.metrics()
            total_cap = self.config.pool_size + self.config.max_overflow
            utilization = m.active_leases / total_cap if total_cap > 0 else 0.0
            total_lookups = m.statement_cache_hits + m.statement_cache_misses
            hit_ratio = m.statement_cache_hits / total_lookups if total_lookups > 0 else 0.0
            return {
                "pool_size": self.config.pool_size,
                "max_overflow": self.config.max_overflow,
                "active_leases": m.active_leases,
                "primary_in_use": m.primary_in_use,
                "overflow_in_use": m.overflow_in_use,
                "utilization_ratio": utilization,
                "waiting_threads": m.waiting_threads,
                "total_acquired": m.total_acquired,
                "total_released": m.total_released,
                "total_expired": m.total_expired,
                "total_timeouts": m.total_timeouts,
                "total_reclaimed": m.total_reclaimed,
                "statement_cache_hits": m.statement_cache_hits,
                "statement_cache_misses": m.statement_cache_misses,
                "statement_cache_evictions": m.statement_cache_evictions,
                "statement_cache_hit_ratio": hit_ratio,
            }

        zpages.register_broker(self.name, provider)
