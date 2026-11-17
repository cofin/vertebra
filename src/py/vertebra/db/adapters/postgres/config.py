"""PostgreSQL configuration supporting Cloud SQL, AlloyDB, and PgBouncer/PgCat/pgdog poolers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, ClassVar, Literal, Self

import msgspec

from vertebra.db.adapters.postgres.options import (
    merge_postgres_options,
    parse_postgres_dsn,
    validate_cockroach_cluster,
)
from vertebra.db.dialer.alloydb import AlloyDBConfig
from vertebra.db.dialer.cloudsql import CloudSQLConfig
from vertebra.db.driver._cloud_auth import AwsAuroraDsqlIamTokenProvider
from vertebra.db.driver._common import POSTGRES_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional

__all__ = (
    "PoolerMode",
    "PostgreSQLConfig",
)

type PoolerMode = Literal["direct", "transaction", "session", "statement", "auto"]


class PostgreSQLConfig(BaseDatabaseConfig, frozen=True, gc=False, kw_only=True):
    """PostgreSQL configuration supporting Cloud SQL, AlloyDB, and PgBouncer/PgCat/pgdog poolers."""

    dialect: ClassVar[str] = "postgres"
    dialect_tag: ClassVar[int] = POSTGRES_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("postgresql", "postgres")
    driver_module: ClassVar[str] = "vertebra.db.adapters.postgres.driver"
    sync_driver_name: ClassVar[str] = "SyncPostgreSQLDriver"
    async_driver_name: ClassVar[str] = "AsyncPostgreSQLDriver"

    dsn: str = "postgresql://localhost:5432/postgres"
    """Primary PostgreSQL connection URI (may point to a PgBouncer, PgCat, or pgdog pooler)."""
    direct_dsn: str | None = None
    """Optional unpooled direct PostgreSQL URI used for `LISTEN/NOTIFY`, migrations, and DDL."""
    pooler_mode: PoolerMode = "direct"
    """Connection pooler multiplexing mode (`"direct"`, `"transaction"`, `"session"`, `"statement"`, or `"auto"`)."""
    pgbouncer: bool = False
    """When True, configure transaction-mode PgBouncer settings (`statement_cache_size = 0`)."""
    pgbouncer_mode: bool = False
    """Alias for `pgbouncer` matching `sqlspec` driver features."""
    protocol_level_pooler_cache: bool = False
    """When True, retain prepared statements in transaction-mode poolers supporting protocol-level statement caching."""
    max_prepared_statements: int | None = None
    """Maximum per-connection prepared statement LRU capacity (`None` defaults to `256` when safe)."""
    min_size: int = 1
    """Minimum number of pooled connections maintained per worker."""
    max_size: int = 10
    """Maximum number of pooled connections permitted per worker."""
    enable_cloud_sql: bool = False
    """When True, enable zero-sidecar Google Cloud SQL (:3307) mTLS connection dialing."""
    cloud_sql_instance: str | None = None
    """Optional Google Cloud SQL instance connection name (`project:region:instance`) for zero-sidecar port 3307 mTLS."""
    cloud_sql_ip_type: Literal["PUBLIC", "PRIVATE", "PSC"] = "PRIVATE"
    """Preferred Google Cloud SQL IP connectivity mode."""
    enable_alloydb: bool = False
    """When True, enable zero-sidecar Google Cloud AlloyDB (:5433) mTLS and MetadataExchange dialing."""
    alloydb_instance_uri: str | None = None
    """Optional Google Cloud AlloyDB instance URI (`projects/.../instances/...`) for zero-sidecar port 5433 mTLS."""
    alloydb_ip_type: Literal["PUBLIC", "PRIVATE", "PSC"] = "PRIVATE"
    """Preferred Google Cloud AlloyDB IP connectivity mode."""
    enable_iam_auth: bool = False
    """When True, authenticate using short-lived Google Cloud IAM OAuth2 tokens (on direct :5432 TCP/TLS or :3307/:5433 dialers)."""
    iam_auth_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    """Optional `GcpTokenCache` options (`service_account_key`, `application_credentials_path`, `scopes`) for direct :5432 or dialer IAM token resolution."""
    token_provider: Any = None
    """Optional cloud database token provider (`DatabaseTokenProvider`) supplying dynamic IAM passwords."""
    enable_alloydb_columnar_engine: bool = False
    """When True, enable `google_columnar_engine.enabled = on` in session startup GUCs."""
    cockroach_cluster: str | None = None
    """Optional CockroachDB cluster routing identifier forwarded as `--cluster=<id>` in startup `options`."""
    application_name: str = "vertebra"
    """PostgreSQL `application_name` GUC reported in `pg_stat_activity`."""
    statement_timeout_ms: int | None = None
    """Optional per-statement execution timeout in milliseconds (`statement_timeout` GUC)."""
    idle_in_transaction_session_timeout_ms: int | None = None
    """Optional timeout in milliseconds before aborting idle open transactions."""
    session_gucs: Mapping[str, str] = msgspec.field(default_factory=dict)
    """Additional PostgreSQL session GUC key-value pairs applied on connection startup."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker`, `SyncSHMBrokerTransport`, or `AsyncSHMBrokerTransport` for cross-worker connection leasing."""

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)
        if self.cockroach_cluster is not None:
            validate_cockroach_cluster(self.cockroach_cluster)
        has_csql = bool(self.enable_cloud_sql or self.cloud_sql_instance)
        has_alloy = bool(self.enable_alloydb or self.alloydb_instance_uri)
        if has_csql and has_alloy:
            raise ImproperConfigurationError("Cannot enable both Cloud SQL and AlloyDB on the same PostgreSQLConfig")
        if self.pgbouncer or self.pgbouncer_mode:
            msgspec.structs.force_setattr(self, "pgbouncer", True)
            msgspec.structs.force_setattr(self, "pgbouncer_mode", True)
            if self.pooler_mode in ("direct", "session", "auto"):
                msgspec.structs.force_setattr(self, "pooler_mode", "transaction")
        msgspec.structs.force_setattr(self, "session_gucs", dict(self.session_gucs))
        msgspec.structs.force_setattr(self, "iam_auth_options", dict(self.iam_auth_options))
        if isinstance(self.token_provider, AwsAuroraDsqlIamTokenProvider):
            for candidate_dsn in (self.dsn, self.direct_dsn):
                if not candidate_dsn:
                    continue
                parsed_candidate = parse_postgres_dsn(candidate_dsn)
                sslmode = parsed_candidate.params.get("sslmode")
                if sslmode is not None and sslmode.strip().lower() == "disable":
                    msg = "AwsAuroraDsqlIamTokenProvider requires TLS and cannot be used with sslmode='disable'"
                    raise ImproperConfigurationError(msg)

    @staticmethod
    def _redact_dsn(dsn: str) -> str:
        """Return a DSN representation with embedded userinfo or libpq passwords masked."""
        redacted = dsn
        if "://" in redacted:
            scheme, rest = redacted.split("://", 1)
            if "@" in rest:
                userinfo, tail = rest.split("@", 1)
                if ":" in userinfo:
                    user, _ = userinfo.split(":", 1)
                    redacted = f"{scheme}://{user}:***@{tail}"
        return re.sub(r"(?i)(password\s*=\s*)('[^']*'|\"[^\"]*\"|\S+)", r"\1***", redacted)

    def __repr__(self) -> str:
        """Return a redacted representation that omits DSN passwords, IAM options, and token secrets."""
        redacted_dsn = self._redact_dsn(self.dsn)
        redacted_direct = self._redact_dsn(self.direct_dsn) if self.direct_dsn is not None else None
        provider_name = type(self.token_provider).__name__ if self.token_provider is not None else None
        redacted_iam_keys = sorted(str(k) for k in self.iam_auth_options)
        return (
            f"PostgreSQLConfig(dsn={redacted_dsn!r}, direct_dsn={redacted_direct!r}, "
            f"pooler_mode={self.pooler_mode!r}, min_size={self.min_size!r}, max_size={self.max_size!r}, "
            f"enable_iam_auth={self.enable_iam_auth!r}, iam_auth_option_keys={redacted_iam_keys!r}, "
            f"token_provider={provider_name!r})"
        )

    @property
    def effective_statement_cache_size(self) -> int:
        """Return the prepared statement cache capacity safe for the configured pooler mode."""
        if self.pooler_mode in ("transaction", "statement") and not self.protocol_level_pooler_cache:
            return 0
        return 256 if self.max_prepared_statements is None else max(0, self.max_prepared_statements)

    @property
    def effective_direct_dsn(self) -> str:
        """Return the direct primary DSN for LISTEN/NOTIFY, logical replication, and migrations."""
        return self.direct_dsn or self.dsn

    def derive_direct_config(self) -> PostgreSQLConfig:
        """Return a session-mode PostgreSQLConfig targeting `effective_direct_dsn` (`sqlspec>=0.68.0`)."""
        if not self.direct_dsn:
            return self
        base = msgspec.structs.asdict(self)
        base["dsn"] = self.direct_dsn
        base["direct_dsn"] = None
        base["pooler_mode"] = "session"
        base["pgbouncer"] = False
        base["pgbouncer_mode"] = False
        base["min_size"] = min(1, self.max_size)
        base["max_size"] = min(2, self.max_size)
        base["session_gucs"] = dict(self.session_gucs)
        base["iam_auth_options"] = dict(self.iam_auth_options)
        return PostgreSQLConfig(**base)

    def resolve_iam_password_sync(self, *, force_refresh: bool = False) -> str | None:
        """Resolve a short-lived cloud IAM token synchronously from `token_provider` or `SyncGcpTokenCache`."""
        if self.token_provider is not None:
            return str(self.token_provider.resolve_sync(force_refresh=force_refresh))
        if not self.enable_iam_auth:
            return None
        opts = dict(self.iam_auth_options)
        if "access_token" in opts or "bearer_token" in opts:
            opts.setdefault("allow_ambient_adc", "false")
        opts.setdefault(
            "scopes",
            "https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/cloud-platform",
        )
        native_mod = import_optional("vertebra._vertebra")
        token_cache_cls = getattr(native_mod, "SyncGcpTokenCache", None) if native_mod is not None else None
        if token_cache_cls is None:
            return opts.get("access_token") or opts.get("bearer_token")
        cache = token_cache_cls(opts)
        snap = cache.get_token(force_refresh=force_refresh)
        return str(snap.access_token)

    async def resolve_iam_password_async(self, *, force_refresh: bool = False) -> str | None:
        """Resolve a short-lived cloud IAM token asynchronously from `token_provider` or `AsyncGcpTokenCache`."""
        if self.token_provider is not None:
            return str(await self.token_provider.resolve_async(force_refresh=force_refresh))
        if not self.enable_iam_auth:
            return None
        opts = dict(self.iam_auth_options)
        if "access_token" in opts or "bearer_token" in opts:
            opts.setdefault("allow_ambient_adc", "false")
        opts.setdefault(
            "scopes",
            "https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/cloud-platform",
        )
        native_mod = import_optional("vertebra._vertebra")
        token_cache_cls = getattr(native_mod, "AsyncGcpTokenCache", None) if native_mod is not None else None
        if token_cache_cls is None:
            return opts.get("access_token") or opts.get("bearer_token")
        cache = token_cache_cls(opts)
        snap = await cache.get_token(force_refresh=force_refresh)
        return str(snap.access_token)

    @staticmethod
    def compute_worker_pool_budget(
        max_connections: int,
        worker_count: int = 1,
        *,
        replica_count: int = 1,
        workers_per_replica: int | None = None,
        reserved_admin_connections: int = 5,
    ) -> int:
        """Compute per-worker pool size from a cluster-wide database connection budget."""
        usable = max(0, max_connections - reserved_admin_connections)
        per_replica = workers_per_replica if workers_per_replica is not None else worker_count
        total_shards = max(1, replica_count) * max(1, per_replica)
        return max(1, usable // total_shards)

    def build_session_gucs(self) -> dict[str, str]:
        """Build the startup/session GUC parameters for the PostgreSQL connection."""
        gucs: dict[str, str] = {"application_name": self.application_name}
        if self.enable_alloydb_columnar_engine or self.alloydb_instance_uri is not None:
            gucs["google_columnar_engine.enabled"] = "on"
            gucs["alloydb.enable_columnar_engine"] = "on"
        if self.statement_timeout_ms is not None:
            gucs["statement_timeout"] = str(self.statement_timeout_ms)
        if self.idle_in_transaction_session_timeout_ms is not None:
            gucs["idle_in_transaction_session_timeout"] = str(self.idle_in_transaction_session_timeout_ms)
        gucs.update(self.session_gucs)
        merged_options = merge_postgres_options(
            gucs.get("options"),
            cockroach_cluster=self.cockroach_cluster,
        )
        if merged_options is not None:
            gucs["options"] = merged_options
        else:
            gucs.pop("options", None)
        return gucs

    def to_cloud_dialer_config(self) -> CloudSQLConfig | AlloyDBConfig | None:
        """Return the zero-sidecar CloudSQLConfig or AlloyDBConfig when configured."""
        if self.alloydb_instance_uri:
            return AlloyDBConfig(
                instance_uri=self.alloydb_instance_uri,
                enable_iam_auth=self.enable_iam_auth,
                enable_columnar_engine=self.enable_alloydb_columnar_engine or True,
            )
        if self.cloud_sql_instance:
            return CloudSQLConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a ``PostgreSQLConfig`` from a ``postgresql://``, ``postgres://``, or libpq DSN."""
        normalized_url = url
        raw_scheme, sep, rest = url.partition("://")
        if sep and "+" in raw_scheme:
            prefix, _, driver_tag = raw_scheme.lower().rpartition("+")
            if prefix in {"postgresql", "postgres"} and driver_tag in {
                "vtb",
                "vertebra",
                "vtb_async",
                "vertebra_async",
            }:
                normalized_url = f"{prefix}://{rest}"
        parsed = parse_postgres_dsn(normalized_url)
        query_params = parsed.params
        kwargs = dict(overrides)
        if "pgbouncer" in query_params and "pgbouncer" not in kwargs:
            kwargs["pgbouncer"] = query_params["pgbouncer"].lower() in {"1", "true", "yes", "on"}
        if "pooler_mode" in query_params and "pooler_mode" not in kwargs:
            kwargs["pooler_mode"] = query_params["pooler_mode"]
        if "protocol_level_pooler_cache" in query_params and "protocol_level_pooler_cache" not in kwargs:
            kwargs["protocol_level_pooler_cache"] = query_params["protocol_level_pooler_cache"].lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        if "cloud_sql_instance" in query_params and "cloud_sql_instance" not in kwargs:
            kwargs["cloud_sql_instance"] = query_params["cloud_sql_instance"]
        if "alloydb_instance_uri" in query_params and "alloydb_instance_uri" not in kwargs:
            kwargs["alloydb_instance_uri"] = query_params["alloydb_instance_uri"]
        if "direct_dsn" in query_params and "direct_dsn" not in kwargs:
            kwargs["direct_dsn"] = query_params["direct_dsn"]
        if "cockroach_cluster" in query_params and "cockroach_cluster" not in kwargs:
            kwargs["cockroach_cluster"] = query_params["cockroach_cluster"]
        if "enable_alloydb_columnar_engine" in query_params and "enable_alloydb_columnar_engine" not in kwargs:
            kwargs["enable_alloydb_columnar_engine"] = query_params["enable_alloydb_columnar_engine"].lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        return cls(dsn=normalized_url, **kwargs)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        parsed = parse_postgres_dsn(self.dsn)
        query_hosts = [item for item in parsed.params.get("host", "").split(",") if item]
        host = (parsed.hosts[0] if parsed.hosts else (query_hosts[0] if query_hosts else "localhost")).lower()
        port = int(parsed.ports[0] if parsed.ports and parsed.ports[0] is not None else 5432)
        user = parsed.user or ""
        database = parsed.database or "postgres"
        pooler_mode = (
            "transaction"
            if (self.pgbouncer or self.pgbouncer_mode or self.pooler_mode == "transaction")
            else str(self.pooler_mode)
        )
        base_key: tuple[Any, ...] = (
            "postgresql",
            host,
            port,
            database,
            user,
            pooler_mode,
            self.cloud_sql_instance or "",
            self.alloydb_instance_uri or "",
            self.cockroach_cluster or "",
        )
        if self.token_provider is None:
            return base_key
        try:
            provider_key: Any = hash(self.token_provider)
        except TypeError:
            provider_key = id(self.token_provider)
        return (*base_key, type(self.token_provider).__qualname__, provider_key)

    def build_sync_driver(self, registry: Any) -> Any:
        """Instantiate `SyncPostgreSQLDriver` with shared `LeaseBroker` and optional cloud dialer."""
        sync_cls, _ = self.resolve_driver_classes()
        broker = self.broker if self.broker is not None else getattr(registry, "_broker", None)
        dialer = (
            registry.get_dialer(dialer_cfg, sync=True)
            if registry is not None and (dialer_cfg := self.to_cloud_dialer_config()) is not None
            else None
        )
        return sync_cls(self, dialer=dialer, broker=broker)

    def build_async_driver(self, registry: Any) -> Any:
        """Instantiate `AsyncPostgreSQLDriver` with shared `LeaseBroker` and optional cloud dialer."""
        _, async_cls = self.resolve_driver_classes()
        broker = self.broker if self.broker is not None else getattr(registry, "_broker", None)
        dialer = (
            registry.get_dialer(dialer_cfg, sync=False)
            if registry is not None and (dialer_cfg := self.to_cloud_dialer_config()) is not None
            else None
        )
        return async_cls(self, dialer=dialer, broker=broker)
