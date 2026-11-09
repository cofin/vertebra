"""Oracle connection configuration, TNS connect descriptors, Cloud Wallets, and 26ai feature flags."""

import io
import zipfile
from pathlib import Path
from typing import Any, ClassVar, Self

import msgspec

from vertebra.db.adapters.oracle.core import (
    ORACLE_DIALECT_TAG,
    OracleCore,
)
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError

__all__ = (
    "OracleConfig",
    "OracleConnectDescriptor",
    "OracleDriverFeatures",
    "OracleSessionAttrs",
    "OracleTopologyConfig",
    "OracleWallet",
    "parse_tnsnames_ora",
    "resolve_tns_alias",
)


class OracleConnectDescriptor(FrozenBaseStruct, frozen=True, gc=False):
    """Structured representation of an Oracle TNS `(DESCRIPTION=...)` connect descriptor."""

    host: str = "localhost"
    """Database listener hostname or IP address."""
    port: int = 1521
    """Oracle Net Listener TCP/TCPS port (`1521` standard, `1522` mTLS wallet)."""
    service_name: str | None = "FREEPDB1"
    """Pluggable database (PDB) or service name (`SERVICE_NAME`)."""
    sid: str | None = None
    """Legacy Oracle System Identifier (`SID`) when `service_name` is not used."""
    security_cert_dn: str | None = None
    """Expected server certificate Distinguished Name (`SSL_SERVER_CERT_DN`) for TCPS verification."""
    retry_count: int = 0
    """Number of TNS connection retry attempts (`RETRY_COUNT`)."""
    retry_delay: int = 0
    """Delay in seconds between TNS connection retry attempts (`RETRY_DELAY`)."""
    transport_connect_timeout: int = 0
    """Socket connection establishment timeout in seconds (`TRANSPORT_CONNECT_TIMEOUT`)."""
    protocol: str = "TCP"
    """TNS transport protocol (`"TCP"` or `"TCPS"`)."""
    server_mode: str = "DEDICATED"
    """Oracle server process architecture (`"DEDICATED"`, `"SHARED"`, or `"POOLED"` for DRCP/PRCP)."""

    def to_tns_descriptor(self) -> str:
        """Render the connect descriptor as a canonical TNS `(DESCRIPTION=...)` string."""
        opts = ""
        if self.transport_connect_timeout > 0:
            opts += f"(TRANSPORT_CONNECT_TIMEOUT={self.transport_connect_timeout})"
        if self.retry_count > 0:
            opts += f"(RETRY_COUNT={self.retry_count})"
        if self.retry_delay > 0:
            opts += f"(RETRY_DELAY={self.retry_delay})"

        address = f"(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))"
        if self.sid and not self.service_name:
            target = f"(SID={self.sid})"
        else:
            svc = self.service_name or "FREEPDB1"
            target = f"(SERVICE_NAME={svc})"
        connect_data = f"(CONNECT_DATA=(SERVER={self.server_mode}){target})"

        security = ""
        if self.security_cert_dn:
            security = f'(SECURITY=(SSL_SERVER_CERT_DN="{self.security_cert_dn}"))'

        return f"(DESCRIPTION={opts}{address}{connect_data}{security})"

    @classmethod
    def from_tns_descriptor(cls, descriptor: str) -> "OracleConnectDescriptor":
        """Parse a TNS `(DESCRIPTION=...)` string or Easy Connect string into `OracleConnectDescriptor`."""
        parsed = OracleCore.parse_tns_descriptor_fields(descriptor)
        return cls(**parsed)

    @staticmethod
    def parse_tnsnames_ora(contents: str) -> dict[str, str]:
        """Parse a `tnsnames.ora` file into a dictionary mapping upper-cased aliases to TNS descriptors."""
        return OracleCore.parse_tnsnames_ora(contents)

    @classmethod
    def resolve_tns_alias(cls, contents: str, alias: str) -> "OracleConnectDescriptor | None":
        """Resolve a TNS alias from `tnsnames.ora` contents into an `OracleConnectDescriptor`."""
        resolved = OracleCore.resolve_tns_alias_raw(contents, alias)
        if resolved is None:
            return None
        if isinstance(resolved, str):
            return cls.from_tns_descriptor(resolved)
        if isinstance(resolved, dict):
            return cls(**resolved)
        return None


def parse_tnsnames_ora(contents: str) -> dict[str, str]:
    """Parse `tnsnames.ora` text into a mapping of alias names to TNS connect descriptors."""
    return OracleConnectDescriptor.parse_tnsnames_ora(contents)


def resolve_tns_alias(contents: str, alias: str) -> OracleConnectDescriptor | None:
    """Resolve an alias in `tnsnames.ora` text into a structured `OracleConnectDescriptor`."""
    return OracleConnectDescriptor.resolve_tns_alias(contents, alias)


class OracleWallet(FrozenBaseStruct, frozen=True, gc=False):
    """In-memory representation of an Oracle Autonomous Database Cloud Wallet."""

    tnsnames_ora: str = ""
    """Raw UTF-8 text of `tnsnames.ora` mapping service aliases to TNS descriptors."""
    sqlnet_ora: str = ""
    """Raw UTF-8 text of `sqlnet.ora` specifying wallet and SSL settings."""
    ewallet_pem: bytes = b""
    """Raw PEM-encoded client certificate and private key bundle (`ewallet.pem`)."""
    cwallet_sso: bytes = b""
    """Raw auto-login SSO wallet payload (`cwallet.sso`)."""
    aliases: dict[str, str] = msgspec.field(default_factory=dict)
    """Parsed mapping of TNS alias names to canonical `(DESCRIPTION=...)` strings."""

    @classmethod
    def from_zip_bytes(cls, data: bytes) -> "OracleWallet":
        """Extract `tnsnames.ora`, `sqlnet.ora`, `ewallet.pem`, and `cwallet.sso` from a `.zip` byte buffer."""
        try:
            with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
                tnsnames = ""
                sqlnet = ""
                ewallet = b""
                cwallet = b""
                for info in zf.infolist():
                    basename = Path(info.filename).name.lower()
                    if basename == "tnsnames.ora":
                        tnsnames = zf.read(info.filename).decode("utf-8", errors="replace")
                    elif basename == "sqlnet.ora":
                        sqlnet = zf.read(info.filename).decode("utf-8", errors="replace")
                    elif basename == "ewallet.pem":
                        ewallet = zf.read(info.filename)
                    elif basename == "cwallet.sso":
                        cwallet = zf.read(info.filename)
                aliases = OracleConnectDescriptor.parse_tnsnames_ora(tnsnames) if tnsnames else {}
                return cls(
                    tnsnames_ora=tnsnames,
                    sqlnet_ora=sqlnet,
                    ewallet_pem=ewallet,
                    cwallet_sso=cwallet,
                    aliases=aliases,
                )
        except zipfile.BadZipFile as exc:
            raise ImproperConfigurationError(f"Invalid Oracle wallet zip archive: {exc}") from exc

    @classmethod
    def from_path(cls, path: str | Path) -> "OracleWallet":
        """Load an Oracle wallet from a `.zip` file path or an extracted wallet directory."""
        resolved = Path(path)
        if not resolved.exists():
            raise ImproperConfigurationError(f"Oracle wallet path does not exist: {resolved}")
        if resolved.is_file():
            return cls.from_zip_bytes(resolved.read_bytes())

        tns_file = resolved / "tnsnames.ora"
        sqlnet_file = resolved / "sqlnet.ora"
        pem_file = resolved / "ewallet.pem"
        sso_file = resolved / "cwallet.sso"
        tnsnames = tns_file.read_text(encoding="utf-8") if tns_file.exists() else ""
        sqlnet = sqlnet_file.read_text(encoding="utf-8") if sqlnet_file.exists() else ""
        ewallet = pem_file.read_bytes() if pem_file.exists() else b""
        cwallet = sso_file.read_bytes() if sso_file.exists() else b""
        aliases = OracleConnectDescriptor.parse_tnsnames_ora(tnsnames) if tnsnames else {}
        return cls(
            tnsnames_ora=tnsnames,
            sqlnet_ora=sqlnet,
            ewallet_pem=ewallet,
            cwallet_sso=cwallet,
            aliases=aliases,
        )

    def resolve_alias(self, alias: str) -> OracleConnectDescriptor | None:
        """Resolve a TNS service alias defined in this wallet's `tnsnames.ora`."""
        return OracleConnectDescriptor.resolve_tns_alias(self.tnsnames_ora, alias)


class OracleSessionAttrs(FrozenBaseStruct, frozen=True, gc=False):
    """Zero-RTT piggybacked Two-Task Common (TTC) end-to-end session tracing attributes."""

    client_identifier: str | None = None
    """Application user or tenant identifier (`V$SESSION.CLIENT_IDENTIFIER`)."""
    module: str | None = None
    """Application subsystem or service name (`V$SESSION.MODULE`)."""
    action: str | None = None
    """Current operation or route handler name (`V$SESSION.ACTION`)."""
    ecid: str | None = None
    """Execution Context ID (`V$SESSION.ECID`) for distributed trace correlation."""

    def encode_ttc_piggyback(self) -> bytes:
        """Encode TTC `0x11` piggyback session attribute frame."""
        return OracleCore.encode_ttc_session_attrs(
            client_identifier=self.client_identifier,
            module=self.module,
            action=self.action,
            ecid=self.ecid,
        )


class OracleTopologyConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Multi-host RAC and Data Guard `(ADDRESS_LIST=...)` topology configuration for Oracle."""

    hosts: tuple[str, ...] = ("localhost",)
    """Ordered tuple of Oracle RAC or Data Guard listener hostnames."""
    port: int = 1521
    """Oracle Net Listener port across the topology hosts."""
    service_name: str = "FREEPDB1"
    """Target Oracle service name across the topology."""
    load_balance: bool = True
    """When True, include `(LOAD_BALANCE=ON)` in the `(ADDRESS_LIST=...)` block."""
    failover: bool = True
    """When True, include `(FAILOVER=ON)` in the `(ADDRESS_LIST=...)` block."""
    protocol: str = "TCP"
    """Transport protocol (`"TCP"` or `"TCPS"`)."""

    def to_tns_descriptor(self) -> str:
        """Render a multi-host `(DESCRIPTION=(ADDRESS_LIST=...)(CONNECT_DATA=...))` TNS string."""
        load_balance_clause = "(LOAD_BALANCE=ON)" if self.load_balance else "(LOAD_BALANCE=OFF)"
        failover_clause = "(FAILOVER=ON)" if self.failover else "(FAILOVER=OFF)"
        addrs = "".join(f"(ADDRESS=(PROTOCOL={self.protocol})(HOST={h})(PORT={self.port}))" for h in self.hosts)
        return (
            f"(DESCRIPTION=(ADDRESS_LIST={load_balance_clause}{failover_clause}{addrs})"
            f"(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"
        )


class OracleDriverFeatures(FrozenBaseStruct, frozen=True, gc=False):
    """Oracle driver feature flags for 26ai `VECTOR`, `OSON`, Arrow PyCapsules, and Direct Path loads."""

    enable_numpy_vectors: bool = True
    """When True, register `DB_TYPE_VECTOR` input/output type handlers converting `numpy.ndarray` and `array.array`."""
    vector_return_format: str = "list"
    """Return format for `VECTOR` columns (`"list"`, `"numpy"`, or `"array"`)."""
    enable_lowercase_column_names: bool = True
    """When True, normalize unquoted uppercase Oracle column names to lowercase in result dictionaries."""
    enable_uuid_binary: bool = True
    """When True, bind and decode `RAW(16)` columns as `uuid.UUID` objects."""
    enable_direct_path_load: bool = True
    """When True, use Oracle Direct Path Load or batched `executemany` on `load_from_arrow`."""
    arraysize: int = 100
    """Cursor `arraysize` controlling the number of rows fetched per network round-trip."""
    prefetchrows: int = 102
    """Cursor `prefetchrows` piggybacked onto initial `EXECUTE` packets."""
    fetch_lobs: bool = False
    """When False, inline `CLOB`/`BLOB` values directly as `str`/`bytes` without secondary LOB locator round-trips."""
    fetch_decimals: bool = False
    """When True, decode `NUMBER` columns as `decimal.Decimal` instead of `int`/`float`."""
    enable_session_tracing: bool = True
    """When True, piggyback `CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, and `ECID` on TTC frames."""


class OracleConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for the Oracle driver backed by the native `vertebra._oracle` module."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("oracle",)
    driver_module: ClassVar[str] = "vertebra.db.adapters.oracle.driver"
    sync_driver_name: ClassVar[str] = "SyncOracleDriver"
    async_driver_name: ClassVar[str] = "AsyncOracleDriver"

    dsn: str = "oracle://localhost:1521/FREEPDB1"
    """Easy Connect string, TNS `(DESCRIPTION=...)` descriptor, or `oracle://` URI."""
    host: str = "localhost"
    """Oracle Net Listener hostname when `dsn` or `wallet_path` is not used."""
    port: int = 1521
    """Oracle Net Listener port (`1521` TCP, `1522` TCPS wallet)."""
    service_name: str = "FREEPDB1"
    """Target Oracle Pluggable Database (PDB) or service name."""
    user: str = "system"
    """Database username for O5LOGON authentication."""
    password: str = ""
    """Database password for O5LOGON authentication."""
    wallet_path: str | None = None
    """Optional path to an Oracle Cloud Wallet `.zip` archive or extracted wallet directory."""
    wallet_zip: str | Path | bytes | None = None
    """Optional in-memory `.zip` bytes or path to an Oracle Autonomous Database Cloud Wallet archive."""
    tns_alias: str | None = None
    """Optional TNS alias resolved from the wallet's `tnsnames.ora` (e.g. `"mydb_high"`)."""
    edition: str | None = None
    """Optional Oracle Edition-Based Redefinition (`ORA$BASE`) edition name."""
    topology: OracleTopologyConfig | None = None
    """Optional multi-host RAC or Data Guard `(ADDRESS_LIST=...)` topology configuration."""
    driver_features: OracleDriverFeatures = msgspec.field(default_factory=OracleDriverFeatures)
    """Oracle 26ai `VECTOR`, `OSON`, Arrow, and Direct Path driver feature configuration."""
    client_identifier: str | None = None
    """Initial zero-RTT `V$SESSION.CLIENT_IDENTIFIER` tracing attribute."""
    module: str | None = None
    """Initial zero-RTT `V$SESSION.MODULE` tracing attribute."""
    action: str | None = None
    """Initial zero-RTT `V$SESSION.ACTION` tracing attribute."""
    ecid: str | None = None
    """Initial zero-RTT `V$SESSION.ECID` distributed execution context identifier."""
    pool_min_size: int = 1
    """Minimum number of pooled sessions maintained per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled sessions permitted per worker."""
    statement_cache_size: int = 256
    """Maximum number of compiled statements cached in the shared-memory statement cache."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating cross-worker connection and statement leases."""

    def connect_descriptor(self) -> OracleConnectDescriptor:
        """Resolve the effective `OracleConnectDescriptor` from wallet, topology, TNS alias, or DSN."""
        if isinstance(self.wallet_zip, (bytes, bytearray)):
            wallet = OracleWallet.from_zip_bytes(bytes(self.wallet_zip))
            alias = self.tns_alias or self.service_name
            resolved = wallet.resolve_alias(alias)
            if resolved is not None:
                return resolved
        elif self.wallet_zip is not None:
            wallet = OracleWallet.from_path(self.wallet_zip)
            alias = self.tns_alias or self.service_name
            resolved = wallet.resolve_alias(alias)
            if resolved is not None:
                return resolved
        if self.wallet_path:
            wallet = OracleWallet.from_path(self.wallet_path)
            alias = self.tns_alias or self.service_name
            resolved = wallet.resolve_alias(alias)
            if resolved is not None:
                return resolved
        if self.topology is not None:
            return OracleConnectDescriptor.from_tns_descriptor(self.topology.to_tns_descriptor())
        if self.dsn:
            return OracleConnectDescriptor.from_tns_descriptor(self.dsn)
        return OracleConnectDescriptor(
            host=self.host,
            port=self.port,
            service_name=self.service_name,
        )

    def derive_pool(
        self,
        *,
        service_name: str | None = None,
        edition: str | None = None,
    ) -> "OracleConfig":
        """Derive a secondary `OracleConfig` targeting a sibling PDB service or EBR edition."""
        target_svc = service_name or self.service_name
        return msgspec.structs.replace(
            self,
            service_name=target_svc,
            edition=edition if edition is not None else self.edition,
        )

    def session_attrs(self) -> OracleSessionAttrs:
        """Return the configured zero-RTT TTC session attributes."""
        return OracleSessionAttrs(
            client_identifier=self.client_identifier,
            module=self.module,
            action=self.action,
            ecid=self.ecid,
        )

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct an `OracleConfig` from an `oracle://` URI."""
        return cls(dsn=url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        desc = self.connect_descriptor()
        return ("oracle", desc.host.lower(), int(desc.port), desc.service_name or desc.sid or "", self.user)
