"""Oracle 26ai Thin-Mode configuration, Exadata/RAC/SCAN/DRCP/TAC descriptors, and pool derivation."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import msgspec

from vertebra.db.oracle.wallet import OracleWallet

__all__ = (
    "OracleConfig",
    "OracleConnectDescriptor",
)

_TNS_SAFE_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class OracleConnectDescriptor(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """High-availability Oracle TNS connect descriptor builder for Exadata, RAC, SCAN, DRCP, and TAC."""

    hosts: tuple[tuple[str, int], ...] = (("localhost", 1521),)
    protocol: Literal["TCP", "TCPS"] = "TCP"
    service_name: str = "FREEPDB1"
    server_mode: Literal["DEDICATED", "POOLED", "SHARED"] = "DEDICATED"
    connect_timeout: int = 5
    transport_connect_timeout: int = 3
    retry_count: int = 3
    retry_delay: int = 1
    load_balance: bool = True
    failover: bool = True
    sdu: int | None = None
    ssl_server_dn_match: bool | None = None

    def to_tns_descriptor(self) -> str:
        """Render the canonical `(DESCRIPTION=...)` connect descriptor string."""
        if not _TNS_SAFE_RE.match(self.service_name):
            msg = f"Invalid TNS service_name: {self.service_name!r}"
            raise ValueError(msg)
        for host, _port in self.hosts:
            if not _TNS_SAFE_RE.match(host):
                msg = f"Invalid TNS host: {host!r}"
                raise ValueError(msg)
        lb = "ON" if self.load_balance else "OFF"
        fo = "ON" if self.failover else "OFF"
        sdu_part = f"(SDU={self.sdu})" if self.sdu is not None else ""
        addresses = "".join(
            f"(ADDRESS=(PROTOCOL={self.protocol})(HOST={host})(PORT={port}))" for host, port in self.hosts
        )
        security_part = ""
        if self.ssl_server_dn_match is True:
            security_part = '(SECURITY=(SSL_SERVER_CERT_DN="*")(SSL_SERVER_DN_MATCH=YES))'
        elif self.ssl_server_dn_match is False:
            security_part = "(SECURITY=(SSL_SERVER_DN_MATCH=NO))"
        return (
            f"(DESCRIPTION=(CONNECT_TIMEOUT={self.connect_timeout})"
            f"(TRANSPORT_CONNECT_TIMEOUT={self.transport_connect_timeout})"
            f"(RETRY_COUNT={self.retry_count})(RETRY_DELAY={self.retry_delay}){sdu_part}"
            f"(ADDRESS_LIST=(LOAD_BALANCE={lb})(FAILOVER={fo}){addresses})"
            f"(CONNECT_DATA=(SERVER={self.server_mode})(SERVICE_NAME={self.service_name})){security_part})"
        )


class OracleConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Zero-Instant-Client Oracle 19c / 26ai Thin-Mode configuration with immutable pool derivation."""

    dsn: str | None = None
    user: str = "system"
    password: str = ""
    host: str = "localhost"
    port: int = 1521
    service_name: str = "FREEPDB1"
    hosts: Sequence[tuple[str, int]] = ()
    wallet_zip: str | Path | bytes | None = None
    wallet_location: str | Path | None = None
    wallet_password: str | None = None
    tns_alias: str | None = None
    ssl_check_hostname: bool = True
    drcp: bool = False
    cclass: str | None = None
    tac_profile: Literal["oltp", "batch", "exadata"] | None = None
    sdu: int | None = None
    min_size: int = 1
    max_size: int = 10
    bind_key: str = "default"
    driver_features: Mapping[str, Any] = msgspec.field(default_factory=lambda: {"fetch_lobs": False})

    def __post_init__(self) -> None:
        msgspec.structs.force_setattr(self, "driver_features", dict(self.driver_features))

    def derive_pool(
        self,
        *,
        bind_key: str,
        min_size: int = 1,
        max_size: int = 4,
        driver_features: Mapping[str, Any] | None = None,
    ) -> OracleConfig:
        """Create an isolated secondary pool config without mutating the parent `driver_features`.

        Prevents queue/worker stores (`fetch_lobs=False`) from mutating LOB behavior on the
        primary web application pool.
        """
        merged_features = dict(self.driver_features)
        if driver_features is not None:
            merged_features.update(driver_features)
        return msgspec.structs.replace(
            self,
            bind_key=bind_key,
            min_size=min_size,
            max_size=max_size,
            driver_features=merged_features,
        )

    def resolve_wallet(self, *, cache_root: str | Path | None = None) -> OracleWallet | None:
        """Extract or resolve the configured Oracle wallet without mutating global TNS_ADMIN."""
        source = self.wallet_zip if self.wallet_zip is not None else self.wallet_location
        if source is None:
            return None
        return OracleWallet.extract(source, target_dir=cache_root, wallet_password=self.wallet_password)

    def build_effective_dsn(self, *, wallet: OracleWallet | None = None) -> str:
        """Build the effective TNS descriptor or Easy Connect string."""
        resolved_wallet = wallet or self.resolve_wallet()
        if resolved_wallet is not None and self.tns_alias:
            return resolved_wallet.resolve_dsn(self.tns_alias)
        if self.dsn:
            if resolved_wallet is not None:
                return resolved_wallet.resolve_dsn(self.dsn)
            return self.dsn
        if self.hosts or self.drcp or self.tac_profile is not None or self.sdu is not None:
            host_list = tuple(self.hosts) if self.hosts else ((self.host, self.port),)
            sdu_val = self.sdu or (65535 if self.tac_profile in {"batch", "exadata"} else None)
            protocol: Literal["TCP", "TCPS"] = "TCPS" if resolved_wallet is not None else "TCP"
            server_mode: Literal["DEDICATED", "POOLED"] = "POOLED" if self.drcp else "DEDICATED"
            desc = OracleConnectDescriptor(
                hosts=host_list,
                protocol=protocol,
                service_name=self.service_name,
                server_mode=server_mode,
                sdu=sdu_val,
                ssl_server_dn_match=self.ssl_check_hostname if resolved_wallet is not None else None,
            )
            return desc.to_tns_descriptor()
        return f"{self.host}:{self.port}/{self.service_name}"

    def to_oracledb_connect_params(self, *, cache_root: str | Path | None = None) -> dict[str, Any]:
        """Return keyword arguments for `oracledb.create_pool` / `oracledb.connect` in Thin Mode."""
        wallet = self.resolve_wallet(cache_root=cache_root)
        params: dict[str, Any] = {
            "user": self.user,
            "password": self.password,
            "dsn": self.build_effective_dsn(wallet=wallet),
            "min": self.min_size,
            "max": self.max_size,
        }
        if self.drcp:
            params["cclass"] = self.cclass or f"VERTEBRA_{self.bind_key.upper()}"
        if self.tac_profile is not None:
            params["retry_count"] = 5 if self.tac_profile == "exadata" else 3
            params["retry_delay"] = 1
            params["tcp_connect_timeout"] = 3.0
        if wallet is not None:
            params["config_dir"] = str(wallet.config_dir)
            params["wallet_location"] = str(wallet.wallet_location)
            if self.wallet_password:
                params["wallet_password"] = self.wallet_password
            if not self.ssl_check_hostname:
                params["ssl_server_dn_match"] = False
        return params
