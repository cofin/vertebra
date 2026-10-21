"""Container specifications for Oracle Autonomous Database Free (23ai/26ai) and ORDS + APEX 24.2."""

from __future__ import annotations

import msgspec

__all__ = (
    "OracleContainer",
    "OrdsContainer",
)


class OracleContainer(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Container specification for ``container-registry.oracle.com/database/adb-free:latest-23ai``."""

    image: str = "container-registry.oracle.com/database/adb-free:latest-23ai"
    workload_type: str = "ATP"
    service_name: str = "myatp_high.adb.oraclecloud.com"
    admin_password: str = "WelcomeOracle26ai!"
    tns_port: int = 1521
    mtls_port: int = 1522
    ords_https_port: int = 8443

    def env_vars(self) -> dict[str, str]:
        """Return container environment variables for Autonomous Database Free."""
        return {
            "WORKLOAD_TYPE": self.workload_type,
            "WALLET_PASSWORD": self.admin_password,
            "ADMIN_PASSWORD": self.admin_password,
        }

    def jdbc_thin_dsn(self, host: str = "127.0.0.1") -> str:
        """Return the thin DSN targeting the container's TLS/TCP listener."""
        return f"{host}:{self.tns_port}/{self.service_name}"


class OrdsContainer(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Container specification for Oracle REST Data Services (ORDS) and Oracle APEX 24.2 (`ApexLang`)."""

    image: str = "container-registry.oracle.com/database/ords:latest"
    http_port: int = 8181
    db_host: str = "127.0.0.1"
    db_port: int = 1521
    db_service: str = "FREEPDB1"
    apex_version: str = "24.2"
    enable_apex_lang: bool = True

    def ords_base_url(self, host: str = "127.0.0.1") -> str:
        """Return the base URL for ORDS REST and APEX endpoints."""
        return f"http://{host}:{self.http_port}/ords"
