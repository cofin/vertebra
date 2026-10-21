"""Zero-sidecar Google Cloud AlloyDB (:5433) mTLS and MetadataExchange dialer."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

__all__ = (
    "AlloyDbAuthType",
    "AlloyDbClusterUri",
    "AlloyDbConfig",
    "AlloyDbDialer",
    "encode_alloydb_metadata_exchange",
)


class AlloyDbAuthType(IntEnum):
    """Authentication mode for AlloyDB MetadataExchangeRequest on port 5433."""

    BUILT_IN = 1
    IAM = 2


@dataclass(frozen=True, slots=True)
class AlloyDbClusterUri:
    """Parsed AlloyDB instance URI (`projects/<p>/locations/<l>/clusters/<c>/instances/<i>`)."""

    project: str
    location: str
    cluster: str
    instance: str

    @classmethod
    def parse(cls, raw: str) -> AlloyDbClusterUri:
        """Parse `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`."""
        trimmed = raw.strip().strip("/")
        parts = trimmed.split("/")
        if (
            len(parts) == 8
            and parts[0] == "projects"
            and parts[2] == "locations"
            and parts[4] == "clusters"
            and parts[6] == "instances"
            and parts[1]
            and parts[3]
            and parts[5]
            and parts[7]
        ):
            return cls(project=parts[1], location=parts[3], cluster=parts[5], instance=parts[7])
        msg = f"Invalid AlloyDB instance URI {raw!r}; expected 'projects/<p>/locations/<l>/clusters/<c>/instances/<i>'"
        raise ValueError(msg)

    def generate_client_cert_url(self, base_endpoint: str = "https://alloydb.googleapis.com") -> str:
        """Return the AlloyDB v1beta generateClientCertificate endpoint URL."""
        base = base_endpoint.rstrip("/")
        return f"{base}/v1beta/projects/{self.project}/locations/{self.location}/clusters/{self.cluster}:generateClientCertificate"


def encode_alloydb_metadata_exchange(
    user_agent: str,
    auth_type: AlloyDbAuthType = AlloyDbAuthType.IAM,
    oauth2_token: str = "",
) -> bytes:
    """Encode a 4-byte big-endian length-prefixed AlloyDB MetadataExchangeRequest protobuf frame."""
    body = bytearray()
    if user_agent:
        ua_bytes = user_agent.encode("utf-8")
        body.append(0x0A)
        body.extend(_encode_varint(len(ua_bytes)))
        body.extend(ua_bytes)
    body.append(0x10)
    body.extend(_encode_varint(int(auth_type)))
    if oauth2_token:
        tok_bytes = oauth2_token.encode("utf-8")
        body.append(0x1A)
        body.extend(_encode_varint(len(tok_bytes)))
        body.extend(tok_bytes)
    return struct.pack(">I", len(body)) + bytes(body)


def _encode_varint(value: int) -> bytes:
    out = bytearray()
    val = value
    while val >= 0x80:
        out.append((val & 0x7F) | 0x80)
        val >>= 7
    out.append(val)
    return bytes(out)


@dataclass(frozen=True, slots=True)
class AlloyDbConfig:
    """Configuration for zero-sidecar AlloyDB connections on port 5433."""

    instance_uri: str
    enable_iam_auth: bool = True
    ip_type: str = "PRIVATE"
    enable_columnar_engine: bool = True
    port: int = 5433
    user_agent: str = "vertebra-db/0.1"


class AlloyDbDialer:
    """Zero-sidecar AlloyDB mTLS dialer performing the MetadataExchange handshake on port 5433."""

    __slots__ = ("_config", "_uri")

    def __init__(self, config: AlloyDbConfig) -> None:
        self._config = config
        self._uri = AlloyDbClusterUri.parse(config.instance_uri)

    @property
    def uri(self) -> AlloyDbClusterUri:
        """Return the parsed AlloyDbClusterUri."""
        return self._uri

    @property
    def port(self) -> int:
        """Return the AlloyDB mTLS server proxy port (default 5433)."""
        return self._config.port

    def build_handshake_frame(self, oauth2_token: str = "") -> bytes:
        """Build the initial MetadataExchangeRequest wire frame for port 5433."""
        mode = AlloyDbAuthType.IAM if self._config.enable_iam_auth else AlloyDbAuthType.BUILT_IN
        return encode_alloydb_metadata_exchange(self._config.user_agent, mode, oauth2_token)
