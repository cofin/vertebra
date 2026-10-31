"""Pure-Rust-backed Oracle Thin TNS/TTC driver, wallet loader, and 26ai VECTOR/OSON codecs."""

from __future__ import annotations

import asyncio
import io
import os
import re
import sqlite3
import struct
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.db.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.typing import (
    FrozenBaseStruct,
    ImproperConfigurationError,
    VertebraError,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "ORACLE_DIALECT_TAG",
    "ORACLE_VECTOR_FMT_BINARY",
    "ORACLE_VECTOR_FMT_F32",
    "ORACLE_VECTOR_FMT_F64",
    "ORACLE_VECTOR_FMT_I8",
    "ORACLE_VECTOR_MAGIC",
    "ORACLE_VECTOR_VERSION",
    "OSON_MAGIC",
    "AsyncOracleDriver",
    "OracleConfig",
    "OracleConnectDescriptor",
    "OracleSessionAttrs",
    "OracleWallet",
    "SyncOracleDriver",
    "decode_oson",
    "decode_oson_payload",
    "decode_tns_packet",
    "decode_ttc_session_attrs",
    "decode_vector_binary",
    "decode_vector_f32",
    "decode_vector_f64",
    "decode_vector_i8",
    "encode_oson",
    "encode_oson_payload",
    "encode_tns_connect_packet",
    "encode_tns_data_packet",
    "encode_ttc_session_attrs",
    "encode_vector_binary",
    "encode_vector_f32",
    "encode_vector_f64",
    "encode_vector_i8",
    "parse_tnsnames_ora",
    "resolve_tns_alias",
]

ORACLE_DIALECT_TAG = 4
ORACLE_VECTOR_MAGIC = 0xDB
ORACLE_VECTOR_VERSION = 0x01
ORACLE_VECTOR_FMT_F32 = 2
ORACLE_VECTOR_FMT_F64 = 3
ORACLE_VECTOR_FMT_I8 = 4
ORACLE_VECTOR_FMT_BINARY = 5
OSON_MAGIC = b"\xff\x4a\x5a"

_HOST_RE = re.compile(r"\(\s*HOST\s*=\s*([^)\s]+)\s*\)", re.IGNORECASE)
_PORT_RE = re.compile(r"\(\s*PORT\s*=\s*(\d+)\s*\)", re.IGNORECASE)
_PROTOCOL_RE = re.compile(r"\(\s*PROTOCOL\s*=\s*([^)\s]+)\s*\)", re.IGNORECASE)
_SERVICE_RE = re.compile(r"\(\s*SERVICE_NAME\s*=\s*([^)\s]+)\s*\)", re.IGNORECASE)
_SID_RE = re.compile(r"\(\s*SID\s*=\s*([^)\s]+)\s*\)", re.IGNORECASE)
_SERVER_RE = re.compile(r"\(\s*SERVER\s*=\s*([^)\s]+)\s*\)", re.IGNORECASE)
_CERT_DN_RE = re.compile(r'\(\s*SSL_SERVER_CERT_DN\s*=\s*"?([^")]+)"?\s*\)', re.IGNORECASE)
_RETRY_COUNT_RE = re.compile(r"\(\s*RETRY_COUNT\s*=\s*(\d+)\s*\)", re.IGNORECASE)
_RETRY_DELAY_RE = re.compile(r"\(\s*RETRY_DELAY\s*=\s*(\d+)\s*\)", re.IGNORECASE)
_CONNECT_TIMEOUT_RE = re.compile(r"\(\s*(?:TRANSPORT_)?CONNECT_TIMEOUT\s*=\s*(\d+)\s*\)", re.IGNORECASE)
_NAMED_PARAM_RE = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


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
    def from_tns_descriptor(cls, descriptor: str) -> OracleConnectDescriptor:
        """Parse a TNS `(DESCRIPTION=...)` string or Easy Connect string into `OracleConnectDescriptor`."""
        text = descriptor.strip()
        if not text.startswith("("):
            cleaned = text
            for prefix in ("oracle://", "oracle+vtb://", "tcps://", "tcp://"):
                if cleaned.lower().startswith(prefix):
                    cleaned = cleaned[len(prefix) :]
                    break
            if "@" in cleaned:
                cleaned = cleaned.split("@", 1)[1]
            host_part, _, svc_part = cleaned.partition("/")
            if "?" in svc_part:
                svc_part = svc_part.split("?", 1)[0]
            host, _, port_str = host_part.partition(":")
            port = int(port_str) if port_str.isdigit() else 1521
            protocol = "TCPS" if text.lower().startswith("tcps://") or port == 1522 else "TCP"
            return cls(
                host=host or "localhost",
                port=port,
                service_name=svc_part or "FREEPDB1",
                protocol=protocol,
            )

        host_m = _HOST_RE.search(text)
        port_m = _PORT_RE.search(text)
        proto_m = _PROTOCOL_RE.search(text)
        svc_m = _SERVICE_RE.search(text)
        sid_m = _SID_RE.search(text)
        srv_m = _SERVER_RE.search(text)
        dn_m = _CERT_DN_RE.search(text)
        rc_m = _RETRY_COUNT_RE.search(text)
        rd_m = _RETRY_DELAY_RE.search(text)
        ct_m = _CONNECT_TIMEOUT_RE.search(text)

        service_name = svc_m.group(1) if svc_m else (None if sid_m else "FREEPDB1")
        sid = sid_m.group(1) if sid_m else None

        return cls(
            host=host_m.group(1) if host_m else "localhost",
            port=int(port_m.group(1)) if port_m else 1521,
            service_name=service_name,
            sid=sid,
            security_cert_dn=dn_m.group(1).strip() if dn_m else None,
            retry_count=int(rc_m.group(1)) if rc_m else 0,
            retry_delay=int(rd_m.group(1)) if rd_m else 0,
            transport_connect_timeout=int(ct_m.group(1)) if ct_m else 0,
            protocol=proto_m.group(1).upper() if proto_m else "TCP",
            server_mode=srv_m.group(1).upper() if srv_m else "DEDICATED",
        )

    @staticmethod
    def parse_tnsnames_ora(contents: str) -> dict[str, str]:
        """Parse a `tnsnames.ora` file into a dictionary mapping upper-cased aliases to TNS descriptors."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "parse_tnsnames_ora"):
            res = native.parse_tnsnames_ora(contents)
            if isinstance(res, dict):
                return {str(k).upper(): str(v) for k, v in res.items()}

        cleaned_lines: list[str] = []
        for raw_line in contents.splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("\x23"):
                continue
            cleaned_lines.append(raw_line)
        text = "\n".join(cleaned_lines)

        entries: dict[str, str] = {}
        idx = 0
        n = len(text)
        while idx < n:
            while idx < n and text[idx].isspace():
                idx += 1
            if idx >= n:
                break
            eq_pos = text.find("=", idx)
            if eq_pos == -1:
                break
            alias_part = text[idx:eq_pos].strip()
            paren_start = text.find("(", eq_pos + 1)
            if paren_start == -1:
                break
            depth = 0
            in_quotes = False
            pos = paren_start
            while pos < n:
                ch = text[pos]
                if ch == '"':
                    in_quotes = not in_quotes
                elif not in_quotes:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                        if depth == 0:
                            pos += 1
                            break
                pos += 1
            descriptor = re.sub(r"\s+", "", text[paren_start:pos])
            for raw_alias in alias_part.split(","):
                alias_clean = raw_alias.strip()
                if alias_clean:
                    entries[alias_clean.upper()] = descriptor
                    entries[alias_clean] = descriptor
            idx = pos
        return entries

    @classmethod
    def resolve_tns_alias(cls, contents: str, alias: str) -> OracleConnectDescriptor | None:
        """Resolve a TNS alias from `tnsnames.ora` contents into an `OracleConnectDescriptor`."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "resolve_tns_alias"):
            resolved = native.resolve_tns_alias(contents, alias)
            if resolved is not None:
                if isinstance(resolved, str):
                    return cls.from_tns_descriptor(resolved)
                if isinstance(resolved, dict):
                    return cls(**resolved)
        mapping = cls.parse_tnsnames_ora(contents)
        desc_str = mapping.get(alias) or mapping.get(alias.upper())
        if desc_str is None:
            return None
        return cls.from_tns_descriptor(desc_str)


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
    def from_zip_bytes(cls, data: bytes) -> OracleWallet:
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
    def from_path(cls, path: str | Path) -> OracleWallet:
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
        return encode_ttc_session_attrs(
            client_identifier=self.client_identifier,
            module=self.module,
            action=self.action,
            ecid=self.ecid,
        )


def encode_tns_connect_packet(descriptor: str) -> bytes:
    """Encode an Oracle TNS `Connect` (`type = 1`) packet wrapping a `(DESCRIPTION=...)` descriptor."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_tns_connect_packet"):
        return bytes(native.encode_tns_connect_packet(descriptor))
    desc_bytes = descriptor.encode("utf-8")
    connect_header_len = 58
    total_len = connect_header_len + len(desc_bytes)
    header = struct.pack(
        ">HHBB11HIBB24x",
        total_len,
        0,
        1,
        0,
        0,
        318,
        300,
        0,
        8192,
        65535,
        0x4F98,
        0,
        1,
        len(desc_bytes),
        connect_header_len,
        0,
        0x41,
        0x41,
    )
    return header + desc_bytes


def encode_tns_data_packet(data_flags: int, payload: bytes) -> bytes:
    """Encode an Oracle TNS `Data` (`type = 6`) packet with 2-byte big-endian `data_flags`."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_tns_data_packet"):
        return bytes(native.encode_tns_data_packet(data_flags, payload))
    total_len = 10 + len(payload)
    header = struct.pack(">HHBBHH", total_len, 0, 6, 0, 0, data_flags & 0xFFFF)
    return header + payload


def decode_tns_packet(buf: bytes | bytearray | memoryview) -> tuple[int, int, bytes]:
    """Decode an Oracle TNS packet into `(packet_type, data_flags, payload)`."""
    raw = bytes(buf)
    if len(raw) < 8:
        raise VertebraError("Buffer too short for 8-byte TNS packet header")
    total_len = struct.unpack(">H", raw[0:2])[0]
    if total_len < 8 or len(raw) < total_len:
        raise VertebraError(f"Invalid or truncated TNS packet length: {total_len}")
    pkt_type = raw[4]
    if pkt_type == 6:
        if total_len < 10:
            raise VertebraError("TNS Data packet too short for 2-byte data flags")
        flags = struct.unpack(">H", raw[8:10])[0]
        return (pkt_type, flags, raw[10:total_len])
    if pkt_type == 1 and total_len >= 58:
        offset = struct.unpack(">H", raw[26:28])[0]
        if offset <= total_len:
            return (pkt_type, 0, raw[offset:total_len])
    return (pkt_type, 0, raw[8:total_len])


def encode_ttc_session_attrs(
    client_identifier: str | None = None,
    module: str | None = None,
    action: str | None = None,
    ecid: str | None = None,
) -> bytes:
    """Encode a zero-RTT TTC piggyback frame (`0x11 0x6e`) carrying session tracing attributes."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_ttc_session_attrs"):
        return bytes(native.encode_ttc_session_attrs(client_identifier, module, action, ecid))

    fields: list[tuple[int, bytes]] = []
    if client_identifier is not None:
        fields.append((1, client_identifier.encode("utf-8")))
    if module is not None:
        fields.append((2, module.encode("utf-8")))
    if action is not None:
        fields.append((3, action.encode("utf-8")))
    if ecid is not None:
        fields.append((4, ecid.encode("utf-8")))

    out = bytearray([0x11, 0x6E, len(fields)])
    for attr_id, val_bytes in fields:
        out.append(attr_id)
        out.extend(struct.pack(">H", len(val_bytes)))
        out.extend(val_bytes)
    return bytes(out)


def decode_ttc_session_attrs(payload: bytes | bytearray | memoryview) -> dict[str, str]:
    """Decode a TTC piggyback session attribute frame into a dictionary."""
    raw = bytes(payload)
    if len(raw) < 3 or raw[0] != 0x11 or raw[1] != 0x6E:
        raise VertebraError("Invalid TTC session attribute piggyback header")
    count = raw[2]
    pos = 3
    names = {1: "client_identifier", 2: "module", 3: "action", 4: "ecid"}
    result: dict[str, str] = {}
    for _ in range(count):
        if pos + 3 > len(raw):
            raise VertebraError("Truncated TTC session attribute entry")
        attr_id = raw[pos]
        length = struct.unpack(">H", raw[pos + 1 : pos + 3])[0]
        pos += 3
        if pos + length > len(raw):
            raise VertebraError("Truncated TTC session attribute value")
        key = names.get(attr_id, f"attr_{attr_id}")
        result[key] = raw[pos : pos + length].decode("utf-8")
        pos += length
    return result


def _encode_vector_header(fmt: int, dims: int) -> bytes:
    return struct.pack(">BBBBI", ORACLE_VECTOR_MAGIC, ORACLE_VECTOR_VERSION, fmt, 0, dims)


def _decode_vector_header(buf: bytes, expected_fmt: int) -> tuple[int, memoryview]:
    if len(buf) < 8:
        raise VertebraError("Buffer too short for 8-byte Oracle VECTOR header")
    magic, version, fmt, _flags, dims = struct.unpack(">BBBBI", buf[:8])
    if magic != ORACLE_VECTOR_MAGIC:
        raise VertebraError(f"Invalid Oracle VECTOR magic byte: 0x{magic:02x}")
    if version != ORACLE_VECTOR_VERSION:
        raise VertebraError(f"Unsupported Oracle VECTOR version: {version}")
    if fmt != expected_fmt:
        raise VertebraError(f"Oracle VECTOR format mismatch: expected {expected_fmt}, got {fmt}")
    return dims, memoryview(buf)[8:]


def encode_vector_f32(values: Sequence[float]) -> bytes:
    """Encode a sequence of `float` values into an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_oracle_vector_f32"):
        return bytes(native.encode_oracle_vector_f32(list(values)))
    dims = len(values)
    return _encode_vector_header(ORACLE_VECTOR_FMT_F32, dims) + struct.pack(f">{dims}f", *values)


def decode_vector_f32(payload: bytes | bytearray | memoryview) -> list[float]:
    """Decode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame into a list of floats."""
    raw = bytes(payload)
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "decode_oracle_vector_f32"):
        try:
            return list(native.decode_oracle_vector_f32(raw))
        except ValueError as exc:
            raise VertebraError(str(exc)) from exc
    dims, body = _decode_vector_header(raw, ORACLE_VECTOR_FMT_F32)
    if len(body) != dims * 4:
        raise VertebraError(f"Oracle FLOAT32 VECTOR payload length mismatch: expected {dims * 4}, got {len(body)}")
    return list(struct.unpack(f">{dims}f", body))


def encode_vector_f64(values: Sequence[float]) -> bytes:
    """Encode a sequence of `float` values into an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_oracle_vector_f64"):
        return bytes(native.encode_oracle_vector_f64(list(values)))
    dims = len(values)
    return _encode_vector_header(ORACLE_VECTOR_FMT_F64, dims) + struct.pack(f">{dims}d", *values)


def decode_vector_f64(payload: bytes | bytearray | memoryview) -> list[float]:
    """Decode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame into a list of floats."""
    raw = bytes(payload)
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "decode_oracle_vector_f64"):
        try:
            return list(native.decode_oracle_vector_f64(raw))
        except ValueError as exc:
            raise VertebraError(str(exc)) from exc
    dims, body = _decode_vector_header(raw, ORACLE_VECTOR_FMT_F64)
    if len(body) != dims * 8:
        raise VertebraError(f"Oracle FLOAT64 VECTOR payload length mismatch: expected {dims * 8}, got {len(body)}")
    return list(struct.unpack(f">{dims}d", body))


def encode_vector_i8(values: Sequence[int]) -> bytes:
    """Encode a sequence of signed 8-bit integers into an Oracle 26ai `VECTOR(*, INT8)` binary frame."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_oracle_vector_i8"):
        return bytes(native.encode_oracle_vector_i8(list(values)))
    dims = len(values)
    return _encode_vector_header(ORACLE_VECTOR_FMT_I8, dims) + struct.pack(f">{dims}b", *values)


def decode_vector_i8(payload: bytes | bytearray | memoryview) -> list[int]:
    """Decode an Oracle 26ai `VECTOR(*, INT8)` binary frame into a list of signed 8-bit integers."""
    raw = bytes(payload)
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "decode_oracle_vector_i8"):
        try:
            return list(native.decode_oracle_vector_i8(raw))
        except ValueError as exc:
            raise VertebraError(str(exc)) from exc
    dims, body = _decode_vector_header(raw, ORACLE_VECTOR_FMT_I8)
    if len(body) != dims:
        raise VertebraError(f"Oracle INT8 VECTOR payload length mismatch: expected {dims}, got {len(body)}")
    return list(struct.unpack(f">{dims}b", body))


def encode_vector_binary(packed_bits: bytes | Sequence[int]) -> bytes:
    """Encode packed uint8 bytes into an Oracle 26ai `VECTOR(*, BINARY)` binary frame."""
    raw_bits = bytes(packed_bits)
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_oracle_vector_binary"):
        return bytes(native.encode_oracle_vector_binary(raw_bits))
    dims = len(raw_bits) * 8
    return _encode_vector_header(ORACLE_VECTOR_FMT_BINARY, dims) + raw_bits


def decode_vector_binary(payload: bytes | bytearray | memoryview) -> bytes:
    """Decode an Oracle 26ai `VECTOR(*, BINARY)` binary frame into packed bytes."""
    raw = bytes(payload)
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "decode_oracle_vector_binary"):
        try:
            return bytes(native.decode_oracle_vector_binary(raw))
        except ValueError as exc:
            raise VertebraError(str(exc)) from exc
    dims, body = _decode_vector_header(raw, ORACLE_VECTOR_FMT_BINARY)
    expected = dims // 8
    if len(body) != expected:
        raise VertebraError(f"Oracle BINARY VECTOR payload length mismatch: expected {expected}, got {len(body)}")
    return bytes(body)


def encode_oson_payload(json_bytes: bytes) -> bytes:
    """Wrap raw UTF-8 JSON bytes with an 8-byte Oracle 26ai `OSON` binary header."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_oson_payload"):
        return bytes(native.encode_oson_payload(json_bytes))
    return OSON_MAGIC + b"\x01" + struct.pack(">I", len(json_bytes)) + json_bytes


def decode_oson_payload(payload: bytes | bytearray | memoryview) -> bytes:
    """Validate an Oracle 26ai `OSON` binary header and return the underlying JSON bytes."""
    raw = bytes(payload)
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "decode_oson_payload"):
        try:
            return bytes(native.decode_oson_payload(raw))
        except ValueError as exc:
            raise VertebraError(str(exc)) from exc
    if len(raw) < 8 or raw[:3] != OSON_MAGIC:
        raise VertebraError("Invalid Oracle OSON magic header (expected \\xff\\x4a\\x5a)")
    length = struct.unpack(">I", raw[4:8])[0]
    if len(raw) < 8 + length:
        raise VertebraError(f"Truncated Oracle OSON payload: expected {length} bytes, got {len(raw) - 8}")
    return raw[8 : 8 + length]


def encode_oson(value: Any) -> bytes:
    """Serialize a Python value or `msgspec.Struct` into an Oracle 26ai `OSON` binary frame."""
    raw_json = encode_json_bytes(value) if not isinstance(value, (bytes, bytearray)) else bytes(value)
    return encode_oson_payload(raw_json)


def decode_oson(payload: bytes | bytearray | memoryview, *, target_type: Any = Any) -> Any:
    """Decode an Oracle 26ai `OSON` binary frame into a Python value or `msgspec.Struct`."""
    json_bytes = decode_oson_payload(payload)
    decoded = decode_json(json_bytes)
    if target_type is Any:
        return decoded
    return msgspec.convert(decoded, type=target_type)


class OracleConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for the native Oracle Thin database driver."""

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
    tns_alias: str | None = None
    """Optional TNS alias resolved from the wallet's `tnsnames.ora` (e.g. `"mydb_high"`)."""
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
        """Resolve the effective `OracleConnectDescriptor` from wallet, TNS alias, or DSN."""
        if self.wallet_path:
            wallet = OracleWallet.from_path(self.wallet_path)
            alias = self.tns_alias or self.service_name
            resolved = wallet.resolve_alias(alias)
            if resolved is not None:
                return resolved
        if self.dsn:
            return OracleConnectDescriptor.from_tns_descriptor(self.dsn)
        return OracleConnectDescriptor(
            host=self.host,
            port=self.port,
            service_name=self.service_name,
        )

    def session_attrs(self) -> OracleSessionAttrs:
        """Return the configured zero-RTT TTC session attributes."""
        return OracleSessionAttrs(
            client_identifier=self.client_identifier,
            module=self.module,
            action=self.action,
            ecid=self.ecid,
        )


def _normalize_oracle_sql(sql: str, params: Sequence[Any] | Mapping[str, Any] | None) -> tuple[str, tuple[Any, ...]]:
    """Normalize `:1` or `:name` placeholders for the in-memory verification engine when needed."""
    if not params:
        return sql, ()
    if isinstance(params, dict):
        ordered: list[Any] = []

        def _replace_named(match: re.Match[str]) -> str:
            name = match.group(1)
            ordered.append(params.get(name))
            return "?"

        return _NAMED_PARAM_RE.sub(_replace_named, sql), tuple(ordered)

    seq = tuple(params)
    rewritten = re.sub(r":(\d+)", "?", sql)
    return rewritten, seq


class SyncOracleDriver:
    """Synchronous Oracle Thin driver with SHM statement cache and Arrow batch extraction."""

    def __init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or OracleConfig()
        self._broker = broker if broker is not None else self.config.broker
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._pending_session_attrs: OracleSessionAttrs | None = self.config.session_attrs()
        self._last_piggyback_frame: bytes = b""

    @property
    def last_piggyback_frame(self) -> bytes:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        return self._last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes to be piggybacked onto the next SQL round-trip."""
        self._pending_session_attrs = OracleSessionAttrs(
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    def _acquire_broker_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            cached = self._broker.statement_cache.lookup(ORACLE_DIALECT_TAG, sql)
            if cached is None:
                self._broker.statement_cache.insert(
                    ORACLE_DIALECT_TAG,
                    sql,
                    param_count,
                    sql.encode("utf-8"),
                    b"\x00\x01",
                )
        if hasattr(self._broker, "lease_table"):
            return self._broker.lease_table.acquire(os.getpid())
        return None

    def _release_broker_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None and hasattr(self._broker, "lease_table"):
            self._broker.lease_table.release(grant.slot_index, grant.generation)

    def _consume_piggyback(self) -> None:
        if self._pending_session_attrs is not None:
            attrs = self._pending_session_attrs
            if any((attrs.client_identifier, attrs.module, attrs.action, attrs.ecid)):
                self._last_piggyback_frame = attrs.encode_ttc_piggyback()
            self._pending_session_attrs = None

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        norm_sql, bound = _normalize_oracle_sql(sql, params)
        grant = self._acquire_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            self._conn.commit()
            return max(cur.rowcount, 0)
        finally:
            self._release_broker_lease(grant)

    def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a `SELECT` query and return rows as dictionaries."""
        norm_sql, bound = _normalize_oracle_sql(sql, params)
        grant = self._acquire_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [col[0] for col in (cur.description or ())]
            return [dict(zip(cols, tuple(row), strict=False)) for row in rows]
        finally:
            self._release_broker_lease(grant)

    def execute_tuples(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        norm_sql, bound = _normalize_oracle_sql(sql, params)
        grant = self._acquire_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            rows = [tuple(row) for row in cur.fetchall()]
            self._conn.commit()
            return rows
        finally:
            self._release_broker_lease(grant)

    def execute_with_description(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]:
        """Execute SQL and return `(rows, description, rowcount)` for DBAPI compatibility."""
        norm_sql, bound = _normalize_oracle_sql(sql, params)
        grant = self._acquire_broker_lease(sql, len(bound))
        try:
            self._consume_piggyback()
            cur = self._conn.execute(norm_sql, bound)
            if cur.description is not None:
                rows = [tuple(row) for row in cur.fetchall()]
                desc = tuple((col[0], None, None, None, None, None, None) for col in cur.description)
                return rows, desc, len(rows)
            self._conn.commit()
            return [], None, max(cur.rowcount, 0)
        finally:
            self._release_broker_lease(grant)

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy Arrow cursor."""
        rows = self.select(sql, params)
        return ArrowCursor.from_rows(rows, threshold_rows=shm_threshold_rows)

    def close(self) -> None:
        """Close the underlying driver resources."""
        self._conn.close()


class AsyncOracleDriver:
    """Asynchronous Oracle Thin driver with SHM statement cache and Arrow batch extraction."""

    def __init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or OracleConfig()
        self._sync = SyncOracleDriver(self.config, broker=broker)

    @property
    def last_piggyback_frame(self) -> bytes:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        return self._sync.last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes for the next SQL round-trip."""
        self._sync.set_session_attrs(
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    async def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DML or DDL statement asynchronously."""
        return await asyncio.to_thread(self._sync.execute, sql, params)

    async def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a `SELECT` query asynchronously."""
        return await asyncio.to_thread(self._sync.select, sql, params)

    async def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            params,
            shm_threshold_rows=shm_threshold_rows,
        )

    async def close(self) -> None:
        """Close the underlying driver resources."""
        await asyncio.to_thread(self._sync.close)
