"""Oracle TNS packet helpers, tnsnames.ora parsing, TTC session attributes, and 26ai VECTOR/OSON codecs."""

import datetime
import decimal
import re
import struct
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal

import msgspec

from vertebra._vertebra import VectorView
from vertebra.db.driver import DataError
from vertebra.db.driver._common import ORACLE_DIALECT_TAG
from vertebra.db.rls import TenantScopeConfig
from vertebra.typing import VertebraError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.oracle.config import OracleDriverFeatures

__all__ = (
    "ORACLE_DIALECT_TAG",
    "ORACLE_VECTOR_FMT_BINARY",
    "ORACLE_VECTOR_FMT_F32",
    "ORACLE_VECTOR_FMT_F64",
    "ORACLE_VECTOR_FMT_I8",
    "ORACLE_VECTOR_MAGIC",
    "ORACLE_VECTOR_VERSION",
    "OSON_MAGIC",
    "OracleCore",
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
)

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


_DEFAULT_TENANT_SCOPE = TenantScopeConfig()


class OracleCore:
    """TNS packet helpers, TTC session attributes, and Oracle 26ai `VECTOR` and `OSON` binary codecs."""

    ORACLE_VECTOR_MAGIC: Final[bytes] = b"\xdb\x01"

    @staticmethod
    def build_session_context_sql(
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
        *,
        namespace: str = "VERTEBRA_CTX",
    ) -> tuple[str, dict[str, str]]:
        """Build the bound `DBMS_SESSION.SET_CONTEXT` call that scopes a session to a tenant value."""
        attribute = config.setting_name.rsplit(".", 1)[-1]
        validate_identifier(namespace, "namespace", allow_dots=False)
        validate_identifier(attribute, "attribute", allow_dots=False)
        return (
            "BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;",
            {"namespace": namespace, "attribute": attribute, "value": tenant_value},
        )

    @staticmethod
    def parse_tns_descriptor_fields(descriptor: str) -> dict[str, Any]:
        """Parse a TNS `(DESCRIPTION=...)` string or Easy Connect string into field kwargs."""
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
            return {
                "host": host or "localhost",
                "port": port,
                "service_name": svc_part or "FREEPDB1",
                "protocol": protocol,
            }

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

        return {
            "host": host_m.group(1) if host_m else "localhost",
            "port": int(port_m.group(1)) if port_m else 1521,
            "service_name": service_name,
            "sid": sid,
            "security_cert_dn": dn_m.group(1).strip() if dn_m else None,
            "retry_count": int(rc_m.group(1)) if rc_m else 0,
            "retry_delay": int(rd_m.group(1)) if rd_m else 0,
            "transport_connect_timeout": int(ct_m.group(1)) if ct_m else 0,
            "protocol": proto_m.group(1).upper() if proto_m else "TCP",
            "server_mode": srv_m.group(1).upper() if srv_m else "DEDICATED",
        }

    @staticmethod
    def parse_tnsnames_ora(contents: str) -> dict[str, str]:
        """Parse a `tnsnames.ora` file into a dictionary mapping aliases to TNS descriptors."""
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

    @staticmethod
    def resolve_tns_alias_raw(contents: str, alias: str) -> str | dict[str, Any] | None:
        """Resolve a TNS alias from `tnsnames.ora` contents into a descriptor string or dict."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "resolve_tns_alias"):
            resolved = native.resolve_tns_alias(contents, alias)
            if resolved is not None and isinstance(resolved, (str, dict)):
                return resolved
        mapping = OracleCore.parse_tnsnames_ora(contents)
        return mapping.get(alias) or mapping.get(alias.upper())

    @staticmethod
    def encode_tns_connect_packet(descriptor: str) -> bytes:
        """Encode an Oracle TNS `Connect` (`type = 1`) packet wrapping a `(DESCRIPTION=...)` descriptor."""
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

    @staticmethod
    def encode_tns_data_packet(data_flags: int, payload: bytes) -> bytes:
        """Encode an Oracle TNS `Data` (`type = 6`) packet with 2-byte big-endian `data_flags`."""
        total_len = 10 + len(payload)
        header = struct.pack(">HHBBHH", total_len, 0, 6, 0, 0, data_flags & 0xFFFF)
        return header + payload

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def encode_vector_f32(values: Sequence[float]) -> bytes:
        """Encode a sequence of `float` values into an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "encode_oracle_vector_f32"):
            return bytes(native.encode_oracle_vector_f32(list(values)))
        dims = len(values)
        return OracleCore._encode_vector_header(ORACLE_VECTOR_FMT_F32, dims) + struct.pack(f">{dims}f", *values)

    @staticmethod
    def decode_vector_f32(payload: bytes | bytearray | memoryview) -> list[float]:
        """Decode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame into a list of floats."""
        raw = bytes(payload)
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "decode_oracle_vector_f32"):
            try:
                return list(native.decode_oracle_vector_f32(raw))
            except ValueError as exc:
                raise OracleCore._data_error(str(exc)) from exc
        dims, body = OracleCore._decode_vector_header(raw, ORACLE_VECTOR_FMT_F32)
        if len(body) != dims * 4:
            raise OracleCore._data_error(
                f"Oracle FLOAT32 VECTOR payload length mismatch: expected {dims * 4}, got {len(body)}",
            )
        return list(struct.unpack(f">{dims}f", body))

    @staticmethod
    def encode_vector_f64(values: Sequence[float]) -> bytes:
        """Encode a sequence of `float` values into an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "encode_oracle_vector_f64"):
            return bytes(native.encode_oracle_vector_f64(list(values)))
        dims = len(values)
        return OracleCore._encode_vector_header(ORACLE_VECTOR_FMT_F64, dims) + struct.pack(f">{dims}d", *values)

    @staticmethod
    def decode_vector_f64(payload: bytes | bytearray | memoryview) -> list[float]:
        """Decode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame into a list of floats."""
        raw = bytes(payload)
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "decode_oracle_vector_f64"):
            try:
                return list(native.decode_oracle_vector_f64(raw))
            except ValueError as exc:
                raise OracleCore._data_error(str(exc)) from exc
        dims, body = OracleCore._decode_vector_header(raw, ORACLE_VECTOR_FMT_F64)
        if len(body) != dims * 8:
            raise OracleCore._data_error(
                f"Oracle FLOAT64 VECTOR payload length mismatch: expected {dims * 8}, got {len(body)}",
            )
        return list(struct.unpack(f">{dims}d", body))

    @staticmethod
    def encode_vector_i8(values: Sequence[int]) -> bytes:
        """Encode a sequence of signed 8-bit integers into an Oracle 26ai `VECTOR(*, INT8)` binary frame."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "encode_oracle_vector_i8"):
            return bytes(native.encode_oracle_vector_i8(list(values)))
        dims = len(values)
        return OracleCore._encode_vector_header(ORACLE_VECTOR_FMT_I8, dims) + struct.pack(f">{dims}b", *values)

    @staticmethod
    def decode_vector_i8(payload: bytes | bytearray | memoryview) -> list[int]:
        """Decode an Oracle 26ai `VECTOR(*, INT8)` binary frame into a list of signed 8-bit integers."""
        raw = bytes(payload)
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "decode_oracle_vector_i8"):
            try:
                return list(native.decode_oracle_vector_i8(raw))
            except ValueError as exc:
                raise OracleCore._data_error(str(exc)) from exc
        dims, body = OracleCore._decode_vector_header(raw, ORACLE_VECTOR_FMT_I8)
        if len(body) != dims:
            raise OracleCore._data_error(
                f"Oracle INT8 VECTOR payload length mismatch: expected {dims}, got {len(body)}",
            )
        return list(struct.unpack(f">{dims}b", body))

    @staticmethod
    def encode_vector_binary(packed_bits: bytes | Sequence[int]) -> bytes:
        """Encode packed uint8 bytes into an Oracle 26ai `VECTOR(*, BINARY)` binary frame."""
        raw_bits = bytes(packed_bits)
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "encode_oracle_vector_binary"):
            return bytes(native.encode_oracle_vector_binary(raw_bits))
        dims = len(raw_bits) * 8
        return OracleCore._encode_vector_header(ORACLE_VECTOR_FMT_BINARY, dims) + raw_bits

    @staticmethod
    def decode_vector_binary(payload: bytes | bytearray | memoryview) -> bytes:
        """Decode an Oracle 26ai `VECTOR(*, BINARY)` binary frame into packed bytes."""
        raw = bytes(payload)
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "decode_oracle_vector_binary"):
            try:
                return bytes(native.decode_oracle_vector_binary(raw))
            except ValueError as exc:
                raise OracleCore._data_error(str(exc)) from exc
        dims, body = OracleCore._decode_vector_header(raw, ORACLE_VECTOR_FMT_BINARY)
        expected = dims // 8
        if len(body) != expected:
            raise OracleCore._data_error(
                f"Oracle BINARY VECTOR payload length mismatch: expected {expected}, got {len(body)}",
            )
        return bytes(body)

    @staticmethod
    def encode_vector(
        values: Sequence[float | int] | bytes | VectorView,
        *,
        dtype: Literal["FLOAT32", "FLOAT64", "INT8", "BINARY"] = "FLOAT32",
    ) -> bytes:
        """Encode a `VectorView`, numeric sequence, or packed byte buffer into an Oracle `0xDB 0x01` frame."""
        if isinstance(values, VectorView):
            vv_dtype = values.dtype.lower()
            raw_b = values.to_bytes()
            dims = values.dimensions
            if vv_dtype == "float32":
                return OracleCore.encode_vector_f32(values.to_list())
            if vv_dtype == "float64":
                vals_f64 = struct.unpack(f"={dims}d", raw_b)
                return OracleCore.encode_vector_f64(vals_f64)
            if vv_dtype == "int8":
                vals_i8 = struct.unpack(f"={dims}b", raw_b)
                return OracleCore.encode_vector_i8(vals_i8)
            if vv_dtype == "binary":
                return OracleCore.encode_vector_binary(raw_b)
            raise OracleCore._data_error(f"Unsupported VectorView dtype: {vv_dtype}")

        dtype_upper = dtype.upper()
        if dtype_upper == "FLOAT32":
            if isinstance(values, (bytes, bytearray, memoryview)):
                raw = bytes(values)
                if raw.startswith(OracleCore.ORACLE_VECTOR_MAGIC):
                    OracleCore.decode_vector_f32(raw)
                    return raw
                if len(raw) % 4 != 0:
                    raise OracleCore._data_error("FLOAT32 byte buffer length must be a multiple of 4")
                count = len(raw) // 4
                return OracleCore.encode_vector_f32(struct.unpack(f"={count}f", raw))
            return OracleCore.encode_vector_f32([float(v) for v in values])
        if dtype_upper == "FLOAT64":
            if isinstance(values, (bytes, bytearray, memoryview)):
                raw = bytes(values)
                if raw.startswith(OracleCore.ORACLE_VECTOR_MAGIC):
                    OracleCore.decode_vector_f64(raw)
                    return raw
                if len(raw) % 8 != 0:
                    raise OracleCore._data_error("FLOAT64 byte buffer length must be a multiple of 8")
                count = len(raw) // 8
                return OracleCore.encode_vector_f64(struct.unpack(f"={count}d", raw))
            return OracleCore.encode_vector_f64([float(v) for v in values])
        if dtype_upper == "INT8":
            if isinstance(values, (bytes, bytearray, memoryview)):
                raw = bytes(values)
                if raw.startswith(OracleCore.ORACLE_VECTOR_MAGIC):
                    OracleCore.decode_vector_i8(raw)
                    return raw
                return OracleCore.encode_vector_i8(struct.unpack(f"={len(raw)}b", raw))
            return OracleCore.encode_vector_i8([int(v) for v in values])
        if dtype_upper == "BINARY":
            if isinstance(values, (bytes, bytearray, memoryview)):
                raw = bytes(values)
                if raw.startswith(OracleCore.ORACLE_VECTOR_MAGIC):
                    OracleCore.decode_vector_binary(raw)
                    return raw
                return OracleCore.encode_vector_binary(raw)
            return OracleCore.encode_vector_binary(bytes(int(v) & 0xFF for v in values))
        raise OracleCore._data_error(f"Unsupported Oracle VECTOR dtype: {dtype}")

    @staticmethod
    def decode_vector(
        payload: bytes | VectorView | Sequence[float | int],
        *,
        return_format: Literal["vector_view", "list", "bytes"] = "vector_view",
    ) -> VectorView | list[float] | list[int] | bytes:
        """Decode an Oracle `0xDB 0x01` `VECTOR` payload into a `VectorView`, `list`, or `bytes`."""
        if isinstance(payload, VectorView):
            if return_format == "vector_view":
                return payload
            if return_format == "bytes":
                return payload.to_bytes()
            if return_format == "list":
                vv_dtype = payload.dtype.lower()
                if vv_dtype == "float32":
                    return payload.to_list()
                raw_b = payload.to_bytes()
                dims = payload.dimensions
                if vv_dtype == "float64":
                    return list(struct.unpack(f"={dims}d", raw_b))
                if vv_dtype == "int8":
                    return list(struct.unpack(f"={dims}b", raw_b))
                if vv_dtype == "binary":
                    return list(raw_b)
                return list(memoryview(payload).tolist())
            raise OracleCore._data_error(f"Unsupported vector return_format: {return_format}")

        if isinstance(payload, (bytes, bytearray, memoryview)):
            raw = bytes(payload)
            if len(raw) < 8 or raw[:2] != OracleCore.ORACLE_VECTOR_MAGIC:
                raise OracleCore._data_error("Invalid Oracle VECTOR 0xDB 0x01 header")
            fmt = raw[2]
            dims = struct.unpack(">I", raw[4:8])[0]
            if fmt == ORACLE_VECTOR_FMT_F32:
                vals_f32 = OracleCore.decode_vector_f32(raw)
                if return_format == "list":
                    return vals_f32
                if return_format == "bytes":
                    return raw[8:]
                if return_format == "vector_view":
                    return VectorView.from_f32(vals_f32, normalize=False)
            elif fmt == ORACLE_VECTOR_FMT_F64:
                vals_f64 = OracleCore.decode_vector_f64(raw)
                if return_format == "list":
                    return vals_f64
                if return_format == "bytes":
                    return raw[8:]
                if return_format == "vector_view":
                    native_f64 = struct.pack(f"={dims}d", *vals_f64)
                    return VectorView.from_bytes(native_f64, dims, dtype="float64")
            elif fmt == ORACLE_VECTOR_FMT_I8:
                vals_i8 = OracleCore.decode_vector_i8(raw)
                if return_format == "list":
                    return vals_i8
                if return_format == "bytes":
                    return raw[8:]
                if return_format == "vector_view":
                    native_i8 = struct.pack(f"={dims}b", *vals_i8)
                    return VectorView.from_bytes(native_i8, dims, dtype="int8", scale=1.0)
            elif fmt == ORACLE_VECTOR_FMT_BINARY:
                packed_bin = OracleCore.decode_vector_binary(raw)
                if return_format == "list":
                    return list(packed_bin)
                if return_format == "bytes":
                    return packed_bin
                if return_format == "vector_view":
                    return VectorView.from_bytes(packed_bin, dims, dtype="binary")
            else:
                raise OracleCore._data_error(f"Unsupported Oracle VECTOR format code: {fmt}")
            raise OracleCore._data_error(f"Unsupported vector return_format: {return_format}")

        seq_vals = [float(v) for v in payload]
        if return_format == "list":
            return seq_vals
        if return_format == "bytes":
            return struct.pack(f">{len(seq_vals)}f", *seq_vals)
        if return_format == "vector_view":
            return VectorView.from_f32(seq_vals, normalize=False)
        raise OracleCore._data_error(f"Unsupported vector return_format: {return_format}")

    @staticmethod
    def encode_oson_payload(json_bytes: bytes) -> bytes:
        """Wrap raw UTF-8 JSON bytes with an 8-byte Oracle 26ai `OSON` binary header."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "encode_oson_payload"):
            return bytes(native.encode_oson_payload(json_bytes))
        return OSON_MAGIC + b"\x01" + struct.pack(">I", len(json_bytes)) + json_bytes

    @staticmethod
    def decode_oson_payload(payload: bytes | bytearray | memoryview) -> bytes:
        """Validate an Oracle 26ai `OSON` binary header (`\\xffJZ\\x01`) and return the underlying JSON bytes."""
        raw = bytes(payload)
        if len(raw) < 8 or raw[:3] != OSON_MAGIC or raw[3] != 0x01:
            raise OracleCore._data_error("Invalid Oracle OSON magic/version header (expected \\xff\\x4a\\x5a\\x01)")
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "decode_oson_payload"):
            try:
                return bytes(native.decode_oson_payload(raw))
            except ValueError as exc:
                raise OracleCore._data_error(str(exc)) from exc
        length = struct.unpack(">I", raw[4:8])[0]
        if len(raw) < 8 + length:
            raise OracleCore._data_error(
                f"Truncated Oracle OSON payload: expected {length} bytes, got {len(raw) - 8}",
            )
        return raw[8 : 8 + length]

    @staticmethod
    def encode_oson(value: Any) -> bytes:
        """Serialize a Python value or `msgspec.Struct` into an Oracle 26ai `OSON` binary frame."""
        raw_json = encode_json_bytes(value) if not isinstance(value, (bytes, bytearray)) else bytes(value)
        return OracleCore.encode_oson_payload(raw_json)

    @staticmethod
    def decode_oson(payload: bytes | bytearray | memoryview, *, target_type: Any = Any) -> Any:
        """Decode an Oracle 26ai `OSON` binary frame into a Python value or `msgspec.Struct`."""
        json_bytes = OracleCore.decode_oson_payload(payload)
        try:
            decoded = decode_json(json_bytes)
        except Exception as exc:
            raise OracleCore._data_error(f"Invalid JSON payload inside OSON frame: {exc}") from exc
        if target_type is Any:
            return decoded
        return msgspec.convert(decoded, type=target_type)

    @staticmethod
    def coerce_bind_parameter(
        value: Any,
        *,
        features: OracleDriverFeatures | None = None,
    ) -> Any:
        """Coerce a Python bind parameter according to `OracleDriverFeatures`."""
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            enable_uuid = True if features is None else features.enable_uuid_binary
            return value.bytes if enable_uuid else str(value)
        if isinstance(value, VectorView):
            return OracleCore.encode_vector(value)
        if isinstance(value, (dict, msgspec.Struct)):
            return OracleCore.encode_oson(value)
        return value

    @staticmethod
    def coerce_row_cell(
        value: Any,
        column_type: str | None = None,
        *,
        features: OracleDriverFeatures | None = None,
    ) -> Any:
        """Coerce a decoded Oracle row cell according to column metadata and `OracleDriverFeatures`."""
        if value is None:
            return None
        col_upper = (column_type or "").upper()
        enable_uuid = True if features is None else features.enable_uuid_binary
        fetch_decimals = False if features is None else features.fetch_decimals
        vec_fmt_raw = "list" if features is None else features.vector_return_format

        if isinstance(value, (bytes, bytearray, memoryview)):
            raw = bytes(value)
            if len(raw) >= 8 and raw[:3] == OSON_MAGIC and raw[3] == 0x01:
                return OracleCore.decode_oson(raw)
            if len(raw) >= 8 and raw[:2] == OracleCore.ORACLE_VECTOR_MAGIC:
                ret_fmt: Literal["vector_view", "list", "bytes"] = (
                    "vector_view"
                    if vec_fmt_raw == "vector_view"
                    else ("bytes" if vec_fmt_raw == "bytes" else "list")
                )
                return OracleCore.decode_vector(raw, return_format=ret_fmt)
            if enable_uuid and len(raw) == 16 and ("RAW" in col_upper or not col_upper):
                return uuid.UUID(bytes=raw)
            return raw

        if isinstance(value, VectorView):
            if vec_fmt_raw == "list":
                return OracleCore.decode_vector(value, return_format="list")
            if vec_fmt_raw == "bytes":
                return value.to_bytes()
            return value

        if fetch_decimals and isinstance(value, (int, float)) and not isinstance(value, bool):
            return decimal.Decimal(str(value))
        if not fetch_decimals and isinstance(value, decimal.Decimal) and value == value.to_integral_value():
            return int(value)
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value
        return value

    @staticmethod
    def normalize_sql(
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None,
    ) -> tuple[str, tuple[Any, ...]]:
        """Normalize `:1` or `:name` placeholders into positional parameters."""
        if not params:
            return sql, ()
        if isinstance(params, Mapping):
            cleaned_map = {str(k).lstrip(":"): v for k, v in params.items()}
            ordered: list[Any] = []

            def _replace_named(match: re.Match[str]) -> str:
                name = match.group(1)
                ordered.append(cleaned_map.get(name))
                return "?"

            return _NAMED_PARAM_RE.sub(_replace_named, sql), tuple(ordered)

        seq = tuple(params)
        rewritten = re.sub(r":(\d+)", "?", sql)
        return rewritten, seq

    @staticmethod
    def _data_error(message: str) -> DataError:
        err = DataError(message)
        err.sqlstate = "22000"
        return err

    @staticmethod
    def _encode_vector_header(fmt: int, dims: int) -> bytes:
        return struct.pack(">BBBBI", ORACLE_VECTOR_MAGIC, ORACLE_VECTOR_VERSION, fmt, 0, dims)

    @staticmethod
    def _decode_vector_header(buf: bytes, expected_fmt: int) -> tuple[int, memoryview]:
        if len(buf) < 8:
            raise OracleCore._data_error("Buffer too short for 8-byte Oracle VECTOR header")
        magic, version, fmt, _flags, dims = struct.unpack(">BBBBI", buf[:8])
        if magic != ORACLE_VECTOR_MAGIC:
            raise OracleCore._data_error(f"Invalid Oracle VECTOR magic byte: 0x{magic:02x}")
        if version != ORACLE_VECTOR_VERSION:
            raise OracleCore._data_error(f"Unsupported Oracle VECTOR version: {version}")
        if fmt != expected_fmt:
            raise OracleCore._data_error(f"Oracle VECTOR format mismatch: expected {expected_fmt}, got {fmt}")
        return dims, memoryview(buf)[8:]


encode_tns_connect_packet = OracleCore.encode_tns_connect_packet
encode_tns_data_packet = OracleCore.encode_tns_data_packet
decode_tns_packet = OracleCore.decode_tns_packet
encode_ttc_session_attrs = OracleCore.encode_ttc_session_attrs
decode_ttc_session_attrs = OracleCore.decode_ttc_session_attrs
encode_vector_f32 = OracleCore.encode_vector_f32
decode_vector_f32 = OracleCore.decode_vector_f32
encode_vector_f64 = OracleCore.encode_vector_f64
decode_vector_f64 = OracleCore.decode_vector_f64
encode_vector_i8 = OracleCore.encode_vector_i8
decode_vector_i8 = OracleCore.decode_vector_i8
encode_vector_binary = OracleCore.encode_vector_binary
decode_vector_binary = OracleCore.decode_vector_binary
encode_oson_payload = OracleCore.encode_oson_payload
decode_oson_payload = OracleCore.decode_oson_payload
encode_oson = OracleCore.encode_oson
decode_oson = OracleCore.decode_oson
