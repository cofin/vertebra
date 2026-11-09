"""SQL Server TDS packet helpers and `@P1` placeholder rewriter."""

import struct

from vertebra.db.driver._common import MSSQL_DIALECT_TAG
from vertebra.typing import VertebraError
from vertebra.utils.module_loader import import_optional

__all__ = (
    "MSSQL_DIALECT_TAG",
    "TDS_PKT_ATTENTION",
    "TDS_PKT_BULK_LOAD",
    "TDS_PKT_PRE_LOGIN",
    "TDS_PKT_RPC",
    "TDS_PKT_SQL_BATCH",
    "TDS_PKT_TABULAR_RESULT",
    "TDS_PKT_TDS7_LOGIN",
    "TDS_PKT_TRANS_MGR_REQ",
    "MSSQLCore",
    "decode_tds_packet",
    "encode_tds_packet",
    "rewrite_mssql_placeholders",
)

TDS_PKT_SQL_BATCH = 0x01
TDS_PKT_RPC = 0x03
TDS_PKT_TABULAR_RESULT = 0x04
TDS_PKT_ATTENTION = 0x06
TDS_PKT_BULK_LOAD = 0x07
TDS_PKT_TRANS_MGR_REQ = 0x0E
TDS_PKT_TDS7_LOGIN = 0x10
TDS_PKT_PRE_LOGIN = 0x12

_VALID_TDS_PACKET_TYPES = frozenset(
    {
        TDS_PKT_SQL_BATCH,
        TDS_PKT_RPC,
        TDS_PKT_TABULAR_RESULT,
        TDS_PKT_ATTENTION,
        TDS_PKT_BULK_LOAD,
        TDS_PKT_TRANS_MGR_REQ,
        TDS_PKT_TDS7_LOGIN,
        TDS_PKT_PRE_LOGIN,
    }
)


class MSSQLCore:
    """TDS 7.4 packet framing and T-SQL placeholder translation."""

    @staticmethod
    def encode_tds_packet(
        packet_type: int,
        packet_id: int,
        is_eom: bool,
        payload: bytes,
    ) -> bytes:
        """Encode an 8-byte TDS packet header (`[type, status, len_be_u16, spid_be_u16, packet_id, window]`) + payload."""
        total_len = 8 + len(payload)
        status = 0x01 if is_eom else 0x00
        header = struct.pack(">BBHHBB", packet_type & 0xFF, status, total_len & 0xFFFF, 0, packet_id & 0xFF, 0)
        return header + payload

    @staticmethod
    def decode_tds_packet(buf: bytes | bytearray | memoryview) -> tuple[int, bool, int, bytes]:
        """Decode a TDS packet into `(packet_type, is_eom, packet_id, payload)`."""
        raw = bytes(buf)
        if len(raw) < 8:
            raise VertebraError("Buffer too short for 8-byte TDS packet header")
        pkt_type, status, total_len, _spid, packet_id, _window = struct.unpack(">BBHHBB", raw[:8])
        if pkt_type not in _VALID_TDS_PACKET_TYPES:
            raise VertebraError(f"Unsupported TDS packet type byte: 0x{pkt_type:02x}")
        if total_len < 8 or len(raw) < total_len:
            raise VertebraError(f"Invalid or truncated TDS packet length: expected {total_len}, got {len(raw)}")
        is_eom = (status & 0x01) != 0
        return (pkt_type, is_eom, packet_id, raw[8:total_len])

    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]:
        """Rewrite `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...` outside quotes, brackets, and comments."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "rewrite_mssql_placeholders"):
            rewritten, count = native.rewrite_mssql_placeholders(sql)
            return str(rewritten), int(count)

        out: list[str] = []
        state = "NORMAL"
        i = 0
        n = len(sql)
        seq_index = 0
        max_param = 0

        while i < n:
            ch = sql[i]
            if state == "NORMAL":
                if ch == "'":
                    out.append(ch)
                    state = "SINGLE_QUOTE"
                    i += 1
                elif ch == '"':
                    out.append(ch)
                    state = "DOUBLE_QUOTE"
                    i += 1
                elif ch == "[":
                    out.append(ch)
                    state = "BRACKET"
                    i += 1
                elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
                    out.append("--")
                    state = "LINE_COMMENT"
                    i += 2
                elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
                    out.append("/*")
                    state = "BLOCK_COMMENT"
                    i += 2
                elif ch == "?":
                    seq_index += 1
                    max_param = max(max_param, seq_index)
                    out.append(f"@P{seq_index}")
                    i += 1
                elif ch == "$" and i + 1 < n and sql[i + 1].isdigit() and sql[i + 1] != "0":
                    j = i + 1
                    while j < n and sql[j].isdigit():
                        j += 1
                    num = int(sql[i + 1 : j])
                    seq_index = max(seq_index, num)
                    max_param = max(max_param, num)
                    out.append(f"@P{num}")
                    i = j
                else:
                    out.append(ch)
                    i += 1
            elif state == "SINGLE_QUOTE":
                out.append(ch)
                if ch == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        out.append("'")
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "DOUBLE_QUOTE":
                out.append(ch)
                if ch == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        out.append('"')
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "BRACKET":
                out.append(ch)
                if ch == "]":
                    if i + 1 < n and sql[i + 1] == "]":
                        out.append("]")
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "LINE_COMMENT":
                out.append(ch)
                i += 1
                if ch == "\n":
                    state = "NORMAL"
            elif state == "BLOCK_COMMENT":
                if ch == "*" and i + 1 < n and sql[i + 1] == "/":
                    out.append("*/")
                    state = "NORMAL"
                    i += 2
                else:
                    out.append(ch)
                    i += 1

        return "".join(out), max_param


encode_tds_packet = MSSQLCore.encode_tds_packet
decode_tds_packet = MSSQLCore.decode_tds_packet
rewrite_mssql_placeholders = MSSQLCore.rewrite_placeholders
