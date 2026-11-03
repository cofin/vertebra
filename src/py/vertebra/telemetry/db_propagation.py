"""Zero-RTT database trace propagation for Oracle Thin mode and PostgreSQL sqlcommenter."""

import contextlib
from typing import Any
from urllib.parse import quote

from vertebra._vertebra import sanitize_sql as _native_sanitize_sql
from vertebra.telemetry.otel import TraceContext
from vertebra.utils.text import truncate_utf8_bytes as _truncate_utf8_bytes

__all__ = ("SqlTracePropagator",)

_ORACLE_MAX_MODULE_BYTES = 48
_ORACLE_MAX_ACTION_BYTES = 64
_ORACLE_MAX_CLIENT_ID_BYTES = 64
_ORACLE_MAX_ECONTEXT_ID_BYTES = 64


class SqlTracePropagator:
    """Propagates W3C trace context into Oracle Thin connections and SQL queries."""

    @staticmethod
    def truncate_utf8_bytes(value: str, max_bytes: int) -> str:
        """Truncate a string to at most ``max_bytes`` of UTF-8 without splitting multi-byte codepoints."""
        return _truncate_utf8_bytes(value, max_bytes)

    @staticmethod
    def apply_oracle_trace(
        conn: Any,
        trace: TraceContext,
        *,
        module: str = "Vertebra",
        action: str = "db.query",
        client_identifier: str | None = None,
    ) -> dict[str, str]:
        """Piggyback OTel trace context onto python-oracledb Thin connection attributes with zero extra RTT."""
        mod_val = SqlTracePropagator.truncate_utf8_bytes(module, _ORACLE_MAX_MODULE_BYTES)
        act_val = SqlTracePropagator.truncate_utf8_bytes(action, _ORACLE_MAX_ACTION_BYTES)
        raw_ecid = f"{trace.trace_id}:{trace.span_id}" if trace.span_id else trace.trace_id
        ecid_val = SqlTracePropagator.truncate_utf8_bytes(raw_ecid, _ORACLE_MAX_ECONTEXT_ID_BYTES)
        with contextlib.suppress(AttributeError, TypeError):
            conn.module = mod_val
        with contextlib.suppress(AttributeError, TypeError):
            conn.action = act_val
        with contextlib.suppress(AttributeError, TypeError):
            conn.econtext_id = ecid_val
        with contextlib.suppress(AttributeError, TypeError):
            conn.ecid = ecid_val
        applied: dict[str, str] = {
            "module": mod_val,
            "action": act_val,
            "econtext_id": ecid_val,
            "ecid": ecid_val,
        }
        if client_identifier is not None:
            cid_val = SqlTracePropagator.truncate_utf8_bytes(client_identifier, _ORACLE_MAX_CLIENT_ID_BYTES)
            with contextlib.suppress(AttributeError, TypeError):
                conn.client_identifier = cid_val
            applied["client_identifier"] = cid_val
        return applied

    @staticmethod
    def append_sqlcommenter(
        sql: str,
        trace: TraceContext,
        *,
        db_driver: str = "vertebra-pg",
        route: str | None = None,
        controller: str | None = None,
    ) -> str:
        """Append a Google sqlcommenter trace comment for Cloud SQL / AlloyDB Query Insights."""
        if "/*traceparent=" in sql:
            return sql
        traceparent = trace.format() if isinstance(trace, TraceContext) else str(trace)
        pairs: list[tuple[str, str]] = [
            ("db_driver", db_driver),
            ("traceparent", traceparent),
        ]
        if controller:
            pairs.append(("controller", controller))
        if route:
            pairs.append(("route", route))
        pairs.sort(key=lambda item: item[0])
        encoded = ",".join(f"{quote(k, safe='')}='{quote(v, safe='')}'" for k, v in pairs)
        stripped = sql.rstrip()
        if stripped.endswith(";"):
            return f"{stripped[:-1]} /*{encoded}*/;"
        return f"{stripped} /*{encoded}*/"

    @staticmethod
    def sanitize_sql(sql: str) -> str:
        """Collapse high-cardinality IN (...) and multi-row VALUES (...) lists for db.query.text."""
        return _native_sanitize_sql(sql)
