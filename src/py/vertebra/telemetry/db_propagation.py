"""Zero-RTT database trace propagation for Oracle Thin mode and PostgreSQL sqlcommenter."""

import re
from typing import Any
from urllib.parse import quote

from vertebra.telemetry.otel import TraceContext

__all__ = ("SqlTracePropagator",)

_IN_LIST_RE = re.compile(r"\bIN\s*\([^()]*\)", re.IGNORECASE)
_VALUES_LIST_RE = re.compile(r"\bVALUES\s*\([^()]*\)(?:\s*,\s*\([^()]*\))+", re.IGNORECASE)


class SqlTracePropagator:
    """Propagates W3C trace context into Oracle Thin connections and SQL queries."""

    @staticmethod
    def apply_oracle_trace(
        conn: Any,
        trace: TraceContext,
        *,
        module: str = "Vertebra",
        action: str = "db.query",
        client_identifier: str | None = None,
    ) -> dict[str, str]:
        """Piggyback OTel trace context onto ``python-oracledb`` Thin connection attributes with zero extra RTT."""
        mod_val = module[:48]
        act_val = action[:64]
        ecid_val = f"{trace.trace_id}:{trace.span_id}"[:64] if trace.span_id else trace.trace_id[:64]
        conn.module = mod_val
        conn.action = act_val
        conn.ecid = ecid_val
        applied: dict[str, str] = {
            "module": mod_val,
            "action": act_val,
            "ecid": ecid_val,
        }
        if client_identifier is not None:
            cid_val = client_identifier[:64]
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
        """Append a Google ``sqlcommenter`` trace comment for Cloud SQL / AlloyDB Query Insights."""
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
        """Collapse high-cardinality ``IN (...)`` and multi-row ``VALUES (...)`` lists for ``db.query.text``."""
        sanitized = _IN_LIST_RE.sub("IN (?)", sql)
        return _VALUES_LIST_RE.sub("VALUES (?)", sanitized)
