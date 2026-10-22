"""Oracle APEX scoped session context manager, VPD context propagation, and auth handoff."""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Protocol

import msgspec

from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "ApexHandoffClaims",
    "ApexSessionAuth",
    "ApexSessionContext",
    "OracleConnectionLike",
    "OraclePoolLike",
    "OracleSessionManager",
)

_APEX_ITEM_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")


class OracleConnectionLike(Protocol):
    """Protocol for an async ``python-oracledb`` Thin-mode connection."""

    module: str | None
    action: str | None
    ecid: str | None
    econtext_id: str | None
    client_identifier: str | None

    async def execute(
        self,
        statement: str,
        parameters: dict[str, Any] | list[Any] | None = None,
    ) -> Any:
        """Execute a SQL or PL/SQL statement on the connection."""
        ...


class OraclePoolLike(Protocol):
    """Protocol for an async ``python-oracledb`` connection pool."""

    def acquire(self) -> Any:
        """Acquire a connection from the pool as an async context manager."""
        ...


class ApexSessionContext(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Identity, VPD namespace, and session context for Oracle APEX."""

    app_id: int
    page_id: int
    session_id: int
    app_user: str
    workspace_id: int | None = None
    client_info: str | None = None
    vpd_context: dict[str, dict[str, str]] = msgspec.field(default_factory=dict)
    page_items: dict[str, str] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        for item_name in self.page_items:
            if not _APEX_ITEM_RE.match(item_name):
                msg = f"Invalid APEX page item identifier: {item_name!r}"
                raise ValueError(msg)
        for namespace, attrs in self.vpd_context.items():
            if not _APEX_ITEM_RE.match(namespace):
                msg = f"Invalid Oracle VPD context namespace: {namespace!r}"
                raise ValueError(msg)
            for attr_name in attrs:
                if not _APEX_ITEM_RE.match(attr_name):
                    msg = f"Invalid Oracle VPD context attribute: {attr_name!r}"
                    raise ValueError(msg)


class ApexHandoffClaims(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Signed session handoff claims minted by an Oracle APEX page orauthentication callback."""

    app_id: int
    page_id: int
    session_id: int
    app_user: str
    exp: int
    workspace_id: int | None = None
    roles: tuple[str, ...] = ()
    page_items: dict[str, str] = msgspec.field(default_factory=dict)

    def to_session_context(self) -> ApexSessionContext:
        """Convert verified handoff claims into an ``ApexSessionContext``."""
        return ApexSessionContext(
            app_id=self.app_id,
            page_id=self.page_id,
            session_id=self.session_id,
            app_user=self.app_user,
            workspace_id=self.workspace_id,
            page_items=dict(self.page_items),
        )


class ApexSessionAuth:
    """Authenticates requests into a Vertebra app using signed Oracle APEX handoff tokens."""

    __slots__ = ("_secret", "header_name")

    def __init__(self, secret: str | bytes, *, header_name: str = "x-apex-handoff-token") -> None:
        self._secret = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        self.header_name = header_name.lower()

    def issue_handoff_token(self, claims: ApexHandoffClaims) -> str:
        """Issue an HMAC-SHA256 signed base64url handoff token for ``claims``."""
        payload_bytes = encode_json_bytes(msgspec.to_builtins(claims))
        encoded_payload = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode("ascii")
        sig = hmac.new(self._secret, encoded_payload.encode("ascii"), hashlib.sha256).hexdigest()
        return f"{encoded_payload}.{sig}"

    def verify_handoff_token(
        self,
        token: str,
        *,
        now_epoch: int | None = None,
    ) -> ApexHandoffClaims | None:
        """Verify signature and ``exp`` of an APEX handoff token, returning claims when valid."""
        parts = token.split(".", 1)
        if len(parts) != 2:
            return None
        encoded_payload, provided_sig = parts
        expected_sig = hmac.new(
            self._secret,
            encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(provided_sig, expected_sig):
            return None
        padding = "=" * (-len(encoded_payload) % 4)
        try:
            raw_bytes = base64.urlsafe_b64decode(encoded_payload + padding)
            decoded = decode_json(raw_bytes)
            claims = msgspec.convert(decoded, type=ApexHandoffClaims)
        except Exception:
            return None
        current_epoch = int(time.time()) if now_epoch is None else now_epoch
        if claims.exp <= current_epoch:
            return None
        return claims

    def authenticate_headers(
        self,
        headers: Mapping[str, str],
        *,
        now_epoch: int | None = None,
    ) -> ApexSessionContext | None:
        """Extract and verify an ``ApexSessionContext`` from request headers."""
        token: str | None = None
        for key, val in headers.items():
            if key.lower() == self.header_name:
                token = val.strip()
                break
        if not token:
            return None
        claims = self.verify_handoff_token(token, now_epoch=now_epoch)
        if claims is None:
            return None
        return claims.to_session_context()


class OracleSessionManager:
    """Manages zero-RTT OTel tagging and guaranteed PL/SQL APEX session attachment/detachment."""

    __slots__ = ()

    @staticmethod
    def build_attach_plsql(context: ApexSessionContext) -> tuple[str, dict[str, Any]]:
        """Build a single batched PL/SQL block to initialize VPD, client info, and APEX state."""
        statements: list[str] = [
            "DBMS_SESSION.SET_IDENTIFIER(:app_user);",
        ]
        binds: dict[str, Any] = {
            "app_user": context.app_user,
            "app_id": context.app_id,
            "page_id": context.page_id,
            "session_id": context.session_id,
        }
        if context.client_info is not None:
            statements.append("DBMS_APPLICATION_INFO.SET_CLIENT_INFO(:client_info);")
            binds["client_info"] = context.client_info
        ctx_idx = 0
        for namespace, attrs in context.vpd_context.items():
            if not _APEX_ITEM_RE.match(namespace):
                msg = f"Invalid Oracle VPD context namespace: {namespace!r}"
                raise ValueError(msg)
            for attr_name, attr_val in attrs.items():
                if not _APEX_ITEM_RE.match(attr_name):
                    msg = f"Invalid Oracle VPD context attribute: {attr_name!r}"
                    raise ValueError(msg)
                ns_bind = f"vpd_ns_{ctx_idx}"
                attr_bind = f"vpd_attr_{ctx_idx}"
                val_bind = f"vpd_val_{ctx_idx}"
                statements.append(f"DBMS_SESSION.SET_CONTEXT(:{ns_bind}, :{attr_bind}, :{val_bind});")
                binds[ns_bind] = namespace
                binds[attr_bind] = attr_name
                binds[val_bind] = attr_val
                ctx_idx += 1
        statements.append(
            "APEX_SESSION.ATTACH(p_app_id => :app_id, p_page_id => :page_id, p_session_id => :session_id);"
        )
        for idx, (item_name, item_val) in enumerate(context.page_items.items()):
            if not _APEX_ITEM_RE.match(item_name):
                msg = f"Invalid APEX page item identifier: {item_name!r}"
                raise ValueError(msg)
            k_bind = f"item_k_{idx}"
            v_bind = f"item_v_{idx}"
            statements.append(f"APEX_UTIL.SET_SESSION_STATE(:{k_bind}, :{v_bind});")
            binds[k_bind] = item_name
            binds[v_bind] = item_val
        body = "\n    ".join(statements)
        return f"BEGIN\n    {body}\nEND;", binds

    @staticmethod
    def build_detach_plsql() -> str:
        """Build the cleanup PL/SQL block that detaches APEX and clears VPD client identifier."""
        return "BEGIN\n    APEX_SESSION.DETACH;\n    DBMS_SESSION.CLEAR_IDENTIFIER;\nEND;"

    @classmethod
    @asynccontextmanager
    async def acquire_scoped(
        cls,
        pool: OraclePoolLike,
        context: ApexSessionContext,
        *,
        trace_id: str | None = None,
        span_id: str | None = None,
        module: str = "Vertebra",
        action: str = "apex.request",
    ) -> AsyncIterator[OracleConnectionLike]:
        """Acquire a pooled connection with APEX/VPD state attached and guaranteed teardown."""
        async with pool.acquire() as conn:
            conn.module = module[:48]
            conn.action = action[:64]
            conn.client_identifier = context.app_user[:64]
            if trace_id is not None:
                ecid_val = (f"{trace_id}:{span_id}" if span_id else trace_id)[:64]
                conn.ecid = ecid_val
                if hasattr(conn, "econtext_id"):
                    conn.econtext_id = ecid_val
            attach_sql, binds = cls.build_attach_plsql(context)
            await conn.execute(attach_sql, binds)
            try:
                yield conn
            finally:
                try:
                    await conn.execute(cls.build_detach_plsql())
                finally:
                    conn.client_identifier = None
                    conn.action = None
                    conn.ecid = None
                    if hasattr(conn, "econtext_id"):
                        conn.econtext_id = None
