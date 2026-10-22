"""Oracle APEX and E-Business Suite (EBS) scoped session context manager."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Protocol

import msgspec

__all__ = (
    "ApexSessionContext",
    "OracleConnectionLike",
    "OraclePoolLike",
    "OracleSessionManager",
)


class OracleConnectionLike(Protocol):
    """Protocol for an async ``python-oracledb`` Thin-mode connection."""

    module: str | None
    action: str | None
    ecid: str | None
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
    """Identity and session context for Oracle APEX and Oracle E-Business Suite."""

    app_id: int
    page_id: int
    session_id: int
    app_user: str
    workspace_id: int | None = None
    ebs_user_id: int | None = None
    ebs_resp_id: int | None = None
    ebs_resp_appl_id: int | None = None
    ebs_org_id: int | None = None
    page_items: dict[str, str] = msgspec.field(default_factory=dict)


class OracleSessionManager:
    """Manages zero-RTT OTel tagging and guaranteed PL/SQL session attachment/detachment."""

    __slots__ = ()

    @staticmethod
    def build_attach_plsql(context: ApexSessionContext) -> tuple[str, dict[str, Any]]:
        """Build a single batched PL/SQL block to initialize EBS, MOAC, VPD, and APEX state."""
        statements: list[str] = [
            "DBMS_SESSION.SET_IDENTIFIER(:app_user);",
        ]
        binds: dict[str, Any] = {
            "app_user": context.app_user,
            "app_id": context.app_id,
            "page_id": context.page_id,
            "session_id": context.session_id,
        }
        if context.ebs_user_id is not None and context.ebs_resp_id is not None and context.ebs_resp_appl_id is not None:
            statements.append(
                "APPS.FND_GLOBAL.APPS_INITIALIZE("
                "user_id => :ebs_user_id, "
                "resp_id => :ebs_resp_id, "
                "resp_appl_id => :ebs_resp_appl_id);"
            )
            binds["ebs_user_id"] = context.ebs_user_id
            binds["ebs_resp_id"] = context.ebs_resp_id
            binds["ebs_resp_appl_id"] = context.ebs_resp_appl_id
        if context.ebs_org_id is not None:
            statements.append("MO_GLOBAL.SET_POLICY_CONTEXT('S', :ebs_org_id);")
            binds["ebs_org_id"] = context.ebs_org_id
        statements.append(
            "APEX_SESSION.ATTACH(p_app_id => :app_id, p_page_id => :page_id, p_session_id => :session_id);"
        )
        for idx, (item_name, item_val) in enumerate(context.page_items.items()):
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
        """Acquire a pooled connection with EBS/APEX state attached and guaranteed teardown."""
        async with pool.acquire() as conn:
            conn.module = module[:48]
            conn.action = action[:64]
            conn.client_identifier = context.app_user[:64]
            if trace_id is not None:
                ecid_val = f"{trace_id}:{span_id}" if span_id else trace_id
                conn.ecid = ecid_val[:64]
            attach_sql, binds = cls.build_attach_plsql(context)
            await conn.execute(attach_sql, binds)
            try:
                yield conn
            finally:
                await conn.execute(cls.build_detach_plsql())
                conn.client_identifier = None
                conn.action = None
                conn.ecid = None
