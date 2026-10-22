"""Execution context, caller identity extraction, cancellation, and progress reporting.

This module unifies caller identities from mTLS client certificates, Google Cloud IAP,
JWT claims, and ASGI/Litestar connections into an immutable CallerIdentity passed across
all agentic operations via AgentContext.
"""

import contextvars
from collections.abc import Callable, Coroutine, Mapping
from typing import Any, Literal

import anyio
import msgspec


class CallerIdentity(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized caller identity extracted from mTLS, IAP, JWT, or ASGI connection state."""

    user_id: str | None = None
    email: str | None = None
    tenant_id: str = "default"
    roles: tuple[str, ...] = ()
    scopes: tuple[str, ...] = ()
    auth_scheme: Literal["mtls", "iap", "jwt", "connection", "anonymous"] = "anonymous"
    client_cert_subject: str | None = None

    @property
    def is_authenticated(self) -> bool:
        """Return True when the caller identity is authenticated."""
        return self.user_id is not None or self.client_cert_subject is not None or self.email is not None

    def owner_key(self) -> str | None:
        """Derive a canonical storage isolation key for the authenticated principal."""
        principal = self.user_id or self.email or self.client_cert_subject
        if principal is None:
            return None
        return f"{self.tenant_id}:{principal}"


class CancellationToken:
    """Cooperative cancellation token backed by an anyio Event."""

    def __init__(self) -> None:
        self._event = anyio.Event()
        self._reason: str = ""

    def cancel(self, reason: str = "") -> None:
        """Signal cancellation to all listeners."""
        self._reason = reason
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        """Return True if cancellation has been signalled."""
        return self._event.is_set()

    @property
    def reason(self) -> str:
        """Return the cancellation reason message."""
        return self._reason

    def raise_if_cancelled(self) -> None:
        """Raise an anyio cancellation exception if cancellation was requested."""
        if self._event.is_set():
            cancelled_exc = anyio.get_cancelled_exc_class()
            raise cancelled_exc()

    async def wait_cancelled(self) -> None:
        """Wait asynchronously until cancellation is signalled."""
        await self._event.wait()


type ProgressCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class ProgressReporter:
    """Emits notifications/progress payloads to an active SSE stream or callback."""

    def __init__(
        self,
        callback: ProgressCallback | None = None,
        progress_token: str | int | None = None,
    ) -> None:
        self.callback = callback
        self.progress_token = progress_token

    async def report(
        self,
        progress: float,
        total: float | None = None,
        message: str | None = None,
    ) -> None:
        """Emit a progress update if a progress_token and callback are present."""
        if self.callback is None or self.progress_token is None:
            return

        payload: dict[str, Any] = {
            "progressToken": self.progress_token,
            "progress": progress,
        }
        if total is not None:
            payload["total"] = total
        if message is not None:
            payload["message"] = message

        await self.callback(payload)


class AgentContext:
    """Execution context carrying caller identity, cancellation token, and progress reporter."""

    def __init__(
        self,
        identity: CallerIdentity | None = None,
        *,
        session_id: str | None = None,
        turn_id: str | None = None,
        cancellation_token: CancellationToken | None = None,
        progress_reporter: ProgressReporter | None = None,
        trace_context: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.identity = identity if identity is not None else CallerIdentity()
        self.session_id = session_id
        self.turn_id = turn_id
        self.cancellation_token = cancellation_token if cancellation_token is not None else CancellationToken()
        self.progress_reporter = progress_reporter if progress_reporter is not None else ProgressReporter()
        self.trace_context = trace_context
        self.metadata = metadata if metadata is not None else {}

    @classmethod
    def current(cls) -> "AgentContext | None":
        """Return the currently bound AgentContext for this task, or None."""
        return current_agent_context()

    @classmethod
    def from_request(
        cls,
        request_or_connection: Any,
        *,
        session_id: str | None = None,
        turn_id: str | None = None,
        progress_token: str | int | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> "AgentContext":
        """Extract CallerIdentity from an HTTP request or connection and build AgentContext."""
        headers: dict[str, str] = {}
        scope: dict[str, Any] = {}

        if hasattr(request_or_connection, "headers"):
            raw_headers = request_or_connection.headers
            if isinstance(raw_headers, Mapping):
                for k, v in raw_headers.items():
                    headers[str(k).lower()] = str(v)
            elif isinstance(raw_headers, (list, tuple)):
                for item in raw_headers:
                    if len(item) == 2:
                        k_str = item[0].decode("latin-1") if isinstance(item[0], bytes) else str(item[0])
                        v_str = item[1].decode("latin-1") if isinstance(item[1], bytes) else str(item[1])
                        headers[k_str.lower()] = v_str

        if hasattr(request_or_connection, "scope") and isinstance(request_or_connection.scope, dict):
            scope = request_or_connection.scope

        user_id: str | None = None
        email: str | None = None
        client_cert_subject: str | None = None
        roles: list[str] = []
        scopes: list[str] = []
        auth_scheme: Literal["mtls", "iap", "jwt", "connection", "anonymous"] = "anonymous"

        tenant_id = headers.get("x-tenant-id", "default")

        if "x-vertebra-mtls-subject" in headers:
            client_cert_subject = headers["x-vertebra-mtls-subject"]
            auth_scheme = "mtls"
        elif "x-forwarded-client-cert" in headers:
            xfcc = headers["x-forwarded-client-cert"]
            client_cert_subject = xfcc
            auth_scheme = "mtls"

        if "x-goog-authenticated-user-email" in headers:
            raw_email = headers["x-goog-authenticated-user-email"]
            email = raw_email.split(":", 1)[1] if raw_email.startswith("accounts.google.com:") else raw_email
            auth_scheme = "iap"

        if "authorization" in headers:
            auth_header = headers["authorization"]
            if auth_header.lower().startswith("bearer ") and auth_scheme == "anonymous":
                auth_scheme = "jwt"

        scope_user = scope.get("user") or getattr(request_or_connection, "user", None)
        if scope_user is not None:
            if hasattr(scope_user, "id"):
                user_id = str(scope_user.id)
            elif isinstance(scope_user, dict) and "id" in scope_user:
                user_id = str(scope_user["id"])
            elif isinstance(scope_user, str):
                user_id = scope_user

            if hasattr(scope_user, "email") and email is None:
                email = str(scope_user.email)
            if hasattr(scope_user, "roles"):
                roles.extend([str(r) for r in scope_user.roles])

            if auth_scheme == "anonymous":
                auth_scheme = "connection"

        scope_auth = scope.get("auth") or getattr(request_or_connection, "auth", None)
        if scope_auth is not None:
            if hasattr(scope_auth, "scopes"):
                scopes.extend([str(s) for s in scope_auth.scopes])
            if hasattr(scope_auth, "roles"):
                roles.extend([str(r) for r in scope_auth.roles])

        identity = CallerIdentity(
            user_id=user_id,
            email=email,
            tenant_id=tenant_id,
            roles=tuple(sorted(set(roles))),
            scopes=tuple(sorted(set(scopes))),
            auth_scheme=auth_scheme,
            client_cert_subject=client_cert_subject,
        )

        progress_rep = ProgressReporter(callback=progress_callback, progress_token=progress_token)

        return cls(
            identity=identity,
            session_id=session_id,
            turn_id=turn_id,
            progress_reporter=progress_rep,
        )


_CURRENT_AGENT_CONTEXT: contextvars.ContextVar[AgentContext | None] = contextvars.ContextVar(
    "current_agent_context",
    default=None,
)


def current_agent_context() -> AgentContext | None:
    """Return the currently bound AgentContext for this task, or None."""
    return _CURRENT_AGENT_CONTEXT.get()


def bind_agent_context(ctx: AgentContext) -> contextvars.Token[AgentContext | None]:
    """Bind an AgentContext to the current execution task."""
    return _CURRENT_AGENT_CONTEXT.set(ctx)


__all__ = [
    "AgentContext",
    "CallerIdentity",
    "CancellationToken",
    "ProgressCallback",
    "ProgressReporter",
    "bind_agent_context",
    "current_agent_context",
]
