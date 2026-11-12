"""Execution context, caller identity extraction, cancellation, and progress reporting.

This module unifies caller identities from mTLS client certificates, Google Cloud IAP,
JWT claims, and ASGI/Litestar connections into an immutable CallerIdentity passed across
all agentic operations via AgentContext.
"""

import contextvars
from collections.abc import Callable, Coroutine, Mapping
from typing import TYPE_CHECKING, Any, Literal

import anyio
import msgspec

from vertebra.agent.core.identity import IdentityRequest

if TYPE_CHECKING:
    from vertebra.agent.core.identity import IdentitySource


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
    def from_headers(
        cls,
        headers: Mapping[str, str],
        *,
        identity_source: "IdentitySource | None" = None,
        peer: str | None = None,
        user: Any = None,
        auth: Any = None,
        session_id: str | None = None,
    ) -> "AgentContext":
        """Construct context using trusted application identity and an optional proxy policy."""
        lowered = {str(key).lower(): str(value) for key, value in headers.items()}
        identity = CallerIdentity()
        if identity_source is not None:
            identity = identity_source.resolve(IdentityRequest(headers=lowered, peer=peer)) or identity
        user_id, email = identity.user_id, identity.email
        roles, scopes = list(identity.roles), list(identity.scopes)
        scheme = identity.auth_scheme
        if scheme == "anonymous" and lowered.get("authorization", "").lower().startswith("bearer "):
            scheme = "jwt"
        if user is not None:
            raw_id = (
                user.get("id")
                if isinstance(user, Mapping)
                else getattr(user, "id", user if isinstance(user, str) else None)
            )
            if raw_id is not None:
                user_id = str(raw_id)
            raw_email = user.get("email") if isinstance(user, Mapping) else getattr(user, "email", None)
            if email is None and raw_email is not None:
                email = str(raw_email)
            roles.extend(
                str(role)
                for role in (user.get("roles", ()) if isinstance(user, Mapping) else getattr(user, "roles", ()))
            )
            if scheme == "anonymous":
                scheme = "connection"
        if auth is not None:
            roles.extend(
                str(role)
                for role in (auth.get("roles", ()) if isinstance(auth, Mapping) else getattr(auth, "roles", ()))
            )
            scopes.extend(
                str(scope)
                for scope in (auth.get("scopes", ()) if isinstance(auth, Mapping) else getattr(auth, "scopes", ()))
            )
        return cls(
            identity=msgspec.structs.replace(
                identity,
                user_id=user_id,
                email=email,
                roles=tuple(sorted(set(roles))),
                scopes=tuple(sorted(set(scopes))),
                auth_scheme=scheme,
            ),
            session_id=session_id,
        )

    @classmethod
    def from_scope(cls, scope: Mapping[str, Any], *, identity_source: "IdentitySource | None" = None) -> "AgentContext":
        """Construct context from authenticated ASGI scope state and its immediate peer."""
        client = scope.get("client")
        return cls.from_headers(
            cls._headers(scope.get("headers", {})),
            identity_source=identity_source,
            peer=client[0] if client else None,
            user=scope.get("user"),
            auth=scope.get("auth"),
        )

    @classmethod
    def from_wsgi_environ(
        cls, environ: Mapping[str, Any], *, identity_source: "IdentitySource | None" = None
    ) -> "AgentContext":
        """Construct context from WSGI authentication state and its immediate peer."""
        headers = {
            key[5:].replace("_", "-").lower(): str(value) for key, value in environ.items() if key.startswith("HTTP_")
        }
        if "CONTENT_TYPE" in environ:
            headers["content-type"] = str(environ["CONTENT_TYPE"])
        return cls.from_headers(
            headers, identity_source=identity_source, peer=environ.get("REMOTE_ADDR"), user=environ.get("REMOTE_USER")
        )

    @classmethod
    def from_request(
        cls,
        request_or_connection: Any,
        *,
        identity_source: "IdentitySource | None" = None,
        session_id: str | None = None,
        turn_id: str | None = None,
        progress_token: str | int | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> "AgentContext":
        """Construct context without evaluating framework authentication properties."""
        scope = getattr(request_or_connection, "scope", None)
        context = (
            cls.from_scope(scope, identity_source=identity_source)
            if isinstance(scope, Mapping)
            else cls.from_headers(
                cls._headers(getattr(request_or_connection, "headers", {})), identity_source=identity_source
            )
        )
        context.session_id = session_id
        context.turn_id = turn_id
        context.progress_reporter = ProgressReporter(callback=progress_callback, progress_token=progress_token)
        return context

    def fork(self) -> "AgentContext":
        """Copy request state with independent cancellation and mutable metadata."""
        return type(self)(
            identity=self.identity,
            session_id=self.session_id,
            turn_id=self.turn_id,
            progress_reporter=self.progress_reporter,
            trace_context=self.trace_context,
            metadata=dict(self.metadata),
        )

    @staticmethod
    def _headers(raw: Any) -> dict[str, str]:
        """Normalize mappings and ASGI header pairs using latin-1 decoding."""
        pairs = raw.items() if isinstance(raw, Mapping) else raw
        return {
            (key.decode("latin-1") if isinstance(key, bytes) else str(key)).lower(): value.decode("latin-1")
            if isinstance(value, bytes)
            else str(value)
            for key, value in pairs
        }


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


def reset_agent_context(token: contextvars.Token[AgentContext | None]) -> None:
    """Restore the context value preceding a binding."""
    _CURRENT_AGENT_CONTEXT.reset(token)


__all__ = [
    "AgentContext",
    "CallerIdentity",
    "CancellationToken",
    "ProgressCallback",
    "ProgressReporter",
    "bind_agent_context",
    "current_agent_context",
    "reset_agent_context",
]
