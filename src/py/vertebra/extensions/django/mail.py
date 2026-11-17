"""Django EMAIL_BACKEND implementation with synchronous and asynchronous delivery."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.framework.email import (
    EmailBackendType,
    EmailConfig,
    EmailMessage,
    EmailService,
    SmtpEmailTransport,
)
from vertebra.utils.module_loader import ensure_module, import_optional

_mail_base_module = import_optional("django.core.mail.backends.base")

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from django.core.mail.backends.base import BaseEmailBackend as _BaseEmailBackend
else:
    _BaseEmailBackend = _mail_base_module.BaseEmailBackend if _mail_base_module is not None else object

__all__ = (
    "EmailBackend",
    "EmailConfig",
    "EmailMessage",
)


def __getattr__(name: str) -> Any:
    """Lazily resolve optional Django BaseEmailBackend on demand."""
    if name == "BaseEmailBackend":
        return ensure_module("django.core.mail.backends.base", package="django", extra="django").BaseEmailBackend
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def _run_sync[T](coro: Coroutine[Any, Any, T]) -> T:
    """Execute an async email coroutine synchronously even inside an active event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


class EmailBackend(_BaseEmailBackend):
    """Drop-in ``EMAIL_BACKEND = "vertebra.extensions.django.mail.EmailBackend"``."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 25,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = False,
        fail_silently: bool = False,
        *,
        suppress_send: bool = False,
        alias: str | None = None,
        config: EmailConfig | None = None,
        backend: EmailBackendType | None = None,
        **kwargs: Any,
    ) -> None:
        _ = kwargs
        if _mail_base_module is None:
            ensure_module("django.core.mail.backends.base", package="django", extra="django")
        super().__init__(alias=alias)
        self._fail_silently = fail_silently
        self.fail_silently = fail_silently
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.suppress_send = suppress_send
        self._is_open = False
        if config is not None:
            resolved_config = (
                msgspec.structs.replace(config, backend="memory")
                if suppress_send and config.backend != "memory"
                else config
            )
        else:
            resolved_backend: EmailBackendType = "memory" if suppress_send else (backend or "smtp")
            resolved_config = EmailConfig(
                backend=resolved_backend,
                smtp_host=host,
                smtp_port=port,
                smtp_username=username,
                smtp_password=password,
                use_tls=use_tls,
                use_starttls=use_tls,
            )
        self._service = EmailService(resolved_config)

    @property
    def outbox(self) -> list[EmailMessage]:
        """Return captured ``EmailMessage`` records."""
        return list(self._service.outbox)

    def open(self) -> bool:
        """Open a live transport connection and return ``True`` when newly opened."""
        if self.suppress_send or self._service.config.backend == "memory" or self._is_open:
            return False
        transport = self._service.transport
        if isinstance(transport, SmtpEmailTransport):
            try:
                if transport._closed:
                    transport._closed = False
                conn = transport._create_connection()
                transport._pool.append(conn)
            except Exception:
                if self.fail_silently:
                    return False
                raise
        self._is_open = True
        return True

    def close(self) -> None:
        """Close the active email transport connections."""
        self._is_open = False
        _run_sync(self._service.close())

    def _normalize_message(self, email_message: Any) -> EmailMessage:
        if isinstance(email_message, EmailMessage):
            return email_message
        if isinstance(email_message, Mapping):
            subject = str(email_message.get("subject", ""))
            raw_to = email_message.get("to") or email_message.get("recipients") or ()
            to_list = tuple(str(r) for r in raw_to)
            from_email = str(email_message.get("from_email") or email_message.get("sender") or "")
            text_body = str(email_message.get("text_body") or email_message.get("body") or "")
            html_val = email_message.get("html_body") or email_message.get("html")
            html_body = str(html_val) if html_val is not None else None
            cc_list = tuple(str(r) for r in (email_message.get("cc") or ()))
            bcc_list = tuple(str(r) for r in (email_message.get("bcc") or ()))
            reply_raw = email_message.get("reply_to")
            reply_to = str(reply_raw) if reply_raw is not None else None
            headers = {str(k): str(v) for k, v in (email_message.get("headers") or {}).items()}
            return EmailMessage(
                subject=subject,
                to=to_list,
                from_email=from_email,
                text_body=text_body,
                html_body=html_body,
                cc=cc_list,
                bcc=bcc_list,
                reply_to=reply_to,
                headers=headers,
            )

        subject = str(getattr(email_message, "subject", ""))
        text_body = str(getattr(email_message, "body", ""))
        from_raw = getattr(email_message, "from_email", None)
        from_email = str(from_raw) if from_raw else ""
        to_list = tuple(str(r) for r in (getattr(email_message, "to", None) or ()))
        cc_list = tuple(str(r) for r in (getattr(email_message, "cc", None) or ()))
        bcc_list = tuple(str(r) for r in (getattr(email_message, "bcc", None) or ()))
        raw_reply_to = getattr(email_message, "reply_to", None)
        if isinstance(raw_reply_to, (list, tuple)):
            reply_to = ", ".join(str(r) for r in raw_reply_to) if raw_reply_to else None
        else:
            reply_to = str(raw_reply_to) if raw_reply_to else None
        raw_headers = getattr(email_message, "extra_headers", None) or getattr(email_message, "headers", None) or {}
        headers = {str(k): str(v) for k, v in raw_headers.items()}
        resolved_html: str | None = None
        for alt_content, alt_mime in getattr(email_message, "alternatives", None) or ():
            if alt_mime == "text/html":
                resolved_html = str(alt_content)
                break
        return EmailMessage(
            subject=subject,
            to=to_list,
            from_email=from_email,
            text_body=text_body,
            html_body=resolved_html,
            cc=cc_list,
            bcc=bcc_list,
            reply_to=reply_to,
            headers=headers,
        )

    def send_messages(self, email_messages: Iterable[Any]) -> int:
        """Synchronously send a batch of Django ``EmailMessage`` objects and return the count sent."""
        sent = 0
        for msg in email_messages:
            try:
                _run_sync(self._service.send(self._normalize_message(msg)))
                sent += 1
            except Exception:
                if not self.fail_silently:
                    raise
        return sent

    async def asend_messages(self, email_messages: Iterable[Any]) -> int:
        """Asynchronously send a batch of Django ``EmailMessage`` objects and return the count sent."""
        sent = 0
        for msg in email_messages:
            try:
                await self._service.send(self._normalize_message(msg))
                sent += 1
            except Exception:
                if not self.fail_silently:
                    raise
        return sent
