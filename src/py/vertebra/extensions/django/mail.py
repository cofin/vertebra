"""Django EMAIL_BACKEND implementation with synchronous and asynchronous delivery."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.extensions.flask.mail import MailBackend, MailConfig, MailMessage

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = (
    "EmailBackend",
    "MailConfig",
    "MailMessage",
)


class EmailBackend:
    """Drop-in ``EMAIL_BACKEND = "vertebra.extensions.django.mail.EmailBackend"``."""

    __slots__ = ("_backend", "_fail_silently")

    def __init__(
        self,
        host: str = "localhost",
        port: int = 25,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = False,
        fail_silently: bool = False,
        *,
        suppress_send: bool = True,
        **kwargs: Any,
    ) -> None:
        _ = kwargs
        self._fail_silently = fail_silently
        self._backend = MailBackend(
            MailConfig(
                host=host,
                port=port,
                username=username,
                password=password,
                use_tls=use_tls,
                suppress_send=suppress_send,
            )
        )

    @property
    def outbox(self) -> list[MailMessage]:
        """Return captured ``MailMessage`` records."""
        return self._backend.outbox

    def _normalize_message(self, email_message: Any) -> MailMessage:
        if isinstance(email_message, MailMessage):
            return email_message
        subject = str(getattr(email_message, "subject", ""))
        body = str(getattr(email_message, "body", ""))
        from_email = getattr(email_message, "from_email", None)
        to_list = tuple(str(r) for r in (getattr(email_message, "to", None) or ()))
        cc_list = tuple(str(r) for r in (getattr(email_message, "cc", None) or ()))
        bcc_list = tuple(str(r) for r in (getattr(email_message, "bcc", None) or ()))
        return MailMessage(
            subject=subject,
            recipients=to_list,
            body=body,
            sender=str(from_email) if from_email is not None else None,
            cc=cc_list,
            bcc=bcc_list,
        )

    def send_messages(self, email_messages: Iterable[Any]) -> int:
        """Synchronously send a batch of Django ``EmailMessage`` objects and return the count sent."""
        sent = 0
        for msg in email_messages:
            try:
                self._backend.send(self._normalize_message(msg))
                sent += 1
            except Exception:
                if not self._fail_silently:
                    raise
        return sent

    async def asend_messages(self, email_messages: Iterable[Any]) -> int:
        """Asynchronously send a batch of Django ``EmailMessage`` objects and return the count sent."""
        sent = 0
        for msg in email_messages:
            try:
                await self._backend.asend(self._normalize_message(msg))
                sent += 1
            except Exception:
                if not self._fail_silently:
                    raise
        return sent
