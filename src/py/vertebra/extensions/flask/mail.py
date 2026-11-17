"""Synchronous and asynchronous email delivery bridge for Flask applications."""

from __future__ import annotations

import smtplib
from email.message import EmailMessage as StdlibEmailMessage
from typing import TYPE_CHECKING, Any, cast

import anyio
import msgspec

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from flask import Flask

__all__ = (
    "MailBackend",
    "MailConfig",
    "MailMessage",
)


class MailConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for the Flask mail backend."""

    host: str = "localhost"
    port: int = 25
    username: str | None = None
    password: str | None = None
    use_tls: bool = False
    default_sender: str = "noreply@localhost"
    suppress_send: bool = False


class MailMessage(msgspec.Struct, frozen=True, gc=False):
    """Structured outbound email message."""

    subject: str
    recipients: tuple[str, ...]
    body: str = ""
    html: str | None = None
    sender: str | None = None
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()
    headers: dict[str, str] = {}


class MailBackend:
    """Dual sync/async mail backend with in-memory outbox capture for testing."""

    __slots__ = ("_config", "_outbox")

    def __init__(self, config: MailConfig | None = None) -> None:
        self._config = config or MailConfig()
        self._outbox: list[MailMessage] = []

    @property
    def config(self) -> MailConfig:
        """Return the ``MailConfig``."""
        return self._config

    @property
    def outbox(self) -> list[MailMessage]:
        """Return captured ``MailMessage`` instances when ``suppress_send`` is enabled."""
        return self._outbox

    def _build_mime(self, message: MailMessage) -> StdlibEmailMessage:
        mime = StdlibEmailMessage()
        mime["Subject"] = message.subject
        mime["From"] = message.sender or self._config.default_sender
        mime["To"] = ", ".join(message.recipients)
        if message.cc:
            mime["Cc"] = ", ".join(message.cc)
        for k, v in message.headers.items():
            mime[k] = v
        mime.set_content(message.body)
        if message.html is not None:
            mime.add_alternative(message.html, subtype="html")
        return mime

    def send(self, message: MailMessage) -> None:
        """Synchronously send ``message`` or append to ``outbox`` when ``suppress_send`` is True."""
        self._outbox.append(message)
        if self._config.suppress_send:
            return
        mime = self._build_mime(message)
        with smtplib.SMTP(self._config.host, self._config.port) as smtp:
            if self._config.use_tls:
                smtp.starttls()
            if self._config.username and self._config.password:
                smtp.login(self._config.username, self._config.password)
            smtp.send_message(mime)

    async def asend(self, message: MailMessage) -> None:
        """Asynchronously send ``message`` via worker thread."""
        if self._config.suppress_send:
            self._outbox.append(message)
            return
        await anyio.to_thread.run_sync(self.send, message)

    def init_app(self, app: Flask | Any) -> None:
        """Attach this ``MailBackend`` to a Flask application."""
        ensure_module("flask", extra="flask")
        cast("Any", app).mail = self
        extensions = getattr(app, "extensions", None)
        if isinstance(extensions, dict):
            extensions["vertebra_mail"] = self
