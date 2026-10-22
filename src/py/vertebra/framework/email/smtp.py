"""Pooled SMTP email transport supporting STARTTLS, implicit TLS, and RFC 5322 MIME."""

from __future__ import annotations

import asyncio
import contextlib
import email.message
import email.utils
import smtplib
import ssl
from collections import deque
from typing import TYPE_CHECKING, Any

from vertebra.framework.email.base import EmailDeliveryError, EmailReceipt

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from vertebra.framework.email.base import EmailAttachment, EmailConfig, EmailMessage

__all__ = ("SmtpEmailTransport",)


class SmtpEmailTransport:
    """Pooled SMTP client transport with TLS and health check re-connection."""

    __slots__ = (
        "_closed",
        "_config",
        "_pool",
        "_semaphore",
        "_smtp_factory",
    )

    def __init__(
        self,
        config: EmailConfig,
        *,
        smtp_factory: Callable[[], smtplib.SMTP] | None = None,
    ) -> None:
        """Initialize pooled SMTP transport."""
        self._config = config
        self._smtp_factory = smtp_factory
        self._pool: deque[smtplib.SMTP] = deque()
        self._semaphore = asyncio.Semaphore(config.pool_size)
        self._closed = False

    def build_mime_message(self, message: EmailMessage) -> email.message.EmailMessage:
        """Construct RFC 5322 MIME message from domain EmailMessage."""
        self._validate_header_injection("subject", message.subject)
        self._validate_header_injection("from", message.from_email)
        for recipient in (*message.to, *message.cc, *message.bcc):
            self._validate_header_injection("recipient", recipient)
        if message.reply_to:
            self._validate_header_injection("reply_to", message.reply_to)

        mime_msg = email.message.EmailMessage()
        mime_msg["Subject"] = message.subject
        mime_msg["From"] = message.from_email
        mime_msg["To"] = ", ".join(message.to)
        if message.cc:
            mime_msg["Cc"] = ", ".join(message.cc)
        if message.reply_to:
            mime_msg["Reply-To"] = message.reply_to

        msg_id = email.utils.make_msgid()
        mime_msg["Message-ID"] = msg_id

        for k, v in message.headers.items():
            self._validate_header_injection(k, v)
            mime_msg[k] = v

        if message.html_body and message.text_body:
            mime_msg.set_content(message.text_body)
            mime_msg.add_alternative(message.html_body, subtype="html")
        elif message.html_body:
            mime_msg.set_content(message.html_body, subtype="html")
        else:
            mime_msg.set_content(message.text_body or "")

        for att in message.attachments:
            self._attach_file(mime_msg, att)

        return mime_msg

    def _validate_header_injection(self, field_name: str, value: str) -> None:
        """Reject CR/LF characters in header values."""
        if "\r" in value or "\n" in value:
            msg = f"Invalid header in {field_name!r}: contains CR or LF characters"
            raise ValueError(msg)

    def _attach_file(
        self,
        mime_msg: email.message.EmailMessage,
        attachment: EmailAttachment,
    ) -> None:
        """Add binary attachment with proper content-type splitting."""
        maintype = "application"
        subtype = "octet-stream"
        if "/" in attachment.content_type:
            parts = attachment.content_type.split("/", 1)
            maintype = parts[0].strip()
            subtype = parts[1].strip()

        kwargs: dict[str, Any] = {
            "maintype": maintype,
            "subtype": subtype,
            "filename": attachment.filename,
        }
        if attachment.content_id:
            kwargs["cid"] = attachment.content_id

        mime_msg.add_attachment(attachment.content, **kwargs)

    def _create_connection(self) -> smtplib.SMTP:
        """Create and authenticate a new synchronous SMTP connection."""
        if self._smtp_factory is not None:
            return self._smtp_factory()

        client: smtplib.SMTP
        if self._config.use_tls:
            context = ssl.create_default_context()
            client = smtplib.SMTP_SSL(
                self._config.smtp_host,
                self._config.smtp_port,
                timeout=self._config.timeout_seconds,
                context=context,
            )
        else:
            client = smtplib.SMTP(
                self._config.smtp_host,
                self._config.smtp_port,
                timeout=self._config.timeout_seconds,
            )

        client.ehlo()
        if self._config.use_starttls and not self._config.use_tls:
            context = ssl.create_default_context()
            client.starttls(context=context)
            client.ehlo()

        if self._config.smtp_username and self._config.smtp_password:
            client.login(self._config.smtp_username, self._config.smtp_password)

        return client

    def _is_healthy(self, client: smtplib.SMTP) -> bool:
        """Perform NOOP health check on active connection."""
        try:
            status, _ = client.noop()
            return status == 250
        except (smtplib.SMTPException, OSError):
            return False

    def _send_sync(
        self,
        client: smtplib.SMTP,
        mime_msg: email.message.EmailMessage,
        all_recipients: list[str],
    ) -> None:
        """Execute synchronous send_message."""
        client.send_message(mime_msg, to_addrs=all_recipients)

    async def _acquire_connection(self) -> smtplib.SMTP:
        """Get a healthy connection from pool or create a new one."""
        while self._pool:
            conn = self._pool.popleft()
            healthy = await asyncio.to_thread(self._is_healthy, conn)
            if healthy:
                return conn
            with contextlib.suppress(Exception):
                await asyncio.to_thread(conn.close)
        return await asyncio.to_thread(self._create_connection)

    async def _release_connection(self, conn: smtplib.SMTP, *, healthy: bool = True) -> None:
        """Return connection to pool or close if unhealthy/closed."""
        if self._closed or not healthy:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(conn.close)
            return

        if len(self._pool) < self._config.pool_size:
            self._pool.append(conn)
        else:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(conn.close)

    async def send(self, message: EmailMessage) -> EmailReceipt:
        """Dispatch message via SMTP pool with retry on disconnect."""
        if self._closed:
            raise RuntimeError("SmtpEmailTransport is closed")

        mime_msg = self.build_mime_message(message)
        recipients = [r.strip() for r in (*message.to, *message.cc, *message.bcc) if r.strip()]
        if not recipients:
            raise ValueError("EmailMessage has no recipients")

        async with self._semaphore:
            conn = await self._acquire_connection()
            try:
                await asyncio.to_thread(self._send_sync, conn, mime_msg, recipients)
                await self._release_connection(conn, healthy=True)
            except smtplib.SMTPServerDisconnected:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(conn.close)
                conn = await asyncio.to_thread(self._create_connection)
                try:
                    await asyncio.to_thread(self._send_sync, conn, mime_msg, recipients)
                    await self._release_connection(conn, healthy=True)
                except Exception as exc:
                    await self._release_connection(conn, healthy=False)
                    raise EmailDeliveryError(
                        f"SMTP delivery failed after reconnect: {exc}",
                        provider="smtp",
                        retryable=True,
                    ) from exc
            except Exception as exc:
                await self._release_connection(conn, healthy=False)
                retryable = isinstance(exc, (smtplib.SMTPConnectError, OSError))
                raise EmailDeliveryError(
                    f"SMTP delivery failed: {exc}",
                    provider="smtp",
                    retryable=retryable,
                ) from exc

        return EmailReceipt(
            message_id=str(mime_msg["Message-ID"]),
            provider="smtp",
            accepted_recipients=tuple(recipients),
            raw_response={"status": "delivered"},
        )

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Send a sequence of messages sequentially."""
        receipts: list[EmailReceipt] = []
        for msg in messages:
            receipt = await self.send(msg)
            receipts.append(receipt)
        return receipts

    async def close(self) -> None:
        """Close all pooled connections."""
        self._closed = True
        while self._pool:
            conn = self._pool.popleft()
            try:
                await asyncio.to_thread(conn.quit)
            except Exception:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(conn.close)
