"""Core protocols, value objects, and service for transactional email providers."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, runtime_checkable

import msgspec

from vertebra._typing import UnsupportedBackendError
from vertebra.framework.queues.base import TaskDef, TaskRetryPolicy
from vertebra.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.framework.queues.base import QueueService, TaskEnvelope

__all__ = (
    "EmailAttachment",
    "EmailBackendType",
    "EmailConfig",
    "EmailDeliveryError",
    "EmailMessage",
    "EmailReceipt",
    "EmailService",
    "EmailTransport",
    "WebhookEvent",
)

type EmailBackendType = Literal["memory", "smtp", "resend", "sendgrid", "mailgun", "ses"]


class EmailDeliveryError(RuntimeError):
    """Raised when an email transport encounters a delivery failure."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        status_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.status_code = status_code
        self.retryable = retryable


class EmailAttachment(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Binary or text email attachment."""

    filename: str
    content: bytes
    content_type: str = "application/octet-stream"
    content_id: str | None = None


class EmailMessage(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Outbound transactional email message."""

    subject: str
    to: tuple[str, ...]
    from_email: str = ""
    text_body: str = ""
    html_body: str | None = None
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()
    reply_to: str | None = None
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    attachments: tuple[EmailAttachment, ...] = ()
    idempotency_key: str | None = None
    tags: dict[str, str] = msgspec.field(default_factory=dict)
    metadata: dict[str, str] = msgspec.field(default_factory=dict)


class EmailReceipt(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Delivery confirmation receipt returned by an email transport."""

    message_id: str
    provider: EmailBackendType
    accepted_recipients: tuple[str, ...] = ()
    raw_response: dict[str, Any] = msgspec.field(default_factory=dict)


class WebhookEvent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Typed representation of an incoming provider delivery or bounce webhook."""

    provider: EmailBackendType
    event_type: str
    message_id: str
    recipient: str | None = None
    timestamp: int = 0
    payload: dict[str, Any] = msgspec.field(default_factory=dict)


class EmailConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for EmailService and provider transports."""

    backend: EmailBackendType = "memory"
    default_from: str = "noreply@example.com"
    smtp_host: str = "localhost"
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    use_tls: bool = False
    use_starttls: bool = True
    pool_size: int = 4
    timeout_seconds: float = 10.0
    api_key: str | None = None
    api_base_url: str | None = None
    mailgun_domain: str | None = None
    webhook_secret: str | None = None
    max_retries: int = 3
    initial_backoff_seconds: float = 0.5
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    configuration_set_name: str | None = None


@runtime_checkable
class EmailTransport(Protocol):
    """Protocol for outbound email transports."""

    async def send(self, message: EmailMessage) -> EmailReceipt:
        """Send a single email message and return its receipt."""
        ...

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Send a batch of email messages and return their receipts."""
        ...

    async def close(self) -> None:
        """Release underlying network and pool resources."""
        ...


class EmailService:
    """Transactional email dispatcher supporting direct and background queue-backed delivery."""

    __slots__ = ("_config", "_queue_service", "_transport")

    def __init__(
        self,
        config: EmailConfig | None = None,
        *,
        transport: EmailTransport | None = None,
        queue_service: QueueService | None = None,
    ) -> None:
        self._config = config or EmailConfig()
        self._transport: EmailTransport = transport or self._build_transport(self._config)
        self._queue_service = queue_service
        if self._queue_service is not None:
            self._register_queue_task()

    @property
    def config(self) -> EmailConfig:
        """Return the active EmailConfig."""
        return self._config

    @property
    def transport(self) -> EmailTransport:
        """Return the active EmailTransport."""
        return self._transport

    @property
    def queue_service(self) -> QueueService | None:
        """Return the bound QueueService if configured."""
        return self._queue_service

    @property
    def outbox(self) -> tuple[EmailMessage, ...]:
        """Return captured messages if the transport supports outbox recording."""
        outbox_attr = getattr(self._transport, "outbox", ())
        if isinstance(outbox_attr, tuple):
            return outbox_attr
        return ()

    def clear_outbox(self) -> None:
        """Clear the captured outbox if the transport supports clear."""
        clear_fn = getattr(self._transport, "clear", None)
        if callable(clear_fn):
            clear_fn()

    async def send(self, message: EmailMessage) -> str:
        """Send message directly and return receipt message_id for backward compatibility."""
        receipt = await self.send_with_receipt(message)
        return receipt.message_id

    async def send_with_receipt(self, message: EmailMessage) -> EmailReceipt:
        """Send message directly and return full EmailReceipt."""
        normalized = self._normalize_message(message)
        return await self._transport.send(normalized)

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Send batch of messages directly and return list of receipts."""
        normalized_list = [self._normalize_message(m) for m in messages]
        return await self._transport.send_batch(normalized_list)

    async def enqueue(
        self,
        message: EmailMessage,
        *,
        delay_seconds: float | None = None,
    ) -> TaskEnvelope:
        """Enqueue message for background worker delivery via QueueService."""
        if self._queue_service is None:
            msg = "EmailService.enqueue requires a configured QueueService"
            raise RuntimeError(msg)
        normalized = self._normalize_message(message)
        idempotency_key = normalized.idempotency_key or f"email:{uuid.uuid4().hex}"
        return await self._queue_service.enqueue(
            "vertebra.email.send",
            normalized,
            lane="io",
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
        )

    async def enqueue_batch(
        self,
        messages: Sequence[EmailMessage],
        *,
        delay_seconds: float | None = None,
    ) -> list[TaskEnvelope]:
        """Enqueue multiple messages for background worker delivery."""
        envelopes: list[TaskEnvelope] = []
        for msg in messages:
            env = await self.enqueue(msg, delay_seconds=delay_seconds)
            envelopes.append(env)
        return envelopes

    async def close(self) -> None:
        """Close transport resources."""
        await self._transport.close()

    def _normalize_message(self, message: EmailMessage) -> EmailMessage:
        """Ensure from_email is set to default_from if empty."""
        if not message.from_email:
            return msgspec.structs.replace(message, from_email=self._config.default_from)
        return message

    def _register_queue_task(self) -> None:
        """Register the vertebra.email.send task on QueueService."""
        if self._queue_service is None:
            return

        async def _deliver_email_task(message: Any) -> EmailReceipt:
            msg_obj = message if isinstance(message, EmailMessage) else msgspec.convert(message, type=EmailMessage)
            return await self.send_with_receipt(msg_obj)

        task_def = TaskDef(
            name="vertebra.email.send",
            fn=_deliver_email_task,
            lane="io",
            retry_policy=TaskRetryPolicy(
                max_retries=self._config.max_retries,
                initial_backoff_seconds=self._config.initial_backoff_seconds,
            ),
        )
        self._queue_service.register(task_def)

    @staticmethod
    def _build_transport(config: EmailConfig) -> EmailTransport:
        """Construct matching EmailTransport based on config.backend."""
        if config.backend == "memory":
            transport_cls = import_string("vertebra.framework.email.memory.MemoryEmailTransport")
            return cast("EmailTransport", transport_cls(config))
        if config.backend == "smtp":
            transport_cls = import_string("vertebra.framework.email.smtp.SmtpEmailTransport")
            return cast("EmailTransport", transport_cls(config))
        if config.backend in ("resend", "sendgrid", "mailgun"):
            if config.backend == "resend":
                mod_path = "vertebra.framework.email.providers.ResendEmailTransport"
            elif config.backend == "sendgrid":
                mod_path = "vertebra.framework.email.providers.SendGridEmailTransport"
            else:
                mod_path = "vertebra.framework.email.providers.MailgunEmailTransport"
            transport_cls = import_string(mod_path)
            return cast("EmailTransport", transport_cls(config))
        if config.backend == "ses":
            transport_cls = import_string("vertebra.framework.email.ses.SesEmailTransport")
            return cast("EmailTransport", transport_cls(config))
        msg = f"Unsupported email backend: {config.backend!r}"
        raise UnsupportedBackendError(msg)
