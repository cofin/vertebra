"""Transactional email subsystem supporting memory, SMTP, Resend, SendGrid, Mailgun, and SES."""

from __future__ import annotations

from vertebra.framework.email.base import (
    EmailAttachment,
    EmailBackendType,
    EmailConfig,
    EmailDeliveryError,
    EmailMessage,
    EmailReceipt,
    EmailService,
    EmailTransport,
    WebhookEvent,
)
from vertebra.framework.email.memory import MemoryEmailTransport
from vertebra.framework.email.providers import (
    HttpResponseData,
    HttpTransportClient,
    MailgunEmailTransport,
    ResendEmailTransport,
    SendGridEmailTransport,
    UrllibHttpTransportClient,
    WebhookVerifier,
)
from vertebra.framework.email.ses import SESEmailTransport
from vertebra.framework.email.smtp import SMTPEmailTransport

__all__ = (
    "EmailAttachment",
    "EmailBackendType",
    "EmailConfig",
    "EmailDeliveryError",
    "EmailMessage",
    "EmailReceipt",
    "EmailService",
    "EmailTransport",
    "HttpResponseData",
    "HttpTransportClient",
    "MailgunEmailTransport",
    "MemoryEmailTransport",
    "ResendEmailTransport",
    "SendGridEmailTransport",
    "SESEmailTransport",
    "SMTPEmailTransport",
    "UrllibHttpTransportClient",
    "WebhookEvent",
    "WebhookVerifier",
)
