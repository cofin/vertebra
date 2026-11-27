"""Unit and contract tests for transactional email transports and webhooks."""

from __future__ import annotations

import base64
import email
import email.message
import hashlib
import hmac
import smtplib
from collections.abc import Mapping
from typing import Any, cast

import pytest

from tests.unit.framework.queue_test_doubles import FakeRedisStreamClient
from vertebra.framework.email import (
    EmailAttachment,
    EmailConfig,
    EmailDeliveryError,
    EmailMessage,
    EmailService,
    HttpResponseData,
    MailgunEmailTransport,
    MemoryEmailTransport,
    ResendEmailTransport,
    SendGridEmailTransport,
    SESEmailTransport,
    SMTPEmailTransport,
    WebhookVerifier,
)
from vertebra.framework.queues import QueueConfig, QueueService, RedisQueueBackend
from vertebra.utils.serializers import decode_json


class MockHttpTransportClient:
    """Mock HTTP client capturing requests and returning recorded responses."""

    __slots__ = ("calls", "responses")

    def __init__(self, responses: list[HttpResponseData] | None = None) -> None:
        """Initialize mock HTTP client."""
        self.responses: list[HttpResponseData] = list(responses or [])
        self.calls: list[dict[str, Any]] = []

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float = 10.0,
    ) -> HttpResponseData:
        """Record call and return next response or default 200 OK."""
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self.responses:
            return self.responses.pop(0)
        return HttpResponseData(status_code=200, headers={}, body=b'{"id":"mock_ok"}')


class MockSmtpClient:
    """Mock SMTP client simulating connection pooling and disconnections."""

    __slots__ = ("connected", "disconnections_remaining", "sent_messages")

    def __init__(self, *, disconnections_remaining: int = 0) -> None:
        """Initialize mock SMTP client."""
        self.sent_messages: list[tuple[email.message.EmailMessage, list[str]]] = []
        self.disconnections_remaining = disconnections_remaining
        self.connected = True

    def noop(self) -> tuple[int, bytes]:
        """Simulate SMTP NOOP."""
        if not self.connected:
            raise smtplib.SMTPServerDisconnected("Disconnected")
        return (250, b"OK")

    def ehlo(self) -> tuple[int, bytes]:
        """Simulate EHLO."""
        return (250, b"OK")

    def starttls(self, context: Any = None) -> tuple[int, bytes]:
        """Simulate STARTTLS."""
        return (220, b"Ready")

    def login(self, user: str, password: str) -> tuple[int, bytes]:
        """Simulate login."""
        return (235, b"Auth successful")

    def send_message(
        self,
        msg: email.message.EmailMessage,
        to_addrs: list[str],
    ) -> dict[str, Any]:
        """Record or raise disconnect."""
        if self.disconnections_remaining > 0:
            self.disconnections_remaining -= 1
            self.connected = False
            raise smtplib.SMTPServerDisconnected("Server unexpectedly closed connection")
        self.sent_messages.append((msg, to_addrs))
        return {}

    def quit(self) -> tuple[int, bytes]:
        """Simulate quit."""
        self.connected = False
        return (221, b"Bye")

    def close(self) -> None:
        """Simulate close."""
        self.connected = False


@pytest.mark.anyio
async def test_memory_transport_idempotency_and_recipient_lookup() -> None:
    """Test MemoryEmailTransport stores outbox, enforces idempotency, and filters."""
    transport = MemoryEmailTransport()
    msg1 = EmailMessage(
        subject="Welcome",
        to=("user1@example.com",),
        cc=("manager@example.com",),
        idempotency_key="reg-user-123",
        text_body="Welcome to the platform.",
    )
    msg2 = EmailMessage(
        subject="Receipt",
        to=("user2@example.com",),
        text_body="Your receipt.",
    )

    receipt1 = await transport.send(msg1)
    receipt1_dup = await transport.send(msg1)

    assert receipt1.message_id == receipt1_dup.message_id
    assert len(transport.outbox) == 1
    assert len(transport.receipts) == 1

    receipt2 = await transport.send(msg2)
    assert receipt2.message_id != receipt1.message_id
    assert len(transport.outbox) == 2

    user1_msgs = transport.find_by_recipient("user1@example.com")
    assert len(user1_msgs) == 1
    assert user1_msgs[0].subject == "Welcome"

    manager_msgs = transport.find_by_recipient("MANAGER@example.com")
    assert len(manager_msgs) == 1

    transport.clear()
    assert len(transport.outbox) == 0
    assert len(transport.receipts) == 0


@pytest.mark.anyio
async def test_smtp_transport_mime_multipart_attachments_pooling_and_header_injection_guard() -> None:
    """Test SMTPEmailTransport builds RFC 5322 MIME, reuses pool, and refuses injection."""
    config = EmailConfig(backend="smtp", pool_size=2)
    mock_clients: list[MockSmtpClient] = []

    def mock_factory() -> smtplib.SMTP:
        client = MockSmtpClient()
        mock_clients.append(client)
        return cast("smtplib.SMTP", client)

    transport = SMTPEmailTransport(config, smtp_factory=mock_factory)

    att1 = EmailAttachment(
        filename="report.pdf",
        content=b"%PDF-1.4 report content",
        content_type="application/pdf",
    )
    att2 = EmailAttachment(
        filename="logo.png",
        content=b"\x89PNG\r\n\x1a\nimage content",
        content_type="image/png",
        content_id="logo_cid",
    )
    msg = EmailMessage(
        subject="Monthly Report",
        to=("alice@example.com",),
        from_email="billing@example.com",
        cc=("bob@example.com",),
        bcc=("audit@example.com",),
        text_body="Hello Alice, please see attached report.",
        html_body="<p>Hello Alice, please see <b>attached report</b>.</p>",
        attachments=(att1, att2),
        headers={"X-Department": "Finance"},
    )

    mime = transport.build_mime_message(msg)
    assert mime["Subject"] == "Monthly Report"
    assert mime["From"] == "billing@example.com"
    assert mime["To"] == "alice@example.com"
    assert mime["Cc"] == "bob@example.com"
    assert mime["X-Department"] == "Finance"

    raw_mime = mime.as_bytes()
    parsed_mime = email.message_from_bytes(raw_mime)
    assert parsed_mime.is_multipart()

    receipt = await transport.send(msg)
    assert receipt.provider == "smtp"
    assert "alice@example.com" in receipt.accepted_recipients
    assert "bob@example.com" in receipt.accepted_recipients
    assert "audit@example.com" in receipt.accepted_recipients

    await transport.send(msg)
    await transport.send(msg)
    assert len(mock_clients) == 1

    flaky_clients: list[MockSmtpClient] = []

    def flaky_factory() -> smtplib.SMTP:
        client = MockSmtpClient(disconnections_remaining=1 if not flaky_clients else 0)
        flaky_clients.append(client)
        return cast("smtplib.SMTP", client)

    reconnect_transport = SMTPEmailTransport(config, smtp_factory=flaky_factory)
    await reconnect_transport.send(msg)
    assert len(flaky_clients) == 2

    bad_msg = EmailMessage(
        subject="Injected Subject\r\nBcc: hacker@example.com",
        to=("victim@example.com",),
        from_email="admin@example.com",
    )
    with pytest.raises(ValueError, match="contains CR or LF"):
        transport.build_mime_message(bad_msg)

    bad_hdr_msg = EmailMessage(
        subject="Clean",
        to=("victim@example.com",),
        from_email="admin@example.com",
        headers={"X-Custom": "Value\nEvil-Header: 123"},
    )
    with pytest.raises(ValueError, match="contains CR or LF"):
        transport.build_mime_message(bad_hdr_msg)

    await transport.close()
    await reconnect_transport.close()


@pytest.mark.anyio
async def test_resend_transport_single_batch_and_429_retry_after() -> None:
    """Test ResendEmailTransport single, batch, idempotency key, and 429 retry."""
    config = EmailConfig(
        backend="resend",
        api_key="re_secret_key",
        api_base_url="https://api.resend.com",
        max_retries=2,
        initial_backoff_seconds=0.01,
    )
    mock_http = MockHttpTransportClient(
        [
            HttpResponseData(status_code=429, headers={"retry-after": "0.01"}, body=b"rate limited"),
            HttpResponseData(status_code=200, headers={}, body=b'{"id":"re_single_123"}'),
            HttpResponseData(
                status_code=200,
                headers={},
                body=b'{"data":[{"id":"re_b1"},{"id":"re_b2"}]}',
            ),
        ]
    )

    transport = ResendEmailTransport(config, client=mock_http)
    msg1 = EmailMessage(
        subject="Welcome",
        to=("alice@example.com",),
        from_email="support@example.com",
        text_body="Welcome text",
        html_body="<p>Welcome html</p>",
        idempotency_key="idem-key-resend-1",
        tags={"category": "onboarding"},
    )

    receipt = await transport.send(msg1)
    assert receipt.message_id == "re_single_123"
    assert receipt.provider == "resend"

    assert len(mock_http.calls) == 2
    call1 = mock_http.calls[0]
    assert call1["headers"]["authorization"] == "Bearer re_secret_key"
    assert call1["headers"]["idempotency-key"] == "idem-key-resend-1"

    decoded_body = decode_json(call1["body"])
    assert decoded_body["to"] == ["alice@example.com"]
    assert decoded_body["tags"] == [{"name": "category", "value": "onboarding"}]

    msg2 = EmailMessage(
        subject="Update",
        to=("bob@example.com",),
        from_email="support@example.com",
        text_body="Update",
    )
    batch_receipts = await transport.send_batch([msg1, msg2])
    assert len(batch_receipts) == 2
    assert batch_receipts[0].message_id == "re_b1"
    assert batch_receipts[1].message_id == "re_b2"
    assert mock_http.calls[-1]["url"] == "https://api.resend.com/emails/batch"


@pytest.mark.anyio
async def test_sendgrid_and_mailgun_transports_contract_and_error_handling() -> None:
    """Test SendGrid and Mailgun request formation and 401 error propagation."""
    sg_config = EmailConfig(backend="sendgrid", api_key="sg_secret_key")
    sg_mock = MockHttpTransportClient(
        [
            HttpResponseData(status_code=202, headers={"x-message-id": "sg_msg_id_999"}, body=b""),
            HttpResponseData(status_code=401, headers={}, body=b'{"error":"bad_api_key"}'),
        ]
    )
    sg_transport = SendGridEmailTransport(sg_config, client=sg_mock)

    att = EmailAttachment(
        filename="test.txt",
        content=b"hello text",
        content_type="text/plain",
    )
    msg = EmailMessage(
        subject="SendGrid Test",
        to=("client@example.com",),
        from_email="info@example.com",
        text_body="Hello plain text",
        metadata={"user_id": "u123"},
        attachments=(att,),
    )
    sg_receipt = await sg_transport.send(msg)
    assert sg_receipt.message_id == "sg_msg_id_999"
    assert sg_receipt.provider == "sendgrid"

    sg_call = sg_mock.calls[0]
    assert sg_call["headers"]["authorization"] == "Bearer sg_secret_key"
    sg_payload = decode_json(sg_call["body"])
    assert sg_payload["personalizations"][0]["to"] == [{"email": "client@example.com"}]
    assert sg_payload["personalizations"][0]["custom_args"] == {"user_id": "u123"}
    assert sg_payload["attachments"][0]["filename"] == "test.txt"

    with pytest.raises(EmailDeliveryError) as exc_info:
        await sg_transport.send(msg)
    assert exc_info.value.status_code == 401
    assert not exc_info.value.retryable

    mg_config = EmailConfig(
        backend="mailgun",
        api_key="mg_key_123",
        mailgun_domain="mg.example.com",
    )
    mg_mock = MockHttpTransportClient(
        [
            HttpResponseData(
                status_code=200,
                headers={},
                body=b'{"id":"<20261022.mg123@mg.example.com>","message":"Queued."}',
            ),
        ]
    )
    mg_transport = MailgunEmailTransport(mg_config, client=mg_mock)
    mg_receipt = await mg_transport.send(msg)
    assert mg_receipt.message_id == "<20261022.mg123@mg.example.com>"
    assert mg_receipt.provider == "mailgun"

    mg_call = mg_mock.calls[0]
    expected_auth = "Basic " + base64.b64encode(b"api:mg_key_123").decode()
    assert mg_call["headers"]["authorization"] == expected_auth
    assert "multipart/form-data" in mg_call["headers"]["content-type"]
    assert b'Content-Disposition: form-data; name="to"' in mg_call["body"]


def test_webhook_verifier_resend_sendgrid_and_mailgun_signatures() -> None:
    """Test WebhookVerifier constant-time HMAC validation and expiration checks."""
    now_ts = 1_700_000_000

    secret_raw = "my_svix_secret_12345678901234567890"
    secret_b64 = base64.b64encode(secret_raw.encode()).decode()
    secret_key = f"whsec_{secret_b64}"
    resend_body = b'{"type":"email.delivered","data":{"email_id":"msg_resend_1","to":["bob@example.com"]}}'
    svix_id = "msg_resend_1"
    svix_ts = str(now_ts)
    signed_content = f"{svix_id}.{svix_ts}.".encode() + resend_body
    sig_digest = base64.b64encode(hmac.new(secret_raw.encode(), signed_content, hashlib.sha256).digest()).decode()
    resend_headers = {
        "svix-id": svix_id,
        "svix-timestamp": svix_ts,
        "svix-signature": f"v1,{sig_digest}",
    }

    event = WebhookVerifier.verify_resend(
        resend_body,
        resend_headers,
        secret_key,
        now=now_ts,
    )
    assert event.provider == "resend"
    assert event.event_type == "email.delivered"
    assert event.message_id == "msg_resend_1"
    assert event.recipient == "bob@example.com"

    with pytest.raises(ValueError, match="Invalid Resend Svix"):
        WebhookVerifier.verify_resend(
            resend_body + b"tampered",
            resend_headers,
            secret_key,
            now=now_ts,
        )

    with pytest.raises(ValueError, match="out of tolerance"):
        WebhookVerifier.verify_resend(
            resend_body,
            resend_headers,
            secret_key,
            now=now_ts + 500,
        )

    sg_secret = "sendgrid_hmac_secret"
    sg_body = b'[{"event":"processed","sg_message_id":"sg_ev_1","email":"alice@example.com","timestamp":1700000000}]'
    sg_signed = f"{now_ts}".encode() + sg_body
    sg_sig = base64.b64encode(hmac.new(sg_secret.encode(), sg_signed, hashlib.sha256).digest()).decode()
    sg_headers = {
        "X-Twilio-Email-Event-Webhook-Signature": sg_sig,
        "X-Twilio-Email-Event-Webhook-Timestamp": str(now_ts),
    }

    sg_events = WebhookVerifier.verify_sendgrid(
        sg_body,
        sg_headers,
        sg_secret,
        now=now_ts,
    )
    assert len(sg_events) == 1
    assert sg_events[0].provider == "sendgrid"
    assert sg_events[0].event_type == "processed"
    assert sg_events[0].recipient == "alice@example.com"

    mg_secret = "mailgun_api_secret_key"
    mg_token = "mg_tok_123"
    mg_sig = hmac.new(
        mg_secret.encode(),
        f"{now_ts}{mg_token}".encode(),
        hashlib.sha256,
    ).hexdigest()
    mg_payload = {
        "signature": {
            "timestamp": str(now_ts),
            "token": mg_token,
            "signature": mg_sig,
        },
        "event-data": {
            "event": "opened",
            "recipient": "carol@example.com",
            "message": {"headers": {"message-id": "<opened_msg@mg.example.com>"}},
        },
    }

    mg_event = WebhookVerifier.verify_mailgun(mg_payload, mg_secret, now=now_ts)
    assert mg_event.provider == "mailgun"
    assert mg_event.event_type == "opened"
    assert mg_event.recipient == "carol@example.com"
    assert mg_event.message_id == "<opened_msg@mg.example.com>"


@pytest.mark.anyio
async def test_ses_transport_rust_sigv4_headers_simple_and_raw_mime() -> None:
    """Test SESEmailTransport SigV4 signing, Simple body, and Raw MIME attachment payload."""
    config = EmailConfig(
        backend="ses",
        aws_region="us-east-1",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        aws_session_token="session-tok-123",
        configuration_set_name="prod-emails",
    )
    mock_http = MockHttpTransportClient(
        [
            HttpResponseData(status_code=200, headers={}, body=b'{"MessageId":"ses-msg-001"}'),
            HttpResponseData(status_code=200, headers={}, body=b'{"MessageId":"ses-msg-002"}'),
        ]
    )
    transport = SESEmailTransport(config, client=mock_http)

    simple_msg = EmailMessage(
        subject="Simple SES",
        to=("ses-recipient@example.com",),
        from_email="ses-sender@example.com",
        text_body="Hello Simple SES",
        html_body="<p>Hello Simple SES</p>",
    )
    receipt1 = await transport.send(simple_msg, timestamp=1_700_000_000)
    assert receipt1.message_id == "ses-msg-001"
    assert receipt1.provider == "ses"

    call1 = mock_http.calls[0]
    headers1 = {k.lower(): v for k, v in call1["headers"].items()}
    auth_header = headers1["authorization"]
    assert auth_header.startswith(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20231114/us-east-1/ses/aws4_request"
    )
    assert "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token" in auth_header
    assert headers1["x-amz-security-token"] == "session-tok-123"
    assert headers1["x-amz-date"] == "20231114T221320Z"

    simple_body = decode_json(call1["body"])
    assert "Simple" in simple_body["Content"]
    assert simple_body["ConfigurationSetName"] == "prod-emails"

    att_msg = EmailMessage(
        subject="Raw SES with Attachment",
        to=("ses-recipient@example.com",),
        from_email="ses-sender@example.com",
        text_body="Hello Raw SES",
        attachments=(
            EmailAttachment(
                filename="document.txt",
                content=b"attached file content",
                content_type="text/plain",
            ),
        ),
    )
    receipt2 = await transport.send(att_msg, timestamp=1_700_000_000)
    assert receipt2.message_id == "ses-msg-002"

    call2 = mock_http.calls[1]
    raw_body = decode_json(call2["body"])
    assert "Raw" in raw_body["Content"]
    assert "Data" in raw_body["Content"]["Raw"]


@pytest.mark.anyio
@pytest.mark.parametrize("backend_url", ["memory://", "redis://localhost:6379/0"])
async def test_email_service_enqueue_and_enqueue_batch_via_queue_service(backend_url: str) -> None:
    """Test EmailService integrates with QueueService for async background delivery."""
    q_config = QueueConfig(backend_url=backend_url)
    q_service = (
        QueueService(q_config, backend=RedisQueueBackend(q_config, client=FakeRedisStreamClient()))
        if backend_url.startswith("redis")
        else QueueService(q_config)
    )

    email_config = EmailConfig(backend="memory", default_from="service@example.com")
    email_service = EmailService(email_config, queue_service=q_service)

    msg1 = EmailMessage(
        subject="Queued Email 1",
        to=("user1@example.com",),
        text_body="Body 1",
    )
    msg2 = EmailMessage(
        subject="Queued Email 2",
        to=("user2@example.com",),
        text_body="Body 2",
    )
    msg3 = EmailMessage(
        subject="Queued Email 3",
        to=("user3@example.com",),
        text_body="Body 3",
    )

    env1 = await email_service.enqueue(msg1)
    envs = await email_service.enqueue_batch([msg2, msg3])

    assert env1.task_name == "vertebra.email.send"
    assert env1.lane == "io"
    assert len(envs) == 2

    assert len(email_service.outbox) == 0

    drained = await q_service.drain_once(lanes=("io",))
    assert len(drained) == 3

    assert len(email_service.outbox) == 3
    outbox_subjects = [m.subject for m in email_service.outbox]
    assert "Queued Email 1" in outbox_subjects
    assert "Queued Email 2" in outbox_subjects
    assert "Queued Email 3" in outbox_subjects
    await q_service.close()
