---
type: Spec
flow_id: transactional-email-providers
title: "Chapter 2: Transactional Email Providers, Webhook Verification, and Queued Delivery"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:40:00Z
updated_at: 2026-10-22T04:40:00Z
description: "Implement the EmailTransport protocol, pooled SmtpEmailTransport, MemoryEmailTransport, Resend/SendGrid/Mailgun REST transports with webhook HMAC verification, Rust SigV4 SesEmailTransport, and QueueService-backed EmailService.enqueue delivery."
tags: [feature, email, storage, rust, queues]
parent_prd: vertebra-queues-and-messaging
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 2: Transactional Email Providers, Webhook Verification, and Queued Delivery

**Flow ID:** `transactional-email-providers`

## Specification

### Parent Context

This chapter is Chapter 2 of the [`vertebra-queues-and-messaging`](../vertebra-queues-and-messaging/spec.md) Master PRD and resolves the transactional email provider follow-up recorded in `vertebra-design-quality` Task `5.7` (`FW-08`). It owns parent requirements **R6**, **R7**, **R8**, and the email portion of **R9**. Task `2.3` (`EmailService.enqueue`) depends on Chapter 1 Task `1.1` (`QueueService` and `QueueBackend`).

### Code Analysis Summary

Inspected at repository checkout:

- **`src/py/vertebra/framework/email/__init__.py` (79 lines)**:
  - Defines `EmailBackendType = Literal["memory", "smtp", "resend", "sendgrid", "mailgun", "ses"]` (`:17`), `EmailAttachment` (`:20-25`), `EmailMessage` (`:28-40`), `EmailConfig` (`:43-50`), and `EmailService` (`:53-79`).
  - `EmailService.send` (`:72-75`) unconditionally appends to `self._outbox` and returns `f"msg-{len(self._outbox)}"` regardless of `EmailConfig.backend` (`FW-08`).
  - `EmailConfig` (`:43-50`) lacks TLS settings (`smtp_username`, `smtp_password`, `use_tls`, `use_starttls`, `pool_size`, `timeout_seconds`), provider settings (`api_base_url`, `mailgun_domain`, `webhook_secret`), and AWS SES settings (`aws_region`, `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `configuration_set_name`).
  - `EmailMessage` (`:28-40`) lacks `idempotency_key: str | None = None`, `tags: dict[str, str]`, and `metadata: dict[str, str]`.
- **`src/rs/vertebra-storage/src/signing.rs` (429 lines) & `src/rs/vertebra-storage/src/lib.rs` (663 lines)**:
  - `signing.rs:97-186` implements `S3SignConfig` and `sign_s3_v4_url` (computing `AWS4` -> `k_date` -> `k_region` -> `k_service` -> `k_signing` HMAC-SHA256 key derivation at `:173-180` and `sha256_hex` at `:22-26`).
  - `lib.rs:478-504` binds `py_sign_s3_v4_url` into `vertebra._storage` with `py.detach`.
  - Adding `sign_aws_sigv4_headers` in `signing.rs` and `py_sign_aws_sigv4_headers` in `lib.rs` allows `SesEmailTransport` to sign AWS SES v2 `POST /v2/email/outbound-emails` requests in Rust (`py.detach`) with zero `boto3`/`botocore` dependencies (Decision D3).

#### Findings carried

| Finding | Source | Resolution in this chapter |
| --- | --- | --- |
| `FW-08` | `vertebra-design-quality` (`framework.md`) | Replace in-memory-only `EmailService.send` with pluggable `EmailTransport` implementations (`MemoryEmailTransport`, `SmtpEmailTransport`, `ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`, `SesEmailTransport`), webhook HMAC verification, and `QueueService`-backed `EmailService.enqueue`. |

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, `vertebra.utils.serializers` (`encode_json_bytes`, `decode_json`), zero inline `#` comments, PEP 585/604/695 typing.
- [PyO3 FFI and GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md) — `py.detach(|| ...)` around `sign_aws_sigv4_headers` in `vertebra-storage` so cryptographic signing never holds the GIL.
- [Python Module and Class Layout](../../knowledge/patterns/python/module-and-class-layout.md) — clean domain separation across `base.py`, `memory.py`, `smtp.py`, `providers.py`, and `ses.py`.

### Requirements

| ID | Requirement |
| --- | --- |
| C2-R1 | Define `EmailTransport` protocol, `EmailReceipt`, `EmailDeliveryError`, `WebhookEvent`, and enriched `EmailAttachment`, `EmailMessage`, `EmailConfig`, and `EmailService` in `src/py/vertebra/framework/email/base.py`, `MemoryEmailTransport` in `src/py/vertebra/framework/email/memory.py`, and pooled `SmtpEmailTransport` (STARTTLS, implicit TLS, MIME `multipart/alternative` + attachments) in `src/py/vertebra/framework/email/smtp.py`. |
| C2-R2 | Implement `ResendEmailTransport`, `SendGridEmailTransport`, and `MailgunEmailTransport` in `src/py/vertebra/framework/email/providers.py` with batch send, idempotency keys, retry/backoff on `429`/`5xx` honoring `Retry-After`, and constant-time webhook HMAC verification (`WebhookVerifier`). |
| C2-R3 | Add `sign_aws_sigv4_headers` (`src/rs/vertebra-storage/src/signing.rs`) and `py_sign_aws_sigv4_headers` (`src/rs/vertebra-storage/src/lib.rs`) using `py.detach`, implement `SesEmailTransport` in `src/py/vertebra/framework/email/ses.py` for AWS SES v2 `/v2/email/outbound-emails`, and wire `EmailService.enqueue(...)` / `EmailService.enqueue_batch(...)` to `QueueService` (Decisions D3 & D4). |
| C2-R4 | Validate all six email transports, MIME formatting, retry/backoff, webhook signature verification, Rust SigV4 SES signing, and background queued delivery in `src/py/tests/unit/framework/test_email_providers.py`. |

### Target Layout

```text
src/rs/vertebra-storage/
  src/
    signing.rs                 AwsSigV4Headers, sign_aws_sigv4_headers (service="ses"|"s3", payload SHA-256, X-Amz-Security-Token)
    lib.rs                     py_sign_aws_sigv4_headers exposed on vertebra._storage with py.detach
src/py/vertebra/
  _storage.pyi                 Type stub declaration for py_sign_aws_sigv4_headers
  framework/
    email/
      __init__.py              Public re-exports for EmailService, EmailConfig, EmailMessage, EmailAttachment, EmailReceipt, EmailTransport, transports, and WebhookVerifier
      base.py                  EmailBackendType, EmailAttachment, EmailMessage, EmailReceipt, EmailConfig, EmailDeliveryError, WebhookEvent, EmailTransport, EmailService
      memory.py                MemoryEmailTransport (in-memory outbox capture, filtering, and clear)
      smtp.py                  SmtpEmailTransport (pooled smtplib.SMTP / SMTP_SSL, STARTTLS, RFC 5322 MIME multipart/alternative + attachments)
      providers.py             HttpTransportClient, ResendEmailTransport, SendGridEmailTransport, MailgunEmailTransport, WebhookVerifier
      ses.py                   SesEmailTransport (AWS SES v2 REST API signed via vertebra._storage.py_sign_aws_sigv4_headers)
src/py/tests/unit/framework/
  test_email_providers.py      Unit and contract tests for SMTP, Resend, SendGrid, Mailgun, SES, webhooks, and QueueService delivery
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Provider rate-limiting (`HTTP 429`) drops transactional emails during traffic bursts. | `ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`, and `SesEmailTransport` honor `Retry-After` headers and apply bounded exponential backoff with jitter on `429`, `500`, `502`, `503`, and `504` while forwarding `Idempotency-Key` headers to prevent duplicate sends. |
| Stale SMTP connections in pool fail with `SMTPServerDisconnected`. | `SmtpEmailTransport` validates pooled connections with `NOOP` before `send_message` and transparently reconnects once on `SMTPServerDisconnected`. |
| Webhook replay attacks using captured provider signatures. | `WebhookVerifier` enforces a configurable timestamp tolerance window (`tolerance_seconds = 300`) and uses `hmac.compare_digest` for constant-time signature verification. |

## Implementation Plan

### Phase 1: Core Protocol, SMTP, Memory, and REST API Transports
- [ ] 2.1 `EmailTransport` protocol, `SmtpEmailTransport`, and `MemoryEmailTransport` ([worksheet](tasks/2.1.md))
- [ ] 2.2 API email providers (`ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`) and webhook HMAC verification ([worksheet](tasks/2.2.md))
- [ ] Checkpoint: `uv run ruff check src/py/vertebra/framework/email/ && uv run mypy src/py/vertebra/framework/email/`

### Phase 2: Rust SigV4 AWS SES Transport, Queued Delivery & Verification
- [ ] 2.3 `SesEmailTransport` via Rust `vertebra._storage` SigV4 signing and `EmailService.enqueue(...)` integration ([worksheet](tasks/2.3.md))
- [ ] 2.4 Unit and contract tests for all email transports, webhook verification, and queued delivery ([worksheet](tasks/2.4.md))
- [ ] Chapter gate: `make dev && make lint-all && make rs-test && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C2-R1 | 2.1 | `uv run pytest src/py/tests/unit/framework/test_framework_subdomains.py -k email -q` |
| C2-R2 | 2.2, 2.4 | `uv run pytest src/py/tests/unit/framework/test_email_providers.py -k "resend or sendgrid or mailgun or webhook" -q` |
| C2-R3 | 2.3, 2.4 | `make dev && cd src/rs && cargo test -p vertebra-storage && uv run pytest src/py/tests/unit/framework/test_email_providers.py -k "ses or enqueue" -q` |
| C2-R4 | 2.4 | `uv run pytest src/py/tests/unit/framework/test_email_providers.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| FW-08 | 2.1, 2.2, 2.3, 2.4 |

## Continuity Snapshot

- Active flow: `transactional-email-providers`, state `planned`. Current task: none. Next step: `/flow:implement transactional-email-providers` after Chapter 1 (`distributed-queue-backends`) completes.
- Plan identity: plan_revision 2, plan_commit "". State identity: state_revision 0.
- Blockers: none. Decisions: inherited D3 (Rust `vertebra._storage` SigV4 header signing for SES v2) and D4 (`QueueService`-backed `EmailService.enqueue`) from `vertebra-queues-and-messaging`.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`.
