# PRD: Security & User Framework

## Overview
This PRD defines the architecture for a comprehensive security and user management framework within Vertebra. It aims to bridge Bun's native security capabilities (hashing, crypto) with Python's developer experience, providing a "Django-class" user management system.

The design prioritizes a **Universal** approach: the User Framework must function identically whether running in standard **Litestar ASGI** mode, **Litestar Native** mode, or pure **Vertebra Native** handlers. It emphasizes a "Broker-First" authentication strategy where the Bun runtime handles ingress validation.

## Functional Requirements

### Bun Security Integration (IPC)
- [ ] **Password Hashing:** Expose `Bun.password` (Argon2, Bcrypt) via Python IPC utilities (`vertebra.security.hash_password`, `verify_password`).
- [ ] **Crypto Utilities:** Expose Bun's native crypto helpers (hashing, random) to Python.
- [ ] **CSRF & CSP:** Implement CSRF token generation/verification and Content Security Policy helpers utilizing Bun's native capabilities where applicable.

### User Framework
- [ ] **User Model Mixin:** Create a `VertebraUserMixin` (and/or decorator) compatible with:
    - [ ] **Standard:** Pydantic, Attrs, Dataclasses.
    - [ ] **High-Performance/ORM:** `msgspec.Struct` (Default), `SQLAlchemy` Declarative Models.
    - [ ] Must provide standard properties: `is_authenticated`, `is_anonymous`.
    - [ ] Must allow mapping schema fields to auth claims.
- [ ] **Anonymous User:** Implement an `AnonymousUser` class that is injected when no authentication is present.
- [ ] **Universal Access:**
    - [ ] **Context:** Access via `vertebra.context.user` (works in Native & ASGI).
    - [ ] **Dependency Injection:** Provide `current_user` dependency for Native handlers.
    - [ ] **Middleware:**
        -   **Native:** Native Middleware hook for Vertebra Native mode.

### Authentication Strategy
- [ ] **Broker-Centric Verification:**
    -   Broker validates standard tokens (JWT, OAuth2) at ingress.
    -   Broker passes validated claims/headers to Python workers.
- [ ] **Provider Support (Configurable):**
    - [ ] **Cloud Identity:** Built-in support for **Google IAP** (Identity-Aware Proxy) header validation.
    - [ ] **OAuth2/OIDC:** Native config presets for **Google**, **GitHub**, and standard OIDC providers.
    - [ ] **Token Management:**
        -   Support for **Refresh Tokens** and rotation flows.
        -   Support for **JWTs via HTTP-Only Cookies** (auto-extraction and setting).
- [ ] **Python Authentication Provider:**
    -   Consumes validated claims from the Broker.
    -   Hydrates the User proxy object.

### Permissions & Scopes
- [ ] **Granular Scopes:** First-class support for `scopes` in the User model.
- [ ] **Intuitive Check API:**
    -   `user.has_perm('users:read')`
    -   `user.has_scope('admin')`

### Data Loading Strategy
- [ ] **Lazy-Loading Proxy:**
    -   `user` object is initialized as a proxy containing only token claims.
    -   Accessing a missing field triggers a fetch from the backing store.
    -   Fetched data is cached for the request duration.
- [ ] **Manual Refresh:** Provide `await user.refresh()` to force a re-fetch.

## Non-Functional Requirements
- **Performance:** IPC calls for hashing must be optimized. User loading must minimize DB hits.
- **Portability:** The `User` object must behave identically in ASGI and Native contexts.
- **Security:** Defaults must be secure (HTTP-only cookies, strong hashing).

## Technical Approach

### Architecture
1.  **Core Library (`vertebra.auth`):**
    -   Framework-agnostic implementations of `UserMixin`, `AnonymousUser`, `LazyProxy`, and Permission logic.
    -   Uses `contextvars` for global state.
2.  **Vertebra Broker (Bun):**
    -   Intercepts and validates `Authorization` headers and Cookies.
    -   Exposes IPC endpoints for Password/Crypto functions.
3.  **Integrations:**
    -   **Vertebra Native:** Direct integration into the Native Request lifecycle.

### Implementation Details
-   **IPC:** Define new command IDs for `HASH_PASSWORD`, `VERIFY_PASSWORD`.
-   **Mixin Strategy:** Use structural subtyping or inspection to handle the differences between Pydantic/SQLAlchemy/Msgspec fields.
-   **Config:** Use a unified config object (e.g., `AuthConfig`) to define providers, cookie settings, and IAP behavior.

## Acceptance Criteria
- [ ] **Provider Support:** Can configure Google IAP or GitHub OAuth2 via a simple config object.
- [ ] **Cookie Auth:** Can seamlessly use HTTP-only cookies for JWT storage.
- [ ] **Scope Checking:** `user.has_scope()` works correctly based on token claims.
- [ ] **Model Support:** `VertebraUserMixin` works correctly on a SQLAlchemy model, a Msgspec struct, and a Pydantic model.
- [ ] **Dual-Runtime:** The same User code works in a standard route and a Vertebra Native handler.
- [ ] **IPC Hashing:** `vertebra.security.hash_password` works via Bun IPC.

## Out of Scope
- Implementation of a full UI for login/registration (API & Logic only).
- Litestar-specific middleware (Focus on Vertebra Native framework).

## Dependencies
- **Internal:** `vertebra-core` (IPC), `vertebra-js` (Broker updates).
- **External:** `sqlalchemy`, `msgspec`.

## Risk Assessment
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Mixin Complexity | High | Medium | Heavy testing across all supported model types; prioritize composition over inheritance if mixins get too messy. |
| OAuth/OIDC Variance | Medium | Low | Stick to standard OIDC specs where possible; allow custom provider classes for edge cases. |

## Open Questions
- Specific implementation details for IAP header parsing.
