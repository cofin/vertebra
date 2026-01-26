"""Security module for Vertebra.

Provides:
- SecurityClient: IPC client for cryptographic operations (hashing, CSRF, etc.)
- auth: Authentication primitives (models, proxy, state)
"""

from vertebra.security.client import SecurityClient

__all__ = ["SecurityClient"]
