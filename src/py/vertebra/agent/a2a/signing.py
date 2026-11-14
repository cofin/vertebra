"""Agent Card RFC 7515 JSON Web Signature (JWS) signing and verification.

This module provides AgentCardSigner for cryptographic signing of A2A 1.0 AgentCard
manifests using HMAC-SHA256 (HS256) JWS detached or compact envelopes.
"""

import base64
import hashlib
import hmac
from typing import Any

from vertebra.agent.a2a.models import AgentCard, AgentCardSignature
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = [
    "AgentCardSigner",
]


class AgentCardSigner:
    """RFC 7515 JWS HS256 signer and verifier for A2A 1.0 AgentCard payloads."""

    PROTECTED_HEADER: dict[str, str] = {"alg": "HS256", "typ": "JOSE"}

    @classmethod
    def canonical_payload_bytes(cls, card: AgentCard) -> bytes:
        """Serialize card dictionary without signatures in key-sorted canonical JSON."""
        data = card.to_dict()
        data.pop("signatures", None)
        canonical = cls._canonicalize_value(data)
        return encode_json_bytes(canonical)

    @classmethod
    def sign(cls, card: AgentCard, secret: bytes | str) -> AgentCard:
        """Sign an AgentCard with HMAC-SHA256 and return a new AgentCard with signature."""
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        protected_header_json = encode_json_bytes(cls._canonicalize_value(cls.PROTECTED_HEADER))
        protected_b64 = cls._b64url_encode(protected_header_json)
        payload_bytes = cls.canonical_payload_bytes(card)
        payload_b64 = cls._b64url_encode(payload_bytes)

        signing_input = f"{protected_b64}.{payload_b64}".encode("ascii")
        sig = hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
        sig_b64 = cls._b64url_encode(sig)

        card_sig = AgentCardSignature(
            protected=protected_b64,
            signature=sig_b64,
        )

        return AgentCard(
            name=card.name,
            description=card.description,
            version=card.version,
            supported_interfaces=card.supported_interfaces,
            capabilities=card.capabilities,
            default_input_modes=card.default_input_modes,
            default_output_modes=card.default_output_modes,
            skills=card.skills,
            provider=card.provider,
            signatures=(card_sig,),
        )

    @classmethod
    def verify(cls, card: AgentCard, secret: bytes | str) -> bool:
        """Verify the first valid JWS signature on the card using HMAC-SHA256."""
        if not card.signatures:
            return False

        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        payload_bytes = cls.canonical_payload_bytes(card)
        payload_b64 = cls._b64url_encode(payload_bytes)

        for sig in card.signatures:
            try:
                header_raw = cls._b64url_decode(sig.protected)
                header = decode_json(header_raw)
                if not isinstance(header, dict) or header.get("alg") != "HS256":
                    continue

                signing_input = f"{sig.protected}.{payload_b64}".encode("ascii")
                expected_sig = hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
                actual_sig = cls._b64url_decode(sig.signature)

                if hmac.compare_digest(actual_sig, expected_sig):
                    return True
            except Exception:
                continue

        return False

    @staticmethod
    def _b64url_encode(data: bytes) -> str:
        """Encode bytes using URL-safe Base64 without trailing '=' padding."""
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    @staticmethod
    def _b64url_decode(s: str) -> bytes:
        """Decode URL-safe Base64 string with optional stripped padding."""
        rem = len(s) % 4
        if rem:
            s += "=" * (4 - rem)
        return base64.urlsafe_b64decode(s.encode("ascii"))

    @classmethod
    def _canonicalize_value(cls, obj: Any) -> Any:
        """Recursively sort dictionary keys for deterministic canonical JSON encoding."""
        if isinstance(obj, dict):
            return {k: cls._canonicalize_value(v) for k, v in sorted(obj.items())}
        if isinstance(obj, (list, tuple)):
            return [cls._canonicalize_value(item) for item in obj]
        return obj
