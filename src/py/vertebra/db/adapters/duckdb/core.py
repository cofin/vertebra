"""DuckDB analytical SQL inspection helpers."""

from __future__ import annotations

import datetime
import re
from collections.abc import Sequence
from decimal import Decimal
from typing import Any
from uuid import UUID

from vertebra.db.driver._common import DUCKDB_DIALECT_TAG
from vertebra.utils.serializers import encode_json

__all__ = (
    "DUCKDB_DIALECT_TAG",
    "DuckDBCore",
)

_CREDENTIAL_REJECTION_PATTERN = re.compile(r"\(HTTP (?:401|403)\)")


class DuckDBCore:
    """Protocol and SQL helpers for DuckDB analytical execution."""

    @staticmethod
    def normalize_param(value: Any) -> Any:
        """Normalize rich Python bind values into DuckDB-compatible scalar types."""
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list, tuple)):
            return encode_json(value)
        return value

    @staticmethod
    def invalidate_rejected_credentials(
        exc: BaseException,
        dynamic_secrets: Sequence[Any],
        secret_generations: dict[str, int],
    ) -> None:
        """Invalidate tracked dynamic secret generations on HTTP 401/403 httpfs rejections."""
        if not _CREDENTIAL_REJECTION_PATTERN.search(str(exc)):
            return
        for secret in dynamic_secrets:
            provider = secret.value_provider
            if provider is None:
                continue
            key = secret.name.lower()
            applied_gen = secret_generations.get(key)
            if applied_gen is None:
                continue
            target = getattr(provider, "__self__", provider)
            invalidate_fn = getattr(target, "invalidate", None)
            if callable(invalidate_fn):
                invalidate_fn(applied_gen)
                secret_generations.pop(key, None)

    @staticmethod
    def probe_provider_generation(provider: Any) -> int | None:
        """Read an integer generation counter from a dynamic secret provider if exposed."""
        target = getattr(provider, "__self__", provider)
        gen = getattr(target, "generation", None)
        if isinstance(gen, int) and not isinstance(gen, bool):
            return gen
        return None

    @staticmethod
    def probe_provider_needs_refresh(provider: Any) -> bool:
        """Query whether a dynamic secret provider signals an urgent credential refresh."""
        target = getattr(provider, "__self__", provider)
        fn = getattr(target, "needs_refresh", None)
        if callable(fn):
            return bool(fn())
        return False
