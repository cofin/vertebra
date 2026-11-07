"""Rust-accelerated text case conversion, slugification, SQL identifier quoting/splitting/validation, and UTF-8 truncation."""

from __future__ import annotations

from vertebra._vertebra import (
    camelize as _native_camelize,
)
from vertebra._vertebra import (
    kebabize as _native_kebabize,
)
from vertebra._vertebra import (
    normalize_identifier as _native_normalize_identifier,
)
from vertebra._vertebra import (
    pascalize as _native_pascalize,
)
from vertebra._vertebra import (
    quote_backtick_identifier as _native_quote_backtick_identifier,
)
from vertebra._vertebra import (
    quote_bracket_identifier as _native_quote_bracket_identifier,
)
from vertebra._vertebra import (
    quote_dialect_identifier as _native_quote_dialect_identifier,
)
from vertebra._vertebra import (
    quote_identifier as _native_quote_identifier,
)
from vertebra._vertebra import (
    slugify as _native_slugify,
)
from vertebra._vertebra import (
    snake_case as _native_snake_case,
)
from vertebra._vertebra import (
    split_qualified_identifier as _native_split_qualified_identifier,
)
from vertebra._vertebra import (
    truncate as _native_truncate,
)
from vertebra._vertebra import (
    truncate_utf8_bytes as _native_truncate_utf8_bytes,
)
from vertebra._vertebra import (
    validate_identifier as _native_validate_identifier,
)

__all__ = (
    "camelize",
    "kebabize",
    "normalize_identifier",
    "pascalize",
    "quote_backtick_identifier",
    "quote_bracket_identifier",
    "quote_dialect_identifier",
    "quote_identifier",
    "slugify",
    "snake_case",
    "split_qualified_identifier",
    "truncate",
    "truncate_utf8_bytes",
    "validate_identifier",
)


def snake_case(value: str) -> str:
    """Convert a string (camelCase, PascalCase, kebab-case, spaces, or acronyms) to snake_case."""
    return _native_snake_case(value)


def camelize(value: str) -> str:
    """Convert a string to camelCase."""
    return _native_camelize(value)


def pascalize(value: str) -> str:
    """Convert a string to PascalCase."""
    return _native_pascalize(value)


def kebabize(value: str) -> str:
    """Convert a string to kebab-case."""
    return _native_kebabize(value)


def slugify(value: str, separator: str = "-") -> str:
    """Convert text into a URL- and filename-safe ASCII slug using ``separator``."""
    return _native_slugify(value, separator)


def quote_identifier(identifier: str) -> str:
    """Quote a single SQL identifier with double quotes, escaping internal double quotes."""
    return _native_quote_identifier(identifier)


def quote_backtick_identifier(identifier: str) -> str:
    """Quote a single SQL identifier with backticks, escaping internal backticks."""
    return _native_quote_backtick_identifier(identifier)


def quote_bracket_identifier(identifier: str) -> str:
    """Quote a single SQL Server / T-SQL identifier with square brackets."""
    return _native_quote_bracket_identifier(identifier)


def quote_dialect_identifier(identifier: str, dialect: str = "ansi") -> str:
    """Quote a single or dot-qualified SQL identifier using the target dialect's quoting style."""
    return _native_quote_dialect_identifier(identifier, dialect)


def split_qualified_identifier(
    identifier: str,
    *,
    quote_chars: str = '"`',
    allow_bracket_quotes: bool = True,
) -> list[str]:
    """Split a potentially dot-qualified SQL identifier while respecting quoted segments."""
    return _native_split_qualified_identifier(
        identifier,
        quote_chars=quote_chars,
        allow_bracket_quotes=allow_bracket_quotes,
    )


def normalize_identifier(identifier: str, dialect: str | None = None) -> str:
    """Validate and normalize a single or dot-qualified SQL identifier, optionally quoting per dialect."""
    return _native_normalize_identifier(identifier, dialect)


def validate_identifier(
    identifier: str,
    field_name: str = "identifier",
    *,
    allow_dots: bool = True,
    allow_hyphens: bool = False,
) -> str:
    """Validate a SQL, PL/SQL, channel, or storage identifier against unsafe characters."""
    return _native_validate_identifier(
        identifier,
        field_name,
        allow_dots=allow_dots,
        allow_hyphens=allow_hyphens,
    )


def truncate_utf8_bytes(value: str, max_bytes: int) -> str:
    """Truncate ``value`` to at most ``max_bytes`` UTF-8 bytes without splitting any codepoint."""
    return _native_truncate_utf8_bytes(value, max_bytes)


def truncate(value: str, max_chars: int, *, suffix: str = "...") -> str:
    """Truncate ``value`` to at most ``max_chars`` characters, appending ``suffix`` when truncated."""
    return _native_truncate(value, max_chars, suffix=suffix)
