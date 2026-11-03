"""Rust-accelerated text case conversion, slugification, SQL identifier quoting/splitting/validation, and UTF-8 truncation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from vertebra.utils.module_loader import import_optional

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

_native_vtb: Any = import_optional("vertebra._vertebra")

_LATIN_FOLD_MAP: dict[str, str] = {
    "À": "a",
    "Á": "a",
    "Â": "a",
    "Ã": "a",
    "Ä": "a",
    "Å": "a",
    "à": "a",
    "á": "a",
    "â": "a",
    "ã": "a",
    "ä": "a",
    "å": "a",
    "Ā": "a",
    "ā": "a",
    "Ă": "a",
    "ă": "a",
    "Ą": "a",
    "ą": "a",
    "Æ": "ae",
    "æ": "ae",
    "Ç": "c",
    "ç": "c",
    "Ć": "c",
    "ć": "c",
    "Č": "c",
    "č": "c",
    "Ď": "d",
    "ď": "d",
    "Đ": "d",
    "đ": "d",
    "Ð": "d",
    "ð": "d",
    "È": "e",
    "É": "e",
    "Ê": "e",
    "Ë": "e",
    "è": "e",
    "é": "e",
    "ê": "e",
    "ë": "e",
    "Ē": "e",
    "ē": "e",
    "Ė": "e",
    "ė": "e",
    "Ę": "e",
    "ę": "e",
    "Ě": "e",
    "ě": "e",
    "Ğ": "g",
    "ğ": "g",
    "Ģ": "g",
    "ģ": "g",
    "Ì": "i",
    "Í": "i",
    "Î": "i",
    "Ï": "i",
    "ì": "i",
    "í": "i",
    "î": "i",
    "ï": "i",
    "Ī": "i",
    "ī": "i",
    "Į": "i",
    "į": "i",
    "İ": "i",
    "ı": "i",
    "Ķ": "k",
    "ķ": "k",
    "Ĺ": "l",
    "ĺ": "l",
    "Ļ": "l",
    "ļ": "l",
    "Ľ": "l",
    "ľ": "l",
    "Ł": "l",
    "ł": "l",
    "Ñ": "n",
    "ñ": "n",
    "Ń": "n",
    "ń": "n",
    "Ņ": "n",
    "ņ": "n",
    "Ň": "n",
    "ň": "n",
    "Ò": "o",
    "Ó": "o",
    "Ô": "o",
    "Õ": "o",
    "Ö": "o",
    "Ø": "o",
    "ò": "o",
    "ó": "o",
    "ô": "o",
    "õ": "o",
    "ö": "o",
    "ø": "o",
    "Ō": "o",
    "ō": "o",
    "Ő": "o",
    "ő": "o",
    "Œ": "oe",
    "œ": "oe",
    "Ŕ": "r",
    "ŕ": "r",
    "Ř": "r",
    "ř": "r",
    "Ś": "s",
    "ś": "s",
    "Š": "s",
    "š": "s",
    "Ş": "s",
    "ş": "s",
    "Ș": "s",
    "ș": "s",
    "ß": "ss",
    "Ť": "t",
    "ť": "t",
    "Ţ": "t",
    "ţ": "t",
    "Ț": "t",
    "ț": "t",
    "Þ": "t",
    "þ": "t",
    "Ù": "u",
    "Ú": "u",
    "Û": "u",
    "Ü": "u",
    "ù": "u",
    "ú": "u",
    "û": "u",
    "ü": "u",
    "Ū": "u",
    "ū": "u",
    "Ů": "u",
    "ů": "u",
    "Ű": "u",
    "ű": "u",
    "Ų": "u",
    "ų": "u",
    "Ý": "y",
    "ý": "y",
    "ÿ": "y",
    "Ÿ": "y",
    "Ź": "z",
    "ź": "z",
    "Ż": "z",
    "ż": "z",
    "Ž": "z",
    "ž": "z",
}


def snake_case(value: str) -> str:
    """Convert a string (camelCase, PascalCase, kebab-case, spaces, or acronyms) to snake_case."""
    return _snake_case_impl(value)


def camelize(value: str) -> str:
    """Convert a string to camelCase."""
    return _camelize_impl(value)


def pascalize(value: str) -> str:
    """Convert a string to PascalCase."""
    return _pascalize_impl(value)


def kebabize(value: str) -> str:
    """Convert a string to kebab-case."""
    return _kebabize_impl(value)


def slugify(value: str, separator: str = "-") -> str:
    """Convert text into a URL- and filename-safe ASCII slug using ``separator``."""
    return _slugify_impl(value, separator)


def quote_identifier(identifier: str) -> str:
    """Quote a single SQL identifier with double quotes, escaping internal double quotes."""
    return _quote_identifier_impl(identifier)


def quote_backtick_identifier(identifier: str) -> str:
    """Quote a single SQL identifier with backticks, escaping internal backticks."""
    return _quote_backtick_identifier_impl(identifier)


def quote_bracket_identifier(identifier: str) -> str:
    """Quote a single SQL Server / T-SQL identifier with square brackets."""
    return _quote_bracket_identifier_impl(identifier)


def quote_dialect_identifier(identifier: str, dialect: str = "ansi") -> str:
    """Quote a single or dot-qualified SQL identifier using the target dialect's quoting style."""
    return _quote_dialect_identifier_impl(identifier, dialect)


def split_qualified_identifier(
    identifier: str,
    *,
    quote_chars: str = '"`',
    allow_bracket_quotes: bool = True,
) -> list[str]:
    """Split a potentially dot-qualified SQL identifier while respecting quoted segments."""
    return _split_qualified_identifier_impl(
        identifier,
        quote_chars=quote_chars,
        allow_bracket_quotes=allow_bracket_quotes,
    )


def normalize_identifier(identifier: str, dialect: str | None = None) -> str:
    """Validate and normalize a single or dot-qualified SQL identifier, optionally quoting per dialect."""
    return _normalize_identifier_impl(identifier, dialect)


def validate_identifier(
    identifier: str,
    field_name: str = "identifier",
    *,
    allow_dots: bool = True,
    allow_hyphens: bool = False,
) -> str:
    """Validate a SQL, PL/SQL, channel, or storage identifier against unsafe characters."""
    return _validate_identifier_impl(
        identifier,
        field_name,
        allow_dots=allow_dots,
        allow_hyphens=allow_hyphens,
    )


def truncate_utf8_bytes(value: str, max_bytes: int) -> str:
    """Truncate ``value`` to at most ``max_bytes`` UTF-8 bytes without splitting any codepoint."""
    return _truncate_utf8_bytes_impl(value, max_bytes)


def truncate(value: str, max_chars: int, *, suffix: str = "...") -> str:
    """Truncate ``value`` to at most ``max_chars`` characters, appending ``suffix`` when truncated."""
    return _truncate_impl(value, max_chars, suffix=suffix)


def _get_native_fn[**P, R](name: str, fallback: Callable[P, R]) -> Callable[P, R]:
    """Resolve a compiled Rust function from ``vertebra._vertebra`` or return ``fallback``."""
    fn = getattr(_native_vtb, name, None)
    if callable(fn) and not isinstance(fn, type):
        return cast("Callable[P, R]", fn)
    return fallback


def _split_identifier_words(value: str) -> list[str]:
    """Split an identifier into words across case, digit, underscore, hyphen, and space boundaries."""
    words: list[str] = []
    current: list[str] = []
    n = len(value)
    for i, ch in enumerate(value):
        if ch in {"_", "-", ".", "/"} or ch.isspace():
            if current:
                words.append("".join(current))
                current.clear()
            continue
        if ch.isupper() and current:
            prev = value[i - 1]
            next_is_lower = (i + 1 < n) and value[i + 1].islower()
            if prev.islower() or prev.isdigit() or (prev.isupper() and next_is_lower):
                words.append("".join(current))
                current.clear()
        current.append(ch)
    if current:
        words.append("".join(current))
    return words


def _py_snake_case(value: str) -> str:
    """Pure-Python fallback for ``snake_case``."""
    return "_".join(word.lower() for word in _split_identifier_words(value))


def _py_camelize(value: str) -> str:
    """Pure-Python fallback for ``camelize``."""
    words = _split_identifier_words(value)
    if not words:
        return ""
    first = words[0].lower()
    rest = "".join(w[:1].upper() + w[1:].lower() for w in words[1:])
    return first + rest


def _py_pascalize(value: str) -> str:
    """Pure-Python fallback for ``pascalize``."""
    return "".join(w[:1].upper() + w[1:].lower() for w in _split_identifier_words(value))


def _py_kebabize(value: str) -> str:
    """Pure-Python fallback for ``kebabize``."""
    return "-".join(word.lower() for word in _split_identifier_words(value))


def _py_slugify(value: str, separator: str = "-") -> str:
    """Pure-Python fallback for ``slugify``."""
    out: list[str] = []
    pending_sep = False
    for ch in value:
        code = ord(ch)
        if 0x0300 <= code <= 0x036F:
            continue
        if ch.isascii() and ch.isalnum():
            if pending_sep and out:
                out.append(separator)
            pending_sep = False
            out.append(ch.lower())
            continue
        folded = _LATIN_FOLD_MAP.get(ch, "")
        if folded:
            if pending_sep and out:
                out.append(separator)
            pending_sep = False
            out.append(folded)
        elif out:
            pending_sep = True
    return "".join(out)


def _py_quote_identifier(identifier: str) -> str:
    """Pure-Python fallback for ``quote_identifier``."""
    clean = identifier.strip()
    if len(clean) >= 2 and clean.startswith('"') and clean.endswith('"'):
        return clean
    escaped = clean.replace('"', '""')
    return f'"{escaped}"'


def _py_quote_backtick_identifier(identifier: str) -> str:
    """Pure-Python fallback for ``quote_backtick_identifier``."""
    clean = identifier.strip()
    if len(clean) >= 2 and clean.startswith("`") and clean.endswith("`"):
        return clean
    escaped = clean.replace("`", "``")
    return f"`{escaped}`"


def _py_quote_bracket_identifier(identifier: str) -> str:
    """Pure-Python fallback for ``quote_bracket_identifier``."""
    clean = identifier.strip()
    if len(clean) >= 2 and clean.startswith("[") and clean.endswith("]"):
        return clean
    escaped = clean.replace("]", "]]")
    return f"[{escaped}]"


def _py_split_qualified_identifier(
    identifier: str,
    *,
    quote_chars: str = '"`',
    allow_bracket_quotes: bool = True,
) -> list[str]:
    """Pure-Python fallback for ``split_qualified_identifier``."""
    text = identifier.strip()
    if not text:
        return []

    parts: list[str] = []
    current: list[str] = []
    quote_char: str | None = None
    i = 0
    length = len(text)

    while i < length:
        ch = text[i]
        if quote_char is not None:
            if ch == quote_char:
                if i + 1 < length and text[i + 1] == quote_char:
                    current.append(quote_char)
                    i += 2
                    continue
                quote_char = None
                i += 1
                continue
            current.append(ch)
            i += 1
            continue

        if ch in quote_chars:
            quote_char = ch
            i += 1
            continue

        if allow_bracket_quotes and ch == "[":
            quote_char = "]"
            i += 1
            continue

        if ch == ".":
            candidate = "".join(current).strip()
            if candidate:
                parts.append(candidate)
            current.clear()
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _quote_part_for_dialect(part: str, dialect: str) -> str:
    """Quote a single identifier part according to ``dialect``."""
    norm = dialect.strip().lower()
    if norm in {"mysql", "mariadb", "bigquery", "googlesql", "spanner"}:
        return _py_quote_backtick_identifier(part)
    if norm in {"mssql", "tsql", "sqlserver"}:
        return _py_quote_bracket_identifier(part)
    return _py_quote_identifier(part)


def _py_quote_dialect_identifier(identifier: str, dialect: str = "ansi") -> str:
    """Pure-Python fallback for ``quote_dialect_identifier``."""
    parts = _py_split_qualified_identifier(identifier)
    if not parts:
        return _quote_part_for_dialect(identifier.strip(), dialect)
    return ".".join(_quote_part_for_dialect(part, dialect) for part in parts)


def _is_valid_segment(segment: str, allow_hyphens: bool) -> bool:
    """Return True if ``segment`` is a valid unquoted ASCII identifier segment."""
    if not segment:
        return False
    first = segment[0]
    if not (first.isascii() and (first.isalpha() or first == "_")):
        return False
    return all(ch.isascii() and (ch.isalnum() or ch == "_" or (allow_hyphens and ch == "-")) for ch in segment[1:])


def _split_qualified_with_quote_flag(identifier: str) -> list[tuple[str, bool]]:
    """Split a qualified identifier and record whether each segment was quoted."""
    parts: list[tuple[str, bool]] = []
    current: list[str] = []
    quote_char: str | None = None
    was_quoted = False
    i = 0
    length = len(identifier)

    while i < length:
        ch = identifier[i]
        if quote_char is not None:
            if ch == quote_char:
                if i + 1 < length and identifier[i + 1] == quote_char:
                    current.append(quote_char)
                    i += 2
                    continue
                quote_char = None
                i += 1
                continue
            current.append(ch)
            i += 1
            continue

        if ch in {'"', "`"}:
            quote_char = ch
            was_quoted = True
            i += 1
            continue
        if ch == "[":
            quote_char = "]"
            was_quoted = True
            i += 1
            continue
        if ch == ".":
            parts.append(("".join(current).strip(), was_quoted))
            current.clear()
            was_quoted = False
            i += 1
            continue

        current.append(ch)
        i += 1

    if quote_char is not None:
        return []
    parts.append(("".join(current).strip(), was_quoted))
    return parts


def _py_normalize_identifier(identifier: str, dialect: str | None = None) -> str:
    """Pure-Python fallback for ``normalize_identifier``."""
    clean = identifier.strip()
    if not clean:
        msg = "Identifier cannot be empty"
        raise ValueError(msg)

    raw_parts = _split_qualified_with_quote_flag(clean)
    if not raw_parts:
        msg = f"Invalid identifier: {identifier}"
        raise ValueError(msg)

    normalized: list[str] = []
    for part, was_quoted in raw_parts:
        if not part:
            msg = f"Invalid identifier: {identifier}"
            raise ValueError(msg)
        if not was_quoted and not _is_valid_segment(part, allow_hyphens=False):
            msg = f"Invalid identifier part: {part}"
            raise ValueError(msg)
        normalized.append(part)

    clean_dialect = dialect.strip() if dialect is not None else ""
    if not clean_dialect:
        return ".".join(normalized)
    return ".".join(_quote_part_for_dialect(part, clean_dialect) for part in normalized)


def _py_validate_identifier(
    identifier: str,
    field_name: str = "identifier",
    *,
    allow_dots: bool = True,
    allow_hyphens: bool = False,
) -> str:
    """Pure-Python fallback for ``validate_identifier``."""
    if not identifier:
        msg = f"Invalid SQL identifier for {field_name}: {identifier!r}"
        raise ValueError(msg)
    segments = identifier.split(".") if allow_dots else [identifier]
    if not all(_is_valid_segment(seg, allow_hyphens=allow_hyphens) for seg in segments):
        msg = f"Invalid SQL identifier for {field_name}: {identifier!r}"
        raise ValueError(msg)
    return identifier


def _py_truncate_utf8_bytes(value: str, max_bytes: int) -> str:
    """Pure-Python fallback for ``truncate_utf8_bytes``."""
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _py_truncate(value: str, max_chars: int, *, suffix: str = "...") -> str:
    """Pure-Python fallback for ``truncate``."""
    if max_chars <= 0:
        return ""
    if len(value) <= max_chars:
        return value
    if len(suffix) >= max_chars:
        return suffix[:max_chars]
    keep = max_chars - len(suffix)
    return value[:keep] + suffix


_snake_case_impl = _get_native_fn("snake_case", _py_snake_case)
_camelize_impl = _get_native_fn("camelize", _py_camelize)
_pascalize_impl = _get_native_fn("pascalize", _py_pascalize)
_kebabize_impl = _get_native_fn("kebabize", _py_kebabize)
_slugify_impl = _get_native_fn("slugify", _py_slugify)
_quote_identifier_impl = _get_native_fn("quote_identifier", _py_quote_identifier)
_quote_backtick_identifier_impl = _get_native_fn("quote_backtick_identifier", _py_quote_backtick_identifier)
_quote_bracket_identifier_impl = _get_native_fn("quote_bracket_identifier", _py_quote_bracket_identifier)
_quote_dialect_identifier_impl = _get_native_fn("quote_dialect_identifier", _py_quote_dialect_identifier)
_split_qualified_identifier_impl = _get_native_fn("split_qualified_identifier", _py_split_qualified_identifier)
_normalize_identifier_impl = _get_native_fn("normalize_identifier", _py_normalize_identifier)
_validate_identifier_impl = _get_native_fn("validate_identifier", _py_validate_identifier)
_truncate_utf8_bytes_impl = _get_native_fn("truncate_utf8_bytes", _py_truncate_utf8_bytes)
_truncate_impl = _get_native_fn("truncate", _py_truncate)
