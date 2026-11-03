"""Rust-accelerated humanize formatting, English noun inflection, and byte/duration/rate unit parsing."""

from __future__ import annotations

import math

from vertebra.utils.text import _get_native_fn, _split_identifier_words

__all__ = (
    "humanize",
    "humanize_bytes",
    "humanize_duration",
    "humanize_number",
    "humanize_rate",
    "ordinalize",
    "parse_bytes",
    "parse_duration",
    "pluralize",
    "singularize",
    "titleize",
)

_IRREGULAR_NOUNS: tuple[tuple[str, str], ...] = (
    ("person", "people"),
    ("child", "children"),
    ("man", "men"),
    ("woman", "women"),
    ("mouse", "mice"),
    ("goose", "geese"),
    ("tooth", "teeth"),
    ("foot", "feet"),
    ("criterion", "criteria"),
    ("phenomenon", "phenomena"),
    ("index", "indices"),
    ("matrix", "matrices"),
    ("vertex", "vertices"),
    ("quiz", "quizzes"),
)

_UNCOUNTABLE_NOUNS: frozenset[str] = frozenset(
    {
        "data",
        "metadata",
        "series",
        "species",
        "equipment",
        "information",
        "news",
        "traffic",
        "feedback",
        "hardware",
        "software",
        "health",
    }
)


def humanize(value: str, *, capitalize: bool = True) -> str:
    """Convert an identifier (``user_profile_id``, ``getUserById``) into a human-readable phrase."""
    return _humanize_impl(value, capitalize=capitalize)


def titleize(value: str) -> str:
    """Convert an identifier or phrase into Title Case (``get_user_by_id`` -> ``Get User By Id``)."""
    return _titleize_impl(value)


def pluralize(word: str, count: int | None = None, *, include_count: bool = False) -> str:
    """Inflect an English noun to its plural (or singular when ``count`` is ``1`` or ``-1``)."""
    return _pluralize_impl(word, count, include_count=include_count)


def singularize(word: str) -> str:
    """Inflect an English plural noun to its singular form."""
    return _singularize_impl(word)


def ordinalize(value: int) -> str:
    """Format an integer as an English ordinal string (``1`` -> ``1st``, ``2`` -> ``2nd``)."""
    return _ordinalize_impl(value)


def humanize_bytes(num_bytes: int | float, *, binary: bool = True, precision: int = 1) -> str:
    """Format a byte count using IEC binary (``KiB``, ``MiB``, ``GiB``) or SI decimal (``KB``, ``MB``, ``GB``) units."""
    non_negative = max(0, int(round(num_bytes)))
    return _humanize_bytes_impl(non_negative, binary=binary, precision=precision)


def parse_bytes(value: str | int) -> int:
    """Parse a human-formatted byte size string (``64MiB``, ``64 MB``, ``512K``) or int into exact bytes."""
    if isinstance(value, int) and not isinstance(value, bool):
        if value < 0:
            msg = f"Byte size must be non-negative: {value}"
            raise ValueError(msg)
        return value
    return int(_parse_bytes_impl(str(value)))


def humanize_duration(seconds: float | int, *, precision: int = 1) -> str:
    """Format a duration in seconds into human-readable units (``ns``, ``µs``, ``ms``, ``s``, ``m``, ``h``, ``d``)."""
    return _humanize_duration_impl(float(seconds), precision=precision)


def parse_duration(value: str | float | int) -> float:
    """Parse a single or compound duration string (``500ms``, ``1.5s``, ``1m 30s``) or number into seconds."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        val_f = float(value)
        if not math.isfinite(val_f) or val_f < 0.0:
            msg = f"Duration must be non-negative: {value}"
            raise ValueError(msg)
        return val_f
    return float(_parse_duration_impl(str(value)))


def humanize_number(value: int | float, *, precision: int = 1) -> str:
    """Format a number with compact magnitude suffixes (``K``, ``M``, ``B``, ``T``)."""
    return _humanize_number_impl(float(value), precision=precision)


def humanize_rate(
    count: int | float,
    seconds: float = 1.0,
    *,
    unit: str = "req",
    precision: int = 1,
) -> str:
    """Format a throughput rate (``count / seconds``) with compact magnitude suffix (``123.8K req/s``)."""
    return _humanize_rate_impl(float(count), float(seconds), unit=unit, precision=precision)


def _py_humanize(value: str, *, capitalize: bool = True) -> str:
    """Pure-Python fallback for ``humanize``."""
    words = _split_identifier_words(value)
    if len(words) > 1 and words[-1].lower() in {"id", "ids"}:
        words = words[:-1]
    if not words:
        return ""
    lowered = [w.lower() for w in words]
    if capitalize:
        lowered[0] = lowered[0][:1].upper() + lowered[0][1:]
    return " ".join(lowered)


def _py_titleize(value: str) -> str:
    """Pure-Python fallback for ``titleize``."""
    words = _split_identifier_words(value)
    if not words:
        return ""
    return " ".join(w[:1].upper() + w[1:].lower() for w in words)


def _match_casing(template: str, target_lower: str) -> str:
    """Preserve uppercase or title-case styling from ``template`` onto ``target_lower``."""
    if all(not ch.isalpha() or ch.isupper() for ch in template):
        return target_lower.upper()
    if template[:1].isupper():
        return target_lower[:1].upper() + target_lower[1:]
    return target_lower


def _pluralize_single_word(word: str) -> str:
    """Inflect a single non-empty English word to its plural form."""
    lower = word.lower()
    if lower in _UNCOUNTABLE_NOUNS:
        return word
    for singular, plural in _IRREGULAR_NOUNS:
        if lower in {singular, plural}:
            return _match_casing(word, plural)
    if lower.endswith(("analysis", "basis", "diagnosis", "parenthesis", "synopsis", "thesis", "crisis")):
        return f"{word[:-2]}es"
    if lower.endswith(("knife", "wife", "life")) and len(word) > 2:
        return f"{word[:-2]}ves"
    if lower.endswith(("leaf", "half", "wolf", "shelf", "elf", "loaf", "thief")):
        return f"{word[:-1]}ves"
    if lower.endswith("y") and len(word) > 1 and lower[-2] not in {"a", "e", "i", "o", "u"}:
        return f"{word[:-1]}ies"
    if lower.endswith(("s", "sh", "ch", "x", "z")):
        return f"{word}es"
    return f"{word}s"


def _py_pluralize(word: str, count: int | None = None, *, include_count: bool = False) -> str:
    """Pure-Python fallback for ``pluralize``."""
    trimmed = word.strip()
    if not trimmed:
        return str(count) if (count is not None and include_count) else ""
    inflected = _py_singularize(trimmed) if count in {1, -1} else _pluralize_single_word(trimmed)
    if count is not None and include_count:
        return f"{count} {inflected}"
    return inflected


def _py_singularize(word: str) -> str:
    """Pure-Python fallback for ``singularize``."""
    trimmed = word.strip()
    if not trimmed:
        return ""
    lower = trimmed.lower()
    if lower in _UNCOUNTABLE_NOUNS:
        return trimmed
    for singular, plural in _IRREGULAR_NOUNS:
        if lower == plural:
            return _match_casing(trimmed, singular)
        if lower == singular:
            return trimmed
    if lower.endswith(("analyses", "bases", "diagnoses", "parentheses", "synopses", "theses", "crises")):
        return f"{trimmed[:-2]}is"
    if lower.endswith(("vertices", "matrices", "indices")):
        return f"{trimmed[:-4]}ex"
    if lower.endswith("ies") and len(trimmed) > 3:
        return f"{trimmed[:-3]}y"
    if lower.endswith(("knives", "wives", "lives")) and len(trimmed) > 3:
        return f"{trimmed[:-3]}fe"
    if lower.endswith(("leaves", "halves", "wolves", "shelves", "elves", "loaves", "thieves")) and len(trimmed) > 3:
        return f"{trimmed[:-3]}f"
    if lower.endswith(
        ("statuses", "buses", "aliases", "campuses", "bonuses", "viruses", "ches", "shes", "xes", "zes", "sses")
    ):
        return trimmed[:-2]
    if lower.endswith("s") and not lower.endswith(("ss", "us")) and len(trimmed) > 1:
        return trimmed[:-1]
    return trimmed


def _py_ordinalize(value: int) -> str:
    """Pure-Python fallback for ``ordinalize``."""
    rem100 = abs(value) % 100
    if 11 <= rem100 <= 13:
        suffix = "th"
    else:
        rem10 = rem100 % 10
        if rem10 == 1:
            suffix = "st"
        elif rem10 == 2:
            suffix = "nd"
        elif rem10 == 3:
            suffix = "rd"
        else:
            suffix = "th"
    return f"{value}{suffix}"


def _py_humanize_bytes(num_bytes: int | float, *, binary: bool = True, precision: int = 1) -> str:
    """Pure-Python fallback for ``humanize_bytes``."""
    n = max(0, int(round(num_bytes)))
    base = 1024 if binary else 1000
    if n < base:
        return f"{n} B"
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB") if binary else ("B", "KB", "MB", "GB", "TB", "PB")
    val = float(n)
    idx = 0
    while val >= base and idx + 1 < len(units):
        val /= base
        idx += 1
    return f"{val:.{precision}f} {units[idx]}"


def _py_parse_bytes(value: str | int) -> int:
    """Pure-Python fallback for ``parse_bytes``."""
    if isinstance(value, int) and not isinstance(value, bool):
        if value < 0:
            msg = f"Byte size must be non-negative: {value}"
            raise ValueError(msg)
        return value
    clean = str(value).strip()
    if not clean:
        msg = "Byte size string cannot be empty"
        raise ValueError(msg)
    split_idx = len(clean)
    for idx, ch in enumerate(clean):
        if not (ch.isdigit() or ch in {".", "_", "+", "-"}):
            split_idx = idx
            break
    num_str = clean[:split_idx].strip().replace("_", "")
    unit_str = clean[split_idx:].strip().lower()
    try:
        number = float(num_str)
    except ValueError as exc:
        msg = f"Invalid byte size number in {value!r}"
        raise ValueError(msg) from exc
    if not math.isfinite(number) or number < 0.0:
        msg = f"Byte size must be non-negative in {value!r}"
        raise ValueError(msg)
    multipliers: dict[str, float] = {
        "": 1.0,
        "b": 1.0,
        "byte": 1.0,
        "bytes": 1.0,
        "k": 1024.0,
        "kib": 1024.0,
        "m": 1024.0**2,
        "mib": 1024.0**2,
        "g": 1024.0**3,
        "gib": 1024.0**3,
        "t": 1024.0**4,
        "tib": 1024.0**4,
        "p": 1024.0**5,
        "pib": 1024.0**5,
        "kb": 1000.0,
        "mb": 1000.0**2,
        "gb": 1000.0**3,
        "tb": 1000.0**4,
        "pb": 1000.0**5,
    }
    if unit_str not in multipliers:
        msg = f"Unsupported byte unit {unit_str!r} in {value!r}"
        raise ValueError(msg)
    return int(round(number * multipliers[unit_str]))


def _py_humanize_duration(seconds: float | int, *, precision: int = 1) -> str:
    """Pure-Python fallback for ``humanize_duration``."""
    sec = float(seconds)
    if not math.isfinite(sec):
        return "0 ms"
    sign = "-" if sec < 0.0 else ""
    abs_sec = abs(sec)
    if abs_sec == 0.0:
        return "0 ms"
    if abs_sec < 1e-6:
        return f"{sign}{abs_sec * 1e9:.{precision}f} ns"
    if abs_sec < 1e-3:
        return f"{sign}{abs_sec * 1e6:.{precision}f} µs"
    if abs_sec < 1.0:
        return f"{sign}{abs_sec * 1e3:.{precision}f} ms"
    if abs_sec < 60.0:
        return f"{sign}{abs_sec:.{precision}f} s"

    total_secs = int(round(abs_sec))
    days = total_secs // 86_400
    hours = (total_secs % 86_400) // 3_600
    minutes = (total_secs % 3_600) // 60
    secs = total_secs % 60
    parts: list[str] = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    return f"{sign}{' '.join(parts)}"


def _py_parse_duration(value: str | float | int) -> float:
    """Pure-Python fallback for ``parse_duration``."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        val_f = float(value)
        if not math.isfinite(val_f) or val_f < 0.0:
            msg = f"Duration must be non-negative: {value}"
            raise ValueError(msg)
        return val_f
    clean = str(value).strip()
    if not clean:
        msg = "Duration string cannot be empty"
        raise ValueError(msg)
    try:
        plain = float(clean)
        if not math.isfinite(plain) or plain < 0.0:
            msg = f"Duration must be non-negative in {value!r}"
            raise ValueError(msg)
        return plain
    except ValueError:
        pass

    if clean.startswith("+"):
        clean = clean[1:].strip()

    factors: dict[str, float] = {
        "ns": 1e-9,
        "nanosecond": 1e-9,
        "nanoseconds": 1e-9,
        "us": 1e-6,
        "µs": 1e-6,
        "μs": 1e-6,
        "microsecond": 1e-6,
        "microseconds": 1e-6,
        "ms": 1e-3,
        "millisecond": 1e-3,
        "milliseconds": 1e-3,
        "": 1.0,
        "s": 1.0,
        "sec": 1.0,
        "secs": 1.0,
        "second": 1.0,
        "seconds": 1.0,
        "m": 60.0,
        "min": 60.0,
        "mins": 60.0,
        "minute": 60.0,
        "minutes": 60.0,
        "h": 3_600.0,
        "hr": 3_600.0,
        "hrs": 3_600.0,
        "hour": 3_600.0,
        "hours": 3_600.0,
        "d": 86_400.0,
        "day": 86_400.0,
        "days": 86_400.0,
    }
    i = 0
    length = len(clean)
    total = 0.0
    parsed_any = False

    while i < length:
        while i < length and clean[i].isspace():
            i += 1
        if i >= length:
            break
        num_start = i
        while i < length and (clean[i].isdigit() or clean[i] in {".", "_"}):
            i += 1
        if i == num_start:
            msg = f"Expected duration number in {value!r}"
            raise ValueError(msg)
        amount = float(clean[num_start:i].replace("_", ""))
        while i < length and clean[i].isspace():
            i += 1
        unit_start = i
        while i < length and clean[i].isalpha():
            i += 1
        unit = clean[unit_start:i].lower()
        if unit not in factors:
            msg = f"Unsupported duration unit {unit!r} in {value!r}"
            raise ValueError(msg)
        total += amount * factors[unit]
        parsed_any = True

    if not parsed_any:
        msg = f"Invalid duration {value!r}"
        raise ValueError(msg)
    return total


def _py_humanize_number(value: int | float, *, precision: int = 1) -> str:
    """Pure-Python fallback for ``humanize_number``."""
    val = float(value)
    if not math.isfinite(val):
        return "0"
    abs_val = abs(val)
    sign = "-" if val < 0.0 else ""
    if abs_val >= 1e12:
        return f"{sign}{abs_val / 1e12:.{precision}f}T"
    if abs_val >= 1e9:
        return f"{sign}{abs_val / 1e9:.{precision}f}B"
    if abs_val >= 1e6:
        return f"{sign}{abs_val / 1e6:.{precision}f}M"
    if abs_val >= 1e3:
        return f"{sign}{abs_val / 1e3:.{precision}f}K"
    if abs(abs_val - round(abs_val)) < 1e-9:
        return f"{sign}{int(round(abs_val))}"
    return f"{sign}{abs_val:.{precision}f}"


def _py_humanize_rate(
    count: int | float,
    seconds: float = 1.0,
    *,
    unit: str = "req",
    precision: int = 1,
) -> str:
    """Pure-Python fallback for ``humanize_rate``."""
    c_val = float(count)
    s_val = float(seconds)
    rate = (c_val / s_val) if (s_val > 0.0 and math.isfinite(c_val) and math.isfinite(s_val)) else 0.0
    return f"{_py_humanize_number(rate, precision=precision)} {unit}/s"


_humanize_impl = _get_native_fn("humanize", _py_humanize)
_titleize_impl = _get_native_fn("titleize", _py_titleize)
_pluralize_impl = _get_native_fn("pluralize", _py_pluralize)
_singularize_impl = _get_native_fn("singularize", _py_singularize)
_ordinalize_impl = _get_native_fn("ordinalize", _py_ordinalize)
_humanize_bytes_impl = _get_native_fn("humanize_bytes", _py_humanize_bytes)
_parse_bytes_impl = _get_native_fn("parse_bytes", _py_parse_bytes)
_humanize_duration_impl = _get_native_fn("humanize_duration", _py_humanize_duration)
_parse_duration_impl = _get_native_fn("parse_duration", _py_parse_duration)
_humanize_number_impl = _get_native_fn("humanize_number", _py_humanize_number)
_humanize_rate_impl = _get_native_fn("humanize_rate", _py_humanize_rate)
