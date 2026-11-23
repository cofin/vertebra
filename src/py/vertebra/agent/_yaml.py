"""Pure-Python zero-dependency YAML frontmatter subset parser and emitter for OKF v0.2."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any, Final

from vertebra.typing import VertebraError
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "OkfValidationError",
    "_dump_yaml_frontmatter",
    "_parse_yaml_frontmatter",
)


class OkfValidationError(VertebraError):
    """Raised when an OKF v0.2 document, bundle, or archive fails validation."""


_INT_SCALAR_RE: Final[re.Pattern[str]] = re.compile(r"^-?(?:0|[1-9][0-9]*)$")
_FLOAT_SCALAR_RE: Final[re.Pattern[str]] = re.compile(
    r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)$"
)
_NUMERIC_OR_HEX_STR_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|[0-9][0-9a-fA-F]*|0[xX][0-9a-fA-F]+)$"
)
_YAML_FOREIGN_TYPED_STR_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"[+-]?[0-9][0-9_]*(?:\.[0-9_]*)?(?:[eE][+-]?[0-9]+)?"
    r"|[+-]?\.[0-9_]+(?:[eE][+-]?[0-9]+)?"
    r"|[+-]?0[oObBxX][0-9a-fA-F_]+"
    r"|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}(?:[Tt \t].*)?"
    r"|=|<<"
    r")$"
)
_BLOCK_SCALAR_HEADER_RE: Final[re.Pattern[str]] = re.compile(r"^([|>])(?:([+-])([0-9])?|([0-9])([+-])?)?$")
_YAML_RESERVED_WORDS: Final[frozenset[str]] = frozenset(
    {
        "true",
        "false",
        "null",
        "~",
        "yes",
        "no",
        "on",
        "off",
        "y",
        "n",
        "inf",
        "+inf",
        "-inf",
        "nan",
        ".inf",
        "+.inf",
        "-.inf",
        ".nan",
    }
)
_YAML_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({":", "#", '"', "'", "\\", "{", "}", "[", "]"})
_YAML_LEADING_CHARS: Final[frozenset[str]] = frozenset(
    {"-", "?", ":", ",", "[", "]", "{", "}", "&", "*", "!", "|", ">", "'", '"', "%", "@", "`"}
)


def _parse_yaml_frontmatter(yaml_lines: list[str], *, rel_path: str) -> dict[str, Any]:
    tokens = _tokenize_yaml_lines(yaml_lines, rel_path=rel_path)
    if not tokens:
        return {}
    first_indent, first_text, first_line_no = tokens[0]
    if first_indent != 0:
        raise OkfValidationError(f"Unexpected indentation on frontmatter line {first_line_no} in '{rel_path}'")
    if _is_sequence_line(first_text):
        raise OkfValidationError(f"Frontmatter root must be a mapping in '{rel_path}'")
    mapping, next_idx = _parse_yaml_mapping(tokens, 0, 0, rel_path=rel_path)
    if next_idx < len(tokens):
        _, leftover_text, leftover_line_no = tokens[next_idx]
        raise OkfValidationError(
            f"Unexpected trailing frontmatter line {leftover_line_no} ({leftover_text!r}) in '{rel_path}'"
        )
    return mapping


def _tokenize_yaml_lines(
    yaml_lines: list[str],
    *,
    rel_path: str,
) -> list[tuple[int, str, int]]:
    """Tokenize frontmatter lines into `(indent, text, line_no)`, inlining block scalars as quoted strings."""
    tokens: list[tuple[int, str, int]] = []
    idx = 0
    while idx < len(yaml_lines):
        raw_line = yaml_lines[idx]
        line_no = idx + 2
        idx += 1
        if "\t" in raw_line[: len(raw_line) - len(raw_line.lstrip(" \t"))]:
            raise OkfValidationError(f"Tabs are forbidden in YAML indentation on line {line_no} in '{rel_path}'")
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        header = _match_block_scalar_header(stripped, line_no=line_no, rel_path=rel_path)
        if header is not None:
            prefix, parent_offset, style, chomp, explicit_indent = header
            block_text, idx = _read_block_scalar(
                yaml_lines,
                idx,
                parent_indent=indent + parent_offset,
                style=style,
                chomp=chomp,
                explicit_indent=explicit_indent,
                rel_path=rel_path,
            )
            stripped = f"{prefix}{encode_json(block_text)}"
        tokens.append((indent, stripped, line_no))
    return tokens


def _match_block_scalar_header(
    text: str,
    *,
    line_no: int,
    rel_path: str,
) -> tuple[str, int, str, str, int] | None:
    """Detect a `|` or `>` block scalar header at the end of a `key:` or `- ` line.

    Returns:
        `(prefix, parent_offset, style, chomp, explicit_indent)` where `prefix` is the line text up
        to the value, `parent_offset` is the column of the owning key or dash relative to the line
        indentation, `chomp` is `""`, `"-"`, or `"+"`, and `explicit_indent` is `0` when the content
        indentation is auto-detected. `None` when the line does not open a block scalar.
    """
    rest = text
    offset = 0
    parent_offset: int | None = None
    while rest != "-" and _is_sequence_line(rest):
        after_dash = rest[2:].lstrip(" ")
        parent_offset = offset
        offset += len(rest) - len(after_dash)
        rest = after_dash

    colon_pos = _find_mapping_colon(rest)
    if colon_pos is not None:
        value_text = rest[colon_pos + 1 :]
        prefix = f"{text[: offset + colon_pos + 1]} "
        parent_offset = offset
    elif parent_offset is not None:
        value_text = rest
        prefix = text[:offset]
    else:
        return None

    matched = _BLOCK_SCALAR_HEADER_RE.match(_strip_inline_yaml_comment(value_text.strip()))
    if matched is None:
        return None
    chomp = matched.group(2) or matched.group(5) or ""
    indent_digit = matched.group(3) or matched.group(4)
    if indent_digit == "0":
        raise OkfValidationError(f"Block scalar indentation indicator must be 1-9 on line {line_no} in '{rel_path}'")
    return prefix, parent_offset, matched.group(1), chomp, int(indent_digit) if indent_digit else 0


def _read_block_scalar(
    yaml_lines: list[str],
    idx: int,
    *,
    parent_indent: int,
    style: str,
    chomp: str,
    explicit_indent: int,
    rel_path: str,
) -> tuple[str, int]:
    """Read literal (`|`) or folded (`>`) block scalar content starting at `yaml_lines[idx]`.

    Content lines are those indented deeper than `parent_indent`; whitespace-only lines inside the
    block count as empty lines. Chomping follows YAML: clip keeps one trailing newline, `-` strips
    all, and `+` keeps every trailing empty line.
    """
    content_indent = parent_indent + explicit_indent if explicit_indent else 0
    collected: list[str] = []
    while idx < len(yaml_lines):
        raw_line = yaml_lines[idx]
        if not raw_line.strip():
            collected.append("")
            idx += 1
            continue
        lead = len(raw_line) - len(raw_line.lstrip(" "))
        if lead <= parent_indent:
            break
        if not content_indent:
            content_indent = lead
        if lead < content_indent:
            raise OkfValidationError(f"Under-indented block scalar line {idx + 2} in '{rel_path}'")
        collected.append(raw_line[content_indent:])
        idx += 1

    trailing_empty = 0
    while trailing_empty < len(collected) and not collected[len(collected) - 1 - trailing_empty]:
        trailing_empty += 1
    body = collected[: len(collected) - trailing_empty]
    if not body:
        return ("\n" * trailing_empty if chomp == "+" else ""), idx
    text = "\n".join(body) if style == "|" else _fold_block_lines(body)
    if chomp == "-":
        return text, idx
    if chomp == "+":
        return text + "\n" * (trailing_empty + 1), idx
    return f"{text}\n", idx


def _fold_block_lines(lines: list[str]) -> str:
    """Fold `>` block scalar lines: adjacent text lines join with a space and empty lines become newlines.

    Line breaks next to a more-indented line (one starting with a space or tab) are preserved.
    """
    parts: list[str] = []
    previous: str | None = None
    empty_run = 0
    for line in lines:
        if not line:
            empty_run += 1
            continue
        if previous is None:
            parts.append("\n" * empty_run)
        elif previous[0] in " \t" or line[0] in " \t":
            parts.append("\n" * (empty_run + 1))
        elif empty_run == 0:
            parts.append(" ")
        else:
            parts.append("\n" * empty_run)
        parts.append(line)
        previous = line
        empty_run = 0
    return "".join(parts)


def _is_sequence_line(stripped: str) -> bool:
    return stripped == "-" or stripped.startswith("- ")


def _parse_yaml_mapping(
    tokens: list[tuple[int, str, int]],
    idx: int,
    base_indent: int,
    *,
    rel_path: str,
) -> tuple[dict[str, Any], int]:
    mapping: dict[str, Any] = {}
    while idx < len(tokens):
        indent, text, line_no = tokens[idx]
        if indent < base_indent:
            break
        if indent > base_indent:
            raise OkfValidationError(f"Unexpected indentation on line {line_no} in '{rel_path}'")
        if _is_sequence_line(text):
            break

        key, rest = _split_yaml_key_value(text, line_no=line_no, rel_path=rel_path)
        if key in mapping:
            raise OkfValidationError(f"Duplicate frontmatter key {key!r} on line {line_no} in '{rel_path}'")

        idx += 1
        if rest:
            mapping[key] = _parse_yaml_inline_value(rest, line_no=line_no, rel_path=rel_path)
            continue

        value, idx = _parse_yaml_nested_block(tokens, idx, base_indent, rel_path=rel_path)
        mapping[key] = value
    return mapping, idx


def _parse_yaml_nested_block(
    tokens: list[tuple[int, str, int]],
    idx: int,
    parent_indent: int,
    *,
    rel_path: str,
) -> tuple[Any, int]:
    if idx >= len(tokens):
        return None, idx
    next_indent, next_text, _ = tokens[idx]
    if next_indent < parent_indent:
        return None, idx
    if next_indent == parent_indent:
        if _is_sequence_line(next_text):
            return _parse_yaml_sequence(tokens, idx, next_indent, rel_path=rel_path)
        return None, idx
    if _is_sequence_line(next_text):
        return _parse_yaml_sequence(tokens, idx, next_indent, rel_path=rel_path)
    return _parse_yaml_mapping(tokens, idx, next_indent, rel_path=rel_path)


def _parse_yaml_sequence(
    tokens: list[tuple[int, str, int]],
    idx: int,
    seq_indent: int,
    *,
    rel_path: str,
) -> tuple[list[Any], int]:
    items: list[Any] = []
    while idx < len(tokens):
        indent, text, line_no = tokens[idx]
        if indent < seq_indent:
            break
        if indent > seq_indent:
            raise OkfValidationError(f"Unexpected sequence indentation on line {line_no} in '{rel_path}'")
        if not _is_sequence_line(text):
            break

        after_dash = "" if text == "-" else text[2:].lstrip(" ")
        if _is_sequence_line(after_dash):
            nested_indent = seq_indent + len(text) - len(after_dash)
            tokens[idx] = (nested_indent, after_dash, line_no)
            inline_nested, idx = _parse_yaml_sequence(tokens, idx, nested_indent, rel_path=rel_path)
            items.append(inline_nested)
            continue
        item_indent = seq_indent + 2
        idx += 1

        if not after_dash:
            if idx < len(tokens) and tokens[idx][0] > seq_indent:
                child_indent, child_text, _ = tokens[idx]
                if _is_sequence_line(child_text):
                    nested_list, idx = _parse_yaml_sequence(tokens, idx, child_indent, rel_path=rel_path)
                    items.append(nested_list)
                else:
                    nested_map, idx = _parse_yaml_mapping(tokens, idx, child_indent, rel_path=rel_path)
                    items.append(nested_map)
            else:
                items.append(None)
            continue

        if _has_mapping_colon(after_dash):
            item_key, item_rest = _split_yaml_key_value(after_dash, line_no=line_no, rel_path=rel_path)
            item_map: dict[str, Any] = {}
            if item_rest:
                item_map[item_key] = _parse_yaml_inline_value(item_rest, line_no=line_no, rel_path=rel_path)
            else:
                nested_val, idx = _parse_yaml_nested_block(tokens, idx, item_indent, rel_path=rel_path)
                item_map[item_key] = nested_val

            if idx < len(tokens) and tokens[idx][0] == item_indent and not _is_sequence_line(tokens[idx][1]):
                rest_map, idx = _parse_yaml_mapping(tokens, idx, item_indent, rel_path=rel_path)
                for extra_k, extra_v in rest_map.items():
                    if extra_k in item_map:
                        raise OkfValidationError(
                            f"Duplicate frontmatter key {extra_k!r} in sequence mapping in '{rel_path}'"
                        )
                    item_map[extra_k] = extra_v
            items.append(item_map)
            continue

        items.append(_parse_yaml_inline_value(after_dash, line_no=line_no, rel_path=rel_path))

    return items, idx


def _find_mapping_colon(text: str) -> int | None:
    """Return the index of the first unquoted `:` followed by whitespace or end of text, or `None`."""
    quote_char: str | None = None
    escaped = False
    for i, ch in enumerate(text):
        if quote_char is not None:
            if quote_char == '"' and not escaped and ch == "\\":
                escaped = True
                continue
            if not escaped and ch == quote_char:
                quote_char = None
            escaped = False
            continue
        if ch in ('"', "'"):
            quote_char = ch
            continue
        if ch == ":" and (i + 1 == len(text) or text[i + 1] in (" ", "\t")):
            return i
    return None


def _has_mapping_colon(text: str) -> bool:
    return _find_mapping_colon(text) is not None


def _split_yaml_key_value(text: str, *, line_no: int, rel_path: str) -> tuple[str, str]:
    split_pos = _find_mapping_colon(text)
    if split_pos is None:
        raise OkfValidationError(f"Expected 'key: value' on frontmatter line {line_no} in '{rel_path}'")

    raw_key = text[:split_pos].strip()
    raw_val = text[split_pos + 1 :].strip()
    if not raw_key:
        raise OkfValidationError(f"Empty key on frontmatter line {line_no} in '{rel_path}'")
    key = _parse_yaml_key(raw_key, line_no=line_no, rel_path=rel_path)
    return key, _strip_inline_yaml_comment(raw_val)


def _parse_yaml_key(raw_key: str, *, line_no: int, rel_path: str) -> str:
    if raw_key[0] in ('"', "'"):
        parsed = _parse_quoted_scalar(raw_key, line_no=line_no, rel_path=rel_path)
        return parsed
    return raw_key


def _strip_inline_yaml_comment(raw_val: str) -> str:
    quote_char: str | None = None
    escaped = False
    bracket_depth = 0
    for i, ch in enumerate(raw_val):
        if quote_char is not None:
            if quote_char == '"' and not escaped and ch == "\\":
                escaped = True
                continue
            if not escaped and ch == quote_char:
                quote_char = None
            escaped = False
            continue
        if ch in ('"', "'"):
            quote_char = ch
            continue
        if ch in ("[", "{"):
            bracket_depth += 1
            continue
        if ch in ("]", "}") and bracket_depth > 0:
            bracket_depth -= 1
            continue
        if ch == "#" and bracket_depth == 0 and (i == 0 or raw_val[i - 1] in (" ", "\t")):
            return raw_val[:i].rstrip()
    return raw_val


def _parse_yaml_inline_value(raw_val: str, *, line_no: int, rel_path: str) -> Any:
    if not raw_val:
        return None
    if raw_val == "{}":
        return {}
    if raw_val.startswith("[") and raw_val.endswith("]"):
        inner = raw_val[1:-1].strip()
        if not inner:
            return []
        return [
            _parse_yaml_inline_value(item, line_no=line_no, rel_path=rel_path)
            for item in _split_flow_items(inner, line_no=line_no, rel_path=rel_path)
        ]
    if raw_val.startswith("{") and raw_val.endswith("}"):
        inner = raw_val[1:-1].strip()
        if not inner:
            return {}
        result: dict[str, Any] = {}
        for entry in _split_flow_items(inner, line_no=line_no, rel_path=rel_path):
            k, v = _split_yaml_key_value(entry, line_no=line_no, rel_path=rel_path)
            result[k] = _parse_yaml_inline_value(v, line_no=line_no, rel_path=rel_path)
        return result
    if (raw_val.startswith("[") and not raw_val.endswith("]")) or (
        raw_val.startswith("{") and not raw_val.endswith("}")
    ):
        raise OkfValidationError(f"Unclosed YAML flow collection on line {line_no} in '{rel_path}'")
    return _parse_yaml_scalar(raw_val, line_no=line_no, rel_path=rel_path)


def _split_flow_items(inner: str, *, line_no: int, rel_path: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    quote_char: str | None = None
    escaped = False
    depth = 0
    for ch in inner:
        if quote_char is not None:
            current.append(ch)
            if quote_char == '"' and not escaped and ch == "\\":
                escaped = True
                continue
            if not escaped and ch == quote_char:
                quote_char = None
            escaped = False
            continue
        if ch in ('"', "'"):
            quote_char = ch
            current.append(ch)
            continue
        if ch in ("[", "{"):
            depth += 1
            current.append(ch)
            continue
        if ch in ("]", "}"):
            depth -= 1
            if depth < 0:
                raise OkfValidationError(f"Unbalanced YAML flow bracket on line {line_no} in '{rel_path}'")
            current.append(ch)
            continue
        if ch == "," and depth == 0:
            item = "".join(current).strip()
            if not item:
                raise OkfValidationError(f"Empty YAML flow item on line {line_no} in '{rel_path}'")
            items.append(item)
            current.clear()
            continue
        current.append(ch)

    if quote_char is not None or depth != 0:
        raise OkfValidationError(f"Unterminated YAML flow collection on line {line_no} in '{rel_path}'")
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def _parse_yaml_scalar(raw_val: str, *, line_no: int, rel_path: str) -> Any:
    if raw_val[0] in ('"', "'"):
        return _parse_quoted_scalar(raw_val, line_no=line_no, rel_path=rel_path)

    lower = raw_val.lower()
    if lower in ("true",):
        return True
    if lower in ("false",):
        return False
    if lower in ("null", "~"):
        return None
    if _INT_SCALAR_RE.match(raw_val):
        return int(raw_val)
    if _FLOAT_SCALAR_RE.match(raw_val):
        parsed_float = float(raw_val)
        if math.isfinite(parsed_float):
            return parsed_float
    return raw_val


def _parse_quoted_scalar(raw_val: str, *, line_no: int, rel_path: str) -> str:
    quote_char = raw_val[0]
    if len(raw_val) < 2 or raw_val[-1] != quote_char:
        raise OkfValidationError(f"Unterminated quoted string on line {line_no} in '{rel_path}'")
    if quote_char == '"':
        try:
            decoded = decode_json(raw_val)
        except Exception as exc:
            raise OkfValidationError(
                f"Malformed double-quoted string on line {line_no} in '{rel_path}': {exc}"
            ) from exc
        if not isinstance(decoded, str):
            raise OkfValidationError(f"Expected string scalar on line {line_no} in '{rel_path}'")
        return decoded

    inner = raw_val[1:-1]
    idx = 0
    chars: list[str] = []
    while idx < len(inner):
        if inner[idx] == "'":
            if idx + 1 < len(inner) and inner[idx + 1] == "'":
                chars.append("'")
                idx += 2
                continue
            raise OkfValidationError(f"Unescaped single quote on line {line_no} in '{rel_path}'")
        chars.append(inner[idx])
        idx += 1
    return "".join(chars)


def _dump_yaml_frontmatter(frontmatter: Mapping[str, Any]) -> str:
    lines: list[str] = []
    _emit_yaml_mapping(frontmatter, indent=0, out=lines)
    return "\n".join(lines)


def _emit_yaml_mapping(mapping: Mapping[str, Any], *, indent: int, out: list[str]) -> None:
    pad = " " * indent
    for key, value in mapping.items():
        if not isinstance(key, str) or not key:
            raise OkfValidationError(f"Frontmatter keys must be non-empty strings, got {key!r}")
        rendered_key = _emit_yaml_key(key)
        if isinstance(value, Mapping):
            if not value:
                out.append(f"{pad}{rendered_key}: {{}}")
            else:
                out.append(f"{pad}{rendered_key}:")
                _emit_yaml_mapping(value, indent=indent + 2, out=out)
        elif isinstance(value, (list, tuple)):
            if not value:
                out.append(f"{pad}{rendered_key}: []")
            else:
                out.append(f"{pad}{rendered_key}:")
                _emit_yaml_sequence(value, indent=indent + 2, out=out)
        else:
            out.append(f"{pad}{rendered_key}: {_emit_yaml_scalar(value)}")


def _emit_yaml_sequence(items: Sequence[Any], *, indent: int, out: list[str]) -> None:
    pad = " " * indent
    for item in items:
        if isinstance(item, Mapping):
            if not item:
                out.append(f"{pad}- {{}}")
                continue
            first = True
            for sub_key, sub_val in item.items():
                if not isinstance(sub_key, str) or not sub_key:
                    raise OkfValidationError(f"Frontmatter keys must be non-empty strings, got {sub_key!r}")
                rendered_sub_key = _emit_yaml_key(sub_key)
                prefix = f"{pad}- " if first else f"{pad}  "
                first = False
                if isinstance(sub_val, Mapping):
                    if not sub_val:
                        out.append(f"{prefix}{rendered_sub_key}: {{}}")
                    else:
                        out.append(f"{prefix}{rendered_sub_key}:")
                        _emit_yaml_mapping(sub_val, indent=indent + 4, out=out)
                elif isinstance(sub_val, (list, tuple)):
                    if not sub_val:
                        out.append(f"{prefix}{rendered_sub_key}: []")
                    else:
                        out.append(f"{prefix}{rendered_sub_key}:")
                        _emit_yaml_sequence(sub_val, indent=indent + 4, out=out)
                else:
                    out.append(f"{prefix}{rendered_sub_key}: {_emit_yaml_scalar(sub_val)}")
        elif isinstance(item, (list, tuple)):
            if not item:
                out.append(f"{pad}- []")
            else:
                out.append(f"{pad}-")
                _emit_yaml_sequence(item, indent=indent + 2, out=out)
        else:
            out.append(f"{pad}- {_emit_yaml_scalar(item)}")


def _emit_yaml_key(key: str) -> str:
    if _needs_yaml_quoting(key):
        return str(encode_json(key))
    return key


def _emit_yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise OkfValidationError(f"Non-finite float is forbidden in OKF frontmatter: {value!r}")
        return _emit_yaml_float(value)
    if isinstance(value, str):
        if _needs_yaml_quoting(value):
            return str(encode_json(value))
        return value
    raise OkfValidationError(f"Unsupported frontmatter value type: {type(value).__name__}")


def _emit_yaml_float(value: float) -> str:
    """Render a finite float as its shortest repr, forcing a fractional part before any exponent.

    `1e+20` becomes `1.0e+20` so YAML 1.1 readers, which require a dot in floats, agree with this parser.
    """
    text = repr(value)
    mantissa, exp_marker, exponent = text.partition("e")
    if exp_marker and "." not in mantissa:
        return f"{mantissa}.0e{exponent}"
    return text


def _needs_yaml_quoting(value: str) -> bool:
    """Return whether a string must be double-quoted to stay a string for this parser and standard YAML readers."""
    if not value or value != value.strip():
        return True
    if value[0] in _YAML_LEADING_CHARS:
        return True
    if value.lower() in _YAML_RESERVED_WORDS:
        return True
    if _NUMERIC_OR_HEX_STR_RE.match(value) or _YAML_FOREIGN_TYPED_STR_RE.match(value):
        return True
    return any(ord(ch) < 0x20 or ord(ch) == 0x7F or ch in _YAML_SPECIAL_CHARS for ch in value)
