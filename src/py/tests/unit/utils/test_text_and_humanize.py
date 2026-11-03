"""Unit tests for vertebra.utils.text and vertebra.utils.humanize (Rust + Python fallback parity)."""

from __future__ import annotations

import pytest

from vertebra.utils import humanize as humanize_mod
from vertebra.utils import text as text_mod
from vertebra.utils.humanize import (
    humanize,
    humanize_bytes,
    humanize_duration,
    humanize_number,
    humanize_rate,
    ordinalize,
    parse_bytes,
    parse_duration,
    pluralize,
    singularize,
    titleize,
)
from vertebra.utils.text import (
    camelize,
    kebabize,
    normalize_identifier,
    pascalize,
    quote_backtick_identifier,
    quote_bracket_identifier,
    quote_dialect_identifier,
    quote_identifier,
    slugify,
    snake_case,
    split_qualified_identifier,
    truncate,
    truncate_utf8_bytes,
    validate_identifier,
)


@pytest.fixture(params=["native", "python"], autouse=True)
def execution_backend(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Parameterize all tests in this module across compiled Rust and pure-Python fallbacks."""
    mode = str(request.param)
    if mode == "python":
        monkeypatch.setattr(text_mod, "_snake_case_impl", text_mod._py_snake_case)
        monkeypatch.setattr(text_mod, "_camelize_impl", text_mod._py_camelize)
        monkeypatch.setattr(text_mod, "_pascalize_impl", text_mod._py_pascalize)
        monkeypatch.setattr(text_mod, "_kebabize_impl", text_mod._py_kebabize)
        monkeypatch.setattr(text_mod, "_slugify_impl", text_mod._py_slugify)
        monkeypatch.setattr(text_mod, "_quote_identifier_impl", text_mod._py_quote_identifier)
        monkeypatch.setattr(text_mod, "_quote_backtick_identifier_impl", text_mod._py_quote_backtick_identifier)
        monkeypatch.setattr(text_mod, "_quote_bracket_identifier_impl", text_mod._py_quote_bracket_identifier)
        monkeypatch.setattr(text_mod, "_quote_dialect_identifier_impl", text_mod._py_quote_dialect_identifier)
        monkeypatch.setattr(text_mod, "_split_qualified_identifier_impl", text_mod._py_split_qualified_identifier)
        monkeypatch.setattr(text_mod, "_normalize_identifier_impl", text_mod._py_normalize_identifier)
        monkeypatch.setattr(text_mod, "_validate_identifier_impl", text_mod._py_validate_identifier)
        monkeypatch.setattr(text_mod, "_truncate_utf8_bytes_impl", text_mod._py_truncate_utf8_bytes)
        monkeypatch.setattr(text_mod, "_truncate_impl", text_mod._py_truncate)

        monkeypatch.setattr(humanize_mod, "_humanize_impl", humanize_mod._py_humanize)
        monkeypatch.setattr(humanize_mod, "_titleize_impl", humanize_mod._py_titleize)
        monkeypatch.setattr(humanize_mod, "_pluralize_impl", humanize_mod._py_pluralize)
        monkeypatch.setattr(humanize_mod, "_singularize_impl", humanize_mod._py_singularize)
        monkeypatch.setattr(humanize_mod, "_ordinalize_impl", humanize_mod._py_ordinalize)
        monkeypatch.setattr(humanize_mod, "_humanize_bytes_impl", humanize_mod._py_humanize_bytes)
        monkeypatch.setattr(humanize_mod, "_parse_bytes_impl", humanize_mod._py_parse_bytes)
        monkeypatch.setattr(humanize_mod, "_humanize_duration_impl", humanize_mod._py_humanize_duration)
        monkeypatch.setattr(humanize_mod, "_parse_duration_impl", humanize_mod._py_parse_duration)
        monkeypatch.setattr(humanize_mod, "_humanize_number_impl", humanize_mod._py_humanize_number)
        monkeypatch.setattr(humanize_mod, "_humanize_rate_impl", humanize_mod._py_humanize_rate)
    return mode


@pytest.mark.parametrize(
    ("raw", "exp_snake", "exp_camel", "exp_pascal", "exp_kebab"),
    [
        ("HTTPResponseCode", "http_response_code", "httpResponseCode", "HttpResponseCode", "http-response-code"),
        ("getUserById", "get_user_by_id", "getUserById", "GetUserById", "get-user-by-id"),
        ("user_profile_id", "user_profile_id", "userProfileId", "UserProfileId", "user-profile-id"),
        ("user-profile-id", "user_profile_id", "userProfileId", "UserProfileId", "user-profile-id"),
        ("  some  spaced   words  ", "some_spaced_words", "someSpacedWords", "SomeSpacedWords", "some-spaced-words"),
        ("", "", "", "", ""),
    ],
)
def test_case_conversions_native_and_python_parity(
    raw: str,
    exp_snake: str,
    exp_camel: str,
    exp_pascal: str,
    exp_kebab: str,
) -> None:
    """Verify snake_case, camelize, pascalize, and kebabize match across Rust and Python."""
    assert snake_case(raw) == exp_snake
    assert camelize(raw) == exp_camel
    assert pascalize(raw) == exp_pascal
    assert kebabize(raw) == exp_kebab


@pytest.mark.parametrize(
    ("raw", "separator", "expected"),
    [
        ("Café Résumé 2026!", "-", "cafe-resume-2026"),
        ("  /api/v1/users_list/ ", "_", "api_v1_users_list"),
        ("Über Straße", "-", "uber-strasse"),
    ],
)
def test_slugify_native_and_python_parity(
    raw: str,
    separator: str,
    expected: str,
) -> None:
    """Verify slugify folds Latin diacritics and normalizes separators identically in Rust and Python."""
    assert slugify(raw, separator=separator) == expected


@pytest.mark.parametrize(
    ("raw", "expected_double", "expected_backtick", "expected_bracket"),
    [
        ("users", '"users"', "`users`", "[users]"),
        ('user"name', '"user""name"', '`user"name`', '[user"name]'),
        ("my`table", '"my`table"', "`my``table`", "[my`table]"),
        ("my]table", '"my]table"', "`my]table`", "[my]]table]"),
        ('"already_quoted"', '"already_quoted"', '`"already_quoted"`', '["already_quoted"]'),
        ("`already`", '"`already`"', "`already`", "[`already`]"),
        ("[already]", '"[already]"', "`[already]`", "[already]"),
    ],
)
def test_identifier_quoting_parity(
    raw: str,
    expected_double: str,
    expected_backtick: str,
    expected_bracket: str,
) -> None:
    """Verify double-quote, backtick, and bracket identifier quoting across Rust and Python."""
    assert quote_identifier(raw) == expected_double
    assert quote_backtick_identifier(raw) == expected_backtick
    assert quote_bracket_identifier(raw) == expected_bracket


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('"my.schema"."my""table".col', ["my.schema", 'my"table', "col"]),
        ("`proj.id`.`data``set`.`tbl`", ["proj.id", "data`set", "tbl"]),
        ("[dbo].[user]]table]", ["dbo", "user]table"]),
        ("   ", []),
    ],
)
def test_split_qualified_identifier_parity(
    raw: str,
    expected: list[str],
) -> None:
    """Verify split_qualified_identifier preserves quoted segments across Rust and Python."""
    assert split_qualified_identifier(raw) == expected


@pytest.mark.parametrize(
    ("identifier", "dialect", "expected"),
    [
        ("proj.dataset.tbl", "bigquery", "`proj`.`dataset`.`tbl`"),
        ("proj.dataset.tbl", "spanner", "`proj`.`dataset`.`tbl`"),
        ("dbo.users", "mssql", "[dbo].[users]"),
        ("public.users", "postgres", '"public"."users"'),
    ],
)
def test_quote_dialect_identifier_parity(
    identifier: str,
    dialect: str,
    expected: str,
) -> None:
    """Verify dialect-aware identifier quoting across Rust and Python."""
    assert quote_dialect_identifier(identifier, dialect) == expected


@pytest.mark.parametrize(
    ("identifier", "dialect", "expected"),
    [
        ("schema.users", None, "schema.users"),
        ("schema.users", "postgres", '"schema"."users"'),
        ("dataset.events", "bigquery", "`dataset`.`events`"),
        ("dbo.events", "tsql", "[dbo].[events]"),
    ],
)
def test_normalize_identifier_parity(
    identifier: str,
    dialect: str | None,
    expected: str,
) -> None:
    """Verify normalize_identifier validates and quotes qualified identifiers across Rust and Python."""
    assert normalize_identifier(identifier, dialect=dialect) == expected


@pytest.mark.parametrize(
    ("invalid_identifier", "match_pattern"),
    [
        ("   ", "Identifier cannot be empty"),
        ("users; DROP TABLE", "Invalid identifier"),
    ],
)
def test_normalize_identifier_rejects_invalid(
    invalid_identifier: str,
    match_pattern: str,
) -> None:
    """Verify normalize_identifier raises ValueError on empty or unsafe identifiers."""
    with pytest.raises(ValueError, match=match_pattern):
        normalize_identifier(invalid_identifier)


@pytest.mark.parametrize(
    ("identifier", "field_name", "allow_dots", "allow_hyphens"),
    [
        ("public.users_v1", "base_table", True, False),
        ("my-component_1", "component", False, True),
    ],
)
def test_validate_identifier_valid_parity(
    identifier: str,
    field_name: str,
    allow_dots: bool,
    allow_hyphens: bool,
) -> None:
    """Verify validate_identifier accepts valid identifiers across Rust and Python."""
    assert validate_identifier(identifier, field_name, allow_dots=allow_dots, allow_hyphens=allow_hyphens) == identifier


@pytest.mark.parametrize(
    ("identifier", "field_name", "allow_dots", "match_pattern"),
    [
        ("public.users_v1", "base_table", False, "Invalid SQL identifier for base_table"),
        ("users; DROP TABLE", "table", True, "Invalid SQL identifier for table"),
    ],
)
def test_validate_identifier_rejects_invalid(
    identifier: str,
    field_name: str,
    allow_dots: bool,
    match_pattern: str,
) -> None:
    """Verify validate_identifier raises ValueError on disallowed characters."""
    with pytest.raises(ValueError, match=match_pattern):
        validate_identifier(identifier, field_name, allow_dots=allow_dots)


@pytest.mark.parametrize(
    ("max_bytes", "expected"),
    [
        (100, "client_🔥_id"),
        (7, "client_"),
        (8, "client_"),
        (10, "client_"),
        (11, "client_🔥"),
    ],
)
def test_truncate_utf8_bytes_parity(
    max_bytes: int,
    expected: str,
) -> None:
    """Verify truncate_utf8_bytes never splits multi-byte UTF-8 codepoints."""
    assert truncate_utf8_bytes("client_🔥_id", max_bytes) == expected


@pytest.mark.parametrize(
    ("value", "max_chars", "suffix", "expected"),
    [
        ("hello world", 8, "...", "hello..."),
        ("hello", 8, "...", "hello"),
        ("hello world", 6, "…", "hello…"),
        ("hello", 0, "...", ""),
    ],
)
def test_truncate_chars_parity(
    value: str,
    max_chars: int,
    suffix: str,
    expected: str,
) -> None:
    """Verify character truncation and suffix placement across Rust and Python."""
    assert truncate(value, max_chars, suffix=suffix) == expected


@pytest.mark.parametrize(
    ("value", "capitalize", "expected"),
    [
        ("user_profile_id", True, "User profile"),
        ("user_role_ids", True, "User role"),
        ("getUserById", False, "get user by"),
        ("mcp-tool-name", True, "Mcp tool name"),
    ],
)
def test_humanize_parity(
    value: str,
    capitalize: bool,
    expected: str,
) -> None:
    """Verify humanize strips trailing _id and formats identifiers across Rust and Python."""
    assert humanize(value, capitalize=capitalize) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("get_user_by_id", "Get User By Id"),
        ("httpResponseCode", "Http Response Code"),
    ],
)
def test_titleize_parity(value: str, expected: str) -> None:
    """Verify titleize capitalizes each identifier word across Rust and Python."""
    assert titleize(value) == expected


@pytest.mark.parametrize(
    ("word", "count", "include_count", "expected"),
    [
        ("worker", 1, True, "1 worker"),
        ("worker", 4, True, "4 workers"),
        ("Category", None, False, "Categories"),
        ("status", None, False, "statuses"),
        ("person", None, False, "people"),
        ("matrix", None, False, "matrices"),
        ("analysis", None, False, "analyses"),
        ("metadata", None, False, "metadata"),
    ],
)
def test_pluralize_parity(
    word: str,
    count: int | None,
    include_count: bool,
    expected: str,
) -> None:
    """Verify pluralize inflects English nouns across Rust and Python."""
    assert pluralize(word, count, include_count=include_count) == expected


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        ("Categories", "Category"),
        ("statuses", "status"),
        ("people", "person"),
        ("matrices", "matrix"),
        ("analyses", "analysis"),
        ("metadata", "metadata"),
    ],
)
def test_singularize_parity(word: str, expected: str) -> None:
    """Verify singularize inflects plural nouns back to singular across Rust and Python."""
    assert singularize(word) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, "1st"),
        (2, "2nd"),
        (3, "3rd"),
        (4, "4th"),
        (11, "11th"),
        (12, "12th"),
        (13, "13th"),
        (21, "21st"),
        (102, "102nd"),
    ],
)
def test_ordinalize_parity(value: int, expected: str) -> None:
    """Verify ordinalize formats integers as English ordinals across Rust and Python."""
    assert ordinalize(value) == expected


@pytest.mark.parametrize(
    ("num_bytes", "binary", "expected"),
    [
        (0, True, "0 B"),
        (512, True, "512 B"),
        (1024, True, "1.0 KiB"),
        (67_108_864, True, "64.0 MiB"),
        (1_000_000, False, "1.0 MB"),
    ],
)
def test_humanize_bytes_parity(
    num_bytes: int,
    binary: bool,
    expected: str,
) -> None:
    """Verify humanize_bytes formats IEC and SI units across Rust and Python."""
    assert humanize_bytes(num_bytes, binary=binary) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (4096, 4096),
        ("64MiB", 67_108_864),
        ("1 MB", 1_000_000),
        ("512K", 524_288),
    ],
)
def test_parse_bytes_parity(
    raw: str | int,
    expected: int,
) -> None:
    """Verify parse_bytes parses byte size strings and integers across Rust and Python."""
    assert parse_bytes(raw) == expected


@pytest.mark.parametrize("invalid_raw", ["", "100XB"])
def test_parse_bytes_rejects_invalid(invalid_raw: str) -> None:
    """Verify parse_bytes raises ValueError on malformed byte strings."""
    with pytest.raises(ValueError):
        parse_bytes(invalid_raw)


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0.0, "0 ms"),
        (0.000_044_4, "44.4 µs"),
        (0.0125, "12.5 ms"),
        (1.5, "1.5 s"),
        (65.0, "1m 5s"),
        (3661.0, "1h 1m 1s"),
    ],
)
def test_humanize_duration_parity(
    seconds: float,
    expected: str,
) -> None:
    """Verify humanize_duration formats sub-millisecond through multi-hour durations."""
    assert humanize_duration(seconds) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (2.5, 2.5),
        ("500ms", 0.5),
        ("+500ms", 0.5),
        ("1m 30s", 90.0),
        ("100us", 1e-4),
    ],
)
def test_parse_duration_parity(
    raw: str | float,
    expected: float,
) -> None:
    """Verify parse_duration parses single and compound durations across Rust and Python."""
    assert parse_duration(raw) == pytest.approx(expected)


@pytest.mark.parametrize("invalid_raw", ["", "10lightyears"])
def test_parse_duration_rejects_invalid(invalid_raw: str) -> None:
    """Verify parse_duration raises ValueError on invalid units."""
    with pytest.raises(ValueError):
        parse_duration(invalid_raw)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (42, "42"),
        (123_790, "123.8K"),
        (2_500_000, "2.5M"),
    ],
)
def test_humanize_number_parity(
    value: int,
    expected: str,
) -> None:
    """Verify humanize_number formats compact magnitude suffixes across Rust and Python."""
    assert humanize_number(value) == expected


@pytest.mark.parametrize(
    ("count", "seconds", "unit", "expected"),
    [
        (123_790, 1.0, "req", "123.8K req/s"),
        (250_000, 2.0, "msg", "125.0K msg/s"),
    ],
)
def test_humanize_rate_parity(
    count: int,
    seconds: float,
    unit: str,
    expected: str,
) -> None:
    """Verify humanize_rate computes and formats throughput rates across Rust and Python."""
    assert humanize_rate(count, seconds, unit=unit) == expected
