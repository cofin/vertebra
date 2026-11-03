"""Unit tests for vertebra.utils.dispatch, schema, type_converters, and expanded type_guards."""

from __future__ import annotations

import dataclasses
import io
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, TypedDict
from uuid import UUID

import msgspec
import pytest

from vertebra.typing import UNSET, CamelizedBaseStruct, Empty
from vertebra.utils.dispatch import TypeDispatcher
from vertebra.utils.schema import to_schema, to_value_type, transform_dict_keys
from vertebra.utils.text import camelize, kebabize, snake_case
from vertebra.utils.type_converters import (
    DEFAULT_SPECIAL_CHARS,
    build_decimal_converter,
    build_nested_decimal_normalizer,
    build_uuid_coercions,
    should_json_encode_sequence,
    time_iso_convert,
)
from vertebra.utils.type_guards import (
    extract_dataclass_fields,
    extract_dataclass_items,
    get_msgspec_rename_config,
    is_async_readable,
    is_attrs_schema,
    is_dict_row,
    is_iterable_parameters,
    is_mapping_like,
    is_readable,
    is_schema,
    is_schema_or_dict,
    is_schema_or_dict_sequence,
    is_typed_dict,
    supports_close,
)


class _Animal:
    pass


class _Dog(_Animal):
    pass


class _Bulldog(_Dog):
    pass


class _Unrelated:
    pass


class _SampleStruct(msgspec.Struct, frozen=True, gc=False):
    user_id: int
    user_name: str


class _SampleCamelStruct(CamelizedBaseStruct):
    user_id: int
    user_name: str


@dataclasses.dataclass
class _SampleDataclass:
    user_id: int
    user_name: str
    optional_note: str | None = None
    unset_val: Any = UNSET


class _SampleTypedDict(TypedDict):
    user_id: int
    user_name: str


class _AsyncReader:
    async def read(self, _size: int = -1) -> bytes:
        return b"data"


class _ClosableResource:
    def close(self) -> None:
        return None


@pytest.fixture
def animal_dispatcher() -> TypeDispatcher[str]:
    """Build a populated TypeDispatcher for hierarchy resolution tests."""
    dispatcher = TypeDispatcher[str]()
    dispatcher.register_all(((_Animal, "animal"), (_Dog, "dog")))
    return dispatcher


@pytest.fixture
def sample_dataclass_instance() -> _SampleDataclass:
    """Provide a populated dataclass instance with None and Empty sentinel fields."""
    return _SampleDataclass(user_id=7, user_name="Grace", optional_note=None, unset_val=Empty)


@pytest.fixture
def nested_key_payload() -> dict[str, Any]:
    """Provide a nested dictionary/list/tuple structure for key transformation tests."""
    return {"user_profile": [{"first_name": "Ada"}], "tags_list": ({"first_name": "Grace"},)}


@pytest.fixture(
    params=[
        (_SampleStruct, {"user_id": 1, "user_name": "Ada"}, _SampleStruct),
        (_SampleCamelStruct, {"user_id": 2, "user_name": "Grace"}, _SampleCamelStruct),
        (_SampleDataclass, {"user_id": 3, "user_name": "Linus"}, _SampleDataclass),
        (_SampleTypedDict, {"user_id": 4, "user_name": "Ken"}, dict),
    ],
)
def schema_hydration_case(
    request: pytest.FixtureRequest,
) -> tuple[Any, dict[str, Any], type[Any]]:
    """Parameterize schema hydration targets across msgspec, dataclass, and TypedDict."""
    return request.param  # type: ignore[no-any-return]


@pytest.fixture
def sample_uuid() -> UUID:
    """Provide a deterministic UUID fixture for coercion tests."""
    return UUID("12345678-1234-5678-1234-567812345678")


@pytest.mark.parametrize(
    ("lookup_target", "expected"),
    [
        (_Animal, "animal"),
        (_Animal(), "animal"),
        (_Dog, "dog"),
        (_Dog(), "dog"),
        (_Bulldog, "dog"),
        (_Bulldog(), "dog"),
        (_Unrelated, None),
        (_Unrelated(), None),
    ],
)
def test_type_dispatcher_get_and_mro_resolution(
    animal_dispatcher: TypeDispatcher[str],
    lookup_target: Any,
    expected: str | None,
) -> None:
    """Verify TypeDispatcher resolves exact types, subclasses via MRO, and instances."""
    assert animal_dispatcher.get(lookup_target) == expected
    assert animal_dispatcher.get(lookup_target) == expected


def test_type_dispatcher_cache_invalidation_and_mapping_protocol(
    animal_dispatcher: TypeDispatcher[str],
) -> None:
    """Verify TypeDispatcher invalidates cached MRO lookups upon registration."""
    assert _Bulldog in animal_dispatcher
    assert _Unrelated not in animal_dispatcher
    assert animal_dispatcher[_Bulldog] == "dog"

    animal_dispatcher.register(_Bulldog, "bulldog")
    assert animal_dispatcher[_Bulldog] == "bulldog"

    animal_dispatcher.clear_cache()
    assert animal_dispatcher.get(_Bulldog()) == "bulldog"

    with pytest.raises(KeyError):
        _ = animal_dispatcher[_Unrelated]


@pytest.mark.parametrize(
    ("candidate", "expected_readable", "expected_async_readable", "expected_closable"),
    [
        (io.BytesIO(b"hello"), True, False, True),
        (_AsyncReader(), False, True, False),
        (_ClosableResource(), False, False, True),
        ("not-a-stream", False, False, False),
    ],
)
def test_stream_and_closable_type_guards(
    candidate: Any,
    expected_readable: bool,
    expected_async_readable: bool,
    expected_closable: bool,
) -> None:
    """Verify is_readable, is_async_readable, and supports_close."""
    assert is_readable(candidate) is expected_readable
    assert is_async_readable(candidate) is expected_async_readable
    assert supports_close(candidate) is expected_closable


@pytest.mark.parametrize(
    ("candidate", "expected_typed_dict", "expected_schema", "expected_schema_or_dict"),
    [
        (_SampleTypedDict, True, False, False),
        (_SampleStruct, False, True, False),
        (_SampleStruct(user_id=1, user_name="Ada"), False, True, True),
        (_SampleDataclass, False, True, False),
        (_SampleDataclass(user_id=1, user_name="Ada"), False, True, True),
        ({"user_id": 1}, False, False, True),
        ("plain-string", False, False, False),
    ],
)
def test_schema_and_typed_dict_guards(
    candidate: Any,
    expected_typed_dict: bool,
    expected_schema: bool,
    expected_schema_or_dict: bool,
) -> None:
    """Verify is_typed_dict, is_schema, and is_schema_or_dict across schema families."""
    assert is_typed_dict(candidate) is expected_typed_dict
    assert is_schema(candidate) is expected_schema
    assert is_schema_or_dict(candidate) is expected_schema_or_dict
    assert is_attrs_schema(candidate) is False


@pytest.mark.parametrize(
    ("candidate", "expected_dict_row", "expected_mapping", "expected_iterable_params"),
    [
        ({"a": 1}, True, True, False),
        ([1, 2, 3], False, False, True),
        ((1, 2), False, False, True),
        ("string", False, False, False),
        (b"bytes", False, False, False),
    ],
)
def test_collection_and_parameter_type_guards(
    candidate: Any,
    expected_dict_row: bool,
    expected_mapping: bool,
    expected_iterable_params: bool,
) -> None:
    """Verify is_dict_row, is_mapping_like, and is_iterable_parameters."""
    assert is_dict_row(candidate) is expected_dict_row
    assert is_mapping_like(candidate) is expected_mapping
    assert is_iterable_parameters(candidate) is expected_iterable_params


def test_dataclass_extraction_and_msgspec_rename_config(
    sample_dataclass_instance: _SampleDataclass,
) -> None:
    """Verify extract_dataclass_fields, extract_dataclass_items, and get_msgspec_rename_config."""
    fields = extract_dataclass_fields(sample_dataclass_instance, exclude_none=True, exclude_empty=True)
    assert [f.name for f in fields] == ["user_id", "user_name"]

    items = extract_dataclass_items(
        sample_dataclass_instance,
        exclude_none=True,
        exclude_empty=True,
        include={"user_id"},
    )
    assert items == (("user_id", 7),)

    assert get_msgspec_rename_config(_SampleCamelStruct) == "camel"
    assert get_msgspec_rename_config(_SampleStruct) is None
    assert is_schema_or_dict_sequence([{"a": 1}, _SampleStruct(user_id=1, user_name="Ada")]) is True
    assert is_schema_or_dict_sequence([]) is False


@pytest.mark.parametrize(
    ("converter", "expected_top_key", "expected_nested_key"),
    [
        (camelize, "userProfile", "firstName"),
        (kebabize, "user-profile", "first-name"),
        (snake_case, "user_profile", "first_name"),
    ],
)
def test_transform_dict_keys(
    nested_key_payload: dict[str, Any],
    converter: Any,
    expected_top_key: str,
    expected_nested_key: str,
) -> None:
    """Verify transform_dict_keys recursively converts nested dicts, lists, and tuples."""
    transformed = transform_dict_keys(nested_key_payload, converter)
    assert expected_top_key in transformed
    assert transformed[expected_top_key][0][expected_nested_key] == "Ada"


def test_to_schema_single_and_sequence(
    schema_hydration_case: tuple[Any, dict[str, Any], type[Any]],
) -> None:
    """Verify to_schema hydrates single records and sequences across msgspec, dataclass, and TypedDict."""
    schema_type, input_data, expected_type = schema_hydration_case
    single = to_schema(input_data, schema_type=schema_type)
    assert isinstance(single, expected_type)

    seq = to_schema([input_data], schema_type=schema_type)
    assert isinstance(seq, list)
    assert len(seq) == 1
    assert isinstance(seq[0], expected_type)


@pytest.mark.parametrize(
    ("raw_val", "target_type", "expected"),
    [
        ("42", int, 42),
        ("3.5", float, 3.5),
        (123, str, "123"),
        ("true", bool, True),
        ("0", bool, False),
        ("12.34", Decimal, Decimal("12.34")),
        ("12345678-1234-5678-1234-567812345678", UUID, UUID("12345678-1234-5678-1234-567812345678")),
        ("/tmp/demo", Path, Path("/tmp/demo")),
        ({"user_id": 9, "user_name": "Ada"}, _SampleStruct, _SampleStruct(user_id=9, user_name="Ada")),
    ],
)
def test_to_value_type_coercions(
    raw_val: Any,
    target_type: type[Any],
    expected: Any,
) -> None:
    """Verify to_value_type coerces scalar and struct values accurately."""
    assert to_value_type(raw_val, target_type) == expected


@pytest.mark.parametrize(
    ("mode", "expected_scalar", "expected_nested"),
    [
        ("preserve", Decimal("9.50"), {"price": Decimal("9.50"), "items": [Decimal("1.25")]}),
        ("str", "9.50", {"price": "9.50", "items": ["1.25"]}),
        ("float", 9.5, {"price": 9.5, "items": [1.25]}),
    ],
)
def test_decimal_converters_and_nested_normalizer(
    mode: str,
    expected_scalar: Any,
    expected_nested: dict[str, Any],
) -> None:
    """Verify build_decimal_converter and build_nested_decimal_normalizer across modes."""
    conv = build_decimal_converter(mode=mode)
    norm = build_nested_decimal_normalizer(mode=mode)

    assert conv(Decimal("9.50")) == expected_scalar
    assert norm({"price": Decimal("9.50"), "items": [Decimal("1.25")]}) == expected_nested


@pytest.mark.parametrize("native", [False, True])
def test_build_uuid_coercions(sample_uuid: UUID, native: bool) -> None:
    """Verify build_uuid_coercions returns string or native UUID coercions."""
    assert "-" in DEFAULT_SPECIAL_CHARS
    coercions = build_uuid_coercions(native=native)
    expected: UUID | str = sample_uuid if native else str(sample_uuid)
    assert coercions[UUID](sample_uuid) == expected


@pytest.mark.parametrize(
    ("seq", "expected"),
    [
        ([{"a": 1}], True),
        ([1, 2, 3], False),
        ([], False),
    ],
)
def test_should_json_encode_sequence(seq: list[Any], expected: bool) -> None:
    """Verify should_json_encode_sequence identifies sequences of mappings or structs."""
    assert should_json_encode_sequence(seq) is expected


@pytest.mark.parametrize(
    ("temporal_val", "expected_prefix"),
    [
        (date(2026, 11, 2), "2026-11-02"),
        (time(14, 30, 0), "14:30:00"),
        (datetime(2026, 11, 2, 14, 30, tzinfo=UTC), "2026-11-02T14:30:00"),
    ],
)
def test_time_iso_convert(temporal_val: date | time | datetime, expected_prefix: str) -> None:
    """Verify time_iso_convert formats date, time, and datetime values as ISO 8601 strings."""
    assert time_iso_convert(temporal_val).startswith(expected_prefix)
