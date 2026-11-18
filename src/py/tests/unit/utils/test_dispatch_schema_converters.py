"""Unit tests for vertebra.utils.dispatch, schema, type_converters, and expanded type_guards."""

from __future__ import annotations

import dataclasses
import io
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, TypedDict, cast
from uuid import UUID

import msgspec
import pydantic
import pytest

from vertebra.db.adapters.bigquery.core import BigQueryCore
from vertebra.db.adapters.spanner.core import SpannerCore
from vertebra.db.arrow import _hydrate_rows_with_schema
from vertebra.framework.openapi import (
    AsyncAPIDocsConfig,
    ElementsDocsConfig,
    OpenAPIConfig,
    OpenAPISpecBuilder,
    RapiDocDocsConfig,
    RedocDocsConfig,
    ScalarDocsConfig,
    SwaggerDocsConfig,
    render_asyncapi_html,
    render_elements_html,
    render_rapidoc_html,
    render_redoc_html,
    render_scalar_html,
    render_swagger_html,
)
from vertebra.typing import UNSET, CamelizedBaseStruct, Empty
from vertebra.utils.dispatch import TypeDispatcher
from vertebra.utils.schema import can_vectorcall_hydrate_schema, to_schema, to_value_type, transform_dict_keys
from vertebra.utils.serializers import schema_components
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
    is_async_generator_callable,
    is_async_readable,
    is_attrs_schema,
    is_coroutine_callable,
    is_dict_row,
    is_generator_callable,
    is_iterable_parameters,
    is_mapping_like,
    is_msgspec_struct_type,
    is_pydantic_model_type,
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


class _SamplePydanticModel(pydantic.BaseModel):
    user_id: int
    user_name: str


async def _sample_coro_fn() -> int:
    return 1


async def _sample_async_gen_fn() -> Any:
    yield 1


def _sample_sync_gen_fn() -> Any:
    yield 1


def _sample_sync_fn() -> int:
    return 1


class _AsyncCallableObj:
    async def __call__(self) -> int:
        return 2


class _AsyncGenCallableObj:
    async def __call__(self) -> Any:
        yield 2


class _SyncGenCallableObj:
    def __call__(self) -> Any:
        yield 2


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
    return cast("tuple[Any, dict[str, Any], type[Any]]", request.param)


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


@pytest.mark.parametrize(
    ("candidate", "expected_coro", "expected_async_gen", "expected_sync_gen"),
    [
        (_sample_coro_fn, True, False, False),
        (_AsyncCallableObj(), True, False, False),
        (_sample_async_gen_fn, False, True, False),
        (_AsyncGenCallableObj(), False, True, False),
        (_sample_sync_gen_fn, False, False, True),
        (_SyncGenCallableObj(), False, False, True),
        (_sample_sync_fn, False, False, False),
        ("not-callable", False, False, False),
    ],
)
def test_callable_coroutine_and_generator_type_guards(
    candidate: Any,
    expected_coro: bool,
    expected_async_gen: bool,
    expected_sync_gen: bool,
) -> None:
    """Verify is_coroutine_callable, is_async_generator_callable, and is_generator_callable."""
    assert is_coroutine_callable(candidate) is expected_coro
    assert is_async_generator_callable(candidate) is expected_async_gen
    assert is_generator_callable(candidate) is expected_sync_gen


@pytest.mark.parametrize(
    ("candidate", "expected_msgspec_type", "expected_pydantic_type"),
    [
        (_SampleStruct, True, False),
        (_SampleStruct(user_id=1, user_name="Ada"), False, False),
        (_SamplePydanticModel, False, True),
        (_SamplePydanticModel(user_id=1, user_name="Ada"), False, False),
        (_SampleDataclass, False, False),
        ("not-a-type", False, False),
    ],
)
def test_model_class_type_guards(
    candidate: Any,
    expected_msgspec_type: bool,
    expected_pydantic_type: bool,
) -> None:
    """Verify is_msgspec_struct_type and is_pydantic_model_type match model classes only."""
    assert is_msgspec_struct_type(candidate) is expected_msgspec_type
    assert is_pydantic_model_type(candidate) is expected_pydantic_type


class _FlatSchemaStruct(msgspec.Struct):
    count: int
    label: str


class _NestedInnerStruct(msgspec.Struct):
    code: str


class _NestedOuterStruct(msgspec.Struct):
    inner: _NestedInnerStruct
    payload: int | str


@pytest.mark.parametrize(
    ("types", "ref_template", "expected_ref_prefix", "expected_defs"),
    [
        ((), "#/$defs/{name}", "", set()),
        ((_FlatSchemaStruct,), "#/$defs/{name}", "#/$defs/_FlatSchemaStruct", {"_FlatSchemaStruct"}),
        (
            (_NestedOuterStruct,),
            "#/$defs/{name}",
            "#/$defs/_NestedOuterStruct",
            {"_NestedOuterStruct", "_NestedInnerStruct"},
        ),
        (
            (_NestedOuterStruct,),
            "#/components/schemas/{name}",
            "#/components/schemas/_NestedOuterStruct",
            {"_NestedOuterStruct", "_NestedInnerStruct"},
        ),
    ],
)
def test_schema_components_extraction(
    types: tuple[type[Any], ...],
    ref_template: str,
    expected_ref_prefix: str,
    expected_defs: set[str],
) -> None:
    """Verify schema_components extracts normalized schemas and defs across empty, flat, nested, and union types."""
    schemas, defs = schema_components(types, ref_template=ref_template)
    assert isinstance(schemas, tuple)
    assert isinstance(defs, dict)
    if not types:
        assert schemas == ()
        assert defs == {}
        return
    assert schemas[0]["$ref"] == expected_ref_prefix
    assert expected_defs.issubset(defs.keys())
    if _NestedOuterStruct in types:
        outer_props = defs["_NestedOuterStruct"]["properties"]
        expected_inner_ref = ref_template.format(name="_NestedInnerStruct")
        assert outer_props["inner"]["$ref"] == expected_inner_ref
        assert {"anyOf"} <= set(outer_props["payload"].keys())


@pytest.mark.parametrize(
    ("config_obj", "renderer_fn", "expected_marker"),
    [
        (ScalarDocsConfig(), render_scalar_html, "data-url='/spec/openapi.json'"),
        (SwaggerDocsConfig(), render_swagger_html, "SwaggerUIBundle"),
        (RedocDocsConfig(), render_redoc_html, "<redoc spec-url='/spec/openapi.json'"),
        (ElementsDocsConfig(), render_elements_html, "apiDescriptionUrl='/spec/openapi.json'"),
        (RapiDocDocsConfig(), render_rapidoc_html, "<rapi-doc spec-url='/spec/openapi.json'"),
        (AsyncAPIDocsConfig(schema_path="/spec/openapi.json"), render_asyncapi_html, "/spec/openapi.json"),
    ],
)
def test_openapi_neutral_portal_configs_and_renderers(
    config_obj: Any,
    renderer_fn: Any,
    expected_marker: str,
) -> None:
    """Verify canonical OpenAPI portal configs and neutral HTML renderers produce valid HTML snippets."""
    title = "Vertebra Portal Test"
    spec_url = "/spec/openapi.json"
    if isinstance(config_obj, AsyncAPIDocsConfig):
        from_method = config_obj.render_html(title=title)
        from_fn = renderer_fn(config_obj, title=title, schema_url=spec_url)
    elif isinstance(config_obj, ScalarDocsConfig):
        from_method = config_obj.render_html(title=title, default_schema_url=spec_url)
        from_fn = renderer_fn(config_obj, title=title, schema_url=spec_url)
    else:
        from_method = config_obj.render_html(title=title, schema_url=spec_url)
        from_fn = renderer_fn(config_obj, title=title, schema_url=spec_url)
    assert title in from_method
    assert expected_marker in from_method
    assert from_fn == from_method


class _CamelNestedChild(msgspec.Struct, frozen=True, rename="camel"):
    item_name: str
    item_count: int


class _ExplicitAliasChild(msgspec.Struct, frozen=True):
    display_name: str = msgspec.field(name="displayName")


class _KebabNestedEnvelope(msgspec.Struct, frozen=True, rename="kebab"):
    child_rows: list[_CamelNestedChild]
    metadata_map: dict[str, Any] = msgspec.field(default_factory=dict)


class _OptionalCollectionEnvelope(msgspec.Struct, frozen=True):
    child: _CamelNestedChild | None = None
    children: tuple[_CamelNestedChild, ...] = ()
    children_by_key: dict[str, _CamelNestedChild] = msgspec.field(default_factory=dict)
    explicit_child: _ExplicitAliasChild | None = None


def test_openapi_config_validation_and_spec_builder_schema_components() -> None:
    """Verify OpenAPIConfig rejects invalid backends at init and OpenAPISpecBuilder emits component schemas."""
    invalid_backend = cast("Any", "unknown_backend")
    with pytest.raises(ValueError, match="Unsupported docs backend"):
        OpenAPIConfig(enabled_docs=("scalar", invalid_backend))

    with pytest.raises(ValueError, match="Unsupported docs backend"):
        OpenAPIConfig(default_docs=invalid_backend)

    builder = OpenAPISpecBuilder(OpenAPIConfig(title="Schema Test"))
    builder.add_operation(
        "/orders",
        "POST",
        request_body_schema=_NestedOuterStruct,
        response_schema=_FlatSchemaStruct,
    )
    spec = builder.build()
    schemas = spec["components"]["schemas"]
    assert "_NestedOuterStruct" in schemas
    assert "_NestedInnerStruct" in schemas
    assert "_FlatSchemaStruct" in schemas
    assert schemas["_NestedOuterStruct"]["properties"]["inner"]["$ref"] == "#/components/schemas/_NestedInnerStruct"


def test_to_schema_msgspec_opt_in_normalize_nested_and_map_key_preservation() -> None:
    """Verify to_schema defaults to encoded nested names, preserves dict keys, and supports normalize_nested."""
    child_payload = {"item_name": "python", "itemName": "encoded", "item_count": 2}
    row = {
        "child": child_payload,
        "children": [child_payload],
        "children_by_key": {"snake_map_key": child_payload},
        "explicit_child": {"display_name": "Ada"},
    }
    expected_child = _CamelNestedChild(item_name="encoded", item_count=2)

    with pytest.raises(msgspec.ValidationError, match="itemCount"):
        to_schema(row, schema_type=_OptionalCollectionEnvelope)

    hydrated = to_schema(row, schema_type=_OptionalCollectionEnvelope, normalize_nested=True)
    assert hydrated == _OptionalCollectionEnvelope(
        child=expected_child,
        children=(expected_child,),
        children_by_key={"snake_map_key": expected_child},
        explicit_child=_ExplicitAliasChild(display_name="Ada"),
    )
    assert child_payload == {"item_name": "python", "itemName": "encoded", "item_count": 2}

    batch_row = {
        "child_rows": [{"item_name": "pencil", "itemCount": 3}],
        "metadata_map": {"preserve_snake_key": 1},
    }
    batch_hydrated = to_schema([batch_row], schema_type=_KebabNestedEnvelope, normalize_nested=True)
    assert batch_hydrated == [
        _KebabNestedEnvelope(
            child_rows=[_CamelNestedChild(item_name="pencil", item_count=3)],
            metadata_map={"preserve_snake_key": 1},
        )
    ]

    default_map_only = to_schema(
        {"child_rows": [{"itemName": "pen", "itemCount": 1}], "metadata_map": {"raw_snake_key": 9}},
        schema_type=_KebabNestedEnvelope,
    )
    assert default_map_only.metadata_map == {"raw_snake_key": 9}


@pytest.mark.parametrize("hydrator", ["arrow", "bigquery", "spanner"])
def test_row_hydrators_convert_nested_msgspec_structs(hydrator: str) -> None:
    """Verify Arrow, BigQuery, and Spanner row hydrators convert nested Struct fields instead of leaving raw dicts."""
    assert can_vectorcall_hydrate_schema(_SampleStruct) is True
    assert can_vectorcall_hydrate_schema(_SampleCamelStruct) is False
    assert can_vectorcall_hydrate_schema(_OptionalCollectionEnvelope) is False

    encoded_rows = [({"itemName": "notebook", "itemCount": 4},)]
    python_rows = [({"item_name": "notebook", "item_count": 4},)]
    cols = ("child",)

    if hydrator == "arrow":
        res_default = _hydrate_rows_with_schema(encoded_rows, cols, _OptionalCollectionEnvelope)
        res_nested = _hydrate_rows_with_schema(
            python_rows,
            cols,
            _OptionalCollectionEnvelope,
            normalize_nested=True,
        )
    elif hydrator == "bigquery":
        res_default = BigQueryCore.hydrate_query_rows(cols, encoded_rows, schema_type=_OptionalCollectionEnvelope)
        res_nested = BigQueryCore.hydrate_query_rows(
            cols,
            python_rows,
            schema_type=_OptionalCollectionEnvelope,
            normalize_nested=True,
        )
    else:
        res_default = SpannerCore.hydrate_query_rows(cols, encoded_rows, schema_type=_OptionalCollectionEnvelope)
        res_nested = SpannerCore.hydrate_query_rows(
            cols,
            python_rows,
            schema_type=_OptionalCollectionEnvelope,
            normalize_nested=True,
        )

    assert isinstance(res_default[0].child, _CamelNestedChild)
    assert res_default[0].child == _CamelNestedChild(item_name="notebook", item_count=4)
    assert res_nested[0].child == _CamelNestedChild(item_name="notebook", item_count=4)
