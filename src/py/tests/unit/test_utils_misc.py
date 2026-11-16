import os
from dataclasses import dataclass

from vertebra.typing import UNSET, Empty
from vertebra.utils.config_tools import get_env
from vertebra.utils.module_loader import import_string, module_available
from vertebra.utils.serializers import encode_json
from vertebra.utils.type_guards import dataclass_to_dict


def test_env_coercion() -> None:
    os.environ["VERTEBRA_TEST_INT"] = "42"
    assert get_env("VERTEBRA_TEST_INT", 0, int) == 42

    os.environ["VERTEBRA_TEST_BOOL"] = "true"
    assert get_env("VERTEBRA_TEST_BOOL", False, bool) is True


def test_module_loader_import_string() -> None:
    assert import_string("vertebra.utils.serializers.encode_json") is encode_json


def test_module_available() -> None:
    assert module_available("os") is True
    assert module_available("non_existent_module_xyz") is False


def test_dataclass_to_dict_exclude_empty() -> None:
    @dataclass
    class Payload:
        name: str
        empty_val: object = Empty
        unset_val: object = UNSET
        none_val: str | None = None

    item = Payload(name="ok")
    assert dataclass_to_dict(item, exclude_empty=True) == {"name": "ok", "none_val": None}
    assert "empty_val" in dataclass_to_dict(item, exclude_empty=False)
