import os

from vertebra.utils.config_tools import get_env
from vertebra.utils.module_loader import import_string, module_available
from vertebra.utils.singleton import SingletonMeta


def test_singleton():
    class MySingleton(metaclass=SingletonMeta):
        pass

    s1 = MySingleton()
    s2 = MySingleton()
    assert s1 is s2


def test_env_coercion():
    os.environ["VERTEBRA_TEST_INT"] = "42"
    assert get_env("VERTEBRA_TEST_INT", type_hint=int) == 42

    os.environ["VERTEBRA_TEST_BOOL"] = "true"
    assert get_env("VERTEBRA_TEST_BOOL", type_hint=bool) is True


def test_module_loader_import_string():
    import json

    assert import_string("json.dumps") is json.dumps


def test_module_available():
    assert module_available("os") is True
    assert module_available("non_existent_module_xyz") is False


def test_dataclass_to_dict_exclude_empty():
    from dataclasses import dataclass

    from vertebra._typing import UNSET, Empty
    from vertebra.utils.type_guards import dataclass_to_dict

    @dataclass
    class Payload:
        name: str
        empty_val: object = Empty
        unset_val: object = UNSET
        none_val: str | None = None

    item = Payload(name="ok")
    assert dataclass_to_dict(item, exclude_empty=True) == {"name": "ok", "none_val": None}
    assert "empty_val" in dataclass_to_dict(item, exclude_empty=False)
