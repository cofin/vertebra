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
