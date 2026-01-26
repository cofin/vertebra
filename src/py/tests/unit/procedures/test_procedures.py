import threading
from pathlib import Path

import pytest

from vertebra.codegen.ir import PrimitiveType
from vertebra.core.procedures import ProcedureMetadata, ProcedureRegistry, procedure

pytestmark = pytest.mark.unit


def test_procedure_decorator_registers() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    @procedure
    def add(a: int, b: int) -> int:
        return a + b

    meta = registry.list()[0]
    assert "add" in meta.procedure_id
    assert meta.serializer == "msgspec"
    assert meta.params["a"] == PrimitiveType("number")
    assert meta.params["b"] == PrimitiveType("number")
    assert meta.return_type == PrimitiveType("number")
    assert add(1, 2) == 3


def test_procedure_decorator_options() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    @procedure(name="custom.add", serializer="pydantic", metadata={"auth": "required"})
    def add(a: int, b: int) -> int:
        return a + b

    meta = registry.get("custom.add")
    assert meta is not None
    assert meta.metadata["auth"] == "required"
    assert meta.serializer == "pydantic"


def test_procedure_attaches_litestar_opt() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    def handler(value: int) -> int:
        return value

    handler.opt = {}
    wrapped = procedure(handler)
    assert wrapped.opt["vertebra"]["procedure_id"].endswith("handler")


def test_registry_thread_safety() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    def register(idx: int) -> None:
        meta = ProcedureMetadata(
            procedure_id=f"proc.{idx}",
            handler_fn=lambda: None,
            source_file=Path(__file__),
            params={"value": PrimitiveType("number")},
            return_type=PrimitiveType("number"),
            serializer="msgspec",
            metadata={},
        )
        registry.register(meta)

    threads = [threading.Thread(target=register, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(registry.list()) == 10
