from __future__ import annotations

import threading

import pytest

from vertebra.ipc.channel import Channel, SerializationError, _encode_message
from vertebra.procedures import ProcedureRegistry, procedure


@pytest.mark.usefixtures("_clear_registry")
def test_end_to_end_procedure_flow() -> None:
    msgspec = pytest.importorskip("msgspec")
    pydantic = pytest.importorskip("pydantic")

    class User(msgspec.Struct):
        id: int
        name: str

    class UserModel(pydantic.BaseModel):
        id: int
        name: str

    registry = ProcedureRegistry.instance()

    @procedure(name="users.msg", serializer="msgspec")
    def get_msg_user(user_id: int) -> User:
        return User(id=user_id, name="msgspec")

    @procedure(name="users.pyd", serializer="pydantic")
    def get_pyd_user(user_id: int) -> UserModel:
        return UserModel(id=user_id, name="pydantic")

    client, server = Channel.create_pair(slot_size=2048, slots=8)
    errors: list[Exception] = []

    def server_loop() -> None:
        for _ in range(2):
            try:
                proc_id, params = server.receive_sync()
                meta = registry.get(proc_id)
                assert meta is not None
                result = meta.handler_fn(**params)
                server.respond_sync(proc_id, result, response_type=meta.return_type)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)
                break

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    result = client.send_sync("users.msg", {"user_id": 1}, response_type=User)
    assert isinstance(result, User)
    assert result.id == 1

    result2 = client.send_sync("users.pyd", {"user_id": 2}, response_type=UserModel)
    assert isinstance(result2, UserModel)
    assert result2.name == "pydantic"

    thread.join(timeout=2)
    assert not errors


def test_integration_invalid_payload() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    @procedure(name="bad.payload", serializer="msgspec")
    def bad_payload(value: int) -> int:
        return value

    client, server = Channel.create_pair(slot_size=1024, slots=4)
    errors: list[SerializationError] = []

    def server_loop() -> None:
        try:
            server.receive_sync(params_type=dict)
        except SerializationError as exc:
            errors.append(exc)

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    client._handle.send_request(_encode_message("bad.payload", b"not-json"))
    thread.join(timeout=2)

    assert errors
    assert errors[0].procedure_id == "bad.payload"
    assert errors[0].stage == "deserialize_params"


@pytest.fixture(autouse=True)
def _clear_registry() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()
