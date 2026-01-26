import threading

import pytest

from vertebra.messaging.transport.channel import Channel, SerializationError, encode_request
from vertebra.procedures import ProcedureRegistry, procedure

pytestmark = pytest.mark.unit


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

    # Thread coordination events
    ready = threading.Event()
    shutdown = threading.Event()

    def server_loop() -> None:
        # Signal ready before blocking on receive
        ready.set()
        for _ in range(2):
            try:
                proc_id, params = server.receive_sync(timeout_ms=2000)
                meta = registry.get(proc_id)
                assert meta is not None
                result = meta.handler_fn(**params)
                server.respond_sync(proc_id, result, response_type=meta.return_type)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)
                break
        shutdown.set()

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Wait for server to be ready before sending
    if not ready.wait(timeout=2.0):
        pytest.fail("Server thread did not become ready")

    result = client.send_sync("users.msg", {"user_id": 1}, response_type=User, timeout_ms=2000)
    assert isinstance(result, User)
    assert result.id == 1

    result2 = client.send_sync("users.pyd", {"user_id": 2}, response_type=UserModel, timeout_ms=2000)
    assert isinstance(result2, UserModel)
    assert result2.name == "pydantic"

    # Wait for clean shutdown
    if not shutdown.wait(timeout=2.0):
        pytest.fail("Server thread did not shut down cleanly")

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

    # Thread coordination events
    ready = threading.Event()
    shutdown = threading.Event()

    def server_loop() -> None:
        ready.set()
        try:
            server.receive_sync(params_type=dict, timeout_ms=2000)
        except SerializationError as exc:
            errors.append(exc)
        finally:
            shutdown.set()

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Wait for server to be ready before sending
    if not ready.wait(timeout=2.0):
        pytest.fail("Server thread did not become ready")

    client._handle.send_request(encode_request("bad.payload", b"not-json"))

    # Wait for clean shutdown
    if not shutdown.wait(timeout=2.0):
        pytest.fail("Server thread did not shut down cleanly")

    thread.join(timeout=2)

    assert errors
    assert errors[0].procedure_id == "bad.payload"
    assert errors[0].stage == "deserialize_params"


@pytest.fixture(autouse=True)
def _clear_registry() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()
