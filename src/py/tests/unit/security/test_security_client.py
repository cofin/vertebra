from unittest.mock import MagicMock, patch

import msgspec
import pytest

from vertebra.exceptions import ChannelError
from vertebra.security import OP_HASH_PASSWORD, SecurityClient


@pytest.fixture
def mock_vertebra():
    with patch("vertebra.security._vertebra") as m:
        # Mock MpmcRing
        m.MpmcRing.open_named.return_value = MagicMock()

        # Mock SpscRing
        req_ring = MagicMock()
        resp_ring = MagicMock()
        m.SpscRing.create_named.side_effect = [req_ring, resp_ring]

        yield m


def test_connect_registration(mock_vertebra) -> None:
    client = SecurityClient()

    # Mock registration response
    ack = msgspec.msgpack.encode({"ok": True, "id": 1})

    # We need to mock recv_timeout on the mock object returned by create_named
    # Since create_named is called twice (req and resp), we ensure both return mocks that behave
    mock_ring = MagicMock()
    mock_ring.recv_timeout.return_value = ack

    mock_vertebra.SpscRing.create_named.side_effect = [mock_ring, mock_ring]

    client.connect()

    assert client._connected
    mock_vertebra.MpmcRing.open_named.assert_called_once()
    mock_vertebra.SpscRing.create_named.assert_called()

    # Verify register payload sent to control
    args, _ = client.control.send.call_args
    payload = msgspec.msgpack.decode(args[0])
    assert payload["kind"] == "register_client"
    assert payload["req_ring"] == client.req_name


def test_hash_password(mock_vertebra) -> None:
    client = SecurityClient()

    # Mock registration ack AND hash response
    ack = msgspec.msgpack.encode({"ok": True})
    hash_resp = msgspec.msgpack.encode({"ok": True, "result": "hashed_secret"})

    mock_ring = MagicMock()
    # The first call to recv_timeout is for registration ack
    # The second call is for the hash response
    mock_ring.recv_timeout.side_effect = [ack, hash_resp]

    mock_vertebra.SpscRing.create_named.side_effect = [mock_ring, mock_ring]

    hashed = client.hash_password("secret")

    assert hashed == "hashed_secret"

    # Verify OP sent
    # client.request is also mock_ring
    args, _ = mock_ring.send.call_args
    assert args[1] == OP_HASH_PASSWORD
    payload = msgspec.msgpack.decode(args[0])
    assert payload[1] == "secret"


def test_error_handling(mock_vertebra) -> None:
    client = SecurityClient()

    ack = msgspec.msgpack.encode({"ok": True})
    err_resp = msgspec.msgpack.encode({"ok": False, "error": {"message": "Invalid password"}})

    mock_ring = MagicMock()
    mock_ring.recv_timeout.side_effect = [ack, err_resp]
    mock_vertebra.SpscRing.create_named.side_effect = [mock_ring, mock_ring]

    with pytest.raises(ChannelError, match="Invalid password"):
        client.verify_password("pw", "hash")
