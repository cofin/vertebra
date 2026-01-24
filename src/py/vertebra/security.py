import logging
import uuid
from typing import Any

import msgspec

from vertebra import _vertebra
from vertebra.exceptions import ChannelError

logger = logging.getLogger(__name__)

# Opcode constants matched with Rust/TS
OP_HASH_PASSWORD = 8
OP_VERIFY_PASSWORD = 9
OP_GENERATE_RANDOM = 10
OP_CSRF_SIGN = 11


class SecurityClient:
    def __init__(self, broker_name: str = "vertebra-broker") -> None:
        self.broker_name = broker_name
        self.control_name = f"/{broker_name}.control"
        self.client_id = str(uuid.uuid4()).replace("-", "")
        self.req_name = f"/{broker_name}.client.{self.client_id}.req"
        self.resp_name = f"/{broker_name}.client.{self.client_id}.resp"

        self.control: Any = None
        self.request: Any = None
        self.response: Any = None

        self._connected = False
        self._seq = 0

    def connect(self) -> None:
        if self._connected:
            return

        # Open control ring (MPMC)
        # Note: Must match slot_size/slots of BrokerServer
        try:
            self.control = _vertebra.MpmcRing.open_named(self.control_name, 65536, 1024)
        except RuntimeError as e:
            msg = f"Failed to connect to broker control ring: {e}"
            raise ChannelError(msg) from e

        # Create request/response rings (SPSC)
        # We are the producer for req_ring, consumer for resp_ring
        self.request = _vertebra.SpscRing.create_named(self.req_name, 65536, 1024)
        self.response = _vertebra.SpscRing.create_named(self.resp_name, 65536, 1024)

        # Register
        req_id = self._next_id()
        payload = {
            "id": req_id,
            "kind": "register_client",
            "client_id": self.client_id,
            "req_ring": self.req_name,
            "resp_ring": self.resp_name,
        }
        encoded = msgspec.msgpack.encode(payload)
        # MpmcRing send takes (data, flags). It blocks/spins if full?
        # vertebra-py send uses py.detach.
        self.control.send(encoded, 0)

        # Wait for ack on response ring
        ack = self._recv_response()
        if not ack.get("ok"):
            msg = "Registration failed"
            raise ChannelError(msg)

        self._connected = True

    def hash_password(self, password: str) -> str:
        self.connect()
        req_id = self._next_id()
        payload = [req_id, password]
        encoded = msgspec.msgpack.encode(payload)
        self.request.send(encoded, OP_HASH_PASSWORD)

        resp = self._recv_response()
        if not resp.get("ok"):
            raise ChannelError(resp.get("error", {}).get("message", "Unknown error"))
        return resp["result"]

    def verify_password(self, password: str, hash_value: str) -> bool:
        self.connect()
        req_id = self._next_id()
        payload = [req_id, password, hash_value]
        encoded = msgspec.msgpack.encode(payload)
        self.request.send(encoded, OP_VERIFY_PASSWORD)

        resp = self._recv_response()
        if not resp.get("ok"):
            raise ChannelError(resp.get("error", {}).get("message", "Unknown error"))
        return resp["result"]

    def generate_random(self, length: int) -> bytes:
        self.connect()
        req_id = self._next_id()
        payload = [req_id, length]
        encoded = msgspec.msgpack.encode(payload)
        self.request.send(encoded, OP_GENERATE_RANDOM)

        resp = self._recv_response()
        if not resp.get("ok"):
            raise ChannelError(resp.get("error", {}).get("message", "Unknown error"))
        return resp["result"]

    def csrf_sign(self, data: str, secret: str) -> str:
        self.connect()
        req_id = self._next_id()
        payload = [req_id, data, secret]
        encoded = msgspec.msgpack.encode(payload)
        self.request.send(encoded, OP_CSRF_SIGN)

        resp = self._recv_response()
        if not resp.get("ok"):
            raise ChannelError(resp.get("error", {}).get("message", "Unknown error"))
        return resp["result"]

    def _next_id(self) -> int:
        self._seq += 1
        return self._seq

    def _recv_response(self) -> dict[str, Any]:
        # Timeout 5000ms
        buf = self.response.recv_timeout(65536, 5000)
        if buf is None:
            msg = "Timeout waiting for broker response"
            raise ChannelError(msg)
        return msgspec.msgpack.decode(buf)

    def close(self) -> None:
        """Clean up shared memory resources."""
        if self.request:
            try:
                _vertebra.shm_unlink(self.req_name)
                _vertebra.shm_unlink(self.resp_name)
            except OSError as e:
                logger.debug("Failed to unlink shared memory: %s", e)
