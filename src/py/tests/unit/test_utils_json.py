from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import gc
import uuid
from pathlib import Path
from typing import Any

import msgspec
import pytest

from vertebra import wasm as wasm_mod
from vertebra._vertebra import (
    Request,
    WsgiInput,
    call_vectorcall,
    hydrate_one_vectorcall,
    hydrate_rows_vectorcall,
    instantiate_struct_vectorcall,
    orm_to_struct_vectorcall,
    pack_grpc_lpm_frame,
)
from vertebra.typing import (
    BaseStruct,
    CamelizedBaseStruct,
    FrozenBaseStruct,
    FrozenCamelizedStruct,
    ImproperConfigurationError,
    MissingDependencyError,
)
from vertebra.utils.serializers import (
    MsgPackSerializer,
    ZeroCopyCodecRegistry,
    decode_grpc_frame,
    decode_grpc_frames,
    decode_json,
    decode_msgpack,
    encode_grpc_frame_bytes,
    encode_grpc_frame_into,
    encode_json,
    encode_json_bytes,
    encode_msgpack_bytes,
    encode_msgpack_into,
    get_default_json_serializer,
    get_default_msgpack_serializer,
)
from vertebra.wasm import (
    WASM_MAGIC_HEADER,
    WASMExecutionError,
    WASMHttpResponse,
    WASMModuleConfig,
    WASMPluginHost,
    WASMSandboxPolicy,
)


class MyEnum(enum.Enum):
    A = "a"
    B = "b"


@dataclasses.dataclass
class MyData:
    name: str
    age: int


class MyStruct(msgspec.Struct):
    id: int
    data: str


def test_encode_decode_basic() -> None:
    data = {"a": 1, "b": "test"}
    encoded = encode_json(data, as_bytes=True)
    assert decode_json(encoded) == data


def test_encode_datetime() -> None:
    dt = datetime.datetime(2026, 3, 20, 10, 30, tzinfo=datetime.UTC)
    encoded = encode_json({"dt": dt}, as_bytes=True)
    assert b"2026-03-20T10:30:00Z" in encoded


def test_encode_uuid() -> None:
    u = uuid.uuid4()
    encoded = encode_json({"u": u}, as_bytes=True)
    assert str(u).encode() in encoded


def test_encode_decimal() -> None:
    d = decimal.Decimal("42.5")
    encoded = encode_json({"d": d}, as_bytes=True)
    assert b"42.5" in encoded


def test_encode_enum() -> None:
    e = MyEnum.A
    encoded = encode_json({"e": e}, as_bytes=True)
    assert b'"a"' in encoded


def test_encode_dataclass() -> None:
    d = MyData(name="test", age=25)
    encoded = encode_json(d, as_bytes=True)
    assert b'"name":"test"' in encoded
    assert b'"age":25' in encoded


def test_encode_struct() -> None:
    s = MyStruct(id=1, data="hello")
    encoded = encode_json(s, as_bytes=True)
    assert b'"id":1' in encoded
    assert b'"data":"hello"' in encoded


def test_encode_complex_types() -> None:
    data = {
        "set": {1, 2, 3},
        "frozenset": frozenset([4, 5]),
        "decimal": decimal.Decimal("10.0"),
    }
    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)
    assert set(decoded["set"]) == {1, 2, 3}
    assert set(decoded["frozenset"]) == {4, 5}
    assert decoded["decimal"] == "10.0"


def test_encode_json_as_string() -> None:
    data = {"a": 1}
    encoded = encode_json(data, as_bytes=False)
    assert isinstance(encoded, str)
    assert encoded == '{"a":1}'


def test_decode_json_string() -> None:
    data = '{"a":1}'
    decoded = decode_json(data)
    assert decoded == {"a": 1}


def test_msgpack_roundtrip_basic_and_memoryview() -> None:
    """Verify MsgPackSerializer round-trips basic primitives, buffer writes, and memoryview inputs."""
    payload = {"key": "value", "count": 42, "items": [1, 2, 3]}
    encoded = encode_msgpack_bytes(payload)
    assert isinstance(encoded, bytes)

    decoded = decode_msgpack(encoded)
    assert decoded == payload

    view_decoded = decode_msgpack(memoryview(encoded))
    assert view_decoded == payload

    buf = bytearray(b"prefix:")
    offset = len(buf)
    encode_msgpack_into(payload, buf, offset=offset)
    assert decode_msgpack(memoryview(buf)[offset:]) == payload
    assert get_default_json_serializer() is get_default_json_serializer()
    assert get_default_msgpack_serializer() is MsgPackSerializer.default()


def test_msgpack_struct_and_typed_decode() -> None:
    """Verify MsgPackSerializer encodes msgspec.Struct and decodes typed schemas."""
    struct_inst = MyStruct(id=99, data="structured")
    encoded = encode_msgpack_bytes(struct_inst)

    decoded = decode_msgpack(encoded, type=MyStruct)
    assert isinstance(decoded, MyStruct)
    assert decoded.id == 99
    assert decoded.data == "structured"


def test_msgpack_uuid_datetime_path_hooks() -> None:
    """Verify MsgPackSerializer hooks for UUID, datetime, and Path objects."""
    uid = uuid.uuid4()
    now = datetime.datetime(2026, 10, 22, 12, 0, tzinfo=datetime.UTC)
    pth = Path("/var/log/vertebra.log")

    payload = {"uid": uid, "time": now, "path": pth}
    encoded = encode_msgpack_bytes(payload)
    assert isinstance(encoded, bytes)

    decoded = decode_msgpack(encoded)
    assert decoded["uid"] == str(uid)
    assert decoded["path"] == str(pth)

    serializer = MsgPackSerializer.default()
    assert serializer.decode(encoded) == decoded

    path_encoded = encode_msgpack_bytes(pth)
    decoded_path = decode_msgpack(path_encoded, type=Path)
    assert isinstance(decoded_path, Path)
    assert decoded_path == pth

    uuid_encoded = encode_msgpack_bytes(uid)
    decoded_uuid = decode_msgpack(uuid_encoded, type=uuid.UUID)
    assert isinstance(decoded_uuid, uuid.UUID)
    assert decoded_uuid == uid


class SampleBaseItem(BaseStruct):
    """Mutable snake_case base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


class SampleCamelItem(CamelizedBaseStruct):
    """Mutable camelCase base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


class SampleFrozenItem(FrozenBaseStruct, frozen=True):
    """Immutable snake_case base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


class SampleFrozenCamelItem(FrozenCamelizedStruct, frozen=True):
    """Immutable camelCase base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


@pytest.mark.parametrize(
    "cls",
    [SampleBaseItem, SampleCamelItem, SampleFrozenItem, SampleFrozenCamelItem],
    ids=["base", "camel", "frozen", "frozen-camel"],
)
def test_base_struct_hierarchy_shared_struct_config(cls: type[Any]) -> None:
    """Verify BaseStruct hierarchy classes disable GC tracking and omit defaults."""
    cfg = cls.__struct_config__
    assert cfg.gc is False
    assert cfg.omit_defaults is True
    assert cfg.repr_omit_defaults is True


def test_base_struct_hierarchy_configs_immutability_and_wire_encoding() -> None:
    """Verify BaseStruct, CamelizedBaseStruct, FrozenBaseStruct, and FrozenCamelizedStruct contracts."""
    assert SampleBaseItem.__struct_config__.frozen is False
    assert SampleCamelItem.__struct_config__.frozen is False
    assert SampleFrozenItem.__struct_config__.frozen is True
    assert SampleFrozenCamelItem.__struct_config__.frozen is True

    mutable_base = SampleBaseItem(item_name="alpha")
    mutable_camel = SampleCamelItem(item_name="beta")
    frozen_base = SampleFrozenItem(item_name="gamma")
    frozen_camel = SampleFrozenCamelItem(item_name="delta")

    assert not gc.is_tracked(mutable_base)
    assert not gc.is_tracked(mutable_camel)
    assert not gc.is_tracked(frozen_base)
    assert not gc.is_tracked(frozen_camel)

    mutable_base.optional_tag = "updated"
    assert mutable_base.to_dict() == {"item_name": "alpha", "optional_tag": "updated"}

    untyped_frozen_base: Any = frozen_base
    with pytest.raises(AttributeError):
        untyped_frozen_base.item_name = "mutated"

    untyped_frozen_camel: Any = frozen_camel
    with pytest.raises(AttributeError):
        untyped_frozen_camel.item_name = "mutated"

    assert decode_json(encode_json_bytes(frozen_base)) == {"item_name": "gamma"}
    assert decode_json(encode_json_bytes(frozen_camel)) == {"itemName": "delta"}
    assert decode_json(encode_json_bytes(mutable_camel)) == {"itemName": "beta"}
    assert "optional_tag" not in repr(frozen_base)
    assert "optional_tag" not in repr(frozen_camel)


class _DummyORMRow:
    """Synthetic ORM entity for vectorcall attribute projection verification."""

    def __init__(self, id: int, data: str) -> None:
        self.id = id
        self.data = data


class _DummyProtobufMessage:
    """Synthetic Protobuf message implementing FromString and SerializeToString."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    @classmethod
    def FromString(cls, buf: bytes | memoryview) -> _DummyProtobufMessage:
        return cls(bytes(buf))

    def SerializeToString(self) -> bytes:
        return self.payload


def test_c_api_msgspec_and_vectorcall_bridge() -> None:
    """Verify Request.decode_msgspec, WsgiInput.decode_msgspec, and C-API vectorcall row/ORM hydration."""
    raw_json = encode_json_bytes(MyStruct(id=7, data="zero-copy"))
    req = Request.from_parts("POST", "/items", body=raw_json)
    decoded = req.decode_msgspec(MyStruct, "json")
    assert isinstance(decoded, MyStruct)
    assert decoded.id == 7
    assert decoded.data == "zero-copy"

    raw_mp = encode_msgpack_bytes(MyStruct(id=8, data="msgpack-zc"))
    wsgi_in = WsgiInput(raw_mp)
    assert bytes(wsgi_in.body_view) == raw_mp
    decoded_mp = wsgi_in.decode_msgspec(MyStruct, "msgpack")
    assert isinstance(decoded_mp, MyStruct)
    assert decoded_mp.id == 8
    assert decoded_mp.data == "msgpack-zc"

    inst = instantiate_struct_vectorcall(MyStruct, (10, "vec"), None)
    assert isinstance(inst, MyStruct)
    assert inst.id == 10
    assert inst.data == "vec"

    called = call_vectorcall(lambda x, y=2: x + y, (5,), {"y": 15})
    assert called == 20

    row_dict = hydrate_one_vectorcall(MyStruct, {"id": 11, "data": "dict-row"})
    assert isinstance(row_dict, MyStruct)
    assert row_dict.id == 11

    rows_seq = hydrate_rows_vectorcall(
        MyStruct,
        [(1, "one"), (2, "two")],
        ("id", "data"),
    )
    assert len(rows_seq) == 2
    assert rows_seq[0] == MyStruct(id=1, data="one")
    assert rows_seq[1] == MyStruct(id=2, data="two")

    orm_single = orm_to_struct_vectorcall(MyStruct, _DummyORMRow(42, "orm"))
    assert orm_single == MyStruct(id=42, data="orm")
    orm_list = orm_to_struct_vectorcall(MyStruct, [_DummyORMRow(1, "a"), _DummyORMRow(2, "b")])
    assert orm_list == [MyStruct(id=1, data="a"), MyStruct(id=2, data="b")]


def test_zero_copy_grpc_lpm_framing_and_codec_registry() -> None:
    """Verify zero-copy 5-byte LPM gRPC encoding and decoding across msgspec, dataclass, and Protobuf."""
    item = MyStruct(id=55, data="grpc-lpm")
    frame_mv = encode_grpc_frame_into(item, format="application/grpc+msgpack")
    assert len(frame_mv) > 5
    assert frame_mv[0] == 0

    req = Request.from_parts(
        "POST",
        "/vertebra.rpc.ProcedureService/get_item",
        body=bytes(frame_mv),
        headers=[("content-type", "application/grpc+msgpack")],
    )
    decoded_grpc = req.decode_grpc(MyStruct, "msgpack")
    assert decoded_grpc == item

    frame_1 = encode_grpc_frame_bytes(MyStruct(id=1, data="first"), format="msgpack")
    frame_2 = encode_grpc_frame_bytes(MyStruct(id=2, data="second"), format="msgpack")
    stream_req = Request.from_parts("POST", "/stream", body=frame_1 + frame_2)
    frames = stream_req.decode_grpc_frames(MyStruct, "msgpack")
    assert frames == [MyStruct(id=1, data="first"), MyStruct(id=2, data="second")]
    assert decode_grpc_frames(frame_1 + frame_2, target_type=MyStruct, format="msgpack") == frames

    proto_lpm = pack_grpc_lpm_frame(b"raw-proto-bytes", False)
    proto_req = Request.from_parts("POST", "/proto", body=proto_lpm)
    proto_msg = proto_req.decode_grpc(_DummyProtobufMessage, "proto")
    assert isinstance(proto_msg, _DummyProtobufMessage)
    assert proto_msg.payload == b"raw-proto-bytes"

    dc_plan = ZeroCopyCodecRegistry.compile(MyData)
    dc_req = Request.from_parts("POST", "/dc", body=b'{"name":"ada","age":36}')
    dc_val = dc_plan.decode_request(dc_req, format="json")
    assert dc_val == MyData(name="ada", age=36)
    assert (
        decode_grpc_frame(encode_grpc_frame_bytes(dc_val, format="json"), target_type=MyData, format="json") == dc_val
    )


def test_wasm_linear_memory_json_and_msgpack_bridge() -> None:
    """Verify WASMPluginHost.call_json, call_msgpack, and dispatch_http zero-copy buffer bridges."""
    host = WASMPluginHost()
    cfg = WASMModuleConfig(name="echo_guest", wasm_source=WASM_MAGIC_HEADER)
    host.register_module(
        cfg,
        exports={
            "echo_json": lambda mv: bytes(mv),
            "echo_mp": lambda mv: bytes(mv),
            "handle_http": lambda mv: encode_msgpack_bytes(
                WASMHttpResponse(status=201, headers=[("x-wasm", "1")], body=bytes(mv))
            ),
        },
    )
    out_json = host.call_json("echo_guest", "echo_json", MyStruct(id=9, data="wasm-j"), return_type=MyStruct)
    assert out_json == MyStruct(id=9, data="wasm-j")

    out_mp = host.call_msgpack("echo_guest", "echo_mp", MyStruct(id=12, data="wasm-m"), return_type=MyStruct)
    assert out_mp == MyStruct(id=12, data="wasm-m")

    req = Request.from_parts("POST", "/wasm", body=b"hello-wasm", headers=[("x-test", "ok")])
    http_resp = host.dispatch_http("echo_guest", req)
    assert http_resp.status == 201
    assert ("x-wasm", "1") in http_resp.headers


def test_wasm_missing_dependency_error_declares_wasmtime_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify register_module raises MissingDependencyError with extra='wasmtime' when _wasmtime is None."""
    monkeypatch.setattr(wasm_mod, "_wasmtime", None)
    host = WASMPluginHost()
    cfg = WASMModuleConfig(name="guest", wasm_source=WASM_MAGIC_HEADER)
    with pytest.raises(MissingDependencyError, match="wasmtime") as exc_info:
        host.register_module(cfg)
    assert exc_info.value.install_package == "vertebra-core[wasmtime]"


@pytest.mark.parametrize(
    "policy",
    [
        WASMSandboxPolicy(allow_network=True),
        WASMSandboxPolicy(allowed_preopen_dirs=("/tmp",)),
        WASMSandboxPolicy(env_vars={"API_KEY": "test-token-placeholder"}),
        WASMSandboxPolicy(epoch_timeout_ms=250),
    ],
)
def test_wasm_unsupported_policy_fails_closed_on_host_and_exports(
    monkeypatch: pytest.MonkeyPatch,
    policy: WASMSandboxPolicy,
) -> None:
    """Verify non-default WASMSandboxPolicy fields raise ImproperConfigurationError when unsupported."""

    class _MinimalConfig:
        def __init__(self) -> None:
            self.consume_fuel = False

    class _MinimalEngine:
        def __init__(self, config: Any) -> None:
            self.config = config

    class _MinimalModule:
        def __init__(self, engine: Any, raw: bytes) -> None:
            _ = (engine, raw)

    class _MinimalStore:
        def __init__(self, engine: Any) -> None:
            self.engine = engine

        def set_fuel(self, fuel: int) -> None:
            _ = fuel

        def set_limits(self, *, memory_size: int) -> None:
            _ = memory_size

    minimal_wt = type(
        "_MinimalWasmtime",
        (),
        {
            "Config": _MinimalConfig,
            "Engine": _MinimalEngine,
            "Module": _MinimalModule,
            "Store": _MinimalStore,
        },
    )
    monkeypatch.setattr(wasm_mod, "_wasmtime", minimal_wt)

    with pytest.raises(ImproperConfigurationError):
        WASMPluginHost(policy=policy)

    host = WASMPluginHost()
    cfg = WASMModuleConfig(name="exports_guest", wasm_source=WASM_MAGIC_HEADER, policy=policy)
    with pytest.raises(ImproperConfigurationError):
        host.register_module(cfg, exports={"ping": lambda: "pong"})


def test_wasm_host_enforces_wasi_limits_and_epoch_timer_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify WASMPluginHost configures fuel, limits, WASI preopens/env/network, and daemon epoch timers."""
    events: list[tuple[str, Any]] = []
    timers: list[Any] = []

    class _FakeConfig:
        def __init__(self) -> None:
            self.consume_fuel = False
            self.epoch_interruption = False

    class _FakeEngine:
        def __init__(self, config: _FakeConfig) -> None:
            self.config = config
            events.append(("engine_init", (config.consume_fuel, config.epoch_interruption)))

        def increment_epoch(self) -> None:
            events.append(("increment_epoch", None))

    class _FakeModule:
        def __init__(self, engine: _FakeEngine, raw: bytes) -> None:
            _ = (engine, raw)

    class _FakeWasiConfig:
        def __init__(self) -> None:
            self.inherit_env_called = False
            events.append(("wasi_init", None))

        def inherit_env(self) -> None:
            self.inherit_env_called = True
            events.append(("inherit_env", None))

        def inherit_network(self, allow: bool) -> None:
            events.append(("inherit_network", allow))

        def set_env(self, key: str, value: str) -> None:
            events.append(("set_env", (key, value)))

        def preopen_dir(self, host_path: str, guest_path: str) -> None:
            events.append(("preopen_dir", (host_path, guest_path)))

    class _FakeStore:
        def __init__(self, engine: _FakeEngine) -> None:
            self.engine = engine

        def set_fuel(self, fuel: int) -> None:
            events.append(("set_fuel", fuel))

        def set_limits(self, *, memory_size: int) -> None:
            events.append(("set_limits", memory_size))

        def set_epoch_deadline(self, ticks: int) -> None:
            events.append(("set_epoch_deadline", ticks))

        def set_wasi(self, wasi_cfg: _FakeWasiConfig) -> None:
            events.append(("set_wasi", wasi_cfg.inherit_env_called))

    class _FakeInstance:
        def __init__(self, store: _FakeStore, module: _FakeModule, imports: list[Any]) -> None:
            _ = (store, module, imports)
            events.append(("direct_instance", None))

        def exports(self, store: _FakeStore) -> dict[str, Any]:
            _ = store
            return {"ok": lambda _st, x: x + 1}

    class _FakeLinker:
        def __init__(self, engine: _FakeEngine) -> None:
            self.engine = engine

        def define_wasi(self) -> None:
            events.append(("define_wasi", None))

        def instantiate(self, store: _FakeStore, module: _FakeModule) -> _FakeInstance:
            _ = (store, module)
            events.append(("linker_instantiate", None))
            return _FakeInstance(store, module, [])

    class _FakeTimer:
        def __init__(self, interval: float, function: Any) -> None:
            self.interval = interval
            self.function = function
            self.daemon = False
            self.started = False
            self.cancelled = False
            timers.append(self)

        def start(self) -> None:
            self.started = True

        def cancel(self) -> None:
            self.cancelled = True

    fake_wt = type(
        "_FakeWasmtime",
        (),
        {
            "Config": _FakeConfig,
            "Engine": _FakeEngine,
            "Module": _FakeModule,
            "Store": _FakeStore,
            "WasiConfig": _FakeWasiConfig,
            "Linker": _FakeLinker,
            "Instance": _FakeInstance,
        },
    )
    monkeypatch.setattr(wasm_mod, "_wasmtime", fake_wt)
    monkeypatch.setattr("vertebra.wasm.threading.Timer", _FakeTimer)

    policy = WASMSandboxPolicy(
        max_memory_bytes=4096,
        max_fuel=5000,
        epoch_timeout_ms=150,
        allow_network=True,
        allowed_preopen_dirs=("/sandbox/in",),
        env_vars={"APP_ENV": "test-token-placeholder"},
    )
    host = WASMPluginHost(policy=policy)

    class _WasiInstance(_FakeInstance):
        def exports(self, store: _FakeStore) -> dict[str, Any]:
            _ = store
            return {
                "run": lambda _st, val: val * 2,
                "trap": lambda _st: (_ for _ in ()).throw(RuntimeError("epoch trap")),
            }

    def _wasi_instantiate(_self: _FakeLinker, s: _FakeStore, m: _FakeModule) -> _WasiInstance:
        events.append(("linker_instantiate", None))
        return _WasiInstance(s, m, [])

    monkeypatch.setattr(_FakeLinker, "instantiate", _wasi_instantiate)
    host.register_module(WASMModuleConfig(name="wasi_mod", wasm_source=WASM_MAGIC_HEADER, policy=policy))
    assert host.call_export("wasi_mod", "run", 21) == 42

    assert ("engine_init", (True, True)) in events
    assert ("set_fuel", 5000) in events
    assert ("set_limits", 4096) in events
    assert ("set_epoch_deadline", 1) in events
    assert ("inherit_network", True) in events
    assert ("set_env", ("APP_ENV", "test-token-placeholder")) in events
    assert ("preopen_dir", ("/sandbox/in", "/sandbox/in")) in events
    assert ("set_wasi", False) in events
    assert ("define_wasi", None) in events
    assert ("linker_instantiate", None) in events
    assert not any(ev[0] == "inherit_env" for ev in events)
    assert len(timers) == 1
    assert timers[0].daemon is True
    assert timers[0].interval == pytest.approx(0.15)
    assert timers[0].started is True
    assert timers[0].cancelled is True

    with pytest.raises(WASMExecutionError, match="epoch trap"):
        host.call_export("wasi_mod", "trap")
    assert len(timers) == 2
    assert timers[1].daemon is True
    assert timers[1].cancelled is True

    events.clear()
    host.register_module(WASMModuleConfig(name="pure_mod", wasm_source=WASM_MAGIC_HEADER))
    assert host.call_export("pure_mod", "ok", 10) == 11
    assert ("direct_instance", None) in events
    assert not any(ev[0] == "wasi_init" for ev in events)
