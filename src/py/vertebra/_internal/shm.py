"""Shared memory CFFI bindings for the Vertebra broker transport."""

import os
import sys
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING

from cffi import FFI

if TYPE_CHECKING:
    from collections.abc import Iterator


class ShmError(RuntimeError):
    """Raised for shared memory binding errors."""


class VtbStatus(IntEnum):
    ok = 0
    invalid = 1
    not_ready = 2
    timeout = 3
    full = 4
    empty = 5
    sys = 6


def _lib_name() -> str:
    if sys.platform == "darwin":
        return "libvertebra_broker.dylib"
    if sys.platform == "win32":
        return "vertebra_broker.dll"
    return "libvertebra_broker.so"


def _candidate_paths() -> "Iterator[Path]":
    env = os.environ.get("VERTEBRA_SHM_LIB")
    if env:
        yield Path(env)
    package_root = Path(__file__).resolve().parents[1]
    yield package_root / "bin" / _lib_name()
    yield package_root / _lib_name()
    repo_root = package_root.parents[2]
    yield repo_root / "src" / "rs" / "target" / "debug" / _lib_name()
    yield repo_root / "src" / "rs" / "target" / "release" / _lib_name()
    yield repo_root / "target" / "debug" / _lib_name()
    yield repo_root / "target" / "release" / _lib_name()


def _load_lib() -> "tuple[FFI, object]":
    ffi = FFI()
    ffi.cdef(
        """
        typedef enum {
            VTB_STATUS_OK = 0,
            VTB_STATUS_INVALID = 1,
            VTB_STATUS_NOT_READY = 2,
            VTB_STATUS_TIMEOUT = 3,
            VTB_STATUS_FULL = 4,
            VTB_STATUS_EMPTY = 5,
            VTB_STATUS_SYS = 6
        } VtbStatus;

        typedef struct VtbRing VtbRing;
        typedef struct VtbRegion VtbRegion;
        typedef struct {
            size_t slot_size;
            size_t slots;
            uint32_t read_head;
            uint32_t write_head;
            size_t capacity;
        } VtbRingStats;

        const char* vtb_last_error_message(void);
        size_t vtb_ring_capacity(const VtbRing* handle);
        VtbStatus vtb_ring_stats(const VtbRing* handle, VtbRingStats* out_stats);
        VtbStatus vtb_ring_create(const char* name, size_t slot_size, size_t slots, VtbRing** out_handle);
        VtbStatus vtb_ring_open(const char* name, size_t slot_size, size_t slots, VtbRing** out_handle);
        VtbStatus vtb_ring_send(VtbRing* handle, const uint8_t* data, size_t len, uint32_t flags, uint32_t timeout_ms);
        VtbStatus vtb_ring_recv(VtbRing* handle, uint8_t* out, size_t capacity, size_t* out_len, uint32_t* out_flags, uint32_t timeout_ms);
        void vtb_ring_close(VtbRing* handle);

        VtbStatus vtb_region_create(const char* name, size_t size, VtbRegion** out_handle);
        VtbStatus vtb_region_open(const char* name, size_t size, VtbRegion** out_handle);
        void* vtb_region_ptr(VtbRegion* handle);
        size_t vtb_region_len(VtbRegion* handle);
        void vtb_region_close(VtbRegion* handle);
        """
    )
    for path in _candidate_paths():
        if path.exists():
            return ffi, ffi.dlopen(str(path))
    raise ShmError("Unable to locate shared memory C ABI library. Set VERTEBRA_SHM_LIB.")


_FFI: FFI | None = None
_LIB: object | None = None
_LOAD_ERROR: Exception | None = None

try:
    _FFI, _LIB = _load_lib()
    SHM_AVAILABLE = True
except ShmError as exc:
    _LOAD_ERROR = exc
    SHM_AVAILABLE = False


def _get_lib() -> "tuple[FFI, object]":
    if not SHM_AVAILABLE or _FFI is None or _LIB is None:
        message = str(_LOAD_ERROR) if _LOAD_ERROR else "Shared memory library unavailable"
        raise ShmError(message)
    return _FFI, _LIB


def _raise_for_status(status: int) -> None:
    if status == VtbStatus.ok:
        return
    ffi, lib = _get_lib()
    message_ptr = lib.vtb_last_error_message()
    if message_ptr:
        message = ffi.string(message_ptr).decode("utf-8", "replace")
    else:
        message = "Shared memory operation failed"
    if status in (VtbStatus.timeout, VtbStatus.full, VtbStatus.empty):
        raise TimeoutError(f"{message} (status={int(status)})")
    raise ShmError(f"{message} (status={int(status)})")


@dataclass(slots=True)
class ShmRingConfig:
    name: str
    slot_size: int = 65536
    slots: int = 1024


class ShmRing:
    def __init__(self, handle: object, capacity: int) -> None:
        self._handle = handle
        self._capacity = capacity

    @classmethod
    def create(cls, config: "ShmRingConfig") -> "ShmRing":
        ffi, lib = _get_lib()
        handle_ptr = ffi.new("VtbRing**")
        status = lib.vtb_ring_create(config.name.encode("utf-8"), config.slot_size, config.slots, handle_ptr)
        _raise_for_status(VtbStatus(int(status)))
        handle = handle_ptr[0]
        capacity = int(lib.vtb_ring_capacity(handle))
        return cls(handle, capacity)

    @classmethod
    def open(cls, config: "ShmRingConfig") -> "ShmRing":
        ffi, lib = _get_lib()
        handle_ptr = ffi.new("VtbRing**")
        status = lib.vtb_ring_open(config.name.encode("utf-8"), config.slot_size, config.slots, handle_ptr)
        _raise_for_status(VtbStatus(int(status)))
        handle = handle_ptr[0]
        capacity = int(lib.vtb_ring_capacity(handle))
        return cls(handle, capacity)

    @property
    def capacity(self) -> int:
        return self._capacity

    def send(self, payload: bytes, flags: int = 0, timeout_ms: int = 0) -> None:
        ffi, lib = _get_lib()
        buf = ffi.new("uint8_t[]", payload)
        status = lib.vtb_ring_send(self._handle, buf, len(payload), flags, timeout_ms)
        _raise_for_status(VtbStatus(int(status)))

    def recv(self, timeout_ms: int = 0) -> "tuple[bytes, int]":
        ffi, lib = _get_lib()
        out_len = ffi.new("size_t*")
        out_flags = ffi.new("uint32_t*")
        buffer = ffi.new("uint8_t[]", self._capacity)
        status = VtbStatus(int(lib.vtb_ring_recv(self._handle, buffer, self._capacity, out_len, out_flags, timeout_ms)))
        if status in (VtbStatus.timeout, VtbStatus.empty):
            raise TimeoutError("Shared memory receive timed out")
        _raise_for_status(status)
        data = ffi.buffer(buffer, out_len[0])[:]
        return data, int(out_flags[0])

    def stats(self) -> "dict[str, int]":
        ffi, lib = _get_lib()
        stats = ffi.new("VtbRingStats*")
        status = lib.vtb_ring_stats(self._handle, stats)
        _raise_for_status(VtbStatus(int(status)))
        return {
            "slot_size": int(stats.slot_size),
            "slots": int(stats.slots),
            "read_head": int(stats.read_head),
            "write_head": int(stats.write_head),
            "capacity": int(stats.capacity),
        }

    def close(self) -> None:
        if self._handle is None:
            return
        _get_lib()[1].vtb_ring_close(self._handle)
        self._handle = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


@dataclass(slots=True)
class ShmRegionConfig:
    name: str
    size: int


class ShmRegion:
    def __init__(self, handle: object) -> None:
        self._handle = handle

    @classmethod
    def create(cls, config: "ShmRegionConfig") -> "ShmRegion":
        ffi, lib = _get_lib()
        handle_ptr = ffi.new("VtbRegion**")
        status = lib.vtb_region_create(config.name.encode("utf-8"), config.size, handle_ptr)
        _raise_for_status(VtbStatus(int(status)))
        return cls(handle_ptr[0])

    @classmethod
    def open(cls, config: "ShmRegionConfig") -> "ShmRegion":
        ffi, lib = _get_lib()
        handle_ptr = ffi.new("VtbRegion**")
        status = lib.vtb_region_open(config.name.encode("utf-8"), config.size, handle_ptr)
        _raise_for_status(VtbStatus(int(status)))
        return cls(handle_ptr[0])

    def buffer(self) -> memoryview:
        ffi, lib = _get_lib()
        ptr = lib.vtb_region_ptr(self._handle)
        size = int(lib.vtb_region_len(self._handle))
        buf = ffi.buffer(ptr, size)
        return memoryview(buf)

    def close(self) -> None:
        if self._handle is None:
            return
        _get_lib()[1].vtb_region_close(self._handle)
        self._handle = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


__all__ = (
    "SHM_AVAILABLE",
    "ShmError",
    "ShmRing",
    "ShmRingConfig",
    "ShmRegion",
    "ShmRegionConfig",
)
