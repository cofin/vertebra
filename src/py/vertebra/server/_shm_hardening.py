"""Container SHM hardening: anonymous `memfd_create` fallback and `SCM_RIGHTS` descriptor passing."""

from __future__ import annotations

import array
import os
import socket
import tempfile
from dataclasses import dataclass

__all__ = (
    "ShmAllocationPolicy",
    "allocate_anonymous_shm_fd",
    "recv_fd_over_unix_socket",
    "send_fd_over_unix_socket",
)


@dataclass(frozen=True, slots=True)
class ShmAllocationPolicy:
    """Shared-memory allocation policy for Kubernetes (`64 MiB` `/dev/shm`) and read-only rootfs."""

    prefer_memfd: bool = True
    shm_dir: str = "/dev/shm"
    min_free_bytes: int = 16 * 1024 * 1024


def allocate_anonymous_shm_fd(name: str, size_bytes: int) -> int:
    """Allocate an anonymous shared-memory file descriptor via ``os.memfd_create`` or ``TemporaryFile``.

    Avoids ``/dev/shm`` quota exhaustion in Kubernetes pods (`64 MiB` default `emptyDir`) and works
    under ``readOnlyRootFilesystem: true`` by creating an unlinked anonymous RAM-backed file descriptor
    that can be passed to worker processes over ``SCM_RIGHTS``.
    """
    if size_bytes <= 0:
        msg = f"size_bytes must be > 0 (got {size_bytes})"
        raise ValueError(msg)
    memfd_fn = getattr(os, "memfd_create", None)
    if callable(memfd_fn):
        fd = int(memfd_fn(f"vertebra-{name}", os.MFD_CLOEXEC))
    else:
        with tempfile.TemporaryFile() as tmp:
            fd = os.dup(tmp.fileno())
    os.ftruncate(fd, size_bytes)
    return fd


def send_fd_over_unix_socket(sock: socket.socket, fd: int, payload: bytes = b"VTB_SHM") -> int:
    """Send a file descriptor to a peer process over a ``AF_UNIX`` socket using ``SCM_RIGHTS``."""
    fds = array.array("i", [fd])
    return sock.sendmsg([payload], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)])


def recv_fd_over_unix_socket(sock: socket.socket, max_payload_bytes: int = 64) -> tuple[int, bytes]:
    """Receive a file descriptor and payload from a ``AF_UNIX`` socket via ``SCM_RIGHTS``."""
    fds = array.array("i")
    msg, ancdata, _flags, _addr = sock.recvmsg(max_payload_bytes, socket.CMSG_LEN(fds.itemsize))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds.frombytes(cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
    if not fds:
        err = "No file descriptor received in SCM_RIGHTS ancillary data"
        raise RuntimeError(err)
    return int(fds[0]), msg
