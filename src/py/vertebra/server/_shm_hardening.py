"""Container SHM hardening: anonymous `memfd_create` fallback and `SCM_RIGHTS` descriptor passing."""

import array
import os
import socket
import tempfile

import msgspec

__all__ = ("ShmAllocationPolicy",)


class ShmAllocationPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Shared-memory allocation policy for Kubernetes (`64 MiB` `/dev/shm`) and read-only rootfs."""

    prefer_memfd: bool = True
    shm_dir: str = "/dev/shm"
    min_free_bytes: int = 16 * 1024 * 1024

    def allocate_anonymous_shm_fd(self, name: str, size: int) -> tuple[int, str]:
        """Allocate an anonymous shared-memory file descriptor and return ``(fd, backend)``."""
        if size <= 0:
            msg = f"size must be > 0 (got {size})"
            raise ValueError(msg)
        memfd_fn = getattr(os, "memfd_create", None)
        if self.prefer_memfd and callable(memfd_fn):
            fd = int(memfd_fn(f"vertebra-{name}", os.MFD_CLOEXEC))
            backend = "memfd"
        else:
            with tempfile.TemporaryFile() as tmp:
                fd = os.dup(tmp.fileno())
            backend = "tmpfile"
        os.ftruncate(fd, size)
        return fd, backend

    @classmethod
    def allocate_fd(
        cls,
        name: str,
        size: int,
        policy: "ShmAllocationPolicy | None" = None,
    ) -> tuple[int, str]:
        """Allocate an anonymous shared-memory file descriptor using ``policy`` or defaults."""
        active_policy = policy if policy is not None else cls()
        return active_policy.allocate_anonymous_shm_fd(name, size)

    @staticmethod
    def send_fd(sock: socket.socket, fd: int, payload: bytes = b"VTB_SHM") -> None:
        """Send a file descriptor to a peer process over an ``AF_UNIX`` socket using ``SCM_RIGHTS``."""
        fds = array.array("i", [fd])
        sock.sendmsg([payload], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)])

    @staticmethod
    def recv_fd(sock: socket.socket, max_payload_bytes: int = 64) -> int:
        """Receive a file descriptor from an ``AF_UNIX`` socket via ``SCM_RIGHTS``."""
        fds = array.array("i")
        _msg, ancdata, _flags, _addr = sock.recvmsg(
            max_payload_bytes,
            socket.CMSG_LEN(fds.itemsize),
        )
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                fds.frombytes(cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
        if not fds:
            err = "No file descriptor received in SCM_RIGHTS ancillary data"
            raise RuntimeError(err)
        return int(fds[0])
