"""IPC transport client for Bun-backed drivers."""

from __future__ import annotations

import asyncio
import os
import shutil
from dataclasses import dataclass
from typing import Any, Literal

from vertebra._internal.serialization import MessagePackCodec


class BunRuntimeError(RuntimeError):
    """Raised when Bun runtime is required but unavailable."""


class IpcError(RuntimeError):
    """Raised for IPC transport errors."""


@dataclass(frozen=True, slots=True)
class IpcConfig:
    """Configuration for IPC transport."""

    transport: Literal["uds", "tcp"] = "uds"
    path: str | None = None
    host: str = "127.0.0.1"
    port: int = 7001
    timeout: float = 5.0
    enforce_bun: bool = True
    retries: int = 1
    retry_delay: float = 0.05


def require_bun() -> None:
    """Ensure Bun runtime is available for Bun-only drivers."""

    if shutil.which("bun") is None:
        raise BunRuntimeError(
            "Bun runtime is required for Bun-backed drivers. "
            "Install Bun or set VERTEBRA_BUN_SKIP_CHECK=1 to bypass in tests."
        )


class IpcClient:
    """Simple framed IPC client using MessagePack serialization."""

    def __init__(self, config: IpcConfig | None = None, codec: MessagePackCodec | None = None) -> None:
        self._config = config or IpcConfig()
        self._codec = codec or MessagePackCodec()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        """Open a connection to the Bun IPC server."""

        if self._config.enforce_bun and os.getenv("VERTEBRA_BUN_SKIP_CHECK") != "1":
            require_bun()
        if self._config.transport == "uds":
            if not self._config.path:
                raise IpcError("UDS transport requires a socket path")
            self._reader, self._writer = await asyncio.open_unix_connection(self._config.path)
        else:
            self._reader, self._writer = await asyncio.open_connection(
                self._config.host, self._config.port
            )

    async def close(self) -> None:
        """Close the IPC connection."""

        if self._writer is None:
            return
        self._writer.close()
        try:
            await self._writer.wait_closed()
        finally:
            self._reader = None
            self._writer = None

    async def request(self, payload: Any) -> Any:
        """Send a request and await the response."""

        last_error: Exception | None = None
        for attempt in range(self._config.retries + 1):
            try:
                if self._writer is None or self._reader is None:
                    await self.connect()
                if self._writer is None or self._reader is None:
                    raise IpcError("IPC connection unavailable")
                encoded = self._codec.encode(payload)
                frame = len(encoded).to_bytes(4, "big") + encoded
                self._writer.write(frame)
                await self._writer.drain()
                header = await asyncio.wait_for(
                    self._reader.readexactly(4), timeout=self._config.timeout
                )
                size = int.from_bytes(header, "big")
                data = await asyncio.wait_for(
                    self._reader.readexactly(size), timeout=self._config.timeout
                )
                return self._codec.decode(data)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                await self.close()
                if attempt < self._config.retries:
                    await asyncio.sleep(self._config.retry_delay)
                    continue
                raise IpcError("IPC request failed") from exc
        raise IpcError("IPC request failed") from last_error


@dataclass(slots=True)
class _IpcConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class IpcPool:
    """Simple connection pool for IPC requests."""

    def __init__(self, config: IpcConfig | None = None, max_size: int = 10) -> None:
        self._config = config or IpcConfig()
        self._codec = MessagePackCodec()
        self._max_size = max_size
        self._pool: asyncio.LifoQueue[_IpcConnection] = asyncio.LifoQueue()
        self._total = 0
        self._lock = asyncio.Lock()

    async def _create_connection(self) -> _IpcConnection:
        client = IpcClient(self._config, self._codec)
        await client.connect()
        if client._reader is None or client._writer is None:
            raise IpcError("IPC connection unavailable")
        return _IpcConnection(client._reader, client._writer)

    async def acquire(self) -> _IpcConnection:
        try:
            return self._pool.get_nowait()
        except asyncio.QueueEmpty:
            async with self._lock:
                if self._total < self._max_size:
                    self._total += 1
                    return await self._create_connection()
            return await self._pool.get()

    async def release(self, connection: _IpcConnection) -> None:
        if connection.writer.is_closing():
            async with self._lock:
                self._total = max(0, self._total - 1)
            return
        await self._pool.put(connection)

    async def request(self, payload: Any) -> Any:
        connection = await self.acquire()
        try:
            encoded = self._codec.encode(payload)
            frame = len(encoded).to_bytes(4, "big") + encoded
            connection.writer.write(frame)
            await connection.writer.drain()
            header = await asyncio.wait_for(
                connection.reader.readexactly(4), timeout=self._config.timeout
            )
            size = int.from_bytes(header, "big")
            data = await asyncio.wait_for(
                connection.reader.readexactly(size), timeout=self._config.timeout
            )
            return self._codec.decode(data)
        finally:
            await self.release(connection)


__all__ = [
    "BunRuntimeError",
    "IpcConfig",
    "IpcClient",
    "IpcError",
    "IpcPool",
    "require_bun",
]
