from __future__ import annotations

import os


async def app(scope, receive, send) -> None:
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
        return

    if scope["type"] != "http":
        return

    path = scope.get("path", "/")
    test_file = os.getenv("VERTEBRA_TEST_FILE")
    if path == "/file" and test_file:
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.pathsend", "path": test_file})
        return

    worker_id = os.getenv("VERTEBRA_WORKER_ID", "0").encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": worker_id, "more_body": False})
