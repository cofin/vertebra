import asyncio
import contextlib
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import httpx
import jwt
import pytest


async def _wait_for_broker_ready(proc: subprocess.Popen[str], client: httpx.AsyncClient, timeout: float = 20.0) -> None:  # noqa: ASYNC109
    start = time.monotonic()
    while True:
        if proc.poll() is not None:
            msg = "Broker process exited before becoming ready."
            raise RuntimeError(msg)
        try:
            resp = await client.get("/me", timeout=2.0)
            if resp.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        if time.monotonic() - start >= timeout:
            msg = "Broker did not become ready in time."
            raise TimeoutError(msg)
        await asyncio.sleep(0.5)


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.anyio
@pytest.mark.integration
async def test_broker_jwt_auth_integration() -> None:
    # Setup config
    secret = "test-secret"
    port = _get_free_port()
    config_content = f"""
export default {{
  server: {{
    port: {port},
  }},
  python: {{
    app: "tmp.auth_test_app:app",
    venv: "../../.venv"
  }},
  auth: {{
    providers: [
      {{
        type: "jwt",
        secret: "{secret}",
        issuer: "test-issuer",
        audience: "test-audience"
      }}
    ]
  }}
}};
"""
    tmp_dir = Path("tmp")
    tmp_dir.mkdir(exist_ok=True)

    config_path = (tmp_dir / "vertebra.config.ts").resolve()
    config_path.write_text(config_content)

    # Create dummy app
    app_content = """
from typing import Any
from litestar import Litestar, get, Request

@get("/me")
async def me_handler(request: Request) -> dict[str, Any]:
    try:
        user = request.scope.get("user")

        is_auth = False
        uid = None
        username = None

        if user:
            # Check if it has attributes or is dict
            if hasattr(user, "is_authenticated"):
                is_auth = user.is_authenticated
            elif isinstance(user, dict):
                 is_auth = user.get("is_authenticated", False)

            if is_auth:
                if hasattr(user, "id"):
                    uid = user.id
                    username = getattr(user, "username", None)
                elif isinstance(user, dict):
                    uid = user.get("id")
                    username = user.get("username")

        return {
            "is_authenticated": is_auth,
            "id": uid,
            "username": username
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e), "trace": traceback.format_exc()}

app = Litestar(route_handlers=[me_handler])
"""
    app_path = (tmp_dir / "auth_test_app.py").resolve()
    init_path = (tmp_dir / "__init__.py").resolve()

    app_path.write_text(app_content)

    created_init = False
    if not init_path.exists():
        init_path.write_text("")
        created_init = True

    # Start Bun server
    env = os.environ.copy()
    env["VERTEBRA_CONFIG"] = str(config_path)
    env["VERTEBRA_NATIVE"] = "0"

    # Need to make sure PYTHONPATH includes current dir for tmp.auth_test_app
    env["PYTHONPATH"] = f"{Path.cwd()}:{env.get('PYTHONPATH', '')}"

    stdout_file = (tmp_dir / "broker.stdout").open("w")
    stderr_file = (tmp_dir / "broker.stderr").open("w")

    bun_path = shutil.which("bun")
    if bun_path is None:
        pytest.skip("bun not found in PATH")

    proc = subprocess.Popen(
        [bun_path, "run", "src/server/serve.ts"],
        cwd="src/ts",
        env=env,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
    )

    try:
        async with httpx.AsyncClient(base_url=f"http://localhost:{port}", timeout=10.0) as client:
            await _wait_for_broker_ready(proc, client)

            # Generate JWT
            token = jwt.encode(
                {
                    "id": "user-123",
                    "username": "jdoe",
                    "iss": "test-issuer",
                    "aud": "test-audience",
                    "exp": int(time.time()) + 3600,
                },
                secret,
                algorithm="HS256",
            )

            # 1. No auth
            resp = await client.get("/me")
            assert resp.status_code == 200
            assert resp.json()["is_authenticated"] is False

            # 2. Valid JWT
            resp = await client.get("/me", headers={"Authorization": f"Bearer {token}"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["is_authenticated"] is True
            assert data["id"] == "user-123"
            assert data["username"] == "jdoe"

            # 3. Invalid JWT
            resp = await client.get("/me", headers={"Authorization": "Bearer invalid"})
            assert resp.status_code == 200
            assert resp.json()["is_authenticated"] is False

    finally:
        # Stop broker
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=1)

        stdout_file.close()
        stderr_file.close()

        with (tmp_dir / "broker.stdout").open("r"):
            pass
        with (tmp_dir / "broker.stderr").open("r"):
            pass

        # Cleanup
        if config_path.exists():
            config_path.unlink()
        if app_path.exists():
            app_path.unlink()
        if created_init and init_path.exists():
            init_path.unlink()
