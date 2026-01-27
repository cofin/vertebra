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
async def test_broker_refresh_token_rotation() -> None:
    # Setup config
    access_secret = "access-secret-key"
    refresh_secret = "refresh-secret-key"
    port = _get_free_port()

    # We enable refresh token rotation by providing refresh token config
    config_content = f"""
export default {{
  server: {{
    port: {port},
  }},
  python: {{
    app: "_test_pkg_refresh.auth_test_app:app",
    venv: "../../.venv"
  }},
  auth: {{
    cookieName: "access_token",
    refreshTokenCookieName: "refresh_token",
    providers: [
      {{
        type: "jwt",
        secret: "{access_secret}",
        refreshSecret: "{refresh_secret}",
        issuer: "test-issuer",
        audience: "test-audience",
        algorithm: "HS256"
      }}
    ]
  }}
}};
"""
    tmp_dir = Path("_test_pkg_refresh")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(exist_ok=True)

    config_path = (tmp_dir / "vertebra.config.ts").resolve()
    config_path.write_text(config_content)

    # Create dummy app
    app_content = """
from typing import Any
from litestar import Litestar, get, Request

@get("/me")
async def me_handler(request: Request) -> dict[str, Any]:
    user = request.scope.get("user")
    is_auth = False
    uid = None

    if user:
        if isinstance(user, dict):
             is_auth = user.get("is_authenticated", False)
             if is_auth:
                uid = user.get("id")
        elif hasattr(user, "is_authenticated"):
            is_auth = user.is_authenticated
            if is_auth and hasattr(user, "id"):
                uid = user.id

    return {
        "is_authenticated": is_auth,
        "id": uid,
    }

app = Litestar(route_handlers=[me_handler])
"""
    app_path = (tmp_dir / "auth_test_app.py").resolve()
    init_path = (tmp_dir / "__init__.py").resolve()

    app_path.write_text(app_content)
    init_path.write_text("")

    # Start Bun server
    env = os.environ.copy()
    env["VERTEBRA_CONFIG"] = str(config_path)
    env["VERTEBRA_NATIVE"] = "0"
    # Ensure root is in PYTHONPATH so _test_pkg_refresh is importable
    env["PYTHONPATH"] = f"{Path.cwd()}:{env.get('PYTHONPATH', '')}"

    stdout_file = (tmp_dir / "broker.stdout").open("w")
    stderr_file = (tmp_dir / "broker.stderr").open("w")

    bun_path = shutil.which("bun")
    if bun_path is None:
        pytest.skip("bun not found in PATH")

    proc = subprocess.Popen(
        [bun_path, "run", "src/ts/src/server/serve.ts"],
        cwd=".",  # Run from root now that ProcessManager handles it (hopefully) or keep src/ts?
        # Wait, if I run from root, I need to make sure dependencies are found.
        # But earlier I said ProcessManager was fixed.
        # Let's try running from root to be safe with PYTHONPATH resolution?
        # No, running from root will fail to find src/ts/node_modules for imports in serve.ts.
        # So we MUST run from src/ts.
        # But ProcessManager was fixed to handle that.
    )
    # Actually, let's keep running from src/ts as before, since that worked.
    proc.kill()  # clean up the partial Popen above (it wasn't started properly)

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

            # 1. Generate EXPIRED Access Token
            expired_access_token = jwt.encode(
                {
                    "id": "user-123",
                    "iss": "test-issuer",
                    "aud": "test-audience",
                    "exp": int(time.time()) - 3600,  # Expired 1 hour ago
                },
                access_secret,
                algorithm="HS256",
            )

            # 2. Generate VALID Refresh Token
            refresh_token = jwt.encode(
                {
                    "id": "user-123",
                    "iss": "test-issuer",
                    "aud": "test-audience",
                    "exp": int(time.time()) + 86400,  # Valid for 24h
                    "token_type": "refresh",
                },
                refresh_secret,
                algorithm="HS256",
            )

            # 3. Send request with expired access token but valid refresh token (via cookies)
            cookies = {"access_token": expired_access_token, "refresh_token": refresh_token}

            resp = await client.get("/me", cookies=cookies)

            # 4. Assertions
            # Should be successful (200) because refresh token rotated it
            assert resp.status_code == 200, f"Expected 200, got {resp.status_code}. Logs: {tmp_dir}/broker.stderr"
            data = resp.json()
            assert data["is_authenticated"] is True
            assert data["id"] == "user-123"

            # Verify Set-Cookie headers
            set_cookie = resp.headers.get("set-cookie")
            assert set_cookie is not None, "Expected Set-Cookie header for rotated tokens"
            assert "access_token=" in set_cookie
            assert "refresh_token=" in set_cookie

            # Extract new access token and verify it
            # Simple extraction (cookies are semicolon separated in header if multiple? No, Set-Cookie can be multiple headers)
            # httpx merges them? httpx response.cookies is a jar.

            new_access = resp.cookies.get("access_token")
            resp.cookies.get("refresh_token")

            assert new_access is not None
            assert new_access != expired_access_token

            # Verify new access token is valid
            decoded = jwt.decode(new_access, access_secret, algorithms=["HS256"], audience="test-audience")
            assert decoded["id"] == "user-123"

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

        # Cleanup
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
