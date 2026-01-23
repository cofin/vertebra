import os
import time
import subprocess
import httpx
import jwt
import pytest
import signal

@pytest.mark.anyio
@pytest.mark.integration
async def test_broker_jwt_auth_integration():
    # Setup config
    secret = "test-secret"
    config_content = f"""
export default {{
  server: {{
    port: 8085,
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
    config_path = os.path.abspath("tmp/vertebra.config.ts")
    with open(config_path, "w") as f:
        f.write(config_content)

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
    app_path = os.path.abspath("tmp/auth_test_app.py")
    init_path = os.path.abspath("tmp/__init__.py")
    
    with open(app_path, "w") as f:
        f.write(app_content)
    
    created_init = False
    if not os.path.exists(init_path):
        with open(init_path, "w") as f:
            f.write("")
        created_init = True

    # Start Bun server
    env = os.environ.copy()
    env["VERTEBRA_CONFIG"] = config_path
    env["VERTEBRA_NATIVE"] = "0"
    
    # Need to make sure PYTHONPATH includes current dir for tmp.auth_test_app
    env["PYTHONPATH"] = f"{os.getcwd()}:{env.get('PYTHONPATH', '')}"
    
    stdout_file = open("tmp/broker.stdout", "w")
    stderr_file = open("tmp/broker.stderr", "w")

    proc = subprocess.Popen(
        ["bun", "run", "src/server/serve.ts"],
        cwd="src/ts",
        env=env,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True
    )

    try:
        # Wait for start
        time.sleep(5)
        
        # Generate JWT
        token = jwt.encode(
            {
                "id": "user-123",
                "username": "jdoe",
                "iss": "test-issuer",
                "aud": "test-audience",
                "exp": int(time.time()) + 3600
            },
            secret,
            algorithm="HS256"
        )

        async with httpx.AsyncClient(base_url="http://localhost:8085", timeout=10.0) as client:
            # 1. No auth
            print("Requesting /me without auth...")
            resp = await client.get("/me")
            print(f"Response: {resp.status_code} {resp.text}")
            assert resp.status_code == 200
            assert resp.json()["is_authenticated"] is False

            # 2. Valid JWT
            print("Requesting /me with valid JWT...")
            resp = await client.get("/me", headers={"Authorization": f"Bearer {token}"})
            print(f"Response: {resp.status_code} {resp.text}")
            assert resp.status_code == 200
            data = resp.json()
            assert data["is_authenticated"] is True
            assert data["id"] == "user-123"
            assert data["username"] == "jdoe"

            # 3. Invalid JWT
            print("Requesting /me with invalid JWT...")
            resp = await client.get("/me", headers={"Authorization": "Bearer invalid"})
            print(f"Response: {resp.status_code} {resp.text}")
            assert resp.status_code == 200
            assert resp.json()["is_authenticated"] is False

    except Exception as e:
        print(f"TEST FAILURE: {e}")
        raise
    finally:
        # Stop broker
        print("Stopping broker...")
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            print("Broker did not terminate, killing...")
            proc.kill()
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                print("Broker process stuck even after kill")
        
        stdout_file.close()
        stderr_file.close()
        
        with open("tmp/broker.stdout", "r") as f:
            print("\n--- BROKER STDOUT ---")
            print(f.read())
        with open("tmp/broker.stderr", "r") as f:
            print("--- BROKER STDERR ---")
            print(f.read())
        
        # Cleanup
        if os.path.exists(config_path):
            os.remove(config_path)
        if os.path.exists(app_path):
            os.remove(app_path)
        if created_init and os.path.exists(init_path):
            os.remove(init_path)
