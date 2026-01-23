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
    port: 8081,
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

    # Start Bun server
    env = os.environ.copy()
    env["VERTEBRA_CONFIG"] = config_path
    
    # Need to make sure PYTHONPATH includes current dir for tmp.auth_test_app
    env["PYTHONPATH"] = f"{os.getcwd()}:{env.get('PYTHONPATH', '')}"
    
    proc = subprocess.Popen(
        ["bun", "run", "src/server/serve.ts"],
        cwd="src/ts",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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

        async with httpx.AsyncClient(base_url="http://localhost:8081", timeout=10.0) as client:
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
            stdout, stderr = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            print("Broker did not terminate, killing...")
            proc.kill()
            stdout, stderr = proc.communicate()
        
        print("\n--- BROKER STDOUT ---")
        print(stdout)
        print("--- BROKER STDERR ---")
        print(stderr)
        
        # Cleanup
        if os.path.exists(config_path):
            os.remove(config_path)
