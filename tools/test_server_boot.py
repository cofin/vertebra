import time
import requests
import multiprocessing
import sys
import os

# Ensure we can import vertebra
# sys.path.insert(0, os.path.abspath("src/py"))

try:
    from vertebra import _vertebra
except ImportError:
    # It might be installed in site-packages
    try:
        import vertebra._vertebra as _vertebra
    except ImportError:
        print("Could not import vertebra._vertebra. Is it installed?")
        # Try finding it in current directory if built there? No, uv run handles it.
        # Just fail.
        # Actually, let's print sys.path to help debug if it fails
        # print(sys.path)
        sys.exit(1)

def run_server():
    print("Starting server on port 3000...")
    # This blocks
    _vertebra.start_http_server("127.0.0.1", 3000)

def main():
    p = multiprocessing.Process(target=run_server)
    p.start()
    
    # Wait for server to boot
    time.sleep(2)
    
    try:
        url = "http://127.0.0.1:3000"
        print(f"Connecting to {url}...")
        resp = requests.get(url, timeout=2)
        print(f"Response Status: {resp.status_code}")
        print(f"Response Body: {resp.text}")
        
        assert resp.status_code == 200
        assert resp.text == "Hello from Vertebra Rust"
        print("SUCCESS: Server responded correctly.")
    except Exception as e:
        print(f"FAILURE: {e}")
        sys.exit(1)
    finally:
        print("Terminating server...")
        p.terminate()
        p.join(timeout=2)
        if p.is_alive():
            print("Force killing...")
            p.kill()

if __name__ == "__main__":
    main()
