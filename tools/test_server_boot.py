import multiprocessing
import sys
import time

import requests

# Ensure we can import vertebra
# sys.path.insert(0, os.path.abspath("src/py"))

try:
    from vertebra import _vertebra
except ImportError:
    # It might be installed in site-packages
    try:
        from vertebra import _vertebra
    except ImportError:
        # Try finding it in current directory if built there? No, uv run handles it.
        # Just fail.
        # Actually, let's print sys.path to help debug if it fails
        # print(sys.path)
        sys.exit(1)


def run_server() -> None:
    # This blocks
    _vertebra.start_http_server("127.0.0.1", 3000)


def main() -> None:
    p = multiprocessing.Process(target=run_server)
    p.start()

    # Wait for server to boot
    time.sleep(2)

    try:
        url = "http://127.0.0.1:3000"
        resp = requests.get(url, timeout=2)

        assert resp.status_code == 200
        assert resp.text == "Hello from Vertebra Rust"
    except Exception:
        sys.exit(1)
    finally:
        p.terminate()
        p.join(timeout=2)
        if p.is_alive():
            p.kill()


if __name__ == "__main__":
    main()
