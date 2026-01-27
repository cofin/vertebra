import multiprocessing
import os
import sys
import time

# Ensure we can import vertebra
# sys.path.insert(0, os.path.abspath("src/py"))

try:
    from vertebra import _vertebra
except ImportError:
    sys.exit(1)


def run_server() -> None:
    # Use current python executable
    python_path = sys.executable

    # Start server with 1 worker
    # host, port, app_path, python_path, workers
    _vertebra.start_http_server("127.0.0.1", 3001, "test:app", python_path, 1)


def main() -> None:
    # Clean up previous run
    if os.path.exists("/tmp/vertebra_worker_test.txt"):
        os.remove("/tmp/vertebra_worker_test.txt")

    p = multiprocessing.Process(target=run_server)
    p.start()

    # Wait for server and worker to start
    for _ in range(10):
        if os.path.exists("/tmp/vertebra_worker_test.txt"):
            break
        time.sleep(0.5)

    try:
        if os.path.exists("/tmp/vertebra_worker_test.txt"):
            with open("/tmp/vertebra_worker_test.txt") as f:
                content = f.read()

            assert "notify=" in content
            assert "req=" in content
            assert "resp=" in content
        else:
            sys.exit(1)

    finally:
        p.terminate()
        p.join(timeout=2)
        if p.is_alive():
            p.kill()


if __name__ == "__main__":
    main()
