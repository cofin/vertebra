from vertebra._vertebra import Router


def test_rust_router_basic() -> None:
    router = Router()
    router.insert("/hello", 1)
    router.insert("/echo", 2)
    router.insert("/user/:id", 3)

    assert router.match_path("/hello") == 1
    assert router.match_path("/echo") == 2
    assert router.match_path("/user/123") == 3
    assert router.match_path("/notfound") is None


def test_rust_router_invalid_path() -> None:
    router = Router()
    # matchit might raise error for invalid routes if it detects them at insert time
    # but matchit is generally forgiving.
    # We just check that it doesn't crash.
    router.insert("/", 100)
    assert router.match_path("/") == 100
