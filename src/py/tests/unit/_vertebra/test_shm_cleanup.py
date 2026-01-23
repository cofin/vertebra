import pytest

from vertebra._vertebra import ShmRegion


@pytest.mark.unit
def test_shm_cleanup_on_close() -> None:
    name = "/vertebra-test-cleanup"
    region = ShmRegion.create_named(name, 1024)
    region.close()

    # Verify it's gone (or at least handle closed)
    # Note: ShmRegion.close() unlinks it.

    with pytest.raises(RuntimeError):
        ShmRegion.open_named(name, 1024)


@pytest.mark.unit
def test_shm_cleanup_fixture() -> None:
    # Verify our fixture logic works
    pass
