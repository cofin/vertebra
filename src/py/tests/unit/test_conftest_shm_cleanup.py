"""Tests for role-scoped shared-memory cleanup in tests.conftest."""

from pathlib import Path

from tests import conftest as tests_conftest

_SAMPLE_SHM_NAMES = (
    "test-aaaa1111",
    "test-aaaa1111.lock",
    "test-bbbb2222",
    "vtb-live-1",
    "my-vertebra-seg",
)


def _touch_entries(root: Path, names: tuple[str, ...]) -> None:
    """Create empty files under root for each name."""
    for name in names:
        (root / name).touch()


def test_worker_sweep_keeps_segments_it_did_not_issue(tmp_path: Path) -> None:
    """Worker post-test sweep removes only segments matching issued names."""
    _touch_entries(tmp_path, _SAMPLE_SHM_NAMES)

    patterns = tests_conftest._post_test_patterns(["test-aaaa1111"], is_worker=True)
    removed = tests_conftest._sweep_shm(tmp_path, patterns)

    assert removed == 2
    assert not (tmp_path / "test-aaaa1111").exists()
    assert not (tmp_path / "test-aaaa1111.lock").exists()
    assert (tmp_path / "test-bbbb2222").exists()
    assert (tmp_path / "vtb-live-1").exists()
    assert (tmp_path / "my-vertebra-seg").exists()


def test_worker_without_issued_names_sweeps_nothing(tmp_path: Path) -> None:
    """Worker with no issued shm_name returns no patterns and removes nothing."""
    _touch_entries(tmp_path, _SAMPLE_SHM_NAMES)

    patterns = tests_conftest._post_test_patterns([], is_worker=True)
    removed = tests_conftest._sweep_shm(tmp_path, patterns)

    assert patterns == ()
    assert removed == 0
    for name in _SAMPLE_SHM_NAMES:
        assert (tmp_path / name).exists()


def test_controller_sweep_uses_broad_patterns(tmp_path: Path) -> None:
    """Controller post-test sweep uses broad patterns and preserves unrelated files."""
    _touch_entries(tmp_path, (*_SAMPLE_SHM_NAMES, "unrelated"))

    patterns = tests_conftest._post_test_patterns([], is_worker=False)
    removed = tests_conftest._sweep_shm(tmp_path, patterns)

    assert patterns == tests_conftest._BROAD_SHM_PATTERNS
    assert removed == len(_SAMPLE_SHM_NAMES)
    assert [entry.name for entry in tmp_path.iterdir()] == ["unrelated"]


def test_is_xdist_worker_reads_environment() -> None:
    """Detect pytest-xdist workers from the provided environment mapping."""
    assert tests_conftest._is_xdist_worker({"PYTEST_XDIST_WORKER": "gw0"}) is True
    assert tests_conftest._is_xdist_worker({}) is False


def test_sweep_missing_root_returns_zero(tmp_path: Path) -> None:
    """Sweeping a missing directory returns zero without raising."""
    missing_root = tmp_path / "absent"

    removed = tests_conftest._sweep_shm(missing_root, tests_conftest._BROAD_SHM_PATTERNS)

    assert removed == 0


def test_shm_name_is_recorded_for_cleanup(shm_name: str, _issued_shm_names: list[str]) -> None:
    """Requesting shm_name records the generated name in _issued_shm_names."""
    assert _issued_shm_names == [shm_name]
