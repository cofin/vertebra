"""Unit tests for PostgreSQL integration conformance zero-skip terminal gating."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Any

import pytest

from tests.integration.conftest import (
    evaluate_postgres_zero_skip_gate,
    is_postgres_conformance_nodeid,
    pytest_terminal_summary,
)


@dataclass
class _StubReport:
    """Minimal pytest TestReport stand-in for terminal gate checks."""

    nodeid: str
    when: str = "call"


@dataclass
class _StubDeselected:
    """Minimal deselected item carrying only nodeid."""

    nodeid: str


@dataclass
class _StubSession:
    """Minimal pytest Session double recording exitstatus mutations."""

    exitstatus: int | pytest.ExitCode = pytest.ExitCode.OK


@dataclass
class _StubTerminalReporter:
    """Minimal TerminalReporter double capturing stats, writes, and session exit status."""

    stats: dict[str, list[Any]] = field(default_factory=dict)
    _session: _StubSession | None = field(default_factory=_StubSession)
    messages: list[tuple[str, str]] = field(default_factory=list)

    def write_sep(self, sep: str, title: str = "", **kwargs: Any) -> None:
        """Record terminal separator banners emitted during summary failure reporting."""
        _ = kwargs
        self.messages.append((sep, title))


@pytest.mark.parametrize(
    ("nodeid", "expected"),
    [
        ("src/py/tests/integration/db/test_driver_conformance.py::test_sync_execution_contract[postgres-14]", True),
        ("src/py/tests/integration/db/test_driver_conformance.py::test_sync_pipeline_capability[postgres-18]", True),
        ("src/py/tests/integration/db/test_driver_conformance.py::test_sync_execution_contract[mysql]", False),
        ("src/py/tests/integration/db/test_postgres_replication_live.py::test_live_pgoutput_toast_and_bytea", True),
        ("src/py/tests/integration/db/test_postgres_native_features_live.py::test_live_pgvector_roundtrip", True),
        ("src/py/tests/integration/db/test_postgres_command_deadlines.py::test_live_client_deadline[sync]", True),
        (
            "src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py::test_docker_multiprocess_pool_sharing[postgres]",
            True,
        ),
        (
            "src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py::test_docker_multiprocess_pool_sharing[mysql]",
            False,
        ),
        (
            "src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py::test_multiprocess_duckdb_pool_sharing_sigkill_rollback_and_eviction",
            False,
        ),
        ("src/py/tests/unit/db/test_postgres_driver.py::test_example", False),
        ("src/py/tests/integration/db/test_oracle_native.py::test_oracle_26ai_oson", False),
    ],
)
def test_is_postgres_conformance_nodeid(nodeid: str, expected: bool) -> None:
    """Verify nodeid classifier accurately isolates PostgreSQL conformance and live suites."""
    assert is_postgres_conformance_nodeid(nodeid) is expected


@pytest.mark.parametrize(
    ("env", "stats", "expect_failure_substring"),
    [
        (
            {},
            {
                "skipped": [
                    _StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-18]", "setup")
                ]
            },
            None,
        ),
        (
            {"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "0"},
            {
                "skipped": [
                    _StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-14]", "setup")
                ]
            },
            None,
        ),
        (
            {"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1"},
            {},
            "0 PostgreSQL conformance tests executed",
        ),
        (
            {"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1"},
            {"passed": [_StubReport("test_driver_conformance.py::test_sync_execution_contract[sqlite]")]},
            "0 PostgreSQL conformance tests executed",
        ),
        (
            {"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1"},
            {
                "passed": [_StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-18]")],
                "skipped": [
                    _StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-14]", "setup")
                ],
            },
            "rejected 1 skipped PostgreSQL test",
        ),
        (
            {"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1"},
            {
                "passed": [_StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-18]")],
                "deselected": [
                    _StubDeselected("test_driver_conformance.py::test_sync_execution_contract[postgres-15]")
                ],
            },
            "rejected 1 deselected PostgreSQL test",
        ),
        (
            {"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1"},
            {
                "passed": [
                    _StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-14]"),
                    _StubReport("test_driver_conformance.py::test_sync_execution_contract[postgres-18]"),
                ],
                "skipped": [
                    _StubReport("test_driver_conformance.py::test_sync_execution_contract[oracle]", "setup"),
                    _StubReport("src/py/tests/unit/db/test_postgres_driver.py::test_optional_skip", "setup"),
                ],
            },
            None,
        ),
    ],
    ids=[
        "env_unset_allows_skip",
        "env_zero_allows_skip",
        "env_one_empty_session_fails",
        "env_one_non_postgres_only_fails",
        "env_one_pg_skipped_fails",
        "env_one_pg_deselected_fails",
        "env_one_pg_all_passed_non_conformance_skipped_ok",
    ],
)
def test_evaluate_postgres_zero_skip_gate(
    env: dict[str, str],
    stats: dict[str, list[Any]],
    expect_failure_substring: str | None,
) -> None:
    """Evaluate zero-skip enforcement across empty, skipped, deselected, and passing PG conformance runs."""
    reporter = _StubTerminalReporter(stats=stats)
    result = evaluate_postgres_zero_skip_gate(reporter, env=env)
    if expect_failure_substring is None:
        assert result is None
    else:
        assert result is not None
        assert expect_failure_substring in result


def test_pytest_terminal_summary_updates_session_exitstatus(monkeypatch: pytest.MonkeyPatch) -> None:
    """Terminal summary sets session.exitstatus to TESTS_FAILED only when zero-skip enforcement triggers."""
    monkeypatch.setenv("VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS", "1")
    config = pytest.Config.fromdictargs({}, [])

    failing_reporter = _StubTerminalReporter(
        stats={
            "skipped": [_StubReport("test_driver_conformance.py::test_sync_pipeline_capability[postgres-16]", "setup")]
        }
    )
    pytest_terminal_summary(failing_reporter, pytest.ExitCode.OK, config)
    assert failing_reporter._session is not None
    assert failing_reporter._session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert failing_reporter.messages

    passing_reporter = _StubTerminalReporter(
        stats={"passed": [_StubReport("test_driver_conformance.py::test_sync_pipeline_capability[postgres-16]")]}
    )
    pytest_terminal_summary(passing_reporter, pytest.ExitCode.OK, config)
    assert passing_reporter._session is not None
    assert passing_reporter._session.exitstatus == pytest.ExitCode.OK
    assert passing_reporter.messages == []


@pytest.mark.parametrize(
    ("require_zero_skips", "expected_returncode"),
    [
        ("1", 1),
        ("0", 0),
    ],
    ids=["enforced_fails_on_skip", "disabled_allows_skip"],
)
def test_live_pytest_subprocess_enforces_zero_skip_gate(
    require_zero_skips: str,
    expected_returncode: int,
) -> None:
    """Subprocess pytest exits non-zero when zero-skip enforcement rejects skipped postgres-18 conformance."""
    env = dict(os.environ)
    env["VERTEBRA_CONFORMANCE_DOCKER"] = "0"
    env.pop("VERTEBRA_CONFORMANCE_POSTGRES_DSN", None)
    env.pop("VERTEBRA_CONFORMANCE_POSTGRES_18_DSN", None)
    env["VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS"] = require_zero_skips
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "src/py/tests/integration/db/test_driver_conformance.py",
            "-k",
            "postgres-18",
            "-q",
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
        check=False,
    )
    assert proc.returncode == expected_returncode
    output = proc.stdout + proc.stderr
    if require_zero_skips == "1":
        assert "VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 rejected" in output
