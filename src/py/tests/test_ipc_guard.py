from __future__ import annotations

import shutil

import pytest

from vertebra._internal.ipc import BunRuntimeError, require_bun


def test_require_bun_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda _: None)
    with pytest.raises(BunRuntimeError):
        require_bun()
