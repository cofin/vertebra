import os
from unittest.mock import patch

import pytest
from litestar import Controller, Litestar, get

from vertebra.core.worker import AsgiWorker


@pytest.mark.integration
async def test_native_startup_crash_with_controllers() -> None:
    class MyController(Controller):
        path = "/test"

        @get("/")
        async def handler(self) -> dict:
            return {"status": "ok"}

    app = Litestar(route_handlers=[MyController])

    # Ensure native mode is enabled
    with patch.dict(os.environ, {"VERTEBRA_NATIVE": "1"}):
        worker = AsgiWorker(app)

        # This should NOT raise StopIteration anymore.
        await worker.start()

        # Verify that we actually compiled runners
        assert len(worker._native_runners) > 0, "Native runners should be present"
