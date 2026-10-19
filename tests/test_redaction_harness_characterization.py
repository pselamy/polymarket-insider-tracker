"""Characterization of the harness behaviour shared by the redaction-policy tests.

Cluster 3 of the clone inventory gives the repeated redaction harness setups one
home (now ``tests/console_capture.py``). These tests pin, through the public
seams the harnesses drive, the behaviour the shared helpers must keep.
"""

from __future__ import annotations

import logging

import pytest
from fakeredis import FakeAsyncRedis

from tests.console_capture import capture_console_output

CHARACTERIZED_SECRET = "harness-characterization-secret"


def test_console_capture_masks_a_secret_url_on_the_pipeline_logger() -> None:
    logger = logging.getLogger("polymarket_insider_tracker.pipeline")

    output = capture_console_output(
        lambda: logger.error("failed https://x.internal?k=%s", CHARACTERIZED_SECRET)
    )

    assert output
    assert CHARACTERIZED_SECRET not in output
    assert "failed https://x.internal" in output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [f"boom https://x.internal?k={CHARACTERIZED_SECRET}", f"boom-{CHARACTERIZED_SECRET}".encode()],
)
async def test_metadata_start_failure_status_never_carries_the_secret(
    caplog: pytest.LogCaptureFixture, payload: str | bytes
) -> None:
    from polymarket_insider_tracker.ingestor.metadata_sync import (
        MarketMetadataSync,
        MetadataSyncError,
    )
    from tests.fakes import FakeGammaClient
    from tests.ingestor.test_metadata_sync import FakeClobClient

    failing = FakeClobClient(raise_error=RuntimeError(payload))
    sync = MarketMetadataSync(
        redis=FakeAsyncRedis(), clob_client=failing, gamma_client=FakeGammaClient()
    )
    with caplog.at_level(logging.ERROR):
        caplog.clear()
        with pytest.raises(MetadataSyncError, match="initial sync failed"):
            await sync.start()

    assert CHARACTERIZED_SECRET not in caplog.text
    assert sync.stats.last_error is not None
    assert CHARACTERIZED_SECRET not in sync.stats.last_error
    assert "Initial sync failed" in caplog.text
