"""``complete_despite_cancellation``: a cancelled caller defers to the write, then re-raises."""

from __future__ import annotations

import asyncio

import pytest

from polymarket_insider_tracker.assessment_row import complete_despite_cancellation


class _HeldWrite:
    """A write that starts, waits for ``release`` and then finishes or fails."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = False
        self.failure = failure

    async def run(self) -> None:
        self.started.set()
        await self.release.wait()
        if self.failure is not None:
            raise self.failure
        self.finished = True


async def _cancel_mid_write(
    write: _HeldWrite, timeout: float
) -> tuple[asyncio.Task[None], list[str]]:
    errors: list[str] = []
    caller = asyncio.create_task(complete_despite_cancellation(write.run(), errors.append, timeout))
    await write.started.wait()
    caller.cancel()
    await asyncio.sleep(0)
    return caller, errors


async def test_uncancelled_write_failure_propagates_to_the_caller() -> None:
    write = _HeldWrite(failure=RuntimeError("insert failed"))
    write.release.set()

    errors: list[str] = []

    with pytest.raises(RuntimeError, match="insert failed"):
        await complete_despite_cancellation(write.run(), errors.append)
    assert errors == []


async def test_cancelled_caller_waits_for_the_write_then_reraises() -> None:
    write = _HeldWrite()
    caller, errors = await _cancel_mid_write(write, timeout=5.0)
    assert not caller.done()
    write.release.set()

    with pytest.raises(asyncio.CancelledError):
        await caller
    assert write.finished
    assert errors == []


async def test_write_failure_after_cancellation_is_reported_not_raised() -> None:
    write = _HeldWrite(failure=RuntimeError("insert failed"))
    caller, errors = await _cancel_mid_write(write, timeout=5.0)
    write.release.set()

    with pytest.raises(asyncio.CancelledError):
        await caller
    assert errors == ["Durable write failed after cancellation: insert failed"]


async def test_overrunning_write_is_abandoned_at_the_bound() -> None:
    write = _HeldWrite()
    caller, errors = await _cancel_mid_write(write, timeout=0.01)

    with pytest.raises(asyncio.CancelledError):
        await caller
    assert not write.finished
    assert errors == ["Durable write abandoned after the 0.01s cancellation bound"]
