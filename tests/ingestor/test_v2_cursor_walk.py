"""Example tests for one bounded, truthful v2 cursor walk (approved contract items 2-3).

Each cycle is one consistent cursor walk. The first request carries ``limit=1000``, explicit
``taker_only``, and the cycle's fixed ``start``/``end``; every continuation carries only ``cursor``
plus those same filters, never ``offset`` and never a synthesized cursor. The walk follows only the
returned cursor, stops on a null ``next_cursor``, and never exceeds 20 pages or 20,000 raw rows.
Missing, malformed, contradictory, or repeated pagination is an actionable source failure that never
proves continuity. Every page's rows are decoded through the merged v2 row decoder.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import TradeObservation
from polymarket_insider_tracker.ingestor.v2_cursor_walk import (
    MAX_WALK_PAGES,
    CursorWalk,
    StopReason,
)
from polymarket_insider_tracker.ingestor.v2_page import (
    V2PaginationError,
    V2SourceError,
    V2TerminalError,
    V2TransientError,
)
from tests.fakes import CountingBudget, FakeClock, FakeCursorFeed, trade_row, v2_envelope, v2_row
from tests.ingestor._v2_walk_harness import (
    END,
    NOW,
    START,
    BlockingFeed,
    build_walk,
    run_walk,
    v2_rows,
)


class TestFirstRequest:
    async def test_first_request_sends_limit_taker_only_and_cycle_filters(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})

        _, harness = await run_walk(feed, taker_only=True)

        first = harness.feed.requests[0]
        assert first.cursor is None
        assert not first.is_continuation
        assert first.params() == {
            "limit": "1000",
            "taker_only": "true",
            "start": str(START),
            "end": str(END),
        }
        assert "offset" not in first.params()
        assert "cursor" not in first.params()

    @pytest.mark.parametrize("taker_only", [True, False])
    async def test_configured_taker_only_is_always_sent_explicitly(self, taker_only: bool) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})

        await run_walk(feed, taker_only=taker_only)

        assert feed.requests[0].params()["taker_only"] == ("true" if taker_only else "false")


class TestContinuationRequest:
    async def test_continuation_sends_only_cursor_with_the_same_filters(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope(v2_rows(1), next_cursor="page-2"),
                "page-2": v2_envelope([], next_cursor=None),
            }
        )

        _, harness = await run_walk(feed, taker_only=False)

        continuation = harness.feed.requests[1]
        assert continuation.cursor == "page-2"
        assert continuation.is_continuation
        assert continuation.params() == {
            "cursor": "page-2",
            "taker_only": "false",
            "start": str(START),
            "end": str(END),
        }
        assert "limit" not in continuation.params()
        assert "offset" not in continuation.params()


class TestStopping:
    async def test_stops_when_next_cursor_is_null(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope(v2_rows(2), next_cursor=None)})

        result, harness = await run_walk(feed)

        assert result.stop_reason is StopReason.EXHAUSTED
        assert result.pages_fetched == 1
        assert len(harness.feed.requests) == 1

    async def test_stops_after_twenty_pages_without_proof(self) -> None:
        pages: dict[str | None, object] = {None: v2_envelope(v2_rows(1), next_cursor="c0")}
        for index in range(MAX_WALK_PAGES - 1):
            pages[f"c{index}"] = v2_envelope(v2_rows(1), next_cursor=f"c{index + 1}")
        feed = FakeCursorFeed(pages)

        result, harness = await run_walk(feed)

        assert result.stop_reason is StopReason.PAGE_LIMIT
        assert result.pages_fetched == MAX_WALK_PAGES
        assert len(harness.feed.requests) == MAX_WALK_PAGES
        assert harness.feed.cursors() == [
            None,
            *(f"c{index}" for index in range(MAX_WALK_PAGES - 1)),
        ]

    async def test_empty_page_with_a_cursor_keeps_walking(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope([], next_cursor="page-2"),
                "page-2": v2_envelope(v2_rows(2), next_cursor=None),
            }
        )

        result, _ = await run_walk(feed)

        assert result.pages_fetched == 2
        assert len(result.observations) == 2
        assert result.stop_reason is StopReason.EXHAUSTED


class TestPaginationFailures:
    @pytest.mark.parametrize(
        "envelope",
        [
            {"data": []},
            {"data": [], "pagination": "not-an-object"},
            {"data": {"not": "a list"}, "pagination": {"next_cursor": None, "has_more": False}},
        ],
    )
    async def test_missing_or_malformed_envelope_is_a_source_failure(
        self, envelope: dict[str, object]
    ) -> None:
        feed = FakeCursorFeed({None: envelope})

        with pytest.raises(V2SourceError):
            await run_walk(feed)

    @pytest.mark.parametrize(
        "pagination",
        [
            {"next_cursor": 5, "has_more": True},
            {"next_cursor": "x", "has_more": "yes"},
            {"next_cursor": None, "has_more": None},
        ],
    )
    async def test_malformed_pagination_fields_are_a_source_failure(
        self, pagination: dict[str, object]
    ) -> None:
        feed = FakeCursorFeed({None: {"data": [], "pagination": pagination}})

        with pytest.raises(V2PaginationError):
            await run_walk(feed)

    @pytest.mark.parametrize(
        "pagination",
        [
            {"next_cursor": None, "has_more": True},
            {"next_cursor": "x", "has_more": False},
        ],
    )
    async def test_contradictory_pagination_is_a_source_failure(
        self, pagination: dict[str, object]
    ) -> None:
        feed = FakeCursorFeed({None: {"data": [], "pagination": pagination}})

        with pytest.raises(V2PaginationError):
            await run_walk(feed)

    async def test_repeated_cursor_is_a_source_failure(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope(v2_rows(1), next_cursor="same"),
                "same": v2_envelope(v2_rows(1), next_cursor="same"),
            }
        )

        with pytest.raises(V2PaginationError):
            await run_walk(feed)


class TestDecoding:
    async def test_rows_are_decoded_through_the_v2_row_decoder(self) -> None:
        raw = v2_row(trade_row(timestamp=START + 1, transaction=3, wallet=3))
        feed = FakeCursorFeed({None: v2_envelope([raw], next_cursor=None)})

        result, _ = await run_walk(feed)

        expected = parse_v2_trade_row(raw, now=NOW)
        assert isinstance(expected, TradeObservation)
        assert result.observations == (expected,)
        assert result.invalid_rows == ()

    async def test_invalid_rows_are_counted_and_valid_rows_kept(self) -> None:
        broken = v2_row(trade_row(timestamp=START + 1))
        del broken["transaction_hash"]
        valid = v2_row(trade_row(timestamp=START + 1, transaction=1, wallet=1))
        feed = FakeCursorFeed({None: v2_envelope([broken, valid], next_cursor=None)})

        result, _ = await run_walk(feed)

        assert len(result.observations) == 1
        assert len(result.invalid_rows) == 1


class TestProof:
    async def test_stops_early_when_proof_succeeds(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope(v2_rows(2), next_cursor="c0"),
                "c0": v2_envelope(v2_rows(2, offset=2), next_cursor="c1"),
                "c1": v2_envelope(v2_rows(2, offset=4), next_cursor=None),
            }
        )
        calls: list[int] = []

        def proof(observations: Sequence[TradeObservation]) -> bool:
            calls.append(len(observations))
            return len(calls) == 2

        result, harness = await run_walk(feed, proof=proof)

        assert result.stop_reason is StopReason.PROOF
        assert result.completeness_proven is True
        assert result.pages_fetched == 2
        assert len(harness.feed.requests) == 2
        assert calls == [2, 2]


class TestAttemptBudget:
    async def test_every_physical_request_acquires_exactly_one_slot(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope(v2_rows(1), next_cursor="c0"),
                "c0": v2_envelope([], next_cursor=None),
            }
        )

        _, harness = await run_walk(feed)

        assert harness.budget.acquires == len(harness.feed.requests) == 2

    async def test_a_retry_acquires_another_slot(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})
        feed.fail_next(V2TransientError("throttled"))

        result, harness = await run_walk(feed)

        assert result.stop_reason is StopReason.EXHAUSTED
        assert len(harness.feed.requests) == 2
        assert harness.budget.acquires == 2
        assert harness.budget_clock.sleeps == [1.0]

    async def test_exhausted_transient_retries_raise(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})
        for _ in range(5):
            feed.fail_next(V2TransientError("throttled"))
        harness = build_walk(feed)

        with pytest.raises(V2TransientError):
            await harness.walk.run(taker_only=False, start=START, end=END)

        assert harness.budget.acquires == 5

    async def test_terminal_transport_error_is_not_retried(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})
        feed.fail_next(V2TerminalError("incompatible schema"))
        harness = build_walk(feed)

        with pytest.raises(V2TerminalError):
            await harness.walk.run(taker_only=False, start=START, end=END)

        assert harness.budget.acquires == 1
        assert len(feed.requests) == 1


class TestCancellation:
    async def test_cooperative_cancellation_stops_with_a_partial_result(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope(v2_rows(1), next_cursor="c0"),
                "c0": v2_envelope(v2_rows(1), next_cursor=None),
            }
        )
        checks = {"count": 0}

        def cancelled() -> bool:
            checks["count"] += 1
            return checks["count"] > 1

        result, _ = await run_walk(feed, is_cancelled=cancelled)

        assert result.stop_reason is StopReason.CANCELLED
        assert result.pages_fetched == 1
        assert result.completeness_proven is False

    async def test_task_cancellation_propagates(self) -> None:
        feed = BlockingFeed()
        budget_clock = FakeClock(0.0)
        budget = CountingBudget(clock=budget_clock, sleep=budget_clock.sleep)
        walk = CursorWalk(
            feed,
            budget=budget,
            clock=FakeClock(float(NOW)),
            sleeper=budget_clock.sleep,
        )

        task = asyncio.create_task(walk.run(taker_only=False, start=START, end=END))
        await feed.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
