"""Truthful completeness outcomes for the bounded v2 cursor walk (contract item 3).

The approved empty-page table is encoded one test per row, and the loss rule is pinned: a nonempty
walk that stops on documented exhaustion or the 20-page depth limit without proof exposes
``possible_data_loss`` so the caller freezes the complete-through boundary. A walk never exceeds 20
pages or 20,000 raw rows.
"""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.ingestor.v2_cursor_walk import (
    MAX_WALK_PAGES,
    MAX_WALK_ROWS,
    StopReason,
    WalkResult,
)
from polymarket_insider_tracker.ingestor.v2_page import V2EnvelopeError
from tests.fakes import FakeCursorFeed, v2_envelope
from tests.ingestor._v2_walk_harness import run_walk, v2_rows


def _caller_opens_new_gap(result: WalkResult, *, gap_already_open: bool) -> bool:
    """A caller preserves an already-open gap and only opens a new one for ``possible_data_loss``."""
    return gap_already_open or result.possible_data_loss


class TestEmptyPageTable:
    async def test_empty_initial_page_with_terminal_pagination_is_a_quiet_cycle(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})

        result, _ = await run_walk(feed)

        assert result.stop_reason is StopReason.EXHAUSTED
        assert result.raw_rows == 0
        assert result.completeness_proven is False
        assert result.possible_data_loss is False

    async def test_empty_page_with_a_continuation_keeps_walking(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope([], next_cursor="c0"),
                "c0": v2_envelope(v2_rows(2), next_cursor=None),
            }
        )

        result, harness = await run_walk(feed)

        assert result.pages_fetched == 2
        assert len(result.observations) == 2
        assert result.stop_reason is StopReason.EXHAUSTED
        assert harness.feed.cursors() == [None, "c0"]

    async def test_all_empty_continuation_chain_is_still_a_quiet_cycle(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope([], next_cursor="c0"),
                "c0": v2_envelope([], next_cursor=None),
            }
        )

        result, _ = await run_walk(feed)

        assert result.raw_rows == 0
        assert result.possible_data_loss is False

    async def test_empty_terminal_tail_after_nonempty_rows_is_possible_data_loss(self) -> None:
        feed = FakeCursorFeed(
            {
                None: v2_envelope(v2_rows(2), next_cursor="c0"),
                "c0": v2_envelope([], next_cursor=None),
            }
        )

        result, _ = await run_walk(feed)

        assert result.stop_reason is StopReason.EXHAUSTED
        assert result.raw_rows == 2
        assert result.completeness_proven is False
        assert result.possible_data_loss is True

    async def test_empty_cycle_while_a_gap_is_already_open_preserves_it(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope([], next_cursor=None)})

        result, _ = await run_walk(feed)

        assert result.possible_data_loss is False
        assert _caller_opens_new_gap(result, gap_already_open=True) is True
        assert _caller_opens_new_gap(result, gap_already_open=False) is False


class TestBounds:
    async def test_a_full_walk_reaches_at_most_twenty_pages_and_twenty_thousand_rows(self) -> None:
        pages: dict[str | None, object] = {None: v2_envelope(v2_rows(1000), next_cursor="c0")}
        for index in range(MAX_WALK_PAGES - 1):
            pages[f"c{index}"] = v2_envelope(
                v2_rows(1000, offset=1000), next_cursor=f"c{index + 1}"
            )
        feed = FakeCursorFeed(pages)

        result, _ = await run_walk(feed)

        assert result.pages_fetched == MAX_WALK_PAGES
        assert result.raw_rows == MAX_WALK_ROWS == 20_000
        assert result.stop_reason is StopReason.PAGE_LIMIT
        assert result.possible_data_loss is True

    async def test_a_page_larger_than_the_documented_limit_is_a_source_failure(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope(v2_rows(1001), next_cursor="c0")})

        with pytest.raises(V2EnvelopeError):
            await run_walk(feed)

        assert feed.cursors() == [None]

    async def test_a_page_at_the_documented_limit_is_recorded(self) -> None:
        feed = FakeCursorFeed({None: v2_envelope(v2_rows(1000), next_cursor=None)})

        result, _ = await run_walk(feed)

        assert result.pages_fetched == 1
        assert result.raw_rows == 1000
        assert result.stop_reason is StopReason.EXHAUSTED
