"""Property tests for the bounded v2 cursor walk against an independent reference model.

For generated page scripts (random lengths, empty pages, cursors, terminal positions, and injected
transient failures) an independent traversal oracle recomputes the expected request sequence, stop
reason, and raw-row count from the same script alone. The walk must match it exactly, never exceed 20
pages or 20,000 raw rows, never reuse a cursor without failing, and admit exactly one attempt-budget
slot per physical request. Hypothesis is pinned in the dev extra; the profile is derandomized and
bounded so each collected test drives many cases while the collected-ID count stays small.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from polymarket_insider_tracker.ingestor.v2_cursor_walk import (
    MAX_RETRIES,
    MAX_WALK_PAGES,
    MAX_WALK_ROWS,
    StopReason,
)
from polymarket_insider_tracker.ingestor.v2_page import (
    V2_PAGE_LIMIT,
    V2EnvelopeError,
    V2PaginationError,
    V2TransientError,
)
from tests.fakes import FakeCursorFeed, v2_envelope
from tests.ingestor._v2_walk_harness import END, START, build_walk, v2_rows

settings.register_profile(
    "repo",
    derandomize=True,
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("repo")

_SCRIPT_LENGTH = 60


@dataclass(frozen=True)
class ScriptedPage:
    """One generated page: its rows and the cursor it returns."""

    rows: tuple[dict[str, object], ...]
    next_cursor: str | None

    def envelope(self) -> dict[str, object]:
        return v2_envelope(self.rows, next_cursor=self.next_cursor)


@dataclass(frozen=True)
class ReferenceWalk:
    """The oracle's expected physical request cursors, stop reason, and raw-row count."""

    cursors: tuple[str | None, ...]
    stop_reason: StopReason
    raw_rows: int


def reference_walk(
    pages: Mapping[str | None, ScriptedPage],
    failures: Sequence[bool],
    *,
    max_pages: int = MAX_WALK_PAGES,
    max_retries: int = MAX_RETRIES,
    max_rows: int = MAX_WALK_ROWS,
) -> ReferenceWalk:
    """An independent traversal of the scripted pages, mirroring the approved walk rules."""
    return _Reference(pages, list(failures), max_pages, max_retries, max_rows).walk()


@dataclass
class _Reference:
    """The oracle's mutable state; ``walk`` traverses the scripted pages independently."""

    pages: Mapping[str | None, ScriptedPage]
    pending: list[bool]
    max_pages: int
    max_retries: int
    max_rows: int
    cursors: list[str | None] = field(default_factory=list[str | None])
    pages_done: int = 0
    raw_rows: int = 0
    seen: set[str] = field(default_factory=set[str])
    cursor: str | None = None

    def walk(self) -> ReferenceWalk:
        while True:
            stop = self._step()
            if stop is not None:
                return ReferenceWalk(tuple(self.cursors), stop, self.raw_rows)

    def _step(self) -> StopReason | None:
        if self.pages_done >= self.max_pages:
            return StopReason.PAGE_LIMIT
        page = self._fetch()
        if len(page.rows) > V2_PAGE_LIMIT:
            raise V2EnvelopeError("page exceeds the documented limit")
        if self.raw_rows + len(page.rows) > self.max_rows:
            return StopReason.PAGE_LIMIT
        self.pages_done += 1
        self.raw_rows += len(page.rows)
        return self._advance(page.next_cursor)

    def _fetch(self) -> ScriptedPage:
        for attempt in range(self.max_retries + 1):
            self.cursors.append(self.cursor)
            if not self._failed():
                return self.pages[self.cursor]
            self._wait_before_retry(attempt)
        raise AssertionError("retry loop always returns or raises")

    def _failed(self) -> bool:
        return bool(self.pending and self.pending.pop(0))

    def _wait_before_retry(self, attempt: int) -> None:
        if attempt == self.max_retries:
            raise V2TransientError("throttled")

    def _advance(self, next_cursor: str | None) -> StopReason | None:
        if next_cursor is None:
            return StopReason.EXHAUSTED
        if next_cursor in self.seen:
            raise V2PaginationError("repeated cursor")
        self.seen.add(next_cursor)
        self.cursor = next_cursor
        return None


def _failures(flags: Sequence[bool]) -> list[Exception | None]:
    return [V2TransientError("throttled") if flag else None for flag in flags]


@st.composite
def _page_scripts(
    draw: st.DrawFn,
) -> tuple[dict[str | None, ScriptedPage], list[bool]]:
    has_terminal = draw(st.booleans())
    if has_terminal:
        page_count = draw(st.integers(min_value=1, max_value=25))
        terminal_at: int | None = draw(st.integers(min_value=0, max_value=page_count - 1))
    else:
        page_count = draw(st.integers(min_value=MAX_WALK_PAGES, max_value=25))
        terminal_at = None
    row_counts = draw(
        st.lists(st.integers(min_value=0, max_value=3), min_size=page_count, max_size=page_count)
    )
    if draw(st.integers(min_value=0, max_value=9)) == 0:
        oversized_at = draw(
            st.integers(min_value=0, max_value=_last_reachable_index(terminal_at, page_count))
        )
        oversized_rows = V2_PAGE_LIMIT + draw(st.integers(min_value=1, max_value=3))
        row_counts = _with_oversized_row(row_counts, oversized_at, oversized_rows)
    flags = draw(st.lists(st.booleans(), min_size=_SCRIPT_LENGTH, max_size=_SCRIPT_LENGTH))
    return _scripted_pages(page_count, terminal_at, row_counts), flags


def _last_reachable_index(terminal_at: int | None, page_count: int) -> int:
    """The largest page index the bounded walk can actually fetch for this script."""
    if terminal_at is not None:
        return terminal_at
    return min(page_count, MAX_WALK_PAGES) - 1


def _with_oversized_row(row_counts: Sequence[int], index: int, oversized_rows: int) -> list[int]:
    counts = list(row_counts)
    counts[index] = oversized_rows
    return counts


def _scripted_pages(
    page_count: int, terminal_at: int | None, row_counts: Sequence[int]
) -> dict[str | None, ScriptedPage]:
    pages: dict[str | None, ScriptedPage] = {}
    for index in range(page_count):
        pages[_request_cursor(index)] = ScriptedPage(
            rows=tuple(v2_rows(row_counts[index], offset=index * 100)),
            next_cursor=_response_cursor(index, terminal_at),
        )
    return pages


def _request_cursor(index: int) -> str | None:
    return None if index == 0 else f"c{index - 1}"


def _response_cursor(index: int, terminal_at: int | None) -> str | None:
    if terminal_at is not None and index >= terminal_at:
        return None
    return f"c{index}"


class TestReferenceEquivalence:
    @given(_page_scripts())
    def test_walk_matches_the_independent_oracle(
        self, case: tuple[dict[str | None, ScriptedPage], list[bool]]
    ) -> None:
        pages, flags = case
        feed = FakeCursorFeed(
            {cursor: page.envelope() for cursor, page in pages.items()}, script=_failures(flags)
        )
        harness = build_walk(feed)

        try:
            expected = reference_walk(pages, flags)
        except V2TransientError:
            with pytest.raises(V2TransientError):
                asyncio.run(harness.walk.run(taker_only=False, start=START, end=END))
            return
        except V2EnvelopeError:
            with pytest.raises(V2EnvelopeError):
                asyncio.run(harness.walk.run(taker_only=False, start=START, end=END))
            return

        result = asyncio.run(harness.walk.run(taker_only=False, start=START, end=END))

        assert feed.cursors() == list(expected.cursors)
        assert result.stop_reason is expected.stop_reason
        assert result.raw_rows == expected.raw_rows
        assert result.pages_fetched <= MAX_WALK_PAGES
        assert result.raw_rows <= MAX_WALK_ROWS
        assert harness.budget.acquires == len(feed.requests)


class TestCursorReuse:
    @given(st.integers(min_value=1, max_value=15))
    def test_reusing_a_cursor_always_fails(self, reuse_after: int) -> None:
        pages: dict[str | None, object] = {None: v2_envelope(v2_rows(1), next_cursor="c0")}
        for index in range(reuse_after):
            pages[f"c{index}"] = v2_envelope(v2_rows(1), next_cursor=f"c{index + 1}")
        pages[f"c{reuse_after}"] = v2_envelope(v2_rows(1), next_cursor="c0")
        harness = build_walk(FakeCursorFeed(pages))

        with pytest.raises(V2PaginationError):
            asyncio.run(harness.walk.run(taker_only=False, start=START, end=END))
