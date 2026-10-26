"""T027 Part B judgement (research.md R09a, protocol v2): window, sample, ground truth, walks, verdict.

Pure functions over the recorded capture (``t027_capture``) and the recorded per-user walks
(``t027_walk``); nothing here touches the network. The rules are the protocol's, restated:

- window: ``t0`` is the first proven boundary of the capture, ``t1 = t0 + 24 h``, the sample is
  drawn at ``ts = t0 + 1 h``; proven coverage is the window minus every loss interval and minus any
  time past the last proven boundary, and must be at least 20 h;
- frame: distinct lowercased proxy wallets with a delivered trade of notional >= $1,000 in
  ``[t0, t0 + 1 h)``; sample: ``random.Random(seed).sample`` of N = 200 from the sorted frame;
- ground truth: an address's delivered trades in ``(ts, t1]`` at proven times, by identity;
  active = at least one, heavy = at least ``B``;
- a walk is a complete history iff every page answered 200 with a valid envelope and only valid
  rows, the filters (``user``, ``taker_only=false``, ``start=1``, ``end``) are identical on every
  page, each continuation sends exactly the previous ``next_cursor``, and the last page has
  ``next_cursor = null`` and ``has_more = false``; anything else is incomplete and yields
  ``unknown`` age and trade count (Q3);
- Q1 recall, Q2 ordering (newest-first), Q4 earliest-trade stability over two complete reads at
  least 1 h apart, Q5 (a) replay and (b) live boundary checks, and the PASS rule.
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal

from scripts.spec004_evidence.json_shapes import (
    Obj,
    as_array,
    as_int,
    as_number,
    as_object,
    as_text,
)

B_DEFAULT = 5
WINDOW_SECONDS = 86_400
FRAME_SECONDS = 3_600
MIN_PROVEN_SECONDS = 20 * 3_600
SAMPLE_N = 200
FRAME_MIN_NOTIONAL = Decimal(1000)
MIN_ACTIVE = 50
MIN_READ_GAP_SECONDS = 3_600
REQUIRED_FILTERS = {"taker_only": "false", "start": "1"}
FILTER_KEYS = ("user", "taker_only", "start", "end")
FIRST_PAGE_KEYS = frozenset({*FILTER_KEYS, "limit"})
CONTINUATION_KEYS = frozenset({*FILTER_KEYS, "cursor"})
TRANSPORT_FAILURES = frozenset({429, 500, 502, 503, 504})
Q1_AGGREGATE = Decimal("0.99")
Q1_PER_ADDRESS = Decimal("0.95")


# ----- capture ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CapturedTrade:
    identity: str
    wallet: str
    notional: Decimal
    ts: int


@dataclass(frozen=True)
class Interval:
    start: int
    end: int

    def overlap(self, low: int, high: int) -> int:
        return max(0, min(self.end, high) - max(self.start, low))

    def contains(self, ts: int) -> bool:
        return self.start <= ts <= self.end


def captured_trade(record: object) -> CapturedTrade:
    obj = as_object(record)
    return CapturedTrade(
        identity=as_text(obj["identity"]),
        wallet=as_text(obj["wallet"]).lower(),
        notional=Decimal(as_text(obj["notional"])),
        ts=as_int(obj["ts"]),
    )


def loss_interval(record: object) -> Interval:
    obj = as_object(record)
    return Interval(as_int(obj["from_time"]), as_int(obj["to_time"]))


def boundary_times(statuses: Iterable[Obj]) -> list[int]:
    return [as_int(s["boundary_time"]) for s in statuses if s.get("boundary_time") is not None]


def loss_intervals(statuses: Iterable[Obj]) -> tuple[Interval, ...]:
    found = {loss_interval(loss) for s in statuses for loss in as_array(s.get("loss_events", []))}
    return tuple(sorted(found, key=lambda i: (i.start, i.end)))


def merged(intervals: Sequence[Interval]) -> list[Interval]:
    out: list[Interval] = []
    for interval in sorted(intervals, key=lambda i: i.start):
        if out and interval.start <= out[-1].end:
            out[-1] = Interval(out[-1].start, max(out[-1].end, interval.end))
            continue
        out.append(interval)
    return out


@dataclass(frozen=True)
class Window:
    t0: int
    proven_through: int
    losses: tuple[Interval, ...]

    @property
    def t1(self) -> int:
        return self.t0 + WINDOW_SECONDS

    @property
    def sampled_at(self) -> int:
        return self.t0 + FRAME_SECONDS

    def unproven_seconds(self) -> int:
        lost = sum(i.overlap(self.t0, self.t1) for i in merged(self.losses))
        return lost + max(0, self.t1 - max(self.proven_through, self.t0))

    def proven_seconds(self) -> int:
        return WINDOW_SECONDS - min(WINDOW_SECONDS, self.unproven_seconds())

    def proven_intervals(self) -> list[Interval]:
        """The window's proven coverage: ``[t0, min(t1, proven_through)]`` minus every loss."""
        end = min(self.t1, self.proven_through)
        out: list[Interval] = []
        cursor = self.t0
        for loss in merged(self.losses):
            out += [Interval(cursor, min(loss.start - 1, end))] if loss.start > cursor else []
            cursor = max(cursor, loss.end + 1)
        return [*out, Interval(cursor, end)] if cursor <= end else out

    def covers(self, ts: int) -> bool:
        proven = self.t0 <= ts <= self.proven_through
        return proven and not any(loss.contains(ts) for loss in self.losses)


def window_from(statuses: Sequence[Obj]) -> Window:
    times = boundary_times(statuses)
    if not times:
        raise ValueError("the capture never proved a boundary")
    return Window(t0=times[0], proven_through=max(times), losses=loss_intervals(statuses))


# ----- frame, sample, ground truth ----------------------------------------------------------------


def frame(trades: Iterable[CapturedTrade], window: Window) -> list[str]:
    hour = range(window.t0, window.sampled_at)
    return sorted({t.wallet for t in trades if t.ts in hour and t.notional >= FRAME_MIN_NOTIONAL})


def draw_sample(frame_wallets: Sequence[str], seed: int, n: int = SAMPLE_N) -> list[str]:
    return random.Random(seed).sample(list(frame_wallets), min(n, len(frame_wallets)))


def frame_trades(
    trades: Iterable[CapturedTrade], window: Window, sample: Iterable[str]
) -> dict[str, set[str]]:
    """Each sampled address's own >= $1,000 frame-hour trades (for the frame-recall diagnostic)."""
    found: dict[str, set[str]] = {address: set() for address in sample}
    hour = range(window.t0, window.sampled_at)
    for trade in trades:
        if trade.wallet in found and trade.ts in hour and trade.notional >= FRAME_MIN_NOTIONAL:
            found[trade.wallet].add(trade.identity)
    return found


def in_ground_truth(trade: CapturedTrade, window: Window) -> bool:
    return window.sampled_at < trade.ts <= window.t1 and window.covers(trade.ts)


def ground_truth(
    trades: Iterable[CapturedTrade], window: Window, sample: Iterable[str]
) -> dict[str, set[str]]:
    truth: dict[str, set[str]] = {address: set() for address in sample}
    for trade in trades:
        if trade.wallet in truth and in_ground_truth(trade, window):
            truth[trade.wallet].add(trade.identity)
    return truth


# ----- walks --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ReturnedRow:
    identity: str
    ts: int


@dataclass(frozen=True)
class PageRecord:
    """One recorded request of a walk and what came back (``status`` None: no HTTP answer)."""

    address: str
    read: int
    page: int
    requested_at: float
    params: Mapping[str, str]
    status: int | None
    error: str | None
    next_cursor: str | None
    has_more: bool | None
    rows: tuple[ReturnedRow, ...]
    invalid_rows: int

    def answered(self) -> bool:
        return self.status == 200 and self.error is None and self.has_more is not None

    def transport_failure(self) -> bool:
        """No answer, a throttle or a server error: the provider did not serve the page."""
        return self.status is None or self.status in TRANSPORT_FAILURES

    def filters(self) -> tuple[str | None, ...]:
        return tuple(self.params.get(key) for key in FILTER_KEYS)


def optional_text(value: object) -> str | None:
    return None if value is None else as_text(value)


def optional_int(value: object) -> int | None:
    return None if value is None else as_int(value)


def optional_bool(value: object) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise TypeError(f"expected a boolean or null, got {type(value).__name__}")


def returned_row(value: object) -> ReturnedRow:
    pair = as_array(value)
    return ReturnedRow(identity=as_text(pair[0]), ts=as_int(pair[1]))


def page_record(record: object) -> PageRecord:
    obj = as_object(record)
    params = as_object(obj["params"])
    return PageRecord(
        address=as_text(obj["address"]).lower(),
        read=as_int(obj["read"]),
        page=as_int(obj["page"]),
        requested_at=float(as_number(obj["requested_at"])),
        params={key: as_text(value) for key, value in params.items()},
        status=optional_int(obj["status"]),
        error=optional_text(obj["error"]),
        next_cursor=optional_text(obj["next_cursor"]),
        has_more=optional_bool(obj["has_more"]),
        rows=tuple(returned_row(row) for row in as_array(obj["rows"])),
        invalid_rows=as_int(obj["invalid_rows"]),
    )


@dataclass(frozen=True)
class Walk:
    address: str
    read: int
    pages: tuple[PageRecord, ...]

    @property
    def started_at(self) -> float:
        return self.pages[0].requested_at

    def rows(self) -> list[ReturnedRow]:
        return [row for page in self.pages for row in page.rows]

    def identities(self) -> set[str]:
        return {row.identity for row in self.rows()}

    def duplicate_rows(self) -> int:
        return len(self.rows()) - len(self.identities())

    def transport_failure(self) -> bool:
        return any(page.transport_failure() for page in self.pages)


def walks_from(pages: Iterable[PageRecord]) -> dict[tuple[int, str], Walk]:
    grouped: dict[tuple[int, str], list[PageRecord]] = {}
    for page in pages:
        grouped.setdefault((page.read, page.address), []).append(page)
    return {
        key: Walk(key[1], key[0], tuple(sorted(group, key=lambda p: p.page)))
        for key, group in grouped.items()
    }


def filters_valid(walk: Walk) -> bool:
    first = walk.pages[0].filters()
    expected_user = walk.address
    required = all(walk.pages[0].params.get(k) == v for k, v in REQUIRED_FILTERS.items())
    same = all(page.filters() == first for page in walk.pages)
    return required and same and first[0] == expected_user and first[3] is not None


def params_exact(walk: Walk) -> bool:
    """Only the documented parameters: never ``offset`` or any other filter."""
    first, rest = walk.pages[0], walk.pages[1:]
    keys_ok = frozenset(first.params) == FIRST_PAGE_KEYS
    return keys_ok and all(frozenset(page.params) == CONTINUATION_KEYS for page in rest)


def cursors_chained(walk: Walk) -> bool:
    if "cursor" in walk.pages[0].params:
        return False
    pairs = zip(walk.pages, walk.pages[1:], strict=False)
    return all(nxt.params.get("cursor") == prev.next_cursor for prev, nxt in pairs)


def pages_clean(walk: Walk) -> bool:
    return all(page.answered() and page.invalid_rows == 0 for page in walk.pages)


def ends_terminal(walk: Walk) -> bool:
    last = walk.pages[-1]
    return last.next_cursor is None and last.has_more is False


def middle_pages_continue(walk: Walk) -> bool:
    return all(page.has_more is True and page.next_cursor for page in walk.pages[:-1])


COMPLETENESS_CHECKS = {
    "pages-answered-with-valid-rows": pages_clean,
    "documented-parameters-only": params_exact,
    "identical-protocol-filters": filters_valid,
    "cursor-chain": cursors_chained,
    "non-final-pages-continue": middle_pages_continue,
    "terminal-final-page": ends_terminal,
}


def failed_check(walk: Walk) -> str | None:
    """The first complete-history condition the walk fails, or ``None`` for a complete history."""
    if not walk.pages:
        return "no-pages"
    return next((name for name, check in COMPLETENESS_CHECKS.items() if not check(walk)), None)


def is_complete_history(walk: Walk) -> bool:
    """The protocol's complete-history rule; every other walk is incomplete (Q3)."""
    return failed_check(walk) is None


@dataclass(frozen=True)
class Classification:
    """What one walk lets the follow-up runtime claim: values only for a complete history."""

    complete: bool
    trade_count: int | None
    earliest: tuple[int, tuple[str, ...]] | None

    def low_activity(self, boundary: int) -> bool | None:
        return None if self.trade_count is None else self.trade_count < boundary


def earliest_trade(rows: Sequence[ReturnedRow]) -> tuple[int, tuple[str, ...]] | None:
    if not rows:
        return None
    first = min(row.ts for row in rows)
    return first, tuple(sorted(row.identity for row in rows if row.ts == first))


def classify(walk: Walk) -> Classification:
    if not is_complete_history(walk):
        return Classification(complete=False, trade_count=None, earliest=None)
    rows = walk.rows()
    count = len(walk.identities())
    return Classification(complete=True, trade_count=count, earliest=earliest_trade(rows))


def ordering_violations(walk: Walk) -> int:
    """Q2: adjacent returned rows out of newest-first order across every page of the walk."""
    rows = walk.rows()
    return sum(1 for older, newer in zip(rows, rows[1:], strict=False) if older.ts < newer.ts)


# ----- Q5 (a): recorded-response replay at the boundary -------------------------------------------


@dataclass(frozen=True)
class ReplayCase:
    name: str
    walk: Walk
    expect_complete: bool


def boundary_walk(template: PageRecord, rows: Sequence[ReturnedRow]) -> Walk:
    """Exactly ``len(rows)`` trades as a chained two-page complete history, from a recorded page."""
    first = replace(
        template,
        page=0,
        status=200,
        error=None,
        invalid_rows=0,
        rows=tuple(rows[:-1]),
        next_cursor="replay-cursor-1",
        has_more=True,
    )
    continuation = {k: v for k, v in template.params.items() if k not in ("limit", "cursor")}
    second = replace(
        first,
        page=1,
        params={**continuation, "cursor": "replay-cursor-1"},
        rows=tuple(rows[-1:]),
        next_cursor=None,
        has_more=False,
    )
    return Walk(template.address, template.read, (first, second))


def with_last(walk: Walk, page: PageRecord | None) -> Walk:
    kept = walk.pages[:-1] if page is None else (*walk.pages[:-1], page)
    return Walk(walk.address, walk.read, kept)


def replay_cases(template: PageRecord, boundary: int) -> list[ReplayCase]:
    rows = [ReturnedRow(identity=f"replay-{i}", ts=2_000_000_000 - i) for i in range(boundary)]
    full = boundary_walk(template, rows)
    last = full.pages[-1]
    changed_end = {**last.params, "end": str(int(last.params.get("end", "0")) + 1)}
    return [
        ReplayCase("complete-exactly-B", full, expect_complete=True),
        ReplayCase("last-page-dropped", with_last(full, None), expect_complete=False),
        ReplayCase(
            "last-page-http-500", with_last(full, replace(last, status=500, rows=())), False
        ),
        ReplayCase(
            "last-page-timeout",
            with_last(full, replace(last, status=None, error="timeout", rows=(), has_more=None)),
            False,
        ),
        ReplayCase(
            "terminal-flags-contradict", with_last(full, replace(last, has_more=True)), False
        ),
        ReplayCase("filters-changed", with_last(full, replace(last, params=changed_end)), False),
        ReplayCase("invalid-row-on-page", with_last(full, replace(last, invalid_rows=1)), False),
    ]


@dataclass(frozen=True)
class ReplayResult:
    name: str
    complete: bool
    low_activity: bool | None
    holds: bool


def replay_result(case: ReplayCase, boundary: int) -> ReplayResult:
    outcome = classify(case.walk)
    low = outcome.low_activity(boundary)
    if case.expect_complete:
        holds = outcome.complete and outcome.trade_count == boundary and low is False
    else:
        holds = not outcome.complete and low is None
    return ReplayResult(case.name, outcome.complete, low, holds)


def q5a(template: PageRecord, boundary: int) -> list[ReplayResult]:
    return [replay_result(case, boundary) for case in replay_cases(template, boundary)]


# ----- per-address results and the verdict --------------------------------------------------------


@dataclass(frozen=True)
class AddressResult:
    """One sampled address: ground truth, both reads and the per-address checks.

    ``frame_missing`` (diagnostic, not a PASS condition) counts the address's own >= $1,000
    frame-hour trades that a complete read-1 history does not contain.
    """

    address: str
    truth: int
    matched: int
    read1: Classification
    read1_pages: int
    read2: Classification | None
    read_gap_seconds: float | None
    violations: int
    read1_transport_failure: bool = False
    duplicate_rows: int = 0
    frame_missing: int = 0

    def active(self) -> bool:
        return self.truth >= 1

    def heavy(self, boundary: int) -> bool:
        return self.truth >= boundary

    def q1_eligible(self) -> bool:
        """Active, and read 1 was served: a throttle, 5xx or no answer is not a recall miss."""
        return self.active() and not self.read1_transport_failure

    def recall(self) -> Decimal | None:
        return Decimal(self.matched) / Decimal(self.truth) if self.truth else None

    def q4_population(self) -> bool:
        """Q4 covers every address with a complete read-1 history."""
        return self.read1.complete

    def q4_measured(self) -> bool:
        second_complete = self.read2 is not None and self.read2.complete
        return second_complete and (self.read_gap_seconds or 0) >= MIN_READ_GAP_SECONDS

    def stable(self) -> bool:
        return self.read2 is not None and self.read1.earliest == self.read2.earliest

    def q5b_population(self, boundary: int) -> bool:
        return self.heavy(boundary) and self.read1.complete

    def boundary_violation(self, boundary: int) -> bool:
        """Q5 (b): ingestion saw at least ``B`` trades but the complete history counts fewer."""
        count = self.read1.trade_count
        return self.heavy(boundary) and count is not None and count < boundary


def frame_missing(first: Walk, frame_trades: set[str]) -> int:
    return len(frame_trades - first.identities()) if is_complete_history(first) else 0


def address_result(
    address: str,
    truth: set[str],
    walks: Mapping[tuple[int, str], Walk],
    frame_trades: set[str] | None = None,
) -> AddressResult:
    first = walks[(1, address)]
    second = walks.get((2, address))
    return AddressResult(
        address=address,
        truth=len(truth),
        matched=len(truth & first.identities()),
        read1=classify(first),
        read1_pages=len(first.pages),
        read2=None if second is None else classify(second),
        read_gap_seconds=None if second is None else second.started_at - first.started_at,
        violations=sum(ordering_violations(w) for w in (first, second) if w is not None),
        read1_transport_failure=first.transport_failure(),
        duplicate_rows=sum(w.duplicate_rows() for w in (first, second) if w is not None),
        frame_missing=frame_missing(first, frame_trades or set()),
    )


@dataclass(frozen=True)
class Metrics:
    proven_seconds: int
    queried: int
    active: int
    heavy: int
    recall_aggregate: Decimal | None
    recall_minimum: Decimal | None
    ordering_violations: int
    complete_read1: int
    incomplete_read1: int
    stable_population: int
    stable: int
    replay: tuple[ReplayResult, ...]
    boundary_violations: int
    requests: int
    deviations: tuple[str, ...] = ()
    q1_eligible: int = 0
    q4_unmeasured: int = 0
    q5b_population: int = 0
    duplicate_rows: int = 0
    frame_missing: int = 0


def recall_figures(results: Sequence[AddressResult]) -> tuple[Decimal | None, Decimal | None]:
    eligible = [r for r in results if r.q1_eligible()]
    if not eligible:
        return None, None
    aggregate = Decimal(sum(r.matched for r in eligible)) / Decimal(sum(r.truth for r in eligible))
    return aggregate, min(r.recall() or Decimal(0) for r in eligible)


def metrics(
    window: Window,
    results: Sequence[AddressResult],
    replay: Sequence[ReplayResult],
    requests: int,
    deviations: Sequence[str],
    boundary: int = B_DEFAULT,
) -> Metrics:
    aggregate, minimum = recall_figures(results)
    population = [r for r in results if r.q4_population()]
    measured = [r for r in population if r.q4_measured()]
    return Metrics(
        proven_seconds=window.proven_seconds(),
        queried=len(results),
        active=sum(r.active() for r in results),
        heavy=sum(r.heavy(boundary) for r in results),
        recall_aggregate=aggregate,
        recall_minimum=minimum,
        ordering_violations=sum(r.violations for r in results),
        complete_read1=len(population),
        incomplete_read1=len(results) - len(population),
        stable_population=len(measured),
        stable=sum(r.stable() for r in measured),
        replay=tuple(replay),
        boundary_violations=sum(r.boundary_violation(boundary) for r in results),
        requests=requests,
        deviations=tuple(deviations),
        q1_eligible=sum(r.q1_eligible() for r in results),
        q4_unmeasured=len(population) - len(measured),
        q5b_population=sum(r.q5b_population(boundary) for r in results),
        duplicate_rows=sum(r.duplicate_rows for r in results),
        frame_missing=sum(r.frame_missing for r in results),
    )


def inconclusive_reasons(m: Metrics) -> list[str]:
    """Coverage not met, the window not proven, or a protocol deviation (R09a)."""
    reasons = [f"protocol deviation: {d}" for d in m.deviations]
    if m.proven_seconds < MIN_PROVEN_SECONDS:
        reasons.append(f"proven coverage {m.proven_seconds} s < {MIN_PROVEN_SECONDS} s")
    if m.q1_eligible < MIN_ACTIVE:
        reasons.append(f"{m.q1_eligible} active addresses with a served read 1 < {MIN_ACTIVE}")
    if m.q4_unmeasured:
        reasons.append(f"Q4 coverage not met: {m.q4_unmeasured} complete histories lack read 2")
    if m.stable_population == 0:
        reasons.append("no address has two complete-history reads >= 1 h apart (Q4 unmeasured)")
    return reasons


def recall_failures(m: Metrics) -> list[str]:
    failures: list[str] = []
    if m.recall_aggregate is None or m.recall_aggregate < Q1_AGGREGATE:
        failures.append(f"Q1 aggregate recall {m.recall_aggregate} < {Q1_AGGREGATE}")
    if m.recall_minimum is None or m.recall_minimum < Q1_PER_ADDRESS:
        failures.append(f"Q1 minimum per-address recall {m.recall_minimum} < {Q1_PER_ADDRESS}")
    return failures


def fail_reasons(m: Metrics) -> list[str]:
    failures = recall_failures(m)
    if m.ordering_violations:
        failures.append(f"Q2 {m.ordering_violations} ordering violations")
    if m.stable != m.stable_population:
        failures.append(f"Q4 earliest trade stable for {m.stable} of {m.stable_population}")
    failures += [f"Q5 (a) replay case {r.name} does not hold" for r in m.replay if not r.holds]
    if m.boundary_violations:
        failures.append(f"Q5 (b) {m.boundary_violations} live boundary violations")
    return failures


def limits(m: Metrics) -> list[str]:
    """Reported, never judged: what the PASS rule does not cover in this run."""
    notes = [
        "Q3 is true by construction: the classifier is the complete-history rule itself.",
        "Q5 (a) replays the judge's own classifier; no O6 runtime exists yet.",
        f"B = {B_DEFAULT} is the FR-009 protocol default, not a value captured from the product.",
        f"Q5 (b) judged on {m.q5b_population} heavy addresses with a complete read-1 history.",
        f"{m.active - m.q1_eligible} active addresses excluded from Q1"
        " for a transport failure (throttle, 5xx or no answer) in read 1.",
        f"{m.duplicate_rows} duplicate rows across walks (counted once per identity).",
        f"{m.frame_missing} frame-hour trades missing from complete read-1 histories (diagnostic).",
    ]
    return notes


def verdict(m: Metrics) -> tuple[str, list[str]]:
    """INCONCLUSIVE first (as R09a orders it), then FAIL; ``fail_reasons`` is always reported."""
    inconclusive = inconclusive_reasons(m)
    if inconclusive:
        return "INCONCLUSIVE", inconclusive
    failures = fail_reasons(m)
    return ("FAIL", failures) if failures else ("PASS", [])
