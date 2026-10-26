"""T027 Part B rules (research.md R09a v2): window, sample, ground truth, walks, Q1-Q5, verdict."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal

import pytest
from scripts.spec004_evidence import t027_partb as pb

T0 = 1_000_000
ADDRESS = "0xabc"


def status(boundary: int | None, losses: list[tuple[int, int]] | None = None) -> dict[str, object]:
    events = [{"from_time": a, "to_time": b} for a, b in losses or []]
    return {"boundary_time": boundary, "loss_events": events}


def trade(
    ts: int, wallet: str = ADDRESS, notional: str = "1500", identity: str = ""
) -> pb.CapturedTrade:
    return pb.CapturedTrade(identity or f"id-{wallet}-{ts}", wallet, Decimal(notional), ts)


def page(
    index: int,
    rows: list[tuple[str, int]],
    *,
    cursor: str | None = None,
    next_cursor: str | None = None,
    read: int = 1,
    at: float = 0.0,
) -> pb.PageRecord:
    params = {"user": ADDRESS, "taker_only": "false", "start": "1", "end": str(T0 + 90_000)}
    params |= {"cursor": cursor} if cursor else {"limit": "1000"}
    return pb.PageRecord(
        address=ADDRESS,
        read=read,
        page=index,
        requested_at=at + index,
        params=params,
        status=200,
        error=None,
        next_cursor=next_cursor,
        has_more=next_cursor is not None,
        rows=tuple(pb.ReturnedRow(i, ts) for i, ts in rows),
        invalid_rows=0,
    )


def two_page_walk(read: int = 1, at: float = 0.0) -> pb.Walk:
    first = page(0, [("a", 30), ("b", 20)], next_cursor="c1", read=read, at=at)
    second = page(1, [("c", 10)], cursor="c1", read=read, at=at)
    return pb.Walk(ADDRESS, read, (first, second))


# ----- window -------------------------------------------------------------------------------------


def test_the_window_starts_at_the_first_proven_boundary_and_merges_losses() -> None:
    statuses = [
        status(None),
        status(T0),
        status(T0 + 50_000, [(T0 + 100, T0 + 200)]),
        status(T0 + 90_000, [(T0 + 100, T0 + 200), (T0 + 150, T0 + 400)]),
    ]
    window = pb.window_from(statuses)
    assert (window.t0, window.t1, window.sampled_at) == (T0, T0 + 86_400, T0 + 3_600)
    assert window.unproven_seconds() == 300
    assert window.proven_seconds() == 86_100
    assert not window.covers(T0 + 150) and window.covers(T0 + 401)


def test_time_past_the_last_proven_boundary_is_unproven() -> None:
    window = pb.window_from([status(T0), status(T0 + 70_000)])
    assert window.proven_seconds() == 70_000
    assert not window.covers(T0 + 70_001)


def test_a_capture_that_never_proved_a_boundary_has_no_window() -> None:
    with pytest.raises(ValueError, match="never proved"):
        pb.window_from([status(None)])


def test_loss_overlap_is_clipped_to_the_window() -> None:
    assert pb.Interval(10, 30).overlap(20, 100) == 10
    assert pb.Interval(10, 30).overlap(40, 100) == 0


# ----- frame, sample, ground truth ----------------------------------------------------------------


def test_the_frame_is_first_hour_wallets_with_a_1000_dollar_trade() -> None:
    window = pb.window_from([status(T0), status(T0 + 90_000)])
    trades = [
        trade(T0, "0x1"),
        trade(T0 + 3_599, "0x2", notional="1000"),
        trade(T0 + 3_600, "0x3"),
        trade(T0 + 10, "0x4", notional="999.99"),
        trade(T0 - 1, "0x5"),
    ]
    assert pb.frame(trades, window) == ["0x1", "0x2"]


def test_the_sample_is_reproducible_from_the_seed_and_capped_at_n() -> None:
    frame = [f"0x{i:03}" for i in range(300)]
    first = pb.draw_sample(frame, 20261026)
    assert first == pb.draw_sample(frame, 20261026)
    assert len(first) == 200 and len(set(first)) == 200
    assert pb.draw_sample(frame[:5], 1) == pb.draw_sample(frame[:5], 1)
    assert sorted(pb.draw_sample(frame[:5], 1)) == frame[:5]


def test_ground_truth_counts_sampled_trades_after_sampling_at_proven_times() -> None:
    window = pb.window_from([status(T0), status(T0 + 90_000, [(T0 + 5_000, T0 + 5_010)])])
    trades = [
        trade(T0 + 3_600),
        trade(T0 + 3_601),
        trade(T0 + 5_005),
        trade(T0 + 86_400),
        trade(T0 + 86_401),
        trade(T0 + 4_000, "0xother"),
    ]
    truth = pb.ground_truth(trades, window, [ADDRESS])
    assert truth == {ADDRESS: {f"id-{ADDRESS}-{T0 + 3_601}", f"id-{ADDRESS}-{T0 + 86_400}"}}


def test_a_captured_trade_and_a_loss_parse_from_their_json_lines() -> None:
    record = {"identity": "x", "wallet": "0xAB", "notional": "12.5", "ts": 5}
    assert pb.captured_trade(record) == pb.CapturedTrade("x", "0xab", Decimal("12.5"), 5)
    assert pb.loss_interval({"from_time": 1, "to_time": 2}) == pb.Interval(1, 2)


# ----- walks: the complete-history rule (Q3) and ordering (Q2) ------------------------------------


def test_a_chained_terminal_walk_is_complete_with_count_and_earliest() -> None:
    outcome = pb.classify(two_page_walk())
    assert outcome == pb.Classification(True, 3, (10, ("c",)))
    assert outcome.low_activity(5) is True and outcome.low_activity(3) is False


def swap_last(walk: pb.Walk, last: pb.PageRecord) -> pb.Walk:
    return pb.with_last(walk, last)


def last(walk: pb.Walk) -> pb.PageRecord:
    return walk.pages[-1]


def every_page(walk: pb.Walk, key: str, value: str) -> pb.Walk:
    pages = tuple(replace(p, params={**p.params, key: value}) for p in walk.pages)
    return pb.Walk(walk.address, walk.read, pages)


INCOMPLETE: dict[str, Callable[[pb.Walk], pb.Walk]] = {
    "dropped last page": lambda w: pb.with_last(w, None),
    "error": lambda w: swap_last(w, replace(last(w), status=500)),
    "error text": lambda w: swap_last(w, replace(last(w), error="ReadTimeout")),
    "invalid row": lambda w: swap_last(w, replace(last(w), invalid_rows=1)),
    "has_more on last": lambda w: swap_last(w, replace(last(w), has_more=True)),
    "cursor on last": lambda w: swap_last(w, replace(last(w), next_cursor="c2")),
    "wrong cursor": lambda w: swap_last(
        w, replace(last(w), params={**last(w).params, "cursor": "zz"})
    ),
    "changed end": lambda w: swap_last(w, replace(last(w), params={**last(w).params, "end": "1"})),
    "taker only": lambda w: every_page(w, "taker_only", "true"),
    "start floor": lambda w: every_page(w, "start", "9"),
    "other user": lambda w: every_page(w, "user", "0xdef"),
    "cursor on first page": lambda w: pb.Walk(
        ADDRESS,
        1,
        (replace(w.pages[0], params={**w.pages[0].params, "cursor": "x"}), *w.pages[1:]),
    ),
    "middle page claims the end": lambda w: pb.Walk(
        ADDRESS, 1, (replace(w.pages[0], has_more=False), *w.pages[1:])
    ),
    "no pages": lambda w: pb.Walk(w.address, 1, ()),
}


@pytest.mark.parametrize("name", sorted(INCOMPLETE))
def test_every_other_walk_is_incomplete_and_yields_unknown(name: str) -> None:
    outcome = pb.classify(INCOMPLETE[name](two_page_walk()))
    assert outcome == pb.Classification(False, None, None)
    assert outcome.low_activity(5) is None


def test_ordering_violations_count_newer_rows_after_older_ones() -> None:
    walk = pb.Walk(ADDRESS, 1, (page(0, [("a", 10), ("b", 20), ("c", 5), ("d", 6)]),))
    assert pb.ordering_violations(walk) == 2


def test_page_records_parse_from_the_walker_json_line() -> None:
    record = {
        "address": "0xABC",
        "read": 2,
        "page": 1,
        "requested_at": 12.5,
        "params": {"user": "0xabc"},
        "status": None,
        "error": "ConnectTimeout",
        "next_cursor": None,
        "has_more": None,
        "body_sha256": None,
        "rows": [["i", 3]],
        "invalid_rows": 0,
    }
    parsed = pb.page_record(record)
    assert (parsed.address, parsed.read, parsed.status, parsed.rows) == (
        "0xabc",
        2,
        None,
        (pb.ReturnedRow("i", 3),),
    )
    with pytest.raises(TypeError, match="boolean"):
        pb.page_record(record | {"has_more": "yes"})


# ----- Q5 (a) -------------------------------------------------------------------------------------


def test_q5a_replay_holds_for_every_walk_detectable_failure_at_the_boundary() -> None:
    results = pb.q5a(two_page_walk().pages[0], 5)
    assert {r.name for r in results} == {
        "complete-exactly-B",
        "last-page-dropped",
        "last-page-http-500",
        "last-page-timeout",
        "terminal-flags-contradict",
        "filters-changed",
        "invalid-row-on-page",
    }
    assert all(r.holds for r in results)
    exact = next(r for r in results if r.name == "complete-exactly-B")
    assert exact.complete and exact.low_activity is False


def test_q5a_reports_a_classifier_that_would_call_a_partial_walk_low_activity() -> None:
    case = pb.ReplayCase("bad", two_page_walk(), expect_complete=False)
    assert not pb.replay_result(case, 5).holds
    expected = pb.ReplayCase("bad", pb.with_last(two_page_walk(), None), expect_complete=True)
    assert not pb.replay_result(expected, 5).holds


# ----- per-address results, metrics and the verdict ------------------------------------------------


def result(
    truth: int = 3, matched: int = 3, *, second: bool = True, gap: float = 4_000.0
) -> pb.AddressResult:
    complete = pb.classify(two_page_walk())
    return pb.AddressResult(
        address=ADDRESS,
        truth=truth,
        matched=matched,
        read1=complete,
        read1_pages=2,
        read2=complete if second else None,
        read_gap_seconds=gap if second else None,
        violations=0,
    )


def test_address_result_combines_both_reads() -> None:
    truth = {"a", "b", "z"}
    walks = {(1, ADDRESS): two_page_walk(), (2, ADDRESS): two_page_walk(read=2, at=5_000.0)}
    got = pb.address_result(ADDRESS, truth, walks)
    assert (got.truth, got.matched, got.read1_pages, got.read_gap_seconds) == (3, 2, 2, 5_000.0)
    assert got.recall() == Decimal(2) / Decimal(3)
    assert got.q4_population() and got.q4_measured() and got.stable()


def test_a_heavy_address_whose_complete_history_counts_fewer_than_b_is_a_q5b_violation() -> None:
    assert result(truth=5, matched=5).boundary_violation(5)
    assert not result(truth=2, matched=2).boundary_violation(5)


def test_q4_is_measured_only_by_two_complete_reads_an_hour_apart() -> None:
    assert result().q4_measured()
    assert not result(gap=3_599.0).q4_measured()
    assert not result(second=False).q4_measured()
    assert result(second=False).q4_population()


WINDOW = pb.Window(T0, T0 + 90_000, ())


def passing() -> pb.Metrics:
    replay = tuple(pb.q5a(two_page_walk().pages[0], 5))
    return pb.metrics(WINDOW, [result()] * 60, replay, 400, [])


def test_a_receipt_meeting_every_rule_passes() -> None:
    m = passing()
    assert (m.active, m.heavy, m.stable_population, m.stable) == (60, 0, 60, 60)
    assert m.recall_aggregate == 1 and m.recall_minimum == 1
    assert pb.verdict(m) == ("PASS", [])


BROKEN_REPLAY = (pb.ReplayResult("x", True, True, False),)

FAILING = {
    "aggregate recall": replace(passing(), recall_aggregate=Decimal("0.98")),
    "minimum recall": replace(passing(), recall_minimum=Decimal("0.94")),
    "no recall": replace(passing(), recall_aggregate=None, recall_minimum=None),
    "ordering": replace(passing(), ordering_violations=1),
    "stability": replace(passing(), stable=59),
    "replay": replace(passing(), replay=BROKEN_REPLAY),
    "boundary": replace(passing(), boundary_violations=1),
}


@pytest.mark.parametrize("name", sorted(FAILING))
def test_any_violated_inequality_fails(name: str) -> None:
    outcome, reasons = pb.verdict(FAILING[name])
    assert outcome == "FAIL" and len(reasons) >= 1


INCONCLUSIVE = {
    "coverage": replace(passing(), proven_seconds=pb.MIN_PROVEN_SECONDS - 1),
    "too few served active": replace(passing(), q1_eligible=pb.MIN_ACTIVE - 1),
    "read 2 missing": replace(passing(), q4_unmeasured=1),
    "no stability population": replace(passing(), stable_population=0, stable=0),
    "deviation": replace(passing(), deviations=("budget exceeded",)),
}


@pytest.mark.parametrize("name", sorted(INCONCLUSIVE))
def test_unmet_coverage_or_a_deviation_is_inconclusive(name: str) -> None:
    outcome, reasons = pb.verdict(INCONCLUSIVE[name])
    assert outcome == "INCONCLUSIVE" and len(reasons) == 1


def test_recall_figures_are_undefined_without_active_addresses() -> None:
    assert pb.recall_figures([result(truth=0, matched=0)]) == (None, None)
    assert pb.recall_figures([result(4, 4), result(2, 1)]) == (Decimal(5) / 6, Decimal("0.5"))


def test_a_second_read_that_failed_makes_q4_coverage_unmet_not_a_silent_pass() -> None:
    replay = tuple(pb.q5a(two_page_walk().pages[0], 5))
    results = [result()] * 59 + [result(second=False)]
    m = pb.metrics(WINDOW, results, replay, 400, [])
    assert (m.complete_read1, m.stable_population, m.q4_unmeasured) == (60, 59, 1)
    outcome, reasons = pb.verdict(m)
    assert outcome == "INCONCLUSIVE" and reasons == [
        "Q4 coverage not met: 1 complete histories lack read 2"
    ]


def test_a_transport_failure_in_read_one_excludes_the_address_from_q1() -> None:
    served = result(3, 3)
    throttled = replace(result(3, 0), read1_transport_failure=True)
    assert not throttled.q1_eligible()
    assert pb.recall_figures([served, throttled]) == (Decimal(1), Decimal(1))
    m = pb.metrics(WINDOW, [served, throttled], (), 2, [])
    assert "1 active addresses excluded from Q1" in " ".join(pb.limits(m))


def test_a_complete_history_counts_each_identity_once() -> None:
    first = page(0, [("a", 30), ("b", 20)], next_cursor="c1")
    second = page(1, [("b", 20), ("c", 10)], cursor="c1")
    walk = pb.Walk(ADDRESS, 1, (first, second))
    assert pb.classify(walk).trade_count == 3
    assert walk.duplicate_rows() == 1


@pytest.mark.parametrize(
    ("index", "extra"),
    [(0, {"offset": "0"}), (0, {"market": "m"}), (1, {"limit": "1000"}), (1, {"offset": "5"})],
)
def test_any_undocumented_parameter_makes_the_walk_incomplete(
    index: int, extra: dict[str, str]
) -> None:
    walk = two_page_walk()
    pages = list(walk.pages)
    pages[index] = replace(pages[index], params={**pages[index].params, **extra})
    broken = pb.Walk(ADDRESS, 1, tuple(pages))
    assert pb.failed_check(broken) == "documented-parameters-only"
    assert not pb.classify(broken).complete


def test_failed_check_names_the_first_unmet_condition() -> None:
    assert pb.failed_check(two_page_walk()) is None
    assert pb.failed_check(pb.Walk(ADDRESS, 1, ())) == "no-pages"
    assert pb.failed_check(pb.with_last(two_page_walk(), None)) == "terminal-final-page"


def test_proven_intervals_are_the_window_minus_losses() -> None:
    window = pb.Window(T0, T0 + 100_000, (pb.Interval(T0 + 10, T0 + 20), pb.Interval(T0, T0 + 2)))
    assert window.proven_intervals() == [
        pb.Interval(T0 + 3, T0 + 9),
        pb.Interval(T0 + 21, T0 + 86_400),
    ]
    assert pb.Window(T0, T0 - 1, ()).proven_intervals() == []


def test_frame_trades_are_each_sampled_address_s_own_first_hour_large_trades() -> None:
    window = pb.window_from([status(T0), status(T0 + 90_000)])
    trades = [trade(T0 + 5), trade(T0 + 6, notional="10"), trade(T0 + 4_000), trade(T0 + 7, "0xz")]
    assert pb.frame_trades(trades, window, [ADDRESS]) == {ADDRESS: {f"id-{ADDRESS}-{T0 + 5}"}}


def test_a_complete_history_missing_a_frame_trade_is_reported() -> None:
    walks = {(1, ADDRESS): two_page_walk()}
    got = pb.address_result(ADDRESS, {"a"}, walks, {"a", "frame-trade"})
    assert got.frame_missing == 1
    incomplete = {(1, ADDRESS): pb.with_last(two_page_walk(), None)}
    assert pb.address_result(ADDRESS, set(), incomplete, {"x"}).frame_missing == 0
