"""Duplicate-row folding in a page analysis."""

from polymarket_insider_tracker.ingestor.page_analysis import PageAnalysis
from polymarket_insider_tracker.ingestor.trade_rows import (
    InvalidRow,
    OutcomeResolution,
    parse_trade_row,
)
from tests.fakes.trades import trade_row

NOW = 1_700_000_000


def test_a_complete_repeat_replaces_an_earlier_copy_with_unknown_outcome() -> None:
    analysis = PageAnalysis(cutoff=NOW)
    analysis.add_rows(
        [
            trade_row(timestamp=NOW, outcome=None, outcome_index=None),
            trade_row(timestamp=NOW, outcome="Yes", outcome_index=0),
        ],
        now=NOW,
    )

    (observation,) = analysis.observations.values()
    assert observation.outcome_resolution is OutcomeResolution.PROVIDED
    assert observation.event.outcome == "Yes"
    assert analysis.repeats == 1
    assert analysis.row_count == 2


def test_every_row_is_tallied_as_invalid_deferred_repeated_or_kept() -> None:
    analysis = PageAnalysis(cutoff=NOW)
    analysis.add_rows(
        [
            trade_row(timestamp=NOW - 5, transaction=1),
            trade_row(timestamp=NOW - 5, transaction=1),
            trade_row(timestamp=NOW - 5, transaction=1),
            trade_row(timestamp=NOW + 1, transaction=2),
            trade_row(timestamp=NOW + 2, transaction=3),
            trade_row(timestamp=NOW + 2, transaction=4),
            {"timestamp": NOW},
        ],
        now=NOW,
    )

    assert analysis.row_count == 7
    assert analysis.repeats == 2
    assert analysis.deferred == 3
    assert len(analysis.observations) == 1
    assert len(analysis.outcomes) == 6
    assert analysis.latest_seen == NOW + 2
    assert [row.field for row in analysis.invalid] == ["transactionHash"]


def test_a_zero_timestamp_row_is_seen_at_zero() -> None:
    analysis = PageAnalysis(cutoff=NOW)
    analysis.add_rows([trade_row(timestamp=0)], now=NOW)

    assert analysis.latest_seen == 0
    assert analysis.oldest == 0
    assert analysis.newest == 0
    assert analysis.span_seconds == 0


def test_already_decoded_observations_fold_exactly_like_raw_rows() -> None:
    rows = [
        trade_row(timestamp=NOW - 9, transaction=1),
        trade_row(timestamp=NOW - 2, transaction=2),
        trade_row(timestamp=NOW - 2, transaction=2),
        trade_row(timestamp=NOW + 3, transaction=3),
    ]
    from_rows = PageAnalysis(cutoff=NOW)
    from_rows.add_rows(rows, now=NOW)
    parsed = [parse_trade_row(row, now=NOW) for row in rows]
    decoded = [observation for observation in parsed if not isinstance(observation, InvalidRow)]
    from_observations = PageAnalysis(cutoff=NOW)

    from_observations.add_observations(decoded)

    assert from_observations == from_rows
    assert from_observations.row_count == 4
    assert from_observations.repeats == 1
    assert from_observations.deferred == 1
    assert from_observations.span_seconds == 7
