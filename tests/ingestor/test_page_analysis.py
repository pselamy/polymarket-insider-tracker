"""Duplicate-row folding in a page analysis."""

from polymarket_insider_tracker.ingestor.page_analysis import PageAnalysis
from polymarket_insider_tracker.ingestor.trade_rows import OutcomeResolution
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
