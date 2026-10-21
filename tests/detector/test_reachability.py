"""Threshold reachability recomputed from the real detectors and scorer (T028, FR-022, SC-009).

The ceilings published in ``evidence/reachability.md`` are recomputed here through the product
code, so a weight, cap, bonus, quantization or threshold change fails loudly; the default-posture
ceiling must equal the runtime alert-path report (FR-022a). The 65 live-replay cases are part of the
same table: each is re-scored through the product code and compared with the committed row.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Callable
from decimal import Decimal
from itertools import product
from pathlib import Path

import pytest
from scripts.spec004_evidence import reachability as reach
from scripts.spec004_evidence import replay_live
from scripts.spec004_evidence.real_scoring import Market, Wallet, score, size_signal

from polymarket_insider_tracker.detector.alert_path import alert_path_report
from polymarket_insider_tracker.detector.scorer import DEFAULT_ALERT_THRESHOLD

EVIDENCE = Path(__file__).resolve().parents[2] / "specs/004-truthful-detection-contract/evidence"
NICHE, BROAD, UNAVAILABLE_NICHE, UNAVAILABLE_OTHER = range(4)
SMALL, LARGE = reach.BANDS
UNKNOWN_AGE, KNOWN_BEST, KNOWN_TYPICAL = reach.WALLETS
Band = tuple[str, Callable[[Decimal], bool]]


def ceiling(wallet: Wallet | None, regime: int, band: Band) -> Decimal:
    return max(s for s, _ in reach.grid(wallet, regime, band[1], proposed=True))


@pytest.mark.parametrize(
    ("regime", "expected"),
    [
        (NICHE, "0.450"),
        (BROAD, "0.175"),
        (UNAVAILABLE_NICHE, "0.120"),
        (UNAVAILABLE_OTHER, "0.000"),
    ],
)
@pytest.mark.parametrize("band", [SMALL, LARGE], ids=["small", "large"])
def test_pre_t027_ceiling_per_evidence_class(regime: int, expected: str, band: Band) -> None:
    assert ceiling(None, regime, band) == Decimal(expected)


def test_pre_t027_grid_ceiling_is_the_runtime_alert_path_ceiling() -> None:
    report = alert_path_report(DEFAULT_ALERT_THRESHOLD)

    assert reach.gated_ceiling() == report.ceiling == Decimal("0.450")
    assert report.status == "unreachable"


@pytest.mark.parametrize(
    ("wallet", "regime", "band", "expected"),
    [
        (UNKNOWN_AGE, NICHE, SMALL, "0.780"),
        (UNKNOWN_AGE, NICHE, LARGE, "0.780"),
        (UNKNOWN_AGE, BROAD, LARGE, "0.450"),
        (UNKNOWN_AGE, UNAVAILABLE_NICHE, SMALL, "0.384"),
        (UNKNOWN_AGE, UNAVAILABLE_OTHER, LARGE, "0.200"),
        (KNOWN_BEST, NICHE, SMALL, "0.924"),
        (KNOWN_BEST, NICHE, LARGE, "0.972"),
        (KNOWN_BEST, BROAD, LARGE, "0.642"),
        (KNOWN_TYPICAL, NICHE, LARGE, "0.828"),
        (KNOWN_TYPICAL, BROAD, SMALL, "0.450"),
    ],
)
def test_post_t027_scenario_ceilings(
    wallet: Wallet, regime: int, band: Band, expected: str
) -> None:
    assert ceiling(wallet, regime, band) == Decimal(expected)


def test_unknown_age_never_reaches_the_default_threshold_in_any_posture() -> None:
    scores = [
        s
        for wallet, regime, band in product((None, UNKNOWN_AGE), range(4), (SMALL, LARGE))
        for s, _ in reach.grid(wallet, regime, band[1], proposed=True)
    ]

    assert max(scores) == Decimal("0.780")
    assert reach.unknown_confidence_needed() == "0.541"


def test_ratio_rounded_to_the_threshold_does_not_exceed_it() -> None:
    """$1,000 / $49,999 rounds to the ratio 0.0200 (section 3), which is not ``> 0.02``: only
    the niche base applies."""
    signal = size_signal(Market("a", Decimal(49999), False), Decimal(1000), volume_passed=True)

    assert signal == (0.2, True)
    assert score(None, signal) == Decimal("0.120")


def committed_rows() -> dict[str, dict[str, str]]:
    with (EVIDENCE / "replay-live" / "results.csv").open(newline="") as handle:
        return {row["case_id"]: row for row in csv.DictReader(handle)}


def corpus_specs() -> list[dict[str, object]]:
    corpus = json.loads((EVIDENCE / "replay-live" / "cases.json").read_text())
    return [replay_live.merged(corpus["defaults"], case) for case in corpus["cases"]]


SPECS = corpus_specs()
COMMITTED = committed_rows()


def test_the_corpus_has_65_cases_matching_the_committed_rows() -> None:
    assert len(SPECS) == 65
    assert [spec["case_id"] for spec in SPECS] == list(COMMITTED)


@pytest.mark.parametrize("spec", SPECS, ids=[str(spec["case_id"]) for spec in SPECS])
def test_live_replay_case_scores_through_the_product_code(spec: dict[str, object]) -> None:
    row = {key: str(value) for key, value in replay_live.evaluate(spec).items()}

    assert row == COMMITTED[str(spec["case_id"])]
    assert row["alerts_pre_T027_at_0.80"] == "False"
