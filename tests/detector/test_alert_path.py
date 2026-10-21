"""R0 / FR-022a (T029): the default alert path is disclosed as unreachable, overrides still alert."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

import pytest
from aiohttp import ClientSession
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.config_summary import print_config_summary
from polymarket_insider_tracker.detector.alert_path import (
    LiveEvidenceCapability,
    alert_path_report,
    log_alert_path,
    scorer_alert_path_report,
)
from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import (
    DEFAULT_ALERT_THRESHOLD,
    RiskScorer,
    SignalBundle,
)
from polymarket_insider_tracker.detector.size_anomaly import MAX_NICHE_CONFIDENCE
from polymarket_insider_tracker.ingestor.alert_path_health import AlertPathHealthMonitor
from polymarket_insider_tracker.ingestor.health import HealthMonitor
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline
from tests.fakes import make_test_settings


def _best_case_bundle(trade: TradeEvent) -> SignalBundle:
    """The best size-group evidence the detector can produce: saturated volume, niche."""
    metadata = MarketMetadata("m", "Q?", "", (Token("t", "Yes", trade.price),), category="science")
    signal = SizeAnomalySignal(
        trade_event=trade,
        market_metadata=metadata,
        volume_impact=0.325,
        book_impact=0.0,
        is_niche_market=True,
        confidence=MAX_NICHE_CONFIDENCE,
        factors={},
    )
    return SignalBundle(trade_event=trade, size_anomaly_signal=signal)


def test_the_shipped_threshold_is_still_0_80() -> None:
    assert DEFAULT_ALERT_THRESHOLD == 0.80
    assert make_test_settings().detector.alert_threshold == 0.80


def test_default_threshold_is_unreachable_with_ceiling_0_450() -> None:
    report = alert_path_report(DEFAULT_ALERT_THRESHOLD)

    assert report.fields() == {
        "status": "unreachable",
        "threshold": "0.800",
        "ceiling": "0.450",
        "wallet_group_status": "unqualified_pre_T027",
        "wallet_age_source": "none",
    }


@pytest.mark.parametrize(
    ("threshold", "status"), [(0.45, "reachable"), (0.451, "unreachable"), (0.4, "reachable")]
)
def test_the_boundary_uses_the_scorers_inclusive_comparison(threshold: float, status: str) -> None:
    assert alert_path_report(threshold).status == status


def test_the_ceiling_is_what_the_real_scorer_gives_the_best_case(sample_trade: TradeEvent) -> None:
    score, groups = RiskScorer(FakeAsyncRedis()).calculate_weighted_score(
        _best_case_bundle(sample_trade)
    )

    assert (Decimal(str(score)), groups) == (alert_path_report(0.8).ceiling, 1)


async def test_no_default_configuration_assessment_alerts_but_an_override_does(
    sample_trade: TradeEvent,
) -> None:
    bundle = _best_case_bundle(sample_trade)

    default = await RiskScorer(FakeAsyncRedis()).assess(bundle)
    override = await RiskScorer(FakeAsyncRedis(), alert_threshold=0.45).assess(bundle)

    assert (default.weighted_score, default.should_alert) == (0.45, False)
    assert (override.weighted_score, override.should_alert) == (0.45, True)


OPERATOR_WEIGHTS = {"size_anomaly": 0.7, "niche_market": 0.5}


async def test_operator_weights_change_the_ceiling_and_status(sample_trade: TradeEvent) -> None:
    with pytest.warns(DeprecationWarning):
        scorer = RiskScorer(FakeAsyncRedis(), weights=OPERATOR_WEIGHTS)
    assessment = await scorer.assess(_best_case_bundle(sample_trade))

    report = scorer_alert_path_report(scorer)
    assert (report.status, report.threshold, report.ceiling) == (
        "reachable",
        Decimal("0.800"),
        Decimal("0.900"),
    )
    assert (assessment.weighted_score, assessment.should_alert) == (0.9, True)


async def test_a_bound_monitor_reports_from_the_scorers_current_weights(
    caplog: pytest.LogCaptureFixture,
) -> None:
    monitor = AlertPathHealthMonitor(0.8)
    scorer = RiskScorer(FakeAsyncRedis())
    logger = logging.getLogger("alert-path-bind-test")
    with caplog.at_level(logging.INFO, logger=logger.name):
        monitor.bind_scorer(scorer, logger)
    before = await _health(monitor)
    with pytest.warns(DeprecationWarning):
        scorer.set_weights(OPERATOR_WEIGHTS)

    assert [r.levelno for r in caplog.records if r.name == logger.name] == [logging.WARNING]
    assert before.alert_path == alert_path_report(0.8).fields()
    assert (await _health(monitor)).alert_path == alert_path_report(0.8, OPERATOR_WEIGHTS).fields()


def test_unreachable_logs_a_warning_and_reachable_an_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("alert-path-test")
    with caplog.at_level(logging.INFO, logger=logger.name):
        log_alert_path(logger, alert_path_report(0.8))
        log_alert_path(logger, alert_path_report(0.45))

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            "default alert path unreachable: threshold=0.800 ceiling=0.450 "
            "wallet_group_status=unqualified_pre_T027 wallet_age_source=none",
        ),
        (
            logging.INFO,
            "default alert path reachable: threshold=0.450 ceiling=0.450 "
            "wallet_group_status=unqualified_pre_T027 wallet_age_source=none",
        ),
    ]


def test_the_capability_is_a_code_constant_not_a_setting() -> None:
    assert LiveEvidenceCapability().wallet_age_source == "none"
    assert not hasattr(make_test_settings().detector, "wallet_group_status")


def test_config_summary_prints_the_alert_path_without_network(
    capsys: pytest.CaptureFixture[str],
) -> None:
    print_config_summary(make_test_settings())

    assert "  Alert Path: unreachable (threshold 0.800, best-case score 0.450" in (
        capsys.readouterr().out
    )


@dataclass(frozen=True)
class HealthSurface:
    """What the health endpoints answered: HTTP statuses and the /health body."""

    statuses: dict[str, int]
    status: str
    alert_path: object


async def _health(monitor: HealthMonitor) -> HealthSurface:
    monitor.record_event("trades")
    await monitor.start_http_server(port=0)
    try:
        port = monitor.http_addresses[0][1]
        async with ClientSession(f"http://127.0.0.1:{port}") as client:
            statuses: dict[str, int] = {}
            for path in ("/health", "/ready", "/live", "/metrics"):
                async with client.get(path) as response:
                    statuses[path] = response.status
            async with client.get("/health") as response:
                body = await response.json()
    finally:
        await monitor.stop_http_server()
    return HealthSurface(statuses, body["status"], body.get("alert_path"))


async def test_the_alert_path_object_never_changes_any_health_answer() -> None:
    shown = await _health(AlertPathHealthMonitor(0.8))
    plain = await _health(HealthMonitor())

    assert shown.alert_path == alert_path_report(0.8).fields()
    assert plain.alert_path is None
    assert (shown.statuses, shown.status) == (plain.statuses, plain.status)


async def test_the_pipeline_sets_the_alert_path_on_its_monitor() -> None:
    surface = await _health(Pipeline(make_test_settings()).health_monitor)

    assert surface.alert_path == alert_path_report(0.8).fields()
    surface = await _health(Pipeline(make_test_settings(alert_threshold=0.4)).health_monitor)
    assert surface.alert_path == alert_path_report(0.4).fields()
