"""A health monitor that also reports the default alert path (FR-022a, T029).

The ``alert_path`` object is informational: it never changes the overall status, the HTTP status
code, ``/ready``, ``/live`` or ``/metrics``, because an unreachable default is a disclosed
posture (DECISION D2 R0), not a fault.
"""

from __future__ import annotations

import logging
from typing import Any

from polymarket_insider_tracker.detector.alert_path import (
    AlertPathReport,
    alert_path_report,
    log_alert_path,
    scorer_alert_path_report,
)
from polymarket_insider_tracker.detector.scorer import RiskScorer
from polymarket_insider_tracker.ingestor.health import (
    ComponentStatus,
    HealthMonitor,
    HealthReport,
    HealthStatus,
)


class AlertPathHealthMonitor(HealthMonitor):
    """``HealthMonitor`` whose ``/health`` body carries the alert-path report.

    Until a scorer is bound the report uses ``threshold`` with the default weights; once bound it
    is recomputed from the scorer's effective threshold and weights on every answer, so ``/health``
    and the startup record cannot disagree with the scorer (deprecated ``set_weights`` included).
    """

    def __init__(self, threshold: float) -> None:
        super().__init__()
        self._threshold = threshold
        self._scorer: RiskScorer | None = None

    @property
    def alert_path(self) -> AlertPathReport:
        """The report for the bound scorer, or for ``threshold`` before one is bound."""
        if self._scorer is None:
            return alert_path_report(self._threshold)
        return scorer_alert_path_report(self._scorer)

    def bind_scorer(self, scorer: RiskScorer, logger: logging.Logger) -> None:
        """Report from ``scorer`` from now on and emit the startup record (WARNING when
        unreachable, INFO otherwise)."""
        self._scorer = scorer
        log_alert_path(logger, self.alert_path)

    def _build_health_body(
        self,
        report: HealthReport,
        components: dict[str, ComponentStatus],
        overall_status: HealthStatus,
        now: float,
    ) -> dict[str, Any]:
        body = super()._build_health_body(report, components, overall_status, now)
        return {**body, "alert_path": self.alert_path.fields()}
