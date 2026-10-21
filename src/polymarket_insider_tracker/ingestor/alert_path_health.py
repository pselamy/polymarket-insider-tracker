"""A health monitor that also reports the default alert path (FR-022a, T029).

The ``alert_path`` object is informational: it never changes the overall status, the HTTP status
code, ``/ready``, ``/live`` or ``/metrics``, because an unreachable default is a disclosed
posture (DECISION D2 R0), not a fault.
"""

from __future__ import annotations

import logging
from typing import Any

from polymarket_insider_tracker.detector.alert_path import alert_path_report, log_alert_path
from polymarket_insider_tracker.ingestor.health import (
    ComponentStatus,
    HealthMonitor,
    HealthReport,
    HealthStatus,
)


class AlertPathHealthMonitor(HealthMonitor):
    """``HealthMonitor`` whose ``/health`` body carries the alert-path report for ``threshold``."""

    def __init__(self, threshold: float) -> None:
        super().__init__()
        self._alert_path = alert_path_report(threshold)

    def log_alert_path(self, logger: logging.Logger) -> None:
        """Emit the startup record: WARNING when unreachable, INFO otherwise."""
        log_alert_path(logger, self._alert_path)

    def _build_health_body(
        self,
        report: HealthReport,
        components: dict[str, ComponentStatus],
        overall_status: HealthStatus,
        now: float,
    ) -> dict[str, Any]:
        body = super()._build_health_body(report, components, overall_status, now)
        return {**body, "alert_path": self._alert_path.fields()}
