"""The mutation gate fails on any undetected mutant of an enrolled module."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.tooling.module_loading import load_module

GATE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "mutation_gate.py"
gate = load_module("mutation_gate_under_test", GATE_PATH)

PACKAGE = "polymarket_insider_tracker"
# The step-1 enrollment: later changes may add modules, never remove or reorder these.
STEP_1_MODULES = (
    f"{PACKAGE}.endpoints",
    f"{PACKAGE}.ingestor._retry",
    f"{PACKAGE}.ingestor._attempt_budget",
    f"{PACKAGE}.ingestor._cycle_cutoff",
)
# The step-2 enrollment: the secret-redaction module.
REDACTION = f"{PACKAGE}.redaction"
# The step-3 enrollment: three small pure-logic modules.
STEP_3_MODULES = (
    f"{PACKAGE}.ingestor.trade_row_v2",
    f"{PACKAGE}.alerter.channels.pacing",
    f"{PACKAGE}.alerter._history_records",
)
# The step-4 enrollment: the codec, the evaluation clock, volume state/observation, alert path.
STEP_4_MODULES = (
    f"{PACKAGE}.ingestor._publisher_codec",
    f"{PACKAGE}.detector.evaluation_clock",
    f"{PACKAGE}.detector.volume_state",
    f"{PACKAGE}.ingestor.volume_observation",
    f"{PACKAGE}.detector.alert_path",
)
# The step-5 enrollment: page analysis, ingestion readiness, the deprecated API key setting.
STEP_5_MODULES = (
    f"{PACKAGE}.ingestor.page_analysis",
    f"{PACKAGE}.ingestor.readiness",
    f"{PACKAGE}.deprecated_api_key",
)
# The step-6 enrollment: the JSON shape narrowers and the channel response-value validators.
STEP_6_MODULES = (
    f"{PACKAGE}.shapes",
    f"{PACKAGE}.alerter.channels.response_values",
)
ENROLLED = (
    *STEP_1_MODULES,
    REDACTION,
    *STEP_3_MODULES,
    *STEP_4_MODULES,
    *STEP_5_MODULES,
    *STEP_6_MODULES,
)


def _all_killed() -> list[str]:
    return [f"    {module}.x_f__mutmut_{n}: killed" for module in ENROLLED for n in (1, 2)]


def test_enrollment_only_grows() -> None:
    assert gate.ENROLLED_MODULES[: len(ENROLLED)] == ENROLLED


def test_all_enrolled_mutants_detected_passes() -> None:
    assert gate.evaluate(_all_killed()) == []


def test_hung_mutants_count_as_detected() -> None:
    lines = [*_all_killed(), f"    {STEP_1_MODULES[2]}.xǁAǁm__mutmut_9: timeout"]
    assert gate.evaluate(lines) == []


@pytest.mark.parametrize("status", ["survived", "no tests", "suspicious", "segfault"])
def test_any_undetected_status_fails_and_names_the_module(status: str) -> None:
    lines = [*_all_killed(), f"    {STEP_1_MODULES[0]}.x_f__mutmut_7: {status}"]
    assert gate.evaluate(lines) == [f"{STEP_1_MODULES[0]}: 1 mutant(s) {status}"]


def test_an_enrolled_module_without_mutants_fails() -> None:
    lines = [line for line in _all_killed() if STEP_1_MODULES[3] not in line]
    assert gate.evaluate(lines) == [f"{STEP_1_MODULES[3]}: no mutants were generated"]


def test_unenrolled_modules_do_not_affect_the_gate() -> None:
    lines = [*_all_killed(), f"    {PACKAGE}.detector.scorer.x_g__mutmut_3: survived"]
    assert gate.evaluate(lines) == []


@pytest.mark.parametrize("status", ["survived", "no tests"])
def test_a_redaction_mutant_nobody_caught_fails_the_gate(status: str) -> None:
    lines = [*_all_killed(), f"    {REDACTION}.x_redact_url__mutmut_3: {status}"]
    assert gate.evaluate(lines) == [f"{REDACTION}: 1 mutant(s) {status}"]


def test_real_mutmut_result_lines_are_parsed() -> None:
    """Lines copied verbatim from ``mutmut results --all true`` (mutmut 3.8.0)."""
    real = [
        "    polymarket_insider_tracker.endpoints.x_is_official_v1_trades_url__mutmut_16: killed",
        "    polymarket_insider_tracker.ingestor._retry.xǁRetryPolicyǁattempt_limit__mutmut_2: survived",
        "    polymarket_insider_tracker.ingestor._attempt_budget.x__default_sleep__mutmut_1: no tests",
    ]
    problems = gate.evaluate(real)
    assert f"{STEP_1_MODULES[1]}: 1 mutant(s) survived" in problems
    assert f"{STEP_1_MODULES[2]}: 1 mutant(s) no tests" in problems
    assert f"{STEP_1_MODULES[0]}: no mutants were generated" not in problems
