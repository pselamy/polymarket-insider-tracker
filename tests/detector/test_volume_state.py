"""Volume-state classification against the replay corpus and the data-model tables (T009d)."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from polymarket_insider_tracker.detector.volume_state import (
    VolumeState,
    VolumeVerdict,
    classify_volume,
    in_range,
    measured_impact,
    niche_basis,
    volume_ratio,
)
from polymarket_insider_tracker.ingestor.volume_observation import (
    GAMMA_SOURCE,
    JsonValue,
    VolumeObservation,
)

EVIDENCE = (
    Path(__file__).resolve().parents[2]
    / "specs"
    / "004-truthful-detection-contract"
    / "evidence"
    / "replay-live"
)
EVALUATED_AT = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
NAMED_STAMPS = {
    "absent": None,
    "naive": "2026-10-19T11:59:30",
    "unparseable": "yesterday",
    "nonpositive_epoch": "1970-01-01T00:00:00+00:00",
}
RAW_BUILDERS: dict[str, Callable[[str], JsonValue]] = {
    "float": float,
    "nested_float": lambda text: [float(text)],
    "repeat_string": lambda text: "x" * int(text),
    "big_int_digits": lambda text: int("9" * int(text)),
}


def _merge_case(defaults: dict[str, JsonValue], case: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """A case over the corpus defaults; an ``unavailable`` observation inherits nothing."""
    merged = {**defaults, **{k: v for k, v in case.items() if k != "observation"}}
    observed = case.get("observation", {})
    assert isinstance(observed, dict)
    default_observation = defaults["observation"]
    assert isinstance(default_observation, dict)
    base = {} if observed.get("kind") == "unavailable" else default_observation
    merged["observation"] = {**base, **observed}
    return merged


def _corpus() -> list[dict[str, JsonValue]]:
    corpus = json.loads((EVIDENCE / "cases.json").read_text())
    return [_merge_case(corpus["defaults"], case) for case in corpus["cases"]]


def _expected() -> dict[str, dict[str, str]]:
    with (EVIDENCE / "results.csv").open() as handle:
        return {row["case_id"]: row for row in csv.DictReader(handle)}


def _raw(spec: dict[str, JsonValue]) -> JsonValue:
    raw_spec = spec.get("raw_spec")
    if not isinstance(raw_spec, dict):
        return spec.get("raw")
    ((kind, argument),) = raw_spec.items()
    return RAW_BUILDERS[kind](str(argument))


def _stamp(age: JsonValue) -> str | None:
    if isinstance(age, str):
        return NAMED_STAMPS[age]
    assert isinstance(age, int | float) and not isinstance(age, bool)
    microseconds = int(Decimal(str(age)) * 1_000_000)
    return (EVALUATED_AT - timedelta(microseconds=microseconds)).isoformat()


def _stored(observation: VolumeObservation) -> VolumeObservation:
    """The observation as the live path reads it: through the bounded Redis record."""
    restored = VolumeObservation.from_record(json.loads(json.dumps(observation.to_record())))
    assert restored is not None
    return restored


def _classify_case(spec: dict[str, JsonValue]) -> tuple[VolumeVerdict, str]:
    observed = spec["observation"]
    assert isinstance(observed, dict)
    explicit = spec["explicit_daily_volume"]
    unavailable = observed.get("kind") == "unavailable"
    reason = observed.get("reason")
    observation = None
    if not unavailable:
        source = observed.get("source")
        observation = _stored(
            VolumeObservation(
                bool(observed.get("present", True)),
                _raw(observed),
                _stamp(observed.get("acquired_age_seconds")),
                source if isinstance(source, str) else None,
            )
        )
    verdict = classify_volume(
        observation,
        evaluated_at=EVALUATED_AT,
        explicit=Decimal(str(explicit)) if explicit else None,
        unavailable_reason=reason if isinstance(reason, str) else None,
    )
    category_source = spec["category_source"]
    assert isinstance(category_source, str)
    return verdict, category_source


CASES = _corpus()


def test_the_replay_corpus_is_the_whole_table() -> None:
    assert len(CASES) == len(_expected()) == 65
    assert {v.value for v in VolumeState} == {r["volume_state"] for r in _expected().values()}


@pytest.mark.parametrize("spec", CASES, ids=[str(case["case_id"]) for case in CASES])
def test_every_replay_case_classifies_as_the_evidence_expects(spec: dict[str, JsonValue]) -> None:
    row = _expected()[str(spec["case_id"])]
    verdict, category_source = _classify_case(spec)

    basis = niche_basis(verdict, str(spec["category"]), category_source)

    assert (verdict.state.value, verdict.reason) == (row["volume_state"], row["volume_reason"])
    assert (basis == "category") == (row["niche_basis"] == "category")
    assert basis != "numeric" or row["niche_basis"] in ("numeric", "none")
    assert row["niche_basis"] != "numeric-not-niche" or basis == "none"


def _stored_at(
    raw: JsonValue = "30000", age: int = 30, source: str | None = GAMMA_SOURCE
) -> VolumeObservation:
    stamp = (EVALUATED_AT - timedelta(seconds=age)).isoformat()
    return VolumeObservation(True, raw, stamp, source)


def _classify(observation: VolumeObservation | None) -> VolumeVerdict:
    return classify_volume(observation, evaluated_at=EVALUATED_AT)


class TestFacts:
    """The verdict records the stored facts, whatever state wins."""

    @pytest.mark.parametrize(
        ("age", "text"),
        [(30, "30"), (0, "0"), (600, "600"), (-5, "-5")],
    )
    def test_age_is_the_signed_plain_decimal(self, age: int, text: str) -> None:
        assert _classify(_stored_at(age=age)).age_seconds == text

    @pytest.mark.parametrize(
        ("delta", "text"),
        [
            (timedelta(seconds=599, microseconds=999999), "599.999999"),
            (timedelta(seconds=600, microseconds=1), "600.000001"),
            (timedelta(microseconds=-1), "-0.000001"),
            (timedelta(milliseconds=500), "0.5"),
        ],
    )
    def test_fractional_ages_are_exact(self, delta: timedelta, text: str) -> None:
        observation = VolumeObservation(
            True, "30000", (EVALUATED_AT - delta).isoformat(), GAMMA_SOURCE
        )

        assert _classify(observation).age_seconds == text

    def test_a_future_stamp_keeps_its_timestamp_and_negative_age_in_every_state(self) -> None:
        for raw in ("30000", None, "abc", 0):
            verdict = _classify(_stored_at(raw, age=-5))
            assert verdict.age_seconds == "-5"
            assert verdict.acquired_at == "2026-10-19T12:00:05+00:00"

    def test_future_decides_the_state_only_for_a_usable_value(self) -> None:
        assert _classify(_stored_at(age=-5)).state is VolumeState.INVALID_TIME
        assert _classify(_stored_at(0, age=-5)).state is VolumeState.NONPOSITIVE

    def test_unusable_stamps_store_neither_time_nor_age(self) -> None:
        for stamp in NAMED_STAMPS.values():
            verdict = _classify(VolumeObservation(True, "30000", stamp, GAMMA_SOURCE))
            assert (verdict.acquired_at, verdict.age_seconds) == (None, None)

    def test_the_evaluation_instant_is_normalised_to_utc(self) -> None:
        zone = timezone(timedelta(hours=-5))

        verdict = classify_volume(_stored_at(), evaluated_at=EVALUATED_AT.astimezone(zone))

        assert (verdict.state, verdict.age_seconds) == (VolumeState.AVAILABLE, "30")

    def test_a_naive_evaluation_instant_is_a_caller_error(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            classify_volume(_stored_at(), evaluated_at=datetime(2026, 10, 19, 12, 0))


class TestPrecedence:
    def test_unavailable_carries_its_reason_and_no_facts(self) -> None:
        verdict = classify_volume(
            None, evaluated_at=EVALUATED_AT, unavailable_reason="metadata_lookup_failed"
        )

        assert (verdict.state, verdict.reason) == (
            VolumeState.UNAVAILABLE,
            "metadata_lookup_failed",
        )
        assert (verdict.present, verdict.raw, verdict.source, verdict.acquired_at) == (
            False,
            None,
            None,
            None,
        )

    def test_unavailable_without_a_reason_is_no_volume_record(self) -> None:
        assert _classify(None).reason == "no_volume_record"

    def test_an_unqualified_source_is_not_interpreted(self) -> None:
        verdict = _classify(_stored_at("abc", age=9999, source="elsewhere"))

        assert (verdict.state, verdict.reason) == (VolumeState.UNQUALIFIED, "unknown_source")

    def test_missing_wins_over_a_bad_stamp(self) -> None:
        observation = VolumeObservation(False, None, None, GAMMA_SOURCE)

        assert _classify(observation).state is VolumeState.MISSING

    def test_invalid_wins_over_a_bad_stamp_and_over_stale(self) -> None:
        assert _classify(_stored_at("abc", age=9999)).state is VolumeState.INVALID

    def test_nonpositive_wins_over_stale(self) -> None:
        assert _classify(_stored_at("-3", age=9999)).reason == "negative"

    @pytest.mark.parametrize(
        ("age", "state"),
        [(600, VolumeState.AVAILABLE), (601, VolumeState.STALE), (0, VolumeState.AVAILABLE)],
    )
    def test_the_recency_edges_are_inclusive(self, age: int, state: VolumeState) -> None:
        assert _classify(_stored_at(age=age)).state is state


class TestMagnitudeBound:
    """DECISION D4 = M1: 10^-6 <= v < 10^15 and at most 32 plain-notation characters."""

    @pytest.mark.parametrize(
        ("text", "usable"),
        [
            ("0.000001", True),
            ("0.0000009", False),
            ("999999999999999", True),
            ("1000000000000000", False),
            ("0.1234567890123456789012345678901", False),
            ("0.123456789012345678901234567890", True),
            ("1e999999999", False),
            ("1e-30", False),
        ],
    )
    def test_bounds(self, text: str, usable: bool) -> None:
        verdict = _classify(_stored_at(text))

        assert in_range(Decimal(text)) is usable
        assert verdict.state is (VolumeState.AVAILABLE if usable else VolumeState.INVALID)
        assert usable or verdict.reason == "out_of_range"

    def test_zero_and_negative_stay_nonpositive_not_out_of_range(self) -> None:
        assert [_classify(_stored_at(raw)).reason for raw in ("0", "-1e-30", 0, -5)] == [
            "zero",
            "negative",
            "zero",
            "negative",
        ]

    def test_a_float_converts_through_its_shortest_repr(self) -> None:
        verdict = _classify(_stored_at(12345.678901234567))

        assert verdict.daily_volume == Decimal("12345.678901234567")

    def test_a_truncated_numeric_string_is_out_of_range_but_text_is_not_a_number(self) -> None:
        numeric = _stored(_stored_at("1" + "0" * 299))
        text = _stored(_stored_at("x" * 300))

        assert _classify(numeric).reason == "out_of_range"
        assert _classify(text).reason == "not_a_finite_number"


class TestExplicitArgument:
    """data-model 1.3: a caller assertion, stamped at evaluation, recorded as ``caller``."""

    def _explicit(self, value: str, stored: VolumeObservation | None = None) -> VolumeVerdict:
        return classify_volume(stored, evaluated_at=EVALUATED_AT, explicit=Decimal(value))

    def test_positive_value_is_available_with_the_caller_source(self) -> None:
        verdict = self._explicit("30000")

        assert (verdict.state, verdict.reason, verdict.daily_volume) == (
            VolumeState.AVAILABLE,
            "caller",
            Decimal("30000"),
        )
        assert (verdict.source, verdict.age_seconds) == ("caller", "0")
        assert verdict.acquired_at == "2026-10-19T12:00:00+00:00"

    @pytest.mark.parametrize(
        ("value", "state", "reason"),
        [
            ("0", VolumeState.NONPOSITIVE, "zero"),
            ("-1", VolumeState.NONPOSITIVE, "negative"),
            ("1e-30", VolumeState.INVALID, "out_of_range"),
            ("1e15", VolumeState.INVALID, "out_of_range"),
        ],
    )
    def test_unusable_values_keep_the_caller_source(
        self, value: str, state: VolumeState, reason: str
    ) -> None:
        verdict = self._explicit(value)

        assert (verdict.state, verdict.reason, verdict.source) == (state, reason, "caller")
        assert verdict.daily_volume is None

    def test_explicit_value_overrides_the_stored_observation(self) -> None:
        verdict = self._explicit("5000", _stored_at("abc", age=9999))

        assert (verdict.state, verdict.daily_volume) == (VolumeState.AVAILABLE, Decimal("5000"))


class TestImpact:
    def test_ratio_is_the_exact_quotient_rounded_once_half_even(self) -> None:
        assert volume_ratio(Decimal("3250"), Decimal("30000")) == Decimal("0.1083")
        assert volume_ratio(Decimal("1"), Decimal("20000")) == Decimal("0.0000")  # 0.00005 -> even
        assert volume_ratio(Decimal("3"), Decimal("20000")) == Decimal("0.0002")  # 0.00015 -> even

    def test_a_ratio_above_28_digits_does_not_raise(self) -> None:
        assert volume_ratio(Decimal(10) ** 30, Decimal("0.000001")) == Decimal(10) ** 36

    def test_impact_is_measured_only_when_available(self) -> None:
        notional = Decimal("3250")

        assert measured_impact(notional, _classify(_stored_at())) == Decimal("0.1083")
        for observation in (None, _stored_at(age=9999), _stored_at(0), _stored_at("abc")):
            assert measured_impact(notional, _classify(observation)) is None


class TestNicheBasis:
    """DECISION D3 = (d): a category basis only when no value was observed and it is real."""

    @staticmethod
    def _basis(observation: VolumeObservation | None, category: str, source: str) -> str:
        return niche_basis(_classify(observation), category, source)

    def test_numeric_below_the_threshold(self) -> None:
        assert self._basis(_stored_at("49999.99"), "politics", "metadata") == "numeric"

    def test_available_at_or_above_the_threshold_is_not_niche_whatever_the_category(self) -> None:
        assert self._basis(_stored_at("50000"), "science", "metadata") == "none"

    @pytest.mark.parametrize("category", ["science", "tech", "finance", "other"])
    def test_category_basis_when_no_value_was_observed(self, category: str) -> None:
        missing = VolumeObservation(True, None, None, GAMMA_SOURCE)

        assert self._basis(None, category, "metadata") == "category"
        assert self._basis(missing, category, "metadata") == "category"

    def test_a_non_prone_category_gives_no_basis(self) -> None:
        assert self._basis(None, "politics", "metadata") == "none"

    def test_the_lookup_failure_placeholder_is_not_evidence(self) -> None:
        assert self._basis(None, "other", "minimal") == "none"

    @pytest.mark.parametrize(
        "observation",
        [
            _stored_at("abc"),
            _stored_at("0"),
            _stored_at(age=9999),
            _stored_at(source="elsewhere"),
            VolumeObservation(True, "30000", None, GAMMA_SOURCE),
        ],
        ids=["invalid", "nonpositive", "stale", "unqualified", "invalid_time"],
    )
    def test_every_observed_value_state_gives_no_category_basis(
        self, observation: VolumeObservation
    ) -> None:
        assert self._basis(observation, "science", "metadata") == "none"
