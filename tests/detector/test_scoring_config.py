"""Characterization and contract tests for versioned ``scoring_config`` documents."""

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.detector.evaluation_clock import (
    parse_stored_instant,
    read_evaluation_instant,
    system_clock,
    utc_instant,
)
from polymarket_insider_tracker.detector.models import SCORING_ALGORITHM_VERSION
from polymarket_insider_tracker.detector.scorer import RiskScorer
from polymarket_insider_tracker.detector.scoring_config import (
    DEFAULT_WALLET_GROUP_STATUS,
    LEGACY_VERSION,
    VERSION_003,
    VERSION_004_1,
    CorruptScoringConfig,
    Document004,
    JsonValue,
    PureConfig,
    UnsupportedScoringVersion,
    WalletGroupStatus,
    read_scoring_config,
    serialize_document,
    serialize_pure_config,
)

CONTRACTS = (
    Path(__file__).resolve().parents[2] / "specs" / "004-truthful-detection-contract" / "contracts"
)
GOLDEN_003_1 = (
    '{"alert_threshold":"0.800","multi_signal_bonus_2":"1.2","multi_signal_bonus_3":"1.3",'
    '"score_quantum":"0.001","weights":{"fresh_wallet":"0.4","niche_market":"0.25",'
    '"size_anomaly":"0.35"}}'
)
EXAMPLES = (
    "scoring-config-004.1.example.json",
    "scoring-config-004.1.failure-row.json",
    "scoring-config-004.1.nonfinite.example.json",
    "scoring-config-004.1.truncated.example.json",
)


def example_text(name: str) -> str:
    """Return an example document in canonical form, as the writer stores it."""
    doc = json.loads((CONTRACTS / name).read_text())
    return json.dumps(doc, sort_keys=True, separators=(",", ":"))


def example_object(name: str) -> dict[str, JsonValue]:
    loaded: dict[str, JsonValue] = json.loads(example_text(name))
    return loaded


def read_document(text: str) -> Document004:
    config = read_scoring_config(VERSION_004_1, text)
    assert isinstance(config, Document004)
    return config


class TestScorerOutputUnchanged:
    def test_scorer_still_writes_the_003_1_golden_bytes(self) -> None:
        scorer = RiskScorer(FakeAsyncRedis(), alert_threshold=0.8)
        assert SCORING_ALGORITHM_VERSION == VERSION_003
        assert scorer.scoring_config == GOLDEN_003_1

    def test_003_1_row_reads_back_and_reserializes_byte_identically(self) -> None:
        scorer = RiskScorer(FakeAsyncRedis(), alert_threshold=0.8)
        config = read_scoring_config(VERSION_003, scorer.scoring_config)
        assert isinstance(config, PureConfig)
        assert config.alert_threshold == Decimal("0.800")
        assert config.multi_signal_bonus_3 == Decimal("1.3")
        assert config.weights == {
            "fresh_wallet": Decimal("0.4"),
            "niche_market": Decimal("0.25"),
            "size_anomaly": Decimal("0.35"),
        }
        assert serialize_pure_config(config) == scorer.scoring_config

    def test_custom_weights_row_round_trips(self) -> None:
        with pytest.warns(DeprecationWarning):
            scorer = RiskScorer(FakeAsyncRedis(), weights={"fresh_wallet": 0.5})
        config = read_scoring_config(VERSION_003, scorer.scoring_config)
        assert isinstance(config, PureConfig)
        assert serialize_pure_config(config) == scorer.scoring_config


class TestDispatch:
    def test_legacy_row_has_no_configuration(self) -> None:
        assert read_scoring_config(LEGACY_VERSION, None) is None

    def test_legacy_row_with_a_document_is_corrupt(self) -> None:
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(LEGACY_VERSION, GOLDEN_003_1)

    @pytest.mark.parametrize("version", ["004.2", "005.1", ""])
    def test_unknown_version_is_surfaced(self, version: str) -> None:
        with pytest.raises(UnsupportedScoringVersion):
            read_scoring_config(version, GOLDEN_003_1)

    @pytest.mark.parametrize("version", [VERSION_003, VERSION_004_1])
    def test_versioned_row_without_document_is_corrupt(self, version: str) -> None:
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(version, None)

    def test_003_1_row_carrying_004_keys_is_corrupt(self) -> None:
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(VERSION_003, example_text(EXAMPLES[0]))

    def test_004_1_row_missing_evidence_keys_is_corrupt(self) -> None:
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(VERSION_004_1, GOLDEN_003_1)


class TestDocument004:
    @pytest.mark.parametrize("name", EXAMPLES)
    def test_contract_examples_round_trip_byte_identically(self, name: str) -> None:
        text = example_text(name)
        assert serialize_document(read_document(text)) == text

    def test_pre_t027_example_fields(self) -> None:
        document = read_document(example_text(EXAMPLES[0]))
        assert document.evidence_schema == "004.1"
        assert document.pure.multi_signal_bonus_3 == Decimal("1")
        assert document.detector_config.fresh_transaction_boundary == 5
        assert document.wallet_group_status is DEFAULT_WALLET_GROUP_STATUS
        assert document.wallet_group_status is WalletGroupStatus.UNQUALIFIED_PRE_T027
        assert document.detector_failures == ()

    def test_failure_row_has_no_evidence(self) -> None:
        document = read_document(example_text(EXAMPLES[1]))
        assert document.evaluation_evidence is None
        assert document.wallet_group_status is None
        assert document.detector_failures

    def test_qualified_post_t027_document_is_not_a_004_1_row(self) -> None:
        text = example_text("scoring-config-004.2.post-t027.example.json")
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(VERSION_004_1, text)


def mutate(edit: Callable[[dict[str, JsonValue]], None]) -> str:
    doc = example_object(EXAMPLES[0])
    edit(doc)
    return json.dumps(doc)


def evidence_of(doc: dict[str, JsonValue]) -> dict[str, JsonValue]:
    evidence = doc["evaluation_evidence"]
    assert isinstance(evidence, dict)
    return evidence


def set_group_status(doc: dict[str, JsonValue], value: str) -> None:
    wallet = evidence_of(doc)["wallet"]
    assert isinstance(wallet, dict)
    wallet["group_status"] = value


def set_boundary(doc: dict[str, JsonValue], value: JsonValue) -> None:
    config = doc["detector_config"]
    assert isinstance(config, dict)
    config["fresh_transaction_boundary"] = value


def set_config_key(doc: dict[str, JsonValue], key: str, value: JsonValue) -> None:
    config = doc["detector_config"]
    assert isinstance(config, dict)
    config[key] = value


def drop_wallet(doc: dict[str, JsonValue]) -> None:
    del evidence_of(doc)["wallet"]


def drop_evaluated_at(doc: dict[str, JsonValue]) -> None:
    del evidence_of(doc)["evaluated_at"]


def corrupt_failure(doc: dict[str, JsonValue], patch: dict[str, JsonValue]) -> None:
    base: dict[str, JsonValue] = {"detector": "fresh_wallet", "error_class": "E", "message": "m"}
    doc["detector_failures"] = [base | patch]


CORRUPTIONS: dict[str, Callable[[dict[str, JsonValue]], None]] = {
    "version-mismatch": lambda d: d.update(evidence_schema="004.2"),
    "qualified-status": lambda d: set_group_status(d, "qualified"),
    "missing-wallet": drop_wallet,
    "bonus-2": lambda d: d.update(multi_signal_bonus_2="1.3"),
    "bonus-3-003-value": lambda d: d.update(multi_signal_bonus_3="1.3"),
    "quantum": lambda d: d.update(score_quantum="0.01"),
    "threshold-decimals": lambda d: d.update(alert_threshold="0.8"),
    "threshold-exponent": lambda d: d.update(alert_threshold="8E-1"),
    "threshold-not-string": lambda d: d.update(alert_threshold="x"),
    "weights-not-object": lambda d: d.update(weights=[]),
    "weights-missing-group": lambda d: d.update(weights={"fresh_wallet": "0.4"}),
    "weight-not-string": lambda d: d.update(weights={"fresh_wallet": 1}),
    "weight-exponent": lambda d: d.update(
        weights={"fresh_wallet": "4E-1", "niche_market": "0.25", "size_anomaly": "0.35"}
    ),
    "config-decimal-exponent": lambda d: set_config_key(d, "size_volume_threshold", "2E-2"),
    "boundary-zero": lambda d: set_boundary(d, 0),
    "boundary-bool": lambda d: set_boundary(d, True),
    "boundary-too-large": lambda d: set_boundary(d, 2**53),
    "recency": lambda d: set_config_key(d, "volume_recency_seconds", 60),
    "config-not-object": lambda d: d.update(detector_config=[]),
    "config-extra-key": lambda d: set_config_key(d, "extra", "1"),
    "failures-not-array": lambda d: d.update(detector_failures={}),
    "failure-unknown-detector": lambda d: corrupt_failure(d, {"detector": "sniper"}),
    "failure-long-class": lambda d: corrupt_failure(d, {"error_class": "E" * 129}),
    "failure-long-message": lambda d: corrupt_failure(d, {"message": "m" * 201}),
    "failure-extra-key": lambda d: corrupt_failure(d, {"trace": "x"}),
    "evidence-not-object": lambda d: d.update(evaluation_evidence=[]),
    "evaluated-at-naive": lambda d: evidence_of(d).update(evaluated_at="2026-10-19T12:00:00"),
    "evaluated-at-offset": lambda d: evidence_of(d).update(
        evaluated_at="2026-10-19T12:00:00+02:00"
    ),
    "evaluated-at-garbage": lambda d: evidence_of(d).update(evaluated_at="noon"),
    "evaluated-at-missing": drop_evaluated_at,
}


class TestCorruptDocuments:
    @pytest.mark.parametrize("case", sorted(CORRUPTIONS))
    def test_schema_violation_is_reported_not_coerced(self, case: str) -> None:
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(VERSION_004_1, mutate(CORRUPTIONS[case]))

    @pytest.mark.parametrize("version", [VERSION_003, VERSION_004_1])
    @pytest.mark.parametrize("text", ["{", "[]", '{"alert_threshold":0.8}'])
    def test_malformed_json_is_corrupt(self, version: str, text: str) -> None:
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(version, text)

    def test_float_anywhere_is_corrupt(self) -> None:
        text = GOLDEN_003_1.replace('"0.4"', "0.4")
        with pytest.raises(CorruptScoringConfig, match="float"):
            read_scoring_config(VERSION_003, text)

    def test_integer_where_decimal_string_expected_is_corrupt(self) -> None:
        text = GOLDEN_003_1.replace('"1.2"', "1")
        with pytest.raises(CorruptScoringConfig):
            read_scoring_config(VERSION_003, text)


class TestEvaluationClock:
    def test_naive_instant_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            utc_instant(datetime(2026, 10, 19, 12, 0, 0))

    def test_offset_instant_is_normalised_to_utc(self) -> None:
        plus_two = timezone(timedelta(hours=2))
        instant = utc_instant(datetime(2026, 10, 19, 14, 0, 0, tzinfo=plus_two))
        assert instant == datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)
        assert instant.utcoffset() == timedelta(0)

    def test_clock_is_read_exactly_once(self) -> None:
        readings = [datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)]
        assert read_evaluation_instant(readings.pop) == datetime(2026, 10, 19, 12, tzinfo=UTC)
        assert readings == []

    def test_system_clock_is_aware_utc(self) -> None:
        assert system_clock().utcoffset() == timedelta(0)

    def test_stored_instant_requires_explicit_utc_offset(self) -> None:
        assert parse_stored_instant("2026-10-19T12:00:00+00:00").tzinfo is not None
        for bad in ("2026-10-19T12:00:00", "2026-10-19T12:00:00+01:00"):
            with pytest.raises(ValueError, match=r"\+00:00"):
                parse_stored_instant(bad)
