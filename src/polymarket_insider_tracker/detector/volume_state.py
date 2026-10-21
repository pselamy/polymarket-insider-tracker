"""Volume-state classification: the eight states, their precedence and the niche basis.

Contract: ``specs/004-truthful-detection-contract/data-model.md`` sections 1.1b, 1.1c, 1.2, 1.3
and 3. The state is a pure function of (observation, qualified sources, ``evaluated_at``);
rows are tried top to bottom (P1..P8) and the first match wins. Nothing here changes a score:
the verdict is evidence recorded beside the signal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction

from polymarket_insider_tracker.detector.evaluation_clock import (
    Clock,
    read_evaluation_instant,
    utc_instant,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata
from polymarket_insider_tracker.ingestor.volume_observation import (
    GAMMA_SOURCE,
    NUMERIC_TOKEN,
    VolumeObservation,
)

CALLER_SOURCE = "caller"
QUALIFIED_SOURCES = frozenset({GAMMA_SOURCE, CALLER_SOURCE})
RECENCY_SECONDS = 600
VOLUME_MIN = Decimal("0.000001")
VOLUME_LIMIT = Decimal(10) ** 15
VOLUME_MAX_CHARS = 32
NICHE_VOLUME_THRESHOLD = Decimal("50000")
NICHE_PRONE_CATEGORIES = frozenset({"science", "tech", "finance", "other"})
NOT_A_NUMBER = "not_a_finite_number"
OUT_OF_RANGE = "out_of_range"
NO_VOLUME_RECORD = "no_volume_record"
_RATIO_QUANTUM = 10_000
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class VolumeState(StrEnum):
    """The eight mutually exclusive states of data-model section 1.2."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNQUALIFIED = "unqualified"
    MISSING = "missing"
    INVALID = "invalid"
    NONPOSITIVE = "nonpositive"
    INVALID_TIME = "invalid_time"
    STALE = "stale"


Outcome = tuple[VolumeState, str, Decimal | None]


@dataclass(frozen=True)
class Stamp:
    """What the stored acquisition stamp says once read against ``evaluated_at``."""

    acquired_at: datetime | None
    age: Decimal | None
    defect: str | None


@dataclass(frozen=True)
class VolumeVerdict:
    """The state, its reason and the recorded facts; ``daily_volume`` is set only if available."""

    state: VolumeState
    reason: str
    daily_volume: Decimal | None
    present: bool
    raw: object
    source: str | None
    acquired_at: str | None
    age_seconds: str | None


def _plain(value: Decimal) -> str:
    return format(value, "f")


def exact_seconds(delta: timedelta) -> Decimal:
    """A timedelta as exact decimal seconds (microsecond resolution, no float)."""
    return Decimal(delta // timedelta(microseconds=1)).scaleb(-6)


def _age_text(age: Decimal | None) -> str | None:
    return None if age is None else _plain(age.normalize())


def _aware_stamp(text: str | None) -> datetime | str:
    """The stored stamp as an aware UTC instant, or the P6 defect that makes it unusable."""
    if text is None:
        return "absent"
    try:
        acquired = datetime.fromisoformat(text)
    except ValueError:
        return "unparseable"
    if acquired.utcoffset() is None:
        return "naive"
    return "nonpositive_epoch" if acquired <= _EPOCH else acquired.astimezone(UTC)


def read_stamp(text: str | None, evaluated_at: datetime) -> Stamp:
    """Read the stored stamp text against ``evaluated_at`` (age is signed; future stays stored)."""
    acquired = _aware_stamp(text)
    if isinstance(acquired, str):
        return Stamp(None, None, acquired)
    age = exact_seconds(evaluated_at - acquired)
    return Stamp(acquired, age, "future" if age < 0 else None)


def _numeric_text(raw: object) -> str | None:
    """The text a JSON number or numeric string converts from; ``None`` for anything else."""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, str):
        return raw if NUMERIC_TOKEN.fullmatch(raw) else None
    return str(raw) if isinstance(raw, int | float) else None


def _truncated_reason(observation: VolumeObservation) -> str:
    """Reason for a stored ``$truncated`` marker: an oversize number or numeric token is
    out of range, everything else is not a number (decided from the original, section 1.1a)."""
    raw = observation.raw
    kind = raw.get("type") if isinstance(raw, dict) else None
    oversize_number = kind == "number" or (kind == "string" and observation.oversize_numeric)
    return OUT_OF_RANGE if oversize_number else NOT_A_NUMBER


def _parse_raw(observation: VolumeObservation) -> Decimal | str:
    """The finite value of the raw volume, or the P4 reason it is not usable as a number."""
    raw = observation.raw
    if isinstance(raw, dict):
        return _truncated_reason(observation)
    text = _numeric_text(raw)
    if text is None:
        return NOT_A_NUMBER
    value = Decimal(text)
    return value if value.is_finite() else NOT_A_NUMBER


def in_range(value: Decimal) -> bool:
    """Section 1.1c: 10^-6 <= value < 10^15 and at most 32 characters in plain notation."""
    if not VOLUME_MIN <= value < VOLUME_LIMIT:
        return False
    return len(_plain(value)) <= VOLUME_MAX_CHARS


def _value_outcome(value: Decimal) -> Outcome | None:
    """P4 ``out_of_range`` or P5; ``None`` when the value itself is usable (disjoint rows)."""
    if value <= 0:
        return VolumeState.NONPOSITIVE, "zero" if value == 0 else "negative", None
    if not in_range(value):
        return VolumeState.INVALID, OUT_OF_RANGE, None
    return None


def _time_outcome(stamp: Stamp, value: Decimal) -> Outcome:
    """P6..P8 for a finite positive in-range value."""
    if stamp.defect is not None:
        return VolumeState.INVALID_TIME, stamp.defect, None
    if stamp.age is not None and stamp.age > RECENCY_SECONDS:
        return VolumeState.STALE, "older_than_600s", None
    return VolumeState.AVAILABLE, "gamma", value


def _gate_outcome(observation: VolumeObservation) -> Outcome | None:
    """P2 and P3: a value that is not interpreted, or no value at all."""
    if observation.source not in QUALIFIED_SOURCES:
        reason = "no_provenance" if observation.source is None else "unknown_source"
        return VolumeState.UNQUALIFIED, reason, None
    if not observation.present or observation.raw is None:
        return VolumeState.MISSING, "null_or_absent", None
    return None


def _number_or_outcome(observation: VolumeObservation) -> Decimal | Outcome:
    """The usable finite value, or the P4/P5 outcome that rules it out."""
    parsed = _parse_raw(observation)
    if isinstance(parsed, str):
        return VolumeState.INVALID, parsed, None
    return _value_outcome(parsed) or parsed


def _observed_outcome(observation: VolumeObservation, stamp: Stamp) -> Outcome:
    gate = _gate_outcome(observation)
    if gate is not None:
        return gate
    checked = _number_or_outcome(observation)
    if isinstance(checked, tuple):
        return checked
    return _time_outcome(stamp, checked)


def _verdict(
    outcome: Outcome, observation: VolumeObservation | None, stamp: Stamp, source: str | None
) -> VolumeVerdict:
    state, reason, volume = outcome
    return VolumeVerdict(
        state,
        reason,
        volume,
        observation is not None and observation.present,
        None if observation is None else observation.raw,
        source,
        None if stamp.acquired_at is None else stamp.acquired_at.isoformat(),
        _age_text(stamp.age),
    )


def _explicit_outcome(explicit: Decimal) -> Outcome:
    if not explicit.is_finite():
        return VolumeState.INVALID, NOT_A_NUMBER, None
    return _value_outcome(explicit) or (VolumeState.AVAILABLE, "caller", explicit)


def _explicit_verdict(explicit: Decimal, evaluated_at: datetime) -> VolumeVerdict:
    """Section 1.3: a caller assertion, stamped ``evaluated_at`` (age 0), never Gamma data."""
    outcome = _explicit_outcome(explicit)
    stamp = Stamp(evaluated_at, Decimal(0), None)
    observation = VolumeObservation(True, str(explicit), evaluated_at.isoformat(), CALLER_SOURCE)
    return _verdict(outcome, observation, stamp, CALLER_SOURCE)


def classify_volume(
    observation: VolumeObservation | None,
    *,
    evaluated_at: datetime,
    explicit: Decimal | None = None,
    unavailable_reason: str | None = None,
) -> VolumeVerdict:
    """Classify the volume evidence of one evaluation (first matching row P1..P8)."""
    instant = utc_instant(evaluated_at)
    if explicit is not None:
        return _explicit_verdict(explicit, instant)
    if observation is None:
        outcome = (VolumeState.UNAVAILABLE, unavailable_reason or NO_VOLUME_RECORD, None)
        return _verdict(outcome, None, Stamp(None, None, None), None)
    stamp = read_stamp(observation.acquired_at, instant)
    return _verdict(_observed_outcome(observation, stamp), observation, stamp, observation.source)


def volume_ratio(notional: Decimal, volume: Decimal) -> Decimal:
    """``notional / volume`` as an exact rational rounded half-even once to 0.0001 (section 3)."""
    whole, fraction = divmod(
        round(Fraction(notional) / Fraction(volume) * _RATIO_QUANTUM), _RATIO_QUANTUM
    )
    return Decimal(f"{whole}.{fraction:04d}")


def measured_impact(notional: Decimal, verdict: VolumeVerdict) -> Decimal | None:
    """The ratio, only in state ``available``; never a measured zero for a non-measurement."""
    if verdict.state is not VolumeState.AVAILABLE or verdict.daily_volume is None:
        return None
    return volume_ratio(notional, verdict.daily_volume)


@dataclass(frozen=True)
class SizeVolumeEvidence:
    """The volume facts recorded beside a size signal (data-model section 3); not a score input."""

    verdict: VolumeVerdict
    volume_impact_measured: Decimal | None
    niche_basis: str
    category: str
    category_source: str


def _category_basis(verdict: VolumeVerdict, category: str, category_source: str) -> str:
    no_value_observed = verdict.state in (VolumeState.UNAVAILABLE, VolumeState.MISSING)
    real_category = category_source == "metadata" and category in NICHE_PRONE_CATEGORIES
    return "category" if no_value_observed and real_category else "none"


def niche_basis(verdict: VolumeVerdict, category: str, category_source: str) -> str:
    """``numeric``, ``category`` (DECISION D3 (d)) or ``none`` (data-model section 3)."""
    if verdict.daily_volume is not None:  # set only in state ``available``
        return "numeric" if verdict.daily_volume < NICHE_VOLUME_THRESHOLD else "none"
    return _category_basis(verdict, category, category_source)


def size_volume_evidence(
    metadata: MarketMetadata,
    category_source: str,
    notional: Decimal,
    clock: Clock,
    explicit: Decimal | None = None,
) -> SizeVolumeEvidence:
    """Classify ``metadata``'s stored volume at one reading of ``clock``; derive the evidence.

    ``category_source`` is ``minimal`` when the metadata lookup failed and a placeholder stands in
    (reason ``metadata_lookup_failed``), ``metadata`` when it is real.
    """
    reason = "metadata_lookup_failed" if category_source == "minimal" else None
    verdict = classify_volume(
        metadata.volume,
        evaluated_at=read_evaluation_instant(clock),
        explicit=explicit,
        unavailable_reason=reason or metadata.volume_unavailable_reason,
    )
    basis = niche_basis(verdict, metadata.category, category_source)
    impact = measured_impact(notional, verdict)
    return SizeVolumeEvidence(verdict, impact, basis, metadata.category, category_source)
