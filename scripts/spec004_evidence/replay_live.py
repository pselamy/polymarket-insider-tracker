"""Live-realistic replay addendum for slice 004 (research.md R10, evidence/replay-live.md).

Usage:
    python3 -m scripts.spec004_evidence.replay_live                 # print the results CSV
    python3 -m scripts.spec004_evidence.replay_live --check FILE    # exit 0 iff FILE equals the CSV byte for byte; 1 if stale or
                                                                    # missing; 2 on any other arguments

Scoring goes through the product detectors and scorer (``real_scoring``, shared with
reachability.py, T028). The volume-state classifier below is
the executable form of the precedence table in data-model.md section 1; the cases cover every
state and reason. ``old_score_as_wired`` is the 003.1 rule with the live wiring (no volume passed).
Each case is scored in both wallet postures: ``pre_T027`` is the default (FR-007c: the wallet group
is non-scoring, so only the size group counts and the two-group bonus never applies) and
``post_T027`` is the scenario in which T027 has qualified the wallet group.
"""

from __future__ import annotations

import csv
import io
import json
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

from scripts.spec004_evidence.cli import print_or_check
from scripts.spec004_evidence.reachability import verify_constants
from scripts.spec004_evidence.real_scoring import (
    Market,
    Wallet,
    score,
    size_signal,
    wallet_confidence,
)

HERE = (
    Path(__file__).resolve().parents[2] / "specs" / "004-truthful-detection-contract" / "evidence"
)
QUALIFIED_SOURCES = frozenset({"gamma-markets-v1", "caller"})
NICHE_PRONE = frozenset({"science", "tech", "finance", "other"})
NO_OBSERVED_VALUE_STATES = frozenset({"unavailable", "missing"})  # DECISION D3 (d): P1, P3
RECENCY_SECONDS = 600
BOUNDARY = 5  # B: exclusive nonce bound (proposed); old rule is nonce <= 5
THRESHOLDS = ("0.80", "0.78")
# data-model.md section 1.1b: a numeric string is exactly one RFC 8259 JSON number token (ASCII, no
# sign "+", no whitespace, no "_" separators, no leading zeros, no NaN/Infinity spelling).
NUMERIC_STRING = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")
# data-model.md section 1.1c (DECISION D4 default): a positive volume is usable only inside this
# magnitude window and with a short plain-notation form; otherwise invalid/out_of_range.
VOLUME_MIN = Decimal("0.000001")
VOLUME_LIMIT = Decimal(10) ** 15
VOLUME_MAX_CHARS = 32
COLUMNS = (
    "case_id",
    "volume_state",
    "volume_reason",
    "wallet_class",
    "wallet_confidence_post_T027",
    "size_confidence",
    "niche_basis",
    "old_score_as_wired",
    "groups_pre_T027",
    "score_pre_T027",
    *(f"alerts_pre_T027_at_{t}" for t in THRESHOLDS),
    "groups_post_T027",
    "score_post_T027",
    *(f"alerts_post_T027_at_{t}" for t in THRESHOLDS),
    "purpose",
)


@dataclass(frozen=True)
class Observation:
    kind: str
    reason: str | None
    source: str | None
    present: bool
    raw: object
    acquired_age_seconds: object


def is_numeric_candidate(raw: object) -> bool:
    """A JSON number (not a boolean), or a string that is exactly one JSON number token."""
    if isinstance(raw, str):
        return NUMERIC_STRING.fullmatch(raw) is not None
    return isinstance(raw, int | float) and not isinstance(raw, bool)


def parse_finite(raw: object) -> Decimal | None:
    """A finite Decimal from a JSON number or a numeric string (section 1.1b); None otherwise."""
    if not is_numeric_candidate(raw):
        return None
    try:
        value = Decimal(str(raw))
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


Verdict = tuple[str, str, Decimal | None]


def nonpositive_verdict(value: Decimal) -> Verdict:
    return "nonpositive", "zero" if value == 0 else "negative", None


def in_range(value: Decimal) -> bool:
    """Section 1.1c: 10^-6 <= value < 10^15 and at most 32 characters in plain notation."""
    if not VOLUME_MIN <= value < VOLUME_LIMIT:
        return False
    return len(format(value, "f")) <= VOLUME_MAX_CHARS


def unusable_verdict(value: Decimal) -> Verdict | None:
    """P4 out_of_range or P5; None when the value itself is usable (the conditions are disjoint)."""
    if value <= 0:
        return nonpositive_verdict(value)
    if not in_range(value):
        return "invalid", "out_of_range", None
    return None


def explicit_verdict(explicit: Decimal) -> Verdict:
    return unusable_verdict(explicit) or ("available", "caller", explicit)


def age_seconds(age: object) -> Decimal | None:
    """A numeric case age (integer or fractional seconds); None for a named unusable stamp."""
    if isinstance(age, bool) or not isinstance(age, int | float):
        return None
    return Decimal(str(age))


def time_verdict(raw_age: object, value: Decimal) -> Verdict:
    age = age_seconds(raw_age)
    if age is None:
        return "invalid_time", str(raw_age), None
    if age < 0:
        return "invalid_time", "future", None
    if age > RECENCY_SECONDS:
        return "stale", "older_than_600s", None
    return "available", "gamma", value


def value_verdict(obs: Observation) -> Verdict:
    value = parse_finite(obs.raw)
    if value is None:
        return "invalid", "not_a_finite_number", None
    return unusable_verdict(value) or time_verdict(obs.acquired_age_seconds, value)


def observed_verdict(obs: Observation) -> Verdict:
    if obs.source not in QUALIFIED_SOURCES:
        return "unqualified", "no_provenance" if obs.source is None else "unknown_source", None
    if not obs.present or obs.raw is None:
        return "missing", "null_or_absent", None
    return value_verdict(obs)


def classify(obs: Observation, explicit: Decimal | None) -> Verdict:
    """First-match precedence P1..P8; returns (state, reason, usable_volume)."""
    if explicit is not None:
        return explicit_verdict(explicit)
    if obs.kind == "unavailable":
        return "unavailable", obs.reason or "unknown", None
    return observed_verdict(obs)


def optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


RAW_BUILDERS: dict[str, Callable[[str], object]] = {
    "float": float,
    "nested_float": lambda text: [float(text)],
    "repeat_string": lambda text: "x" * int(text),
    "big_int_digits": lambda text: int("9" * int(text)),
}


def decoded_raw(raw: dict[str, object]) -> object:
    """The decoded volume24hr value; ``raw_spec`` builds values plain JSON cannot hold."""
    spec = raw.get("raw_spec")
    if not isinstance(spec, dict):
        return raw.get("raw")
    kind, arg = next(iter(cast(dict[str, object], spec).items()))
    return RAW_BUILDERS[kind](str(arg))


def build_observation(raw: dict[str, object]) -> Observation:
    return Observation(
        kind=str(raw.get("kind", "observed")),
        reason=optional_str(raw.get("reason")),
        source=optional_str(raw.get("source")),
        present=bool(raw.get("present", True)),
        raw=decoded_raw(raw),
        acquired_age_seconds=raw.get("acquired_age_seconds"),
    )


def merged(defaults: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    out = {**defaults, **{k: v for k, v in case.items() if k != "observation"}}
    observation = case.get("observation", {})
    base = {} if observation.get("kind") == "unavailable" else dict(defaults["observation"])
    out["observation"] = {**base, **observation}
    return out


def niche_basis(size: tuple[float, bool] | None, volume: Decimal | None) -> str:
    if size is None:
        return "none"
    if volume is not None:
        return "numeric" if size[1] else "numeric-not-niche"
    return "category" if size[1] else "none"


def old_score(category: str, nonce: int, notional: Decimal) -> Decimal:
    """003.1 rules as wired: no volume passed, category as given."""
    wallet = Wallet("unknown age", nonce, None)
    old_market = Market("m", None, category in NICHE_PRONE)
    old_w = wallet_confidence(wallet, notional, proposed=False) if nonce <= 5 else None
    return score(old_w, size_signal(old_market, notional, volume_passed=False))


def explicit_volume(spec: dict[str, Any]) -> Decimal | None:
    return Decimal(str(spec["explicit_daily_volume"])) if spec["explicit_daily_volume"] else None


def posture_columns(
    posture: str, wconf: float | None, size: tuple[float, bool] | None
) -> dict[str, object]:
    proposed = score(wconf, size)
    return {
        f"groups_{posture}": int(wconf is not None) + int(size is not None),
        f"score_{posture}": proposed,
        **{f"alerts_{posture}_at_{t}": proposed >= Decimal(t) for t in THRESHOLDS},
    }


def result_row(
    spec: dict[str, Any],
    verdict: Verdict,
    wconf: float | None,
    size: tuple[float, bool] | None,
) -> dict[str, object]:
    state, reason, volume = verdict
    return {
        "case_id": spec["case_id"],
        "volume_state": state,
        "volume_reason": reason,
        "wallet_class": "low_activity_unknown_age" if wconf is not None else "none",
        "wallet_confidence_post_T027": "" if wconf is None else f"{wconf:.3f}",
        "size_confidence": "" if size is None else f"{size[0]:.3f}",
        "niche_basis": niche_basis(size, volume),
        "old_score_as_wired": old_score(
            str(spec["category"]), int(spec["nonce"]), Decimal(str(spec["notional"]))
        ),
        **posture_columns("pre_T027", None, size),
        **posture_columns("post_T027", wconf, size),
        "purpose": spec["purpose"],
    }


def evaluate(spec: dict[str, Any]) -> dict[str, object]:
    notional = Decimal(str(spec["notional"]))
    nonce = int(spec["nonce"])
    state, reason, volume = classify(build_observation(spec["observation"]), explicit_volume(spec))
    category = str(spec["category"])
    category_usable = spec["category_source"] == "metadata" and state in NO_OBSERVED_VALUE_STATES
    market = Market("m", volume, category in NICHE_PRONE and category_usable)
    wallet = Wallet("unknown age", nonce, None)
    wconf = wallet_confidence(wallet, notional, proposed=True) if nonce < BOUNDARY else None
    size = size_signal(market, notional, volume_passed=True)
    return result_row(spec, (state, reason, volume), wconf, size)


def results() -> list[dict[str, object]]:
    corpus = json.loads((HERE / "replay-live" / "cases.json").read_text())
    return [evaluate(merged(corpus["defaults"], case)) for case in corpus["cases"]]


def render() -> str:
    verify_constants()
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(results())
    return buffer.getvalue()


def main(argv: list[str]) -> int:
    return print_or_check(argv, render())


if __name__ == "__main__":
    sys.exit(main(sys.argv))
