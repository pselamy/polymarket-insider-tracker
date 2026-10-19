"""Live-realistic replay addendum for slice 004 (research.md R10, evidence/replay-live.md).

Usage:
    python3 replay_live.py                 # print the results CSV
    python3 replay_live.py --check FILE    # exit 1 unless FILE equals the CSV byte for byte

Stdlib only; never imports from src/. Scoring arithmetic is shared with reachability.py (whose
constants are textually verified against the product source). The volume-state classifier below is
the executable form of the precedence table in data-model.md section 1; the cases cover every
state and reason. ``old_score_as_wired`` is the 003.1 rule with the live wiring (no volume passed).
"""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from reachability import Market, Wallet, score, size_signal, verify_constants, wallet_confidence

HERE = Path(__file__).resolve().parent
QUALIFIED_SOURCES = frozenset({"gamma-markets-v1", "caller"})
NICHE_PRONE = frozenset({"science", "tech", "finance", "other"})
RECENCY_SECONDS = 600
BOUNDARY = 5  # B: exclusive nonce bound (proposed); old rule is nonce <= 5
THRESHOLDS = ("0.80", "0.78")
COLUMNS = (
    "case_id",
    "volume_state",
    "volume_reason",
    "wallet_class",
    "wallet_confidence",
    "size_confidence",
    "niche_basis",
    "groups",
    "old_score_as_wired",
    "proposed_score",
    *(f"alerts_at_{t}" for t in THRESHOLDS),
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


def parse_finite(raw: object) -> Decimal | None:
    """A finite Decimal from a JSON number or numeric string; None for anything else."""
    if isinstance(raw, bool) or not isinstance(raw, int | float | str):
        return None
    try:
        value = Decimal(str(raw).strip()) if isinstance(raw, str) else Decimal(str(raw))
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def classify(obs: Observation, explicit: Decimal | None) -> tuple[str, str, Decimal | None]:
    """First-match precedence P1..P8; returns (state, reason, usable_volume)."""
    if explicit is not None:
        if explicit > 0:
            return "available", "caller", explicit
        return "nonpositive", "zero" if explicit == 0 else "negative", None
    if obs.kind == "unavailable":
        return "unavailable", obs.reason or "unknown", None
    if obs.source not in QUALIFIED_SOURCES:
        return "unqualified", "no_provenance" if obs.source is None else "unknown_source", None
    if not obs.present or obs.raw is None:
        return "missing", "null_or_absent", None
    value = parse_finite(obs.raw)
    if value is None:
        return "invalid", "not_a_finite_number", None
    if value <= 0:
        return "nonpositive", "zero" if value == 0 else "negative", None
    age = obs.acquired_age_seconds
    if not isinstance(age, int) or age < 0:
        return "invalid_time", str(age if not isinstance(age, int) else "future"), None
    if age > RECENCY_SECONDS:
        return "stale", "older_than_600s", None
    return "available", "gamma", value


def optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def build_observation(raw: dict[str, object]) -> Observation:
    return Observation(
        kind=str(raw.get("kind", "observed")),
        reason=optional_str(raw.get("reason")),
        source=optional_str(raw.get("source")),
        present=bool(raw.get("present", True)),
        raw=raw.get("raw"),
        acquired_age_seconds=raw.get("acquired_age_seconds"),
    )


def merged(defaults: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    out = {**defaults, **{k: v for k, v in case.items() if k != "observation"}}
    observation = case.get("observation", {})
    base = {} if observation.get("kind") == "unavailable" else dict(defaults["observation"])
    out["observation"] = {**base, **observation}
    return out


def evaluate(spec: dict[str, Any]) -> dict[str, object]:
    notional = Decimal(str(spec["notional"]))
    nonce = int(spec["nonce"])
    explicit = (
        Decimal(str(spec["explicit_daily_volume"])) if spec["explicit_daily_volume"] else None
    )
    state, reason, volume = classify(build_observation(spec["observation"]), explicit)
    category = str(spec["category"])
    real_metadata = spec["category_source"] == "metadata"
    market = Market("m", volume, category in NICHE_PRONE and real_metadata)
    wallet = Wallet("unknown age", nonce, None)
    wconf = wallet_confidence(wallet, notional, proposed=True) if nonce < BOUNDARY else None
    size = size_signal(market, notional, volume_passed=True)
    proposed = score(wconf, size)
    old_market = Market(
        "m", None, category in NICHE_PRONE
    )  # live wiring: no volume, category as given
    old_w = wallet_confidence(wallet, notional, proposed=False) if nonce <= 5 else None
    old = score(old_w, size_signal(old_market, notional, volume_passed=False))
    if size is None:
        basis = "none"
    elif volume is not None:
        basis = "numeric" if size[1] else "numeric-not-niche"
    else:
        basis = "category" if size[1] else "none"
    return {
        "case_id": spec["case_id"],
        "volume_state": state,
        "volume_reason": reason,
        "wallet_class": "low_activity_unknown_age" if wconf is not None else "none",
        "wallet_confidence": "" if wconf is None else f"{wconf:.3f}",
        "size_confidence": "" if size is None else f"{size[0]:.3f}",
        "niche_basis": basis,
        "groups": int(wconf is not None) + int(size is not None),
        "old_score_as_wired": old,
        "proposed_score": proposed,
        **{f"alerts_at_{t}": proposed >= Decimal(t) for t in THRESHOLDS},
        "purpose": spec["purpose"],
    }


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
    text = render()
    if len(argv) == 3 and argv[1] == "--check":
        return 0 if Path(argv[2]).read_text() == text else 1
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
