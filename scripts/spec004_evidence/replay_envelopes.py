"""Build one 004.1 evaluation envelope per live-replay case (contracts/scoring-config.md).

Stdlib only; never imports from src/. The verdicts come from ``replay_live`` (the executable
precedence table) and the scoring arithmetic from ``reachability``; this module only lays the
results out in the stored envelope shape so ``check_envelope`` can validate every volume state.
Envelopes are built in the default pre-T027 posture (FR-007c, ``wallet.group_status =
unqualified_pre_T027``) unless the post-T027 scenario (``qualified``) is requested.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from fractions import Fraction
from typing import Any, cast

from scripts.spec004_evidence import replay_live as rl
from scripts.spec004_evidence.reachability import (
    Market,
    Wallet,
    score,
    size_signal,
    wallet_confidence,
)

EVALUATED_AT = datetime.fromisoformat("2026-10-19T12:00:00+00:00")
WEIGHTS = {
    "fresh_wallet": Decimal("0.4"),
    "niche_market": Decimal("0.25"),
    "size_anomaly": Decimal("0.35"),
}


PRE_T027 = "unqualified_pre_T027"
POST_T027 = "qualified"
GROUP_STATUSES = (PRE_T027, POST_T027)
# contracts/scoring-config.md section 2a: each wallet posture is its own scoring version.
VERSION_BY_STATUS = {PRE_T027: "004.1", POST_T027: "004.2"}
RATIO_QUANTUM = 10_000  # volume_impact has exactly 4 decimals (notional >= 0, volume > 0)

RAW_BOUND_BYTES = 256
RESERVED_KEYS = frozenset({"$truncated", "$nonfinite"})


def canonical(value: object) -> bytes:
    """Canonical UTF-8 text of a decoded value; non-finite floats keep Python's NaN/Infinity tokens."""
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text.encode("utf-8")


def json_type(value: object) -> str:
    if value is None:
        return "null"
    names: dict[type, str] = {
        bool: "boolean",
        int: "number",
        float: "number",
        str: "string",
        list: "array",
    }
    return names.get(type(value), "object")


def children(value: object) -> list[object]:
    if isinstance(value, list):
        return list(cast(list[object], value))
    if isinstance(value, dict):
        return list(cast(dict[str, object], value).values())
    return []


def has_nonfinite(value: object) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    return any(has_nonfinite(item) for item in children(value))


def truncation_reason(value: object, size: int) -> str | None:
    if has_nonfinite(value):
        return "nested_nonfinite"
    if isinstance(value, dict) and RESERVED_KEYS & value.keys():
        return "reserved_key"
    return "oversize" if size > RAW_BOUND_BYTES else None


def nonfinite_tag(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    return "Infinity" if value > 0 else "-Infinity"


def bound_raw(value: object) -> object:
    """The stored form of a decoded volume24hr value (contracts/scoring-config.md raw-value rule)."""
    if isinstance(value, float) and not math.isfinite(value):
        return {"$nonfinite": nonfinite_tag(value)}
    body = canonical(value)
    reason = truncation_reason(value, len(body))
    if reason is None:
        return value
    return {
        "$truncated": True,
        "bytes": len(body),
        "reason": reason,
        "sha256": hashlib.sha256(body).hexdigest(),
        "type": json_type(value),
    }


def plain(value: Decimal) -> str:
    """Plain-notation decimal string without a trailing exponent."""
    return format(value, "f")


def exact_ratio(notional: Decimal, volume: Decimal) -> str:
    """notional / volume as an exact rational rounded half-even to 0.0001 (data-model section 3);
    no fixed-precision decimal context, so every available row is computable."""
    whole, fraction = divmod(
        round(Fraction(notional) / Fraction(volume) * RATIO_QUANTUM), RATIO_QUANTUM
    )
    return f"{whole}.{fraction:04d}"


def exact_seconds(delta: timedelta) -> Decimal:
    """A timedelta as exact decimal seconds (microsecond resolution, no float)."""
    return Decimal(delta // timedelta(microseconds=1)).scaleb(-6)


def stamp(raw_age: object) -> tuple[str | None, str | None]:
    """(acquired_at, signed age seconds); both null when the stamp is unusable."""
    age = rl.age_seconds(raw_age)
    if age is None:
        return None, None
    acquired = EVALUATED_AT - timedelta(microseconds=int(age.scaleb(6)))
    return acquired.isoformat(), plain(exact_seconds(EVALUATED_AT - acquired).normalize())


def volume_stamp(obs: rl.Observation, explicit: Decimal | None) -> tuple[str | None, str | None]:
    if explicit is not None:
        return EVALUATED_AT.isoformat(), "0"
    if obs.kind == "unavailable":
        return None, None
    return stamp(obs.acquired_age_seconds)


def volume_source(obs: rl.Observation, explicit: Decimal | None) -> str | None:
    if explicit is not None:
        return "caller"
    return None if obs.kind == "unavailable" else obs.source


def raw_fields(obs: rl.Observation, explicit: Decimal | None) -> tuple[bool, object]:
    if explicit is not None or obs.kind == "unavailable":
        return False, None
    return obs.present, bound_raw(obs.raw)


def size_block(
    spec: dict[str, Any], verdict: rl.Verdict, size: tuple[float, bool] | None
) -> dict[str, Any]:
    state, reason, volume = verdict
    obs = rl.build_observation(spec["observation"])
    explicit = rl.explicit_volume(spec)
    acquired, age = volume_stamp(obs, explicit)
    present, raw = raw_fields(obs, explicit)
    notional = Decimal(str(spec["notional"]))
    impact = None if volume is None else exact_ratio(notional, volume)
    conf = None if size is None else f"{size[0]:.3f}"
    return {
        "book_impact": None,
        "book_state": "unavailable",
        "category": str(spec["category"]),
        "category_source": str(spec["category_source"]),
        "confidence": conf,
        "daily_volume": None if volume is None else plain(volume),
        "daily_volume_acquired_at": acquired,
        "daily_volume_age_seconds": age,
        "daily_volume_source": volume_source(obs, explicit),
        "factors": {},
        "niche": bool(size and size[1]),
        "niche_basis": rl.niche_basis(size, volume).removesuffix("-not-niche"),
        "signal_emitted": size is not None,
        "volume24hr_present": present,
        "volume24hr_raw": raw,
        "volume_impact": impact,
        "volume_reason": reason,
        "volume_state": state,
    }


def wallet_block(
    classification: str, wconf: float | None, status: str, nonce: int
) -> dict[str, Any]:
    """``wconf`` is the scoring confidence: always None while the group is unqualified."""
    return {
        "age_basis": "none",
        "age_hours": None,
        "age_state": "unknown",
        "boundary": rl.BOUNDARY,
        "classification": classification,
        "confidence": None if wconf is None else f"{wconf:.3f}",
        "factors": {} if wconf is None else {"base": "0.5"},
        "group_status": status,
        "observation_time": "2026-10-19T11:58:10+00:00",
        "retrieval_error": None,
        "subject_kind": "proxy_wallet",
        "tx_count": nonce,
    }


def term(confidence: float | None, weight: Decimal) -> Decimal:
    return Decimal(0) if confidence is None else Decimal(f"{confidence:.3f}") * weight


def contributions(wconf: float | None, size: tuple[float, bool] | None) -> dict[str, str]:
    size_conf = None if size is None else size[0]
    niche_conf = size_conf if size is not None and size[1] else None
    wallet = term(wconf, WEIGHTS["fresh_wallet"])
    size_c = term(size_conf, WEIGHTS["size_anomaly"])
    niche = term(niche_conf, WEIGHTS["niche_market"])
    bonus = Decimal("1.2") if wconf is not None and size is not None else Decimal(1)
    return {
        "bonus": plain(bonus),
        "capped_score": plain(min((wallet + size_c + niche) * bonus, Decimal(1))),
        "fresh_wallet": plain(wallet),
        "niche_market": plain(niche),
        "size_anomaly": plain(size_c),
    }


def build_envelope(
    spec: dict[str, Any], template: dict[str, Any], status: str = PRE_T027
) -> dict[str, Any]:
    """A complete 004.1 (or, post-T027, 004.2) document for one replay case, based on the example's static keys."""
    notional = Decimal(str(spec["notional"]))
    nonce = int(spec["nonce"])
    verdict = rl.classify(rl.build_observation(spec["observation"]), rl.explicit_volume(spec))
    market = Market(
        "m",
        verdict[2],
        str(spec["category"]) in rl.NICHE_PRONE and spec["category_source"] == "metadata",
    )
    candidate = (
        wallet_confidence(Wallet("unknown age", nonce, None), notional, proposed=True)
        if nonce < rl.BOUNDARY
        else None
    )
    classification = "known_nonqualifying" if candidate is None else "low_activity_unknown_age"
    wconf = candidate if status == POST_T027 else None
    size = size_signal(market, notional, volume_passed=True)
    proposed = score(wconf, size)
    doc = cast(dict[str, Any], json.loads(json.dumps(template)))
    doc["evidence_schema"] = VERSION_BY_STATUS[status]
    doc["evaluation_evidence"] = {
        "contributions": contributions(wconf, size),
        "evaluated_at": EVALUATED_AT.isoformat(),
        "notional": plain(notional),
        "result": {
            "should_alert": proposed >= Decimal(doc["alert_threshold"]),
            "signals_triggered": int(wconf is not None) + int(size is not None),
            "weighted_score": plain(proposed.quantize(Decimal("0.001"), ROUND_HALF_EVEN)),
        },
        "size": size_block(spec, verdict, size),
        "wallet": wallet_block(classification, wconf, status, nonce),
    }
    return doc


def replay_specs() -> list[dict[str, Any]]:
    corpus = json.loads((rl.HERE / "replay-live" / "cases.json").read_text())
    return [rl.merged(corpus["defaults"], case) for case in corpus["cases"]]
