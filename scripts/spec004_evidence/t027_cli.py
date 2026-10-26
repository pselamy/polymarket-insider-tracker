"""T027 Part B commands: ``sample``, ``walk`` and ``judge`` (research.md R09a, protocol v2).

``sample`` derives the window from the capture's status snapshots and draws the seeded sample;
``walk`` runs one read of per-user history walks against the shared 1,000-request budget; read 2
walks only the addresses whose read-1 walk is a complete history and refuses to start before
every read-1 walk is at least one hour old; ``judge`` recomputes the window and sample from the
capture (any mismatch with ``sample.json`` is a protocol deviation), builds the ground truth,
classifies every walk, applies the PASS rule and writes the receipt JSON.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import IO

import httpx

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from scripts.spec004_evidence import t027_partb as pb
from scripts.spec004_evidence import t027_walk as tw
from scripts.spec004_evidence.json_shapes import Obj, as_array, as_int, as_object, as_text

ROWS_PER_PAGE = 1000
MIN_PAGE_CAP = 2
MAX_PAGE_CAP = 10


def json_lines(path: Path) -> Iterator[Obj]:
    with path.open() as handle:
        for line in handle:
            yield as_object(json.loads(line))


def statuses(capture: Path) -> list[Obj]:
    return list(json_lines(capture / "status.jsonl"))


def trades(capture: Path) -> Iterator[pb.CapturedTrade]:
    return (pb.captured_trade(record) for record in json_lines(capture / "trades.jsonl"))


def sample_document(capture: Path, seed: int) -> Obj:
    window = pb.window_from(statuses(capture))
    frame = pb.frame(trades(capture), window)
    return {
        "t0": window.t0,
        "t1": window.t1,
        "sampled_at": window.sampled_at,
        "proven_through": window.proven_through,
        "losses": [[i.start, i.end] for i in window.losses],
        "proven_seconds": window.proven_seconds(),
        "seed": seed,
        "frame_size": len(frame),
        "sample": pb.draw_sample(frame, seed),
        "walk_end": window.t1 + 1,
    }


def recorded_pages(paths: Iterable[Path]) -> list[pb.PageRecord]:
    return [pb.page_record(record) for path in paths for record in json_lines(path)]


def page_cap(truth_count: int) -> int:
    """Read-1 page cap: enough newest-first pages to cover the window's trades, plus one."""
    needed = -(-truth_count // ROWS_PER_PAGE) + 1
    return min(MAX_PAGE_CAP, max(MIN_PAGE_CAP, needed))


def read1_plan(capture: Path, document: Obj) -> dict[str, int]:
    """Per-address page caps, fixed from the ground truth before any walk is made."""
    window = pb.window_from(statuses(capture))
    sample = [as_text(a) for a in as_array(document["sample"])]
    truth = sampled_truth(capture, window, sample)
    return {address: page_cap(len(truth[address])) for address in sample}


def read1_walks(prior: Sequence[pb.PageRecord]) -> list[pb.Walk]:
    walks = pb.walks_from(page for page in prior if page.read == 1)
    return sorted(walks.values(), key=lambda w: w.address)


def require_read_gap(walks: Sequence[pb.Walk], now: float) -> None:
    youngest = max(walk.started_at for walk in walks)
    if now < youngest + pb.MIN_READ_GAP_SECONDS:
        raise SystemExit("read 2 must start at least 1 h after the last read-1 walk started")


def read2_plan(prior: Sequence[pb.PageRecord], now: float) -> dict[str, int]:
    """Read 2: only read-1 complete histories, >= 1 h after read 1, one page more than read 1.

    The extra page lets a history that grew between the reads still reach its terminal page, so
    an instability shows up in Q4 instead of as a capped, incomplete second read.
    """
    walks = read1_walks(prior)
    require_read_gap(walks, now)
    return {w.address: len(w.pages) + 1 for w in walks if pb.is_complete_history(w)}


def plan_command(args: argparse.Namespace) -> int:
    document = as_object(json.loads(args.sample.read_text()))
    plan = read1_plan(args.capture, document)
    args.out.write_text(json.dumps({"caps": plan, "total": sum(plan.values())}, indent=2) + "\n")
    print(f"read-1 plan: {len(plan)} addresses, at most {sum(plan.values())} requests")
    return 0


ClientFactory = Callable[[], httpx.AsyncClient]


def plan_caps(path: Path) -> dict[str, int]:
    caps = as_object(as_object(json.loads(path.read_text()))["caps"])
    return {address: as_int(cap) for address, cap in caps.items()}


def walk_command(
    args: argparse.Namespace, client_factory: ClientFactory = httpx.AsyncClient
) -> int:
    document = as_object(json.loads(args.sample.read_text()))
    prior = recorded_pages(args.prior)
    caps = plan_caps(args.plan) if args.read == 1 else read2_plan(prior, time.time())
    budget = tw.Budget(tw.TOTAL_BUDGET - len(prior))
    with args.out.open("x") as out:
        end = as_int(document["walk_end"])
        asyncio.run(walk_all(args, client_factory(), end, caps, budget, out))
    return 0


async def walk_all(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    end: int,
    caps: dict[str, int],
    budget: tw.Budget,
    out: IO[str],
) -> None:
    async with client:
        walker = tw.Walker(
            client=client, read=args.read, end=end, budget=budget, out=out, raw_dir=args.raw_dir
        )
        for address, cap in caps.items():
            await walker.walk(address, cap)


# ----- judge --------------------------------------------------------------------------------------


def sampled_trades(capture: Path, sample: Sequence[str]) -> list[pb.CapturedTrade]:
    wanted = set(sample)
    return [t for t in trades(capture) if t.wallet in wanted]


def sampled_truth(capture: Path, window: pb.Window, sample: Sequence[str]) -> dict[str, set[str]]:
    return pb.ground_truth(sampled_trades(capture, sample), window, sample)


def pacing_deviations(pages: Sequence[pb.PageRecord]) -> list[str]:
    starts = sorted(page.requested_at for page in pages)
    gaps = [b - a for a, b in zip(starts, starts[1:], strict=False)]
    too_fast = sum(1 for gap in gaps if gap < tw.MIN_INTERVAL_SECONDS - 0.01)
    return [f"{too_fast} request starts closer than 0.5 s"] if too_fast else []


def budget_deviations(pages: Sequence[pb.PageRecord]) -> list[str]:
    if len(pages) <= tw.TOTAL_BUDGET:
        return []
    return [f"{len(pages)} requests > budget {tw.TOTAL_BUDGET}"]


def sample_deviations(document: Obj, recomputed: Obj) -> list[str]:
    if recomputed == document:
        return []
    return ["sample.json differs from the window and sample recomputed from the capture"]


def unwalked(pages: Sequence[pb.PageRecord], sample: Sequence[str]) -> list[str]:
    walked = {page.address for page in pages if page.read == 1}
    return [f"no read-1 walk for {address}" for address in sample if address not in walked]


def end_deviations(pages: Sequence[pb.PageRecord], walk_end: int) -> list[str]:
    """Every request of both reads must send the pre-registered ``end``."""
    wrong = sum(1 for page in pages if page.params.get("end") != str(walk_end))
    return [f"{wrong} requests with an end other than {walk_end}"] if wrong else []


def deviations(
    document: Obj, recomputed: Obj, pages: Sequence[pb.PageRecord], sample: Sequence[str]
) -> list[str]:
    return [
        *pacing_deviations(pages),
        *budget_deviations(pages),
        *sample_deviations(document, recomputed),
        *unwalked(pages, sample),
        *end_deviations(pages, as_int(document["walk_end"])),
    ]


def earliest_ts(outcome: pb.Classification | None) -> int | None:
    if outcome is None or outcome.earliest is None:
        return None
    return outcome.earliest[0]


def address_row(result: pb.AddressResult, boundary: int) -> Obj:
    second = result.read2
    return {
        "address": result.address,
        "ground_truth_trades": result.truth,
        "active": result.active(),
        "heavy": result.heavy(boundary),
        "q1_eligible": result.q1_eligible(),
        "matched": result.matched,
        "recall": None if result.recall() is None else str(result.recall()),
        "read1_pages": result.read1_pages,
        "read1_complete": result.read1.complete,
        "read1_trade_count": result.read1.trade_count,
        "read1_earliest": earliest_ts(result.read1),
        "read2_complete": None if second is None else second.complete,
        "read2_trade_count": None if second is None else second.trade_count,
        "read2_earliest": earliest_ts(second),
        "read_gap_seconds": result.read_gap_seconds,
        "q4_identical": result.stable() if result.q4_measured() else None,
        "ordering_violations": result.violations,
        "duplicate_rows": result.duplicate_rows,
        "frame_trades_missing": result.frame_missing,
        "q5b_violation": result.boundary_violation(boundary),
    }


def walk_row(walk: pb.Walk) -> Obj:
    last = walk.pages[-1]
    return {
        "address": walk.address,
        "read": walk.read,
        "complete": pb.is_complete_history(walk),
        "failed_check": pb.failed_check(walk),
        "transport_failure": walk.transport_failure(),
        "pages": [dict(page.params) | {"status": page.status} for page in walk.pages],
        "final_next_cursor": last.next_cursor,
        "final_has_more": last.has_more,
    }


def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def metrics_document(m: pb.Metrics) -> Obj:
    return {
        "proven_seconds": m.proven_seconds,
        "queried": m.queried,
        "active": m.active,
        "q1_eligible": m.q1_eligible,
        "heavy": m.heavy,
        "q1_recall_aggregate": decimal_text(m.recall_aggregate),
        "q1_recall_minimum": decimal_text(m.recall_minimum),
        "q2_ordering_violations": m.ordering_violations,
        "q3_read1_complete": m.complete_read1,
        "q3_read1_incomplete_all_unknown": m.incomplete_read1,
        "q4_population": m.complete_read1,
        "q4_measured": m.stable_population,
        "q4_unmeasured": m.q4_unmeasured,
        "q4_identical": m.stable,
        "q5a_replay": [r.__dict__ for r in m.replay],
        "q5b_population": m.q5b_population,
        "q5b_boundary_violations": m.boundary_violations,
        "duplicate_rows": m.duplicate_rows,
        "frame_trades_missing": m.frame_missing,
        "requests": m.requests,
        "deviations": list(m.deviations),
    }


PROTOCOL_PARAMETERS: Obj = {
    "protocol": "research.md R09a v2, Part B",
    "sample_n": pb.SAMPLE_N,
    "boundary_B": pb.B_DEFAULT,
    "frame_min_notional_usd": str(pb.FRAME_MIN_NOTIONAL),
    "min_proven_seconds": pb.MIN_PROVEN_SECONDS,
    "min_active": pb.MIN_ACTIVE,
    "min_read_gap_seconds": pb.MIN_READ_GAP_SECONDS,
    "q1_aggregate": str(pb.Q1_AGGREGATE),
    "q1_per_address": str(pb.Q1_PER_ADDRESS),
    "query": {"url": OFFICIAL_V2_TRADES_URL, "taker_only": "false", "start": "1"},
    "request_budget": tw.TOTAL_BUDGET,
    "min_request_interval_seconds": tw.MIN_INTERVAL_SECONDS,
    "read1_page_cap": f"clamp(ceil(k/{ROWS_PER_PAGE}) + 1, {MIN_PAGE_CAP}, {MAX_PAGE_CAP})",
    "read2_page_cap": "read-1 pages + 1, read-1 complete histories only",
}

WINDOW_KEYS = ("t0", "t1", "sampled_at", "proven_through", "losses")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def input_hashes(args: argparse.Namespace) -> Obj:
    files = [args.capture / "trades.jsonl", args.capture / "status.jsonl", args.sample, args.plan]
    return {str(path): sha256_of(path) for path in [*files, *args.walks]}


def sample_of(document: Obj) -> list[str]:
    return [as_text(a) for a in as_array(document["sample"])]


@dataclass(frozen=True)
class Judged:
    window: pb.Window
    metrics: pb.Metrics
    results: list[pb.AddressResult]
    walks: list[pb.Walk]


def judge(args: argparse.Namespace, document: Obj) -> Judged:
    window = pb.window_from(statuses(args.capture))
    sample = sample_of(document)
    pages = recorded_pages(args.walks)
    walks = pb.walks_from(pages)
    own = sampled_trades(args.capture, sample)
    truth = pb.ground_truth(own, window, sample)
    frame = pb.frame_trades(own, window, sample)
    walked = [a for a in sample if (1, a) in walks]
    results = [pb.address_result(a, truth[a], walks, frame[a]) for a in walked]
    found = deviations(
        document, sample_document(args.capture, as_int(document["seed"])), pages, sample
    )
    replay = pb.q5a(pages[0], pb.B_DEFAULT)
    m = pb.metrics(window, results, replay, len(pages), found)
    return Judged(window, m, results, list(walks.values()))


def receipt_document(args: argparse.Namespace, document: Obj, judged: Judged) -> Obj:
    m = judged.metrics
    outcome, reasons = pb.verdict(m)
    return {
        "parameters": PROTOCOL_PARAMETERS,
        "provenance": json.loads(args.provenance.read_text()) if args.provenance else None,
        "inputs_sha256": input_hashes(args),
        "window": {k: document[k] for k in WINDOW_KEYS},
        "proven_intervals": [[i.start, i.end] for i in judged.window.proven_intervals()],
        "seed": document["seed"],
        "frame_size": document["frame_size"],
        "walk_end": document["walk_end"],
        "plan": plan_caps(args.plan),
        "metrics": metrics_document(m),
        "verdict": outcome,
        "reasons": reasons,
        "fail_checks": pb.fail_reasons(m),
        "limits": pb.limits(m),
        "addresses": [address_row(r, pb.B_DEFAULT) for r in judged.results],
        "walks": [walk_row(w) for w in sorted(judged.walks, key=lambda w: (w.read, w.address))],
    }


def judge_command(args: argparse.Namespace) -> int:
    document = as_object(json.loads(args.sample.read_text()))
    receipt = receipt_document(args, document, judge(args, document))
    args.out.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(receipt["verdict"], *as_array(receipt["reasons"]), sep="\n")
    return 0


def sample_command(args: argparse.Namespace) -> int:
    args.out.write_text(json.dumps(sample_document(args.capture, args.seed), indent=2) + "\n")
    return 0


def parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser(description="T027 Part B sample, plan, walk and judge commands.")
    sub = top.add_subparsers(dest="command", required=True)
    sample = sub.add_parser("sample")
    sample.add_argument("--capture", type=Path, required=True)
    sample.add_argument("--seed", type=int, required=True)
    sample.add_argument("--out", type=Path, required=True)
    walk = sub.add_parser("walk")
    walk.add_argument("--sample", type=Path, required=True)
    walk.add_argument("--read", type=int, choices=(1, 2), required=True)
    walk.add_argument("--out", type=Path, required=True)
    walk.add_argument("--prior", type=Path, nargs="*", default=[])
    walk.add_argument("--plan", type=Path)
    walk.add_argument("--raw-dir", type=Path)
    plan = sub.add_parser("plan")
    plan.add_argument("--capture", type=Path, required=True)
    plan.add_argument("--sample", type=Path, required=True)
    plan.add_argument("--out", type=Path, required=True)
    judge = sub.add_parser("judge")
    judge.add_argument("--capture", type=Path, required=True)
    judge.add_argument("--sample", type=Path, required=True)
    judge.add_argument("--walks", type=Path, nargs="+", required=True)
    judge.add_argument("--plan", type=Path, required=True)
    judge.add_argument("--provenance", type=Path)
    judge.add_argument("--out", type=Path, required=True)
    return top


COMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "sample": sample_command,
    "plan": plan_command,
    "walk": walk_command,
    "judge": judge_command,
}


def main(argv: list[str]) -> int:
    args = parser().parse_args(argv)
    return COMMANDS[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
