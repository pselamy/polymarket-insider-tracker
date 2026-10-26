"""T027 Part B commands end to end on a synthetic capture and the v2 cursor-feed fake."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest
from scripts.spec004_evidence import t027_cli as cli
from scripts.spec004_evidence import t027_partb as pb

from polymarket_insider_tracker.ingestor.trade_rows import TradeObservation, parse_trade_row
from tests.fakes.trades import synthetic_wallet, trade_row
from tests.fakes.trades_v2 import FakeV2TradesServer
from tests.fakes.v2_walk import v2_row

T0 = 1_600_000_000
WALLET = synthetic_wallet(9)


def write_lines(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def capture_record(raw: dict[str, object]) -> dict[str, object]:
    parsed = parse_trade_row(raw, now=T0 + 200_000)
    assert isinstance(parsed, TradeObservation)
    event = parsed.event
    return {
        "identity": parsed.identity,
        "wallet": event.wallet_address.lower(),
        "notional": str(event.notional_value),
        "ts": parsed.provider_timestamp,
    }


ROWS = [
    trade_row(timestamp=T0 + 10, transaction=1, wallet=9, price="0.5", size="4000"),
    trade_row(timestamp=T0 + 20, transaction=2, wallet=10, price="0.5", size="10"),
    trade_row(timestamp=T0 + 5_000, transaction=3, wallet=9),
    trade_row(timestamp=T0 + 6_000, transaction=4, wallet=9),
]


@pytest.fixture
def capture(tmp_path: Path) -> Path:
    directory = tmp_path / "capture"
    directory.mkdir()
    statuses: list[dict[str, object]] = [
        {"event": "start"},
        {"boundary_time": T0},
        {"boundary_time": T0 + 90_000},
    ]
    write_lines(directory / "status.jsonl", statuses)
    write_lines(directory / "trades.jsonl", [capture_record(r) for r in ROWS])
    return directory


def test_sample_plan_walk_and_judge_end_to_end(capture: Path, tmp_path: Path) -> None:
    sample = tmp_path / "sample.json"
    assert cli.main(["sample", "--capture", str(capture), "--seed", "7", "--out", str(sample)]) == 0
    document = json.loads(sample.read_text())
    assert (document["frame_size"], document["sample"]) == (1, [WALLET])
    assert document["walk_end"] == T0 + 86_401

    plan = tmp_path / "plan.json"
    args = ["plan", "--capture", str(capture), "--sample", str(sample), "--out", str(plan)]
    assert cli.main(args) == 0
    assert json.loads(plan.read_text()) == {"caps": {WALLET: 2}, "total": 2}

    server = FakeV2TradesServer()
    for raw in ROWS:
        if raw["proxyWallet"] == WALLET:
            server.publish(v2_row(raw))
    walks = tmp_path / "walks1.jsonl"
    walk_args = Namespace(
        sample=sample, read=1, out=walks, prior=[], plan=plan, raw_dir=tmp_path / "raw"
    )
    assert cli.walk_command(walk_args, server.client) == 0
    assert (tmp_path / "raw" / "read1" / WALLET / "0.json").exists()

    receipt = tmp_path / "receipt.json"
    provenance = tmp_path / "meta.json"
    provenance.write_text(json.dumps({"commit": "test"}))
    judge = ["judge", "--capture", str(capture), "--sample", str(sample), "--plan", str(plan)]
    judge += ["--provenance", str(provenance)]
    assert cli.main([*judge, "--walks", str(walks), "--out", str(receipt)]) == 0
    result = json.loads(receipt.read_text())
    assert result["verdict"] == "INCONCLUSIVE"
    assert result["metrics"]["q1_recall_aggregate"] == "1"
    assert result["metrics"]["deviations"] == []
    assert result["provenance"] == {"commit": "test"}
    assert set(result["inputs_sha256"]) == {
        str(capture / "trades.jsonl"),
        str(capture / "status.jsonl"),
        str(sample),
        str(plan),
        str(walks),
    }
    assert result["proven_intervals"] == [[T0, T0 + 86_400]]
    (walk_row,) = result["walks"]
    assert (walk_row["complete"], walk_row["failed_check"], walk_row["final_has_more"]) == (
        True,
        None,
        False,
    )
    assert walk_row["pages"][0]["limit"] == "1000" and walk_row["pages"][0]["status"] == 200
    assert "Q3 is true by construction" in result["limits"][0]
    (row,) = result["addresses"]
    assert (row["ground_truth_trades"], row["read1_complete"], row["read1_trade_count"]) == (
        2,
        True,
        3,
    )


def test_a_walk_never_appends_to_an_existing_record(capture: Path, tmp_path: Path) -> None:
    sample = tmp_path / "sample.json"
    cli.main(["sample", "--capture", str(capture), "--seed", "7", "--out", str(sample)])
    existing = tmp_path / "walks.jsonl"
    existing.write_text("")
    args = Namespace(sample=sample, read=1, out=existing, prior=[], plan=None, raw_dir=None)
    args.plan = tmp_path / "plan.json"
    args.plan.write_text(json.dumps({"caps": {}}))
    with pytest.raises(FileExistsError):
        cli.walk_command(args, FakeV2TradesServer().client)


def test_a_request_with_another_end_is_a_deviation() -> None:
    page = recorded(1, 0.0, complete=True)
    assert cli.end_deviations([page], 9) == []
    assert cli.end_deviations([page], 10) == ["1 requests with an end other than 10"]


def test_a_tampered_sample_file_is_a_protocol_deviation(capture: Path) -> None:
    document = cli.sample_document(capture, 7)
    tampered = document | {"seed": 8}
    found = cli.deviations(tampered, document, [], [WALLET])
    assert found == [
        "sample.json differs from the window and sample recomputed from the capture",
        f"no read-1 walk for {WALLET}",
    ]


def recorded(read: int, at: float, *, complete: bool) -> pb.PageRecord:
    params = {"user": WALLET, "taker_only": "false", "start": "1", "end": "9", "limit": "1000"}
    return pb.PageRecord(
        WALLET, read, 0, at, params, 200, None, None if complete else "c", not complete, (), 0
    )


def test_read_two_waits_an_hour_and_rewalks_only_complete_histories() -> None:
    prior = [recorded(1, 100.0, complete=True)]
    with pytest.raises(SystemExit, match="1 h"):
        cli.read2_plan(prior, 100.0 + 3_599)
    assert cli.read2_plan(prior, 100.0 + 3_600) == {WALLET: 2}
    assert cli.read2_plan([recorded(1, 100.0, complete=False)], 10_000.0) == {}


def test_requests_closer_than_half_a_second_and_over_budget_are_deviations() -> None:
    pages = [recorded(1, 0.0, complete=True), recorded(1, 0.2, complete=True)]
    assert cli.pacing_deviations(pages) == ["1 request starts closer than 0.5 s"]
    assert cli.page_cap(0) == 2 and cli.page_cap(1_500) == 3 and cli.page_cap(10**6) == 10
