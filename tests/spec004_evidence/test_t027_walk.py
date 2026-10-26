"""T027 Part B walker against the working v2 cursor-feed fake (research.md R09a v2)."""

from __future__ import annotations

import io
import json

import httpx
import pytest
from scripts.spec004_evidence import t027_partb as pb
from scripts.spec004_evidence import t027_walk as tw

from tests.closeness import is_close
from tests.fakes.trades import FakeClock, synthetic_wallet, trade_row
from tests.fakes.trades_v2 import FakeV2TradesServer
from tests.fakes.v2_walk import v2_row

ADDRESS = synthetic_wallet(7)
END = 2_000_000_000


async def no_sleep(_: float) -> None:
    return None


def server_with(count: int, *, page_size: int = 2, wallet: int = 7) -> FakeV2TradesServer:
    server = FakeV2TradesServer(page_size=page_size)
    for index in range(count):
        server.publish(v2_row(trade_row(timestamp=1_000 + index, transaction=index, wallet=wallet)))
    return server


def walker(client: httpx.AsyncClient, out: io.StringIO, budget: int = 50) -> tw.Walker:
    return tw.Walker(
        client=client,
        read=1,
        end=END,
        budget=tw.Budget(budget),
        out=out,
        pacer=tw.Pacer(clock=FakeClock(0.0), sleep=no_sleep),
        wall_clock=FakeClock(1_900_000_000.0),
    )


def recorded(out: io.StringIO) -> pb.Walk:
    pages = [pb.page_record(json.loads(line)) for line in out.getvalue().splitlines()]
    return pb.walks_from(pages)[(1, ADDRESS)]


async def walk(server: FakeV2TradesServer, cap: int = 10, budget: int = 50) -> pb.Walk:
    out = io.StringIO()
    async with server.client() as client:
        await walker(client, out, budget).walk(ADDRESS, cap)
    return recorded(out)


async def test_a_terminal_walk_is_a_complete_history_with_every_trade() -> None:
    history = await walk(server_with(5))
    outcome = pb.classify(history)
    assert [len(p.rows) for p in history.pages] == [2, 2, 1]
    assert outcome.complete and outcome.trade_count == 5
    assert outcome.earliest is not None and outcome.earliest[0] == 1_000
    assert pb.ordering_violations(history) == 0


async def test_requests_carry_the_protocol_filters_and_only_the_cursor_after_page_one() -> None:
    server = server_with(3)
    await walk(server)
    first, second = (dict(r.params) for r in server.requests)
    assert first == {
        "user": ADDRESS,
        "taker_only": "false",
        "start": "1",
        "end": str(END),
        "limit": "1000",
    }
    assert second == {
        "user": ADDRESS,
        "taker_only": "false",
        "start": "1",
        "end": str(END),
        "cursor": "cursor-2",
    }
    assert all("offset" not in r.params for r in server.requests)


@pytest.mark.parametrize(("cap", "budget"), [(2, 50), (10, 2)])
async def test_a_walk_stopped_by_its_cap_or_the_budget_is_incomplete(cap: int, budget: int) -> None:
    history = await walk(server_with(5), cap=cap, budget=budget)
    assert len(history.pages) == 2
    assert pb.classify(history) == pb.Classification(False, None, None)


async def test_an_untrusted_envelope_stops_the_walk_and_leaves_it_incomplete() -> None:
    server = server_with(5)
    server.fail_next({"data": [], "pagination": {"next_cursor": 3, "has_more": True}})
    history = await walk(server)
    assert len(history.pages) == 1 and history.pages[0].error == "untrusted-envelope"
    assert not pb.classify(history).complete


async def test_an_http_error_is_recorded_without_a_retry() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    out = io.StringIO()
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as client:
        await walker(client, out).walk(ADDRESS, 10)
    history = recorded(out)
    assert [(p.status, p.error) for p in history.pages] == [(503, "http-503")]
    assert not pb.classify(history).complete


async def test_a_transport_failure_is_recorded_as_its_class() -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("no answer", request=request)

    out = io.StringIO()
    async with httpx.AsyncClient(transport=httpx.MockTransport(broken)) as client:
        await walker(client, out).walk(ADDRESS, 10)
    history = recorded(out)
    assert [(p.status, p.error) for p in history.pages] == [(None, "ConnectTimeout")]


async def test_a_foreign_wallet_row_makes_the_page_invalid() -> None:
    server = server_with(2)
    server.publish(v2_row(trade_row(timestamp=999, transaction=99, wallet=8)))
    history = await walk(server, cap=10)
    assert sum(p.invalid_rows for p in history.pages) == 1
    assert not pb.classify(history).complete


async def test_the_pacer_spaces_request_starts_by_half_a_second() -> None:
    clock = FakeClock(10.0)
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)

    pacer = tw.Pacer(clock=clock, sleep=sleep)
    await pacer.wait()
    clock.advance(0.2)
    await pacer.wait()
    assert len(slept) == 1 and is_close(slept[0], 0.3)


def test_the_budget_is_shared_and_never_goes_negative() -> None:
    budget = tw.Budget(1)
    assert budget.take() and not budget.take() and budget.remaining == 0


async def test_a_row_after_the_requested_end_makes_the_page_invalid() -> None:
    server = server_with(2)
    server.publish(v2_row(trade_row(timestamp=END + 1, transaction=50, wallet=7)))
    out = io.StringIO()
    async with server.client() as client:
        late = walker(client, out)
        late.wall_clock = FakeClock(END + 100.0)
        await late.walk(ADDRESS, 10)
    history = recorded(out)
    assert sum(p.invalid_rows for p in history.pages) == 1
    assert pb.failed_check(history) == "pages-answered-with-valid-rows"
