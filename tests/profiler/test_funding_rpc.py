"""Seam tests for the extracted funding log scanner.

These tests pin the observable contract of the definitions moved out of
:mod:`polymarket_insider_tracker.profiler.funding` into
:mod:`polymarket_insider_tracker.profiler._funding_rpc`:

- both import paths resolve to the same objects (constants and the
  pruned-history classifier), with the historical ``funding.<name>`` module
  identity preserved for pickle/qualname compatibility,
- :class:`TransferLogScanner` walks ``eth_getLogs`` with the same chunking,
  limit, pruned-history and failover behavior the tracer relied on,
- the pruned-history classifier keeps its marker/substring semantics.

The classifier property is exercised with a deterministic, table-driven
substring oracle rather than Hypothesis: Hypothesis is not a locked dependency
of this repository and adding one is outside this change's scope.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest

from polymarket_insider_tracker.profiler import _funding_rpc as rpc
from polymarket_insider_tracker.profiler import funding
from polymarket_insider_tracker.profiler._funding_rpc import TransferLogScanner
from tests.fakes import FakeEth, TransferLogIndex
from tests.pickle_reference import pickled_global, resolve_pickled_global
from tests.profiler.funding_support import (
    CHAIN_HEAD,
    PRUNED_HISTORY_ERROR,
    TEST_SOURCE,
    TEST_WALLET,
    polygon_client_on,
    transfer_to,
)


def _legacy_classifier() -> Callable[[BaseException], bool]:
    """The classifier under its historical private ``funding`` name (not a public re-export)."""
    classifier: Callable[[BaseException], bool] = vars(funding)["_is_pruned_history_error"]
    return classifier


# The pinned classifier contract: an RPC error mentioning any of these means pruned history.
PRUNED_HISTORY_MARKERS = ("history has been pruned", "missing trie node", "older than")

RE_EXPORTED_NAMES = (
    "USDC_BRIDGED",
    "USDC_NATIVE",
    "TRANSFER_EVENT_SIGNATURE",
    "TRANSFER_EVENT_TOPIC",
    "DEFAULT_CHUNK_SIZE_BLOCKS",
    "DEFAULT_MAX_LOOKBACK_BLOCKS",
)


def _scanner(
    logs: TransferLogIndex, *, block_number: int = CHAIN_HEAD, **kwargs: Any
) -> TransferLogScanner:
    client = polygon_client_on(FakeEth(block_number=block_number, logs=logs))
    return TransferLogScanner(client, **kwargs)


class TestReExportIdentity:
    """The moved names stay importable from funding and keep their identity."""

    @pytest.mark.parametrize("name", RE_EXPORTED_NAMES)
    def test_constant_objects_are_shared(self, name: str) -> None:
        assert getattr(funding, name) is getattr(rpc, name)

    def test_classifier_object_is_shared(self) -> None:
        assert _legacy_classifier() is rpc.is_pruned_history_error

    def test_classifier_historical_module_identity(self) -> None:
        func = _legacy_classifier()
        assert func.__module__ == funding.__name__
        assert func.__qualname__ == "_is_pruned_history_error"

    def test_classifier_pickle_round_trip_by_reference(self) -> None:
        classifier = _legacy_classifier()
        assert pickled_global(classifier) == (funding.__name__, "_is_pruned_history_error")
        assert resolve_pickled_global(classifier) is rpc.is_pruned_history_error

    def test_transfer_topic_is_0x_prefixed_signature(self) -> None:
        expected = "0x" + funding.TRANSFER_EVENT_SIGNATURE.hex().removeprefix("0x")
        assert expected == funding.TRANSFER_EVENT_TOPIC

    def test_scanner_public_signature(self) -> None:
        signature = inspect.signature(TransferLogScanner.get_transfer_logs)
        assert list(signature.parameters) == [
            "self",
            "to_address",
            "token_address",
            "limit",
            "from_block",
            "to_block",
        ]
        assert signature.parameters["limit"].default == 10
        assert signature.parameters["from_block"].default == 0
        assert signature.parameters["to_block"].default == "latest"


class TestPrunedHistoryClassifier:
    """``is_pruned_history_error`` keeps its marker/substring semantics."""

    @pytest.mark.parametrize("marker", PRUNED_HISTORY_MARKERS)
    @pytest.mark.parametrize("cased", ["lower", "upper", "title"])
    def test_marker_anywhere_is_detected(self, marker: str, cased: str) -> None:
        text = getattr(marker, cased)()
        for prefix in ("", "rpc error: "):
            for suffix in ("", " (code -32701)"):
                assert rpc.is_pruned_history_error(RuntimeError(f"{prefix}{text}{suffix}"))

    def test_detection_matches_substring_rule(self) -> None:
        samples = (
            "",
            "x",
            "connection reset by peer",
            "429 Too Many Requests",
            "History has been pruned",
            "MISSING TRIE NODE",
            "older than 128 blocks",
            "pruned",
            "history",
            "node",
        )
        markers = PRUNED_HISTORY_MARKERS
        for sample in samples:
            expected = any(marker in sample.lower() for marker in markers)
            assert rpc.is_pruned_history_error(RuntimeError(sample)) is expected

    def test_accepts_any_base_exception(self) -> None:
        assert rpc.is_pruned_history_error(KeyboardInterrupt("History has been pruned"))


class TestTransferLogScanner:
    """The scanner walks ``eth_getLogs`` exactly as the tracer's method did."""

    async def test_formats_topics_and_checksums_address(self) -> None:
        logs = TransferLogIndex()
        scanner = _scanner(logs)

        await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=1,
            to_block=8_000,
        )

        query = logs.queries[0]
        assert query.topics[0] == funding.TRANSFER_EVENT_TOPIC
        assert query.topics[1] is None
        assert query.required_topic(2).endswith(TEST_WALLET.lower().replace("0x", ""))
        assert query.address == funding.USDC_BRIDGED

    async def test_chunks_large_ranges(self) -> None:
        logs = TransferLogIndex()
        scanner = _scanner(logs)

        await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=1_000_000,
            to_block=1_025_000,
        )

        assert [(q.from_block, q.to_block) for q in logs.queries] == [
            (1_000_000, 1_008_999),
            (1_009_000, 1_017_999),
            (1_018_000, 1_025_000),
        ]

    async def test_symbolic_from_block_scans_only_the_lookback_window(self) -> None:
        logs = TransferLogIndex()
        scanner = _scanner(logs, chunk_size_blocks=500, max_lookback_blocks=1_000)

        await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block="earliest",
            to_block=5_000,
        )

        assert [(q.from_block, q.to_block) for q in logs.queries] == [
            (4_000, 4_499),
            (4_500, 4_999),
            (5_000, 5_000),
        ]

    async def test_custom_chunk_size_is_respected(self) -> None:
        logs = TransferLogIndex()
        scanner = _scanner(logs, chunk_size_blocks=500)

        await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=1_000,
            to_block=1_999,
        )

        assert [(q.from_block, q.to_block) for q in logs.queries] == [
            (1_000, 1_499),
            (1_500, 1_999),
        ]

    async def test_stops_at_limit_mid_walk(self) -> None:
        logs = TransferLogIndex()
        for i in range(5):
            logs.add_transfer(
                funding.USDC_BRIDGED,
                transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_000_100 + i, seed=i),
            )
        scanner = _scanner(logs)

        result = await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            limit=2,
            from_block=1_000_000,
            to_block=1_027_000,
        )

        assert len(result) == 2
        assert len(logs.queries) == 1

    async def test_skips_flaky_chunk(self) -> None:
        logs = TransferLogIndex()
        logs.fail_chunk(1_000_000, RuntimeError("RPC hiccup"))
        good_log = transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_009_500, seed=0xAA)
        logs.add_transfer(funding.USDC_BRIDGED, good_log)
        scanner = _scanner(logs)

        result = await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=1_000_000,
            to_block=1_018_000,
        )

        assert result == [good_log]
        assert len(logs.queries) == 3

    async def test_breaks_on_pruned_history(self) -> None:
        logs = TransferLogIndex()
        good_log = transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_000_500, seed=0xAA)
        logs.add_transfer(funding.USDC_BRIDGED, good_log)
        logs.fail_chunk(1_009_000, PRUNED_HISTORY_ERROR)
        logs.add_transfer(
            funding.USDC_BRIDGED,
            transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_018_500, seed=0xBB),
        )
        scanner = _scanner(logs)

        result = await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=1_000_000,
            to_block=1_027_000,
        )

        assert result == [good_log]
        assert len(logs.queries) == 2

    async def test_resolves_latest_and_clamps_from_block_zero(self) -> None:
        logs = TransferLogIndex()
        scanner = _scanner(logs, block_number=5_000)

        await scanner.get_transfer_logs(to_address=TEST_WALLET, token_address=funding.USDC_BRIDGED)

        assert [(q.from_block, q.to_block) for q in logs.queries] == [(0, 5_000)]

    async def test_uses_fallback_provider_after_failover(self) -> None:
        primary_logs, fallback_logs = TransferLogIndex(), TransferLogIndex()
        primary = FakeEth(always_fail=True, logs=primary_logs)
        fallback = FakeEth(logs=fallback_logs)
        client = polygon_client_on(primary, fallback=fallback)
        await client.get_transaction_count(TEST_WALLET)
        scanner = TransferLogScanner(client)

        await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=1,
            to_block=8_000,
        )

        assert primary_logs.queries == []
        assert len(fallback_logs.queries) == 1

    async def test_empty_when_start_beyond_end(self) -> None:
        logs = TransferLogIndex()
        scanner = _scanner(logs)

        result = await scanner.get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=funding.USDC_BRIDGED,
            from_block=9_000,
            to_block=1_000,
        )

        assert result == []
        assert logs.queries == []


def _log(**overrides: object) -> dict[str, object]:
    log: dict[str, object] = {
        "topics": [b"\x01" * 32, b"\x02" * 32, b"\x03" * 32],
        "data": (7).to_bytes(32, "big"),
        "transactionHash": b"\xaa" * 32,
        "blockNumber": 123,
    }
    return {**log, **overrides}


def test_parse_transfer_log_reads_the_fields_the_tracer_needs() -> None:
    fields = rpc.parse_transfer_log(_log())

    assert fields == rpc.TransferLogFields(
        from_topic=b"\x02" * 32,
        to_topic=b"\x03" * 32,
        amount=7,
        block_number=123,
        tx_hash=b"\xaa" * 32,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"topics": None},
        {"topics": "0x0102"},
        {"topics": [b"\x01" * 32, b"\x02" * 32]},
        {"topics": [b"\x01" * 32, "0x02", b"\x03" * 32]},
        {"topics": [b"\x01" * 32, b"\x02" * 32, 3]},
        {"data": "0x07"},
        {"data": None},
        {"transactionHash": "0xaa"},
        {"blockNumber": "123"},
        {"blockNumber": None},
    ],
    ids=[
        "topics-missing",
        "topics-text",
        "topics-short",
        "sender-not-bytes",
        "recipient-not-bytes",
        "data-text",
        "data-missing",
        "tx-hash-text",
        "block-number-text",
        "block-number-missing",
    ],
)
def test_parse_transfer_log_rejects_malformed_shapes(override: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="Transfer log"):
        rpc.parse_transfer_log(_log(**override))
