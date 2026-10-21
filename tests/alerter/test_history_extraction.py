"""Seam and contract tests for the extracted alert-history modules.

After the line-count extraction, ``alerter/history.py`` re-exports the record
type and pure helpers from :mod:`...alerter._history_records` and binds the
channel-scoped delivery helpers from :mod:`...alerter._channel_delivery` onto
:class:`AlertHistory`. These tests pin:

- identity: the historical import paths and the private modules resolve to the
  same objects (so monkeypatch seams and pickle references keep working),
- the pure dedup-key helper's format and hour-scoping contract,
- the extracted channel-delivery coroutines' observable Redis behaviour,
- ``AlertRecord`` pickles by reference to the historical module path and carries its fields.
"""

import copy
import dataclasses
import re
from datetime import UTC, datetime, timedelta

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter import _channel_delivery, _history_records, history
from polymarket_insider_tracker.alerter.history import (
    AlertHistory,
    AlertRecord,
    _generate_dedup_key,
    _get_signals_from_assessment,
)
from tests.pickle_reference import pickled_global, resolve_pickled_global

DEDUP_KEY_PATTERN = re.compile(r"^(?P<wallet>[^:]+):(?P<market>[^:]+):(?P<hour>\d{10})$")


def _sample_record() -> AlertRecord:
    return AlertRecord(
        alert_id="alert-1",
        wallet_address="0xwallet",
        market_id="market_1",
        risk_score=0.75,
        signals_triggered=["fresh_wallet"],
        channels_attempted=["discord", "telegram"],
        channels_succeeded=["discord"],
        dedup_key="0xwallet:market_1:2026010416",
    )


class TestImportIdentity:
    """Original-path imports and private modules are the same objects."""

    def test_record_type_is_the_extracted_object(self) -> None:
        assert history.AlertRecord is _history_records.AlertRecord
        assert AlertRecord is _history_records.AlertRecord

    def test_pure_helpers_are_the_extracted_objects(self) -> None:
        assert history._generate_dedup_key is _history_records.generate_dedup_key
        assert history._get_signals_from_assessment is _history_records.get_signals_from_assessment
        assert _generate_dedup_key is _history_records.generate_dedup_key
        assert _get_signals_from_assessment is _history_records.get_signals_from_assessment

    def test_channel_helpers_are_the_extracted_objects(self) -> None:
        bindings = {
            "is_channel_suppressed": "is_channel_suppressed",
            "is_channel_delivered": "is_channel_delivered",
            "record_channel_delivery": "record_channel_delivery",
            "claim_channel_send": "claim_channel_send",
            "release_channel_claim": "release_channel_claim",
            "_compare_and_delete": "compare_and_delete",
            "extend_channel_claim": "extend_channel_claim",
            "_compare_and_expire": "compare_and_expire",
            "record_channel_ambiguous": "record_channel_ambiguous",
        }
        for method_name, function_name in bindings.items():
            assert getattr(AlertHistory, method_name) is getattr(_channel_delivery, function_name)

    def test_record_type_keeps_historical_module_identity(self) -> None:
        expected_module = "polymarket_insider_tracker.alerter.history"
        assert AlertRecord.__module__ == expected_module
        assert AlertRecord.__qualname__ == "AlertRecord"
        assert _generate_dedup_key.__module__ == expected_module
        assert _generate_dedup_key.__qualname__ == "_generate_dedup_key"
        assert _get_signals_from_assessment.__module__ == expected_module
        assert _get_signals_from_assessment.__qualname__ == "_get_signals_from_assessment"

    def test_alert_record_pickle_round_trip(self) -> None:
        record = _sample_record()
        module = "polymarket_insider_tracker.alerter.history"
        assert pickled_global(record) == (module, "AlertRecord")
        assert resolve_pickled_global(record) is AlertRecord
        reduced = record.__reduce_ex__(4)
        assert isinstance(reduced, tuple)
        state = reduced[2]
        assert state == {f.name: getattr(record, f.name) for f in dataclasses.fields(record)}
        assert copy.deepcopy(record) == record

    def test_pure_helpers_pickle_round_trip_through_historical_name(self) -> None:
        module = "polymarket_insider_tracker.alerter.history"
        for helper in (_generate_dedup_key, _get_signals_from_assessment):
            assert pickled_global(helper) == (module, helper.__qualname__)
            assert resolve_pickled_global(helper) is helper


class TestGenerateDedupKeyProperty:
    """Property-style coverage of the pure dedup-key helper.

    A Hypothesis dependency is not part of the project's locked dev extras and
    adding one would change ``pyproject.toml``/``uv.lock`` outside this
    extraction's scope, so the property is exercised over an explicit grid of
    wallets, markets and hour offsets instead.
    """

    WALLETS = ("0x0", "0xAbCdEf", "0x" + "f" * 40)
    MARKETS = ("m", "market_1", "0xcondition-id")

    @pytest.mark.parametrize("wallet", WALLETS)
    @pytest.mark.parametrize("market", MARKETS)
    @pytest.mark.parametrize("hour_offset", range(0, 26))
    def test_key_shape_and_field_separation(
        self, wallet: str, market: str, hour_offset: int
    ) -> None:
        hour = datetime(2026, 1, 4, 0, 0, tzinfo=UTC)
        when = hour + timedelta(hours=hour_offset)
        key = _generate_dedup_key(wallet, market, when)

        match = DEDUP_KEY_PATTERN.match(key)
        assert match is not None
        assert match.group("wallet") == wallet
        assert match.group("market") == market
        assert match.group("hour") == when.strftime("%Y%m%d%H")

    def test_minute_second_microsecond_are_irrelevant(self) -> None:
        early = datetime(2026, 1, 4, 16, 0, 0, 0, tzinfo=UTC)
        late = datetime(2026, 1, 4, 16, 59, 59, 999999, tzinfo=UTC)
        assert _generate_dedup_key("0xw", "m", early) == _generate_dedup_key("0xw", "m", late)

    def test_adjacent_hours_differ(self) -> None:
        first = datetime(2026, 1, 4, 16, 0, tzinfo=UTC)
        second = datetime(2026, 1, 4, 17, 0, tzinfo=UTC)
        assert _generate_dedup_key("0xw", "m", first) != _generate_dedup_key("0xw", "m", second)

    def test_generation_is_deterministic(self) -> None:
        when = datetime(2026, 1, 4, 16, 30, tzinfo=UTC)
        assert _generate_dedup_key("0xw", "m", when) == _generate_dedup_key("0xw", "m", when)


class TestChannelDeliveryModule:
    """Direct behaviour of the extracted channel-delivery coroutines."""

    @pytest.mark.asyncio
    async def test_record_then_suppressed_as_duplicate(self, fake_redis: FakeAsyncRedis) -> None:
        history_instance = AlertHistory(fake_redis)

        assert await _channel_delivery.is_channel_suppressed(
            history_instance, "discord", "0xWallet", "m1"
        ) == (False, None)

        await _channel_delivery.record_channel_delivery(
            history_instance, "discord", "0xWallet", "m1"
        )

        assert await _channel_delivery.is_channel_suppressed(
            history_instance, "discord", "0xWallet", "m1"
        ) == (True, "duplicate")
        assert await _channel_delivery.is_channel_delivered(
            history_instance, "discord", "0xWallet", "m1"
        )

    @pytest.mark.asyncio
    async def test_ambiguous_window_reported(self, fake_redis: FakeAsyncRedis) -> None:
        history_instance = AlertHistory(fake_redis)
        await _channel_delivery.record_channel_ambiguous(history_instance, "telegram", "0xw", "m2")

        assert await _channel_delivery.is_channel_suppressed(
            history_instance, "telegram", "0xw", "m2"
        ) == (True, "ambiguous_timeout")

    @pytest.mark.asyncio
    async def test_claim_is_single_flight_and_owner_scoped(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        history_instance = AlertHistory(fake_redis)

        token = await _channel_delivery.claim_channel_send(history_instance, "discord", "0xw", "m3")
        assert isinstance(token, str) and token

        assert (
            await _channel_delivery.claim_channel_send(history_instance, "discord", "0xw", "m3")
            is None
        )

        assert (
            await _channel_delivery.release_channel_claim(
                history_instance, "discord", "0xw", "m3", "not-the-owner"
            )
            is False
        )
        assert (
            await _channel_delivery.release_channel_claim(
                history_instance, "discord", "0xw", "m3", token
            )
            is True
        )
        assert (
            await _channel_delivery.claim_channel_send(history_instance, "discord", "0xw", "m3")
            is not None
        )

    @pytest.mark.asyncio
    async def test_extend_only_renews_current_owner(self, fake_redis: FakeAsyncRedis) -> None:
        history_instance = AlertHistory(fake_redis)
        token = await _channel_delivery.claim_channel_send(history_instance, "discord", "0xw", "m4")
        assert token is not None

        assert (
            await _channel_delivery.extend_channel_claim(
                history_instance, "discord", "0xw", "m4", "stale", 120
            )
            is False
        )
        assert (
            await _channel_delivery.extend_channel_claim(
                history_instance, "discord", "0xw", "m4", token, 120
            )
            is True
        )
