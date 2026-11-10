"""FreshWalletDetector constructor contract: config defaults, the exclusive fresh_transaction_boundary
and its deprecated max_nonce alias (contracts/detector-api.md section 1).

Split from test_fresh_wallet.py so the analysis tests stay in one file under the size budget.
"""

from __future__ import annotations

import warnings
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from polymarket_insider_tracker.detector.evaluation import WalletClassification
from polymarket_insider_tracker.detector.fresh_wallet import (
    DEFAULT_FRESH_TRANSACTION_BOUNDARY,
    DEFAULT_MAX_AGE_HOURS,
    DEFAULT_MIN_TRADE_SIZE,
    FreshWalletDetector,
)
from tests.detector.test_fresh_wallet import (
    FakeWalletAnalyzer,
    create_trade_event,
    create_wallet_profile,
)


@pytest.fixture
def fake_wallet_analyzer() -> FakeWalletAnalyzer:
    """Create a fake WalletAnalyzer."""
    return FakeWalletAnalyzer()


def _construct_with_deprecation(
    analyzer: FakeWalletAnalyzer, **kwargs: Any
) -> tuple[FreshWalletDetector, list[warnings.WarningMessage]]:
    """Build a detector from deprecated arguments; return it with the warnings it raised."""
    with pytest.deprecated_call() as recorded:
        detector = FreshWalletDetector(analyzer, **kwargs)
    return detector, list(recorded)


class TestFreshWalletDetectorInit:
    """Tests for FreshWalletDetector initialization."""

    @pytest.mark.parametrize(
        ("attribute", "expected"),
        [
            ("min_trade_size", DEFAULT_MIN_TRADE_SIZE),
            ("fresh_transaction_boundary", DEFAULT_FRESH_TRANSACTION_BOUNDARY),
            ("max_age_hours", DEFAULT_MAX_AGE_HOURS),
        ],
        ids=["min_trade_size", "fresh_transaction_boundary", "max_age_hours"],
    )
    def test_default_config(
        self, fake_wallet_analyzer: FakeWalletAnalyzer, attribute: str, expected: object
    ) -> None:
        """A detector built with no overrides uses each module default."""
        # Arrange
        detector = FreshWalletDetector(fake_wallet_analyzer)

        # Act
        actual = getattr(detector, attribute)

        # Assert
        assert actual == expected

    @pytest.mark.parametrize(
        ("attribute", "value"),
        [
            ("min_trade_size", Decimal("5000")),
            ("fresh_transaction_boundary", 11),
            ("max_age_hours", 72.0),
        ],
        ids=["min_trade_size", "fresh_transaction_boundary", "max_age_hours"],
    )
    def test_custom_config(
        self, fake_wallet_analyzer: FakeWalletAnalyzer, attribute: str, value: object
    ) -> None:
        """A detector accepts each custom config value."""
        # Arrange
        kwargs: dict[str, Any] = {attribute: value}
        detector = FreshWalletDetector(fake_wallet_analyzer, **kwargs)

        # Act
        actual = getattr(detector, attribute)

        # Assert
        assert actual == value


class TestFreshTransactionBoundaryConstructorContract:
    """Table-driven test suite for contracts/detector-api.md section 1."""

    def test_omitted_omitted_defaults(self, fake_wallet_analyzer: FakeWalletAnalyzer) -> None:
        detector = FreshWalletDetector(fake_wallet_analyzer)
        assert detector.fresh_transaction_boundary == 5

    @pytest.mark.asyncio
    async def test_omitted_omitted_qualifies_below_boundary(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        detector = FreshWalletDetector(fake_wallet_analyzer)
        fake_wallet_analyzer.register(create_wallet_profile(address="0x4", nonce=4, age_hours=24.0))
        trade4 = create_trade_event(wallet_address="0x4", size=Decimal("3000"))
        assert (await detector.analyze(trade4)) is not None

    @pytest.mark.asyncio
    async def test_omitted_omitted_rejects_at_boundary(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        detector = FreshWalletDetector(fake_wallet_analyzer)
        fake_wallet_analyzer.register(create_wallet_profile(address="0x5", nonce=5, age_hours=24.0))
        trade5 = create_trade_event(wallet_address="0x5", size=Decimal("3000"))
        assert (await detector.analyze(trade5)) is None

    @pytest.mark.asyncio
    async def test_transaction_override_3_3_classifies_known_nonqualifying(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """transaction-override-3-3: fresh_transaction_boundary=3 with nonce=3 does not qualify."""
        # Arrange
        detector = FreshWalletDetector(fake_wallet_analyzer, fresh_transaction_boundary=3)
        fake_wallet_analyzer.register(create_wallet_profile(nonce=3, age_hours=24.0))
        trade = create_trade_event(size=Decimal("3000"))

        # Act
        eval_res = await detector.evaluate(trade, evaluated_at=datetime(2026, 11, 9, tzinfo=UTC))

        # Assert
        assert eval_res.classification == WalletClassification.KNOWN_NONQUALIFYING

    @pytest.mark.asyncio
    async def test_transaction_override_3_3_emits_no_signal(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """transaction-override-3-3: the non-qualifying wallet produces no signal."""
        # Arrange
        detector = FreshWalletDetector(fake_wallet_analyzer, fresh_transaction_boundary=3)
        fake_wallet_analyzer.register(create_wallet_profile(nonce=3, age_hours=24.0))
        trade = create_trade_event(size=Decimal("3000"))

        # Act
        eval_res = await detector.evaluate(trade, evaluated_at=datetime(2026, 11, 9, tzinfo=UTC))

        # Assert
        assert eval_res.signal is None

    @pytest.mark.parametrize(
        ("age_hours", "expected_class"),
        [
            (24.0, WalletClassification.FRESH),
            (None, WalletClassification.LOW_ACTIVITY_UNKNOWN_AGE),
        ],
        ids=["known_age", "unknown_age"],
    )
    @pytest.mark.asyncio
    async def test_transaction_override_10_9_classifies_as_qualifying(
        self,
        fake_wallet_analyzer: FakeWalletAnalyzer,
        age_hours: float | None,
        expected_class: WalletClassification,
    ) -> None:
        """transaction-override-10-9: fresh_transaction_boundary=10 with nonce=9 qualifies."""
        # Arrange
        detector = FreshWalletDetector(fake_wallet_analyzer, fresh_transaction_boundary=10)
        fake_wallet_analyzer.register(create_wallet_profile(nonce=9, age_hours=age_hours))
        trade = create_trade_event(size=Decimal("3000"))

        # Act
        eval_res = await detector.evaluate(trade, evaluated_at=datetime(2026, 11, 9, tzinfo=UTC))

        # Assert
        assert eval_res.classification == expected_class

    @pytest.mark.parametrize(
        "age_hours",
        [24.0, None],
        ids=["known_age", "unknown_age"],
    )
    @pytest.mark.asyncio
    async def test_transaction_override_10_9_emits_a_signal(
        self, fake_wallet_analyzer: FakeWalletAnalyzer, age_hours: float | None
    ) -> None:
        """transaction-override-10-9: the qualifying wallet produces a signal."""
        # Arrange
        detector = FreshWalletDetector(fake_wallet_analyzer, fresh_transaction_boundary=10)
        fake_wallet_analyzer.register(create_wallet_profile(nonce=9, age_hours=age_hours))
        trade = create_trade_event(size=Decimal("3000"))

        # Act
        eval_res = await detector.evaluate(trade, evaluated_at=datetime(2026, 11, 9, tzinfo=UTC))

        # Assert
        assert eval_res.signal is not None

    def test_max_nonce_alone_sets_boundary_to_plus_one(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """Legacy max_nonce=5 alone maps to the exclusive boundary 6."""
        # Arrange and Act
        detector, _ = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)

        # Assert
        assert detector.fresh_transaction_boundary == 6

    def test_max_nonce_alone_warns_exactly_once(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """The deprecated max_nonce argument raises one warning."""
        # Arrange and Act
        _, records = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)

        # Assert
        assert len(records) == 1

    def test_max_nonce_alone_warning_points_at_the_caller(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """The warning is attributed to the file that passed max_nonce."""
        # Arrange and Act
        _, records = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)

        # Assert
        assert records[0].filename == __file__

    def test_max_nonce_alone_warning_names_the_parameter(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """The warning names the replacement parameter."""
        # Arrange and Act
        _, records = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)

        # Assert
        assert "fresh_transaction_boundary" in str(records[0].message)

    def test_max_nonce_alone_warning_names_the_new_boundary(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """The warning states the boundary the value maps to."""
        # Arrange and Act
        _, records = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)

        # Assert
        assert "6" in str(records[0].message)

    @pytest.mark.asyncio
    async def test_max_nonce_alone_qualifies_at_five(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """Legacy max_nonce=5 qualifies a wallet at nonce 5."""
        # Arrange
        detector, _ = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)
        fake_wallet_analyzer.register(create_wallet_profile(address="0x5", nonce=5, age_hours=24.0))
        trade5 = create_trade_event(wallet_address="0x5", size=Decimal("3000"))

        # Act
        result = await detector.analyze(trade5)

        # Assert
        assert result is not None

    @pytest.mark.asyncio
    async def test_max_nonce_alone_does_not_qualify_at_six(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """Legacy max_nonce=5 does not qualify a wallet at nonce 6."""
        # Arrange
        detector, _ = _construct_with_deprecation(fake_wallet_analyzer, max_nonce=5)
        fake_wallet_analyzer.register(create_wallet_profile(address="0x6", nonce=6, age_hours=24.0))
        trade6 = create_trade_event(wallet_address="0x6", size=Decimal("3000"))

        # Act
        result = await detector.analyze(trade6)

        # Assert
        assert result is None

    def test_both_given_matching_pair_sets_the_boundary(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """A matching max_nonce and fresh_transaction_boundary pair sets boundary 6."""
        # Arrange and Act
        detector, _ = _construct_with_deprecation(
            fake_wallet_analyzer, max_nonce=5, fresh_transaction_boundary=6
        )

        # Assert
        assert detector.fresh_transaction_boundary == 6

    def test_both_given_matching_pair_warns_exactly_once(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """A matching pair still raises one deprecation warning."""
        # Arrange and Act
        _, records = _construct_with_deprecation(
            fake_wallet_analyzer, max_nonce=5, fresh_transaction_boundary=6
        )

        # Assert
        assert len(records) == 1

    def test_both_given_matching_pair_warning_names_the_parameter(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """The matching-pair warning names fresh_transaction_boundary."""
        # Arrange and Act
        _, records = _construct_with_deprecation(
            fake_wallet_analyzer, max_nonce=5, fresh_transaction_boundary=6
        )

        # Assert
        assert "fresh_transaction_boundary" in str(records[0].message)

    def test_both_given_matching_pair_warning_names_the_boundary(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        """The matching-pair warning states the boundary."""
        # Arrange and Act
        _, records = _construct_with_deprecation(
            fake_wallet_analyzer, max_nonce=5, fresh_transaction_boundary=6
        )

        # Assert
        assert "6" in str(records[0].message)

    def test_both_given_conflicting_pair_raises_value_error(
        self, fake_wallet_analyzer: FakeWalletAnalyzer
    ) -> None:
        with pytest.raises(ValueError, match="Conflicting parameters"):
            FreshWalletDetector(fake_wallet_analyzer, max_nonce=5, fresh_transaction_boundary=5)

    @pytest.mark.parametrize(
        ("invalid_kwargs", "error_match"),
        [
            ({"max_nonce": -1}, "max_nonce must be a non-negative integer"),
            ({"max_nonce": True}, "max_nonce must be an integer, not boolean"),
            ({"max_nonce": 2**53 - 1}, "exceeds"),
            (
                {"fresh_transaction_boundary": False},
                "fresh_transaction_boundary must be an integer, not boolean",
            ),
            ({"fresh_transaction_boundary": 0}, "fresh_transaction_boundary must be in"),
            ({"fresh_transaction_boundary": -5}, "fresh_transaction_boundary must be in"),
            ({"fresh_transaction_boundary": 2**53}, "fresh_transaction_boundary must be in"),
        ],
    )
    def test_invalid_constructor_inputs(
        self,
        fake_wallet_analyzer: FakeWalletAnalyzer,
        invalid_kwargs: dict[str, Any],
        error_match: str,
    ) -> None:
        with pytest.raises(ValueError, match=error_match):
            FreshWalletDetector(fake_wallet_analyzer, **invalid_kwargs)
