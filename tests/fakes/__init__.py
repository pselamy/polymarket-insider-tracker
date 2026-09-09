"""Test fakes and doubles for polymarket-insider-tracker."""

from tests.fakes.metadata import FakeMetadataSync
from tests.fakes.pipeline import (
    BrokenDatabaseManager,
    ErrorDetector,
    FakeAlertDispatcher,
    FakeAlertFormatter,
    FakeFreshWalletDetector,
    FakeFundingTracer,
    FakeRiskScorer,
    FakeSizeAnomalyDetector,
    SlowDetector,
    make_test_settings,
)
from tests.fakes.redis import FakePipeline, FakeRedis
from tests.fakes.web3 import FakeAsyncWeb3, FakeContract, FakeEth

__all__ = [
    "BrokenDatabaseManager",
    "ErrorDetector",
    "FakeAlertDispatcher",
    "FakeAlertFormatter",
    "FakeAsyncWeb3",
    "FakeContract",
    "FakeEth",
    "FakeFreshWalletDetector",
    "FakeFundingTracer",
    "FakeMetadataSync",
    "FakePipeline",
    "FakeRedis",
    "FakeRiskScorer",
    "FakeSizeAnomalyDetector",
    "SlowDetector",
    "make_test_settings",
]
