"""Boundary fakes shared by the test suite.

Only external boundaries are faked here: alert delivery channels and webhook servers, the
py-clob-client SDK, the gamma-api statistics client, the Polygon JSON-RPC surface of web3, the
market-metadata lookup, and the public trades query. Redis is never hand-built; tests use ``fakeredis`` through the ``fake_redis`` fixture
in ``tests/conftest.py``.
"""

from tests.fakes.alerts import FakeAlertChannel, FakeWebhookServer
from tests.fakes.cancellation import resist_cancellation_for
from tests.fakes.clob import FakeBaseClobClient
from tests.fakes.gamma import FakeGammaClient
from tests.fakes.ingestion import (
    StubPoller,
    ingestion_component,
    publish_after_first_poll,
    run_until,
    stubbed_pipeline,
)
from tests.fakes.metadata import FakeMetadataSync
from tests.fakes.pipeline import (
    FailingDetector,
    Substitutes,
    WiredPipeline,
    detector_trade,
    make_test_settings,
    wire_pipeline,
)
from tests.fakes.trades import (
    FakeClock,
    FakeTradesServer,
    invalid_json,
    non_list_body,
    server_error,
    synthetic_wallet,
    terminal,
    throttled,
    timeout,
    trade_row,
)
from tests.fakes.trades_v2 import FakeV2TradesServer
from tests.fakes.v2_walk import CountingBudget, FakeCursorFeed, v2_envelope, v2_row
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth, TransferLogIndex, transfer_log

__all__ = [
    "FailingDetector",
    "FakeAlertChannel",
    "FakeAsyncWeb3",
    "FakeBaseClobClient",
    "FakeClock",
    "FakeCursorFeed",
    "FakeEth",
    "FakeGammaClient",
    "FakeMetadataSync",
    "FakeTradesServer",
    "FakeV2TradesServer",
    "FakeWebhookServer",
    "CountingBudget",
    "StubPoller",
    "Substitutes",
    "TransferLogIndex",
    "WiredPipeline",
    "detector_trade",
    "ingestion_component",
    "invalid_json",
    "make_test_settings",
    "non_list_body",
    "publish_after_first_poll",
    "resist_cancellation_for",
    "run_until",
    "server_error",
    "stubbed_pipeline",
    "synthetic_wallet",
    "terminal",
    "throttled",
    "timeout",
    "trade_row",
    "transfer_log",
    "v2_envelope",
    "v2_row",
    "wire_pipeline",
]
