"""Boundary fakes shared by the test suite.

Only external boundaries are faked here: alert delivery channels and webhook servers, the
py-clob-client SDK, the gamma-api statistics client, the Polygon JSON-RPC surface of web3, the
market-metadata lookup, and the public trades query. Redis is never hand-built; tests use ``fakeredis`` through the ``fake_redis`` fixture
in ``tests/conftest.py``.
"""

from tests.fakes.alerts import FakeAlertChannel, FakeWebhookServer
from tests.fakes.clob import FakeBaseClobClient
from tests.fakes.gamma import FakeGammaClient
from tests.fakes.metadata import FakeMetadataSync
from tests.fakes.pipeline import (
    BarrierDetector,
    FailingDetector,
    make_test_settings,
    metadata_state,
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
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth, TransferLogIndex, transfer_log

__all__ = [
    "BarrierDetector",
    "FailingDetector",
    "FakeAlertChannel",
    "FakeAsyncWeb3",
    "FakeBaseClobClient",
    "FakeClock",
    "FakeEth",
    "FakeGammaClient",
    "FakeMetadataSync",
    "FakeTradesServer",
    "FakeWebhookServer",
    "TransferLogIndex",
    "invalid_json",
    "make_test_settings",
    "metadata_state",
    "non_list_body",
    "server_error",
    "synthetic_wallet",
    "terminal",
    "throttled",
    "timeout",
    "trade_row",
    "transfer_log",
    "wire_pipeline",
]
