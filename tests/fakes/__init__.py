"""Boundary fakes shared by the test suite.

Only external boundaries are faked here: alert delivery channels and webhook servers, the
py-clob-client SDK, the Polygon JSON-RPC surface of web3, and the market-metadata lookup. Redis is
never hand-built; tests use ``fakeredis`` through the ``fake_redis`` fixture in ``tests/conftest.py``.
"""

from tests.fakes.alerts import FakeAlertChannel, FakeWebhookServer
from tests.fakes.clob import FakeBaseClobClient
from tests.fakes.metadata import FakeMetadataSync
from tests.fakes.pipeline import (
    BarrierDetector,
    FailingDetector,
    make_test_settings,
    wire_pipeline,
)
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth, TransferLogIndex, transfer_log

__all__ = [
    "BarrierDetector",
    "FailingDetector",
    "FakeAlertChannel",
    "FakeAsyncWeb3",
    "FakeBaseClobClient",
    "FakeEth",
    "FakeMetadataSync",
    "FakeWebhookServer",
    "TransferLogIndex",
    "make_test_settings",
    "transfer_log",
    "wire_pipeline",
]
