"""Adversarial tests for centralized URL and error-text redaction.

Round-4 finding 6: the "redacted" configuration summary returned RPC and trades
URLs verbatim, the CLI printed the raw trades URL, and runtime health errors
exposed raw exception text. Secrets must not survive any of these surfaces:
userinfo passwords, lone userinfo tokens, and query-string values.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.ingestor.health import ComponentStatus, HealthMonitor
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.redaction import redact_text, redact_url

DB_SECRET = "db-userinfo-secret"
QUERY_SECRET = "query-string-secret"
TOKEN_SECRET = "lone-userinfo-token-secret"
FRESH_PASSWORD_SECRET = "fresh-userinfo-secret-r7"
FRESH_QUERY_SECRET = "fresh-query-secret-r7"
SIZE_TOKEN_SECRET = "size-lone-token-secret-r7"
SIZE_QUERY_SECRET = "size-query-secret-r7"
SIZE_FRAGMENT_SECRET = "size-fragment-secret-r7"
NESTED_PASSWORD_SECRET = "nested-userinfo-secret-r7"
NESTED_QUERY_SECRET = "nested-query-secret-r7"


def _detector_trade() -> TradeEvent:
    """A real trade value for exercising the detector-failure path."""
    return TradeEvent(
        trade_id="0x" + "e" * 64,
        wallet_address="0x" + "f" * 40,
        market_id="0x" + "d" * 64,
        asset_id="asset_redact_r7",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("5000"),
        timestamp=datetime.now(UTC),
    )


class TestRedactUrl:
    def test_password_is_masked_and_username_kept(self) -> None:
        redacted = redact_url(f"postgresql://tracker:{DB_SECRET}@localhost:5432/db")

        assert DB_SECRET not in redacted
        assert redacted == "postgresql://tracker:***@localhost:5432/db"

    def test_lone_userinfo_token_is_fully_masked(self) -> None:
        redacted = redact_url(f"https://{TOKEN_SECRET}@rpc.example.com/v1")

        assert TOKEN_SECRET not in redacted
        assert redacted == "https://***@rpc.example.com/v1"

    def test_every_query_value_is_masked_while_keys_stay_readable(self) -> None:
        redacted = redact_url(
            f"https://api.example.com/trades?apikey={QUERY_SECRET}&limit=100&token={QUERY_SECRET}"
        )

        assert QUERY_SECRET not in redacted
        assert "apikey=%2A%2A%2A" in redacted or "apikey=***" in redacted
        assert "limit" in redacted

    def test_credential_free_url_passes_through_unchanged(self) -> None:
        url = "https://data-api.polymarket.com/trades"
        assert redact_url(url) == url

    def test_fragment_is_masked(self) -> None:
        redacted = redact_url(f"https://example.com/path#{QUERY_SECRET}")
        assert QUERY_SECRET not in redacted

    def test_malformed_url_with_credentials_does_not_leak(self) -> None:
        malformed = f"wss://https://user:{DB_SECRET}@example.com/ws?key={QUERY_SECRET}"

        redacted = redact_url(malformed)

        assert DB_SECRET not in redacted
        assert QUERY_SECRET not in redacted

    def test_redaction_is_idempotent(self) -> None:
        once = redact_url(f"redis://:{DB_SECRET}@localhost:6379/0?password={QUERY_SECRET}")
        assert redact_url(once) == once


class TestRedactText:
    def test_url_inside_exception_text_is_redacted(self) -> None:
        message = (
            "connection to server failed for "
            f"postgresql://tracker:{DB_SECRET}@db.internal:5432/research?sslpassword={QUERY_SECRET}"
            " (timeout)"
        )

        redacted = redact_text(message)

        assert DB_SECRET not in redacted
        assert QUERY_SECRET not in redacted
        assert "connection to server failed" in redacted
        assert "(timeout)" in redacted

    def test_multiple_urls_are_each_redacted(self) -> None:
        message = (
            f"primary https://{TOKEN_SECRET}@rpc.example.com failed, fallback "
            f"https://user:{DB_SECRET}@fallback.example.com/v2?apikey={QUERY_SECRET} also failed"
        )

        redacted = redact_text(message)

        assert TOKEN_SECRET not in redacted
        assert DB_SECRET not in redacted
        assert QUERY_SECRET not in redacted

    def test_text_without_urls_is_unchanged(self) -> None:
        assert redact_text("plain failure with no URL") == "plain failure with no URL"


class TestHealthOutputRedaction:
    """Health responses are a sink: even a raw error string reaching them must not leak."""

    @pytest.mark.asyncio
    async def test_component_error_with_embedded_dsn_never_reaches_health_output(self) -> None:
        monitor = HealthMonitor()

        async def failing_checker() -> ComponentStatus:
            raise RuntimeError(
                f"connect failed for postgresql://tracker:{DB_SECRET}@db:5432/x?sslpassword={QUERY_SECRET}"
            )

        monitor.set_component_checker("database", failing_checker)
        monitor.set_last_error_provider(
            lambda: f"poll failed for https://{TOKEN_SECRET}@api.example.com?key={QUERY_SECRET}"
        )
        port = 19116
        await monitor.start()
        await monitor.start_http_server(port=port)
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://127.0.0.1:{port}/health") as resp,
            ):
                body = await resp.text()
        finally:
            await monitor.stop()

        assert DB_SECRET not in body
        assert QUERY_SECRET not in body
        assert TOKEN_SECRET not in body
        assert "database" in body

    @pytest.mark.asyncio
    async def test_component_status_error_string_is_redacted_in_serialization(self) -> None:
        status = ComponentStatus(
            status="down",
            last_error=f"redis://user:{DB_SECRET}@cache:6379/0 unreachable",
        )

        serialized = status.to_dict()

        assert DB_SECRET not in str(serialized)
        assert "unreachable" in str(serialized["last_error"])

    @pytest.mark.asyncio
    async def test_metrics_output_contains_no_secret_after_failing_checks(self) -> None:
        monitor = HealthMonitor()

        async def failing_checker() -> ComponentStatus:
            raise RuntimeError(f"https://user:{DB_SECRET}@example.com refused")

        monitor.set_component_checker("database", failing_checker)
        port = 19117
        await monitor.start()
        await monitor.start_http_server(port=port)
        try:
            async with aiohttp.ClientSession() as session:
                await session.get(f"http://127.0.0.1:{port}/health")
                async with session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                    metrics = await resp.text()
        finally:
            await monitor.stop()

        assert DB_SECRET not in metrics


class TestPipelineErrorCaptureRedaction:
    """Errors are also redacted where the pipeline records them, not only at sinks."""

    @pytest.mark.asyncio
    async def test_database_check_failure_embedding_dsn_is_redacted(self) -> None:
        from polymarket_insider_tracker.pipeline import Pipeline
        from tests.fakes import make_test_settings

        class LeakyDatabaseManager:
            def get_async_session(self) -> object:
                raise RuntimeError(
                    f"could not connect to postgresql://tracker:{DB_SECRET}@db:5432/research"
                )

        pipeline = Pipeline(make_test_settings())
        pipeline._db_manager = LeakyDatabaseManager()  # type: ignore[assignment]

        component = await pipeline._check_database()

        assert component.status == "down"
        assert DB_SECRET not in (component.last_error or "")
        assert "could not connect" in (component.last_error or "")

    @pytest.mark.asyncio
    async def test_trade_processing_error_with_url_is_redacted_in_stats(self) -> None:
        from polymarket_insider_tracker.pipeline import Pipeline
        from tests.fakes import make_test_settings

        pipeline = Pipeline(make_test_settings())
        pipeline._record_processing_error(
            f"scoring failed after calling https://{TOKEN_SECRET}@api.example.com?k={QUERY_SECRET}"
        )

        assert TOKEN_SECRET not in (pipeline.stats.last_error or "")
        assert QUERY_SECRET not in (pipeline.stats.last_error or "")
        assert pipeline.stats.errors == 1

    def test_worker_failure_error_is_redacted(self) -> None:
        from polymarket_insider_tracker.pipeline import Pipeline
        from tests.fakes import make_test_settings

        pipeline = Pipeline(make_test_settings())
        pipeline._handle_worker_failure(
            f"poller died talking to https://user:{DB_SECRET}@trades.example.com?auth={QUERY_SECRET}"
        )

        assert DB_SECRET not in (pipeline.stats.last_error or "")
        assert QUERY_SECRET not in (pipeline.stats.last_error or "")


class TestDetectorFailureRedaction:
    """Detector exceptions ride through one boundary before logs, errors, and health."""

    @staticmethod
    def _fresh_exception_texts() -> list[str]:
        return [
            (
                "fresh wallet rpc blew up for "
                f"https://operator:{FRESH_PASSWORD_SECRET}@rpc.example/v1"
                f"?apikey={FRESH_QUERY_SECRET} (connection reset)"
            ),
            (
                "fresh wallet nested-shape failure "
                f"wss://https://operator:{NESTED_PASSWORD_SECRET}@rpc.example/ws"
                f"?key={NESTED_QUERY_SECRET}"
            ),
        ]

    @staticmethod
    def _size_exception_texts() -> list[str]:
        return [
            (
                "size metadata blew up for "
                f"https://{SIZE_TOKEN_SECRET}@meta.example/v2?token={SIZE_QUERY_SECRET} "
                f"(see https://meta.example/help#{SIZE_FRAGMENT_SECRET})"
            ),
            (
                "size metadata nested-shape failure "
                f"wss://https://operator:{NESTED_PASSWORD_SECRET}@meta.example/ws"
                f"?key={NESTED_QUERY_SECRET}"
            ),
        ]

    @staticmethod
    def _secrets() -> list[str]:
        return [
            FRESH_PASSWORD_SECRET,
            FRESH_QUERY_SECRET,
            SIZE_TOKEN_SECRET,
            SIZE_QUERY_SECRET,
            SIZE_FRAGMENT_SECRET,
            NESTED_PASSWORD_SECRET,
            NESTED_QUERY_SECRET,
        ]

    def _assert_redacted(self, message: str, *, keep: str) -> None:
        for secret in self._secrets():
            assert secret not in message
        assert keep in message
        assert "***" in message

    @pytest.mark.asyncio
    async def test_fresh_wallet_failure_is_redacted_everywhere(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from tests.fakes import FailingDetector, FakeEth, make_test_settings, wire_pipeline

        secrets = self._secrets()
        for text in self._fresh_exception_texts():
            pipeline = await wire_pipeline(
                make_test_settings(), redis=FakeAsyncRedis(), eth=FakeEth()
            )
            pipeline._fresh_wallet_detector = FailingDetector(RuntimeError(text))
            with caplog.at_level(logging.WARNING):
                caplog.clear()
                _, error = await pipeline._detect_fresh_wallet(_detector_trade())

            assert error is not None
            self._assert_redacted(error, keep="fresh wallet detection failed")
            for secret in secrets:
                assert secret not in caplog.text
            assert "fresh wallet detection failed" in caplog.text

    @pytest.mark.asyncio
    async def test_size_anomaly_failure_is_redacted_everywhere(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from tests.fakes import FailingDetector, FakeEth, make_test_settings, wire_pipeline

        secrets = self._secrets()
        for text in self._size_exception_texts():
            pipeline = await wire_pipeline(
                make_test_settings(), redis=FakeAsyncRedis(), eth=FakeEth()
            )
            pipeline._size_anomaly_detector = FailingDetector(RuntimeError(text))
            with caplog.at_level(logging.WARNING):
                caplog.clear()
                _, error = await pipeline._detect_size_anomaly(_detector_trade())

            assert error is not None
            self._assert_redacted(error, keep="size anomaly detection failed")
            for secret in secrets:
                assert secret not in caplog.text
            assert "size anomaly detection failed" in caplog.text

    @pytest.mark.asyncio
    async def test_detector_failures_stay_redacted_through_stats_and_health(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.health import HealthStatus
        from tests.fakes import FailingDetector, FakeEth, make_test_settings, wire_pipeline

        pipeline = await wire_pipeline(make_test_settings(), redis=FakeAsyncRedis(), eth=FakeEth())
        pipeline._fresh_wallet_detector = FailingDetector(
            RuntimeError(self._fresh_exception_texts()[0])
        )
        pipeline._size_anomaly_detector = FailingDetector(
            RuntimeError(self._size_exception_texts()[0])
        )
        with caplog.at_level(logging.WARNING):
            await pipeline._on_trade(_detector_trade())

        last_error = pipeline.stats.last_error or ""
        for secret in self._secrets():
            assert secret not in last_error
            assert secret not in caplog.text
        assert "***" in last_error
        assert pipeline.stats.errors == 2

        body = pipeline.health_monitor._build_health_body(
            pipeline.health_monitor.get_health_report(), {}, HealthStatus.HEALTHY, 0.0
        )
        health_text = str(body)
        for secret in self._secrets():
            assert secret not in health_text
        assert "detection failed" in last_error
