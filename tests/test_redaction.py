"""Adversarial tests for centralized URL and error-text redaction.

Round-4 finding 6: the "redacted" configuration summary returned RPC and trades
URLs verbatim, the CLI printed the raw trades URL, and runtime health errors
exposed raw exception text. Secrets must not survive any of these surfaces:
userinfo passwords, lone userinfo tokens, query-string values, fragments, and
(after the round-10 repair) credential-bearing endpoint path segments. The
round-10 repair additionally sanitizes every exception-bearing diagnostic at the
reachable profiler/detector sinks, so these tests drive real components with
narrow repository-failure injection and assert every captured log and exposed
error surface stays secret-free while trade IDs and non-secret diagnostics
remain.
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
PATH_SEGMENT_SECRET = "path-segment-secret-r10"
PATH_FALLBACK_SECRET = "fallback-path-secret-r10"
USERINFO_URL_SECRET = "explicit-userinfo-secret-r10"
QUERY_URL_SECRET = "explicit-query-secret-r10"
FRAGMENT_URL_SECRET = "explicit-fragment-secret-r10"
MALFORMED_NESTED_SECRET = "nested-path-secret-r10"
TRAILING_PAREN_FRAGMENT_SECRET = "trailing-paren-fragment-secret-r10"


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
        redacted = redact_url(f"postgresql://tracker:{DB_SECRET}@localhost:5432")

        assert DB_SECRET not in redacted
        assert redacted == "postgresql://tracker:***@localhost:5432"

    def test_lone_userinfo_token_is_fully_masked(self) -> None:
        redacted = redact_url(f"https://{TOKEN_SECRET}@rpc.example.com")

        assert TOKEN_SECRET not in redacted
        assert redacted == "https://***@rpc.example.com"

    def test_every_query_value_is_masked_while_keys_stay_readable(self) -> None:
        redacted = redact_url(
            f"https://api.example.com/trades?apikey={QUERY_SECRET}&limit=100&token={QUERY_SECRET}"
        )

        assert QUERY_SECRET not in redacted
        assert "apikey=%2A%2A%2A" in redacted or "apikey=***" in redacted
        assert "limit" in redacted

    def test_credential_free_url_passes_through_unchanged(self) -> None:
        url = "https://polygon-rpc.com"
        assert redact_url(url) == url

    def test_root_path_url_passes_through_unchanged(self) -> None:
        assert redact_url("https://polygon-rpc.com/") == "https://polygon-rpc.com/"

    def test_endpoint_path_segment_is_fail_closed_but_endpoint_stays_diagnosable(
        self,
    ) -> None:
        secret = "dedicated-provider-path-secret"
        redacted = redact_url(f"https://rpc.example/v2/{secret}")

        assert secret not in redacted
        assert "rpc.example" in redacted
        assert redacted == "https://rpc.example/***path***"

    def test_path_masking_survives_userinfo_query_and_fragment(self) -> None:
        redacted = redact_url(
            f"https://operator:{DB_SECRET}@rpc.example/v2/{TOKEN_SECRET}"
            f"?apikey={QUERY_SECRET}#{QUERY_SECRET}"
        )

        assert DB_SECRET not in redacted
        assert TOKEN_SECRET not in redacted
        assert QUERY_SECRET not in redacted
        assert "rpc.example" in redacted
        assert "***path***" in redacted

    def test_fragment_is_masked(self) -> None:
        redacted = redact_url(f"https://example.com#{QUERY_SECRET}")
        assert QUERY_SECRET not in redacted

    def test_malformed_url_with_credentials_does_not_leak(self) -> None:
        malformed = f"wss://https://user:{DB_SECRET}@example.com?key={QUERY_SECRET}"

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
            f"https://user:{DB_SECRET}@fallback.example.com?apikey={QUERY_SECRET} also failed"
        )

        redacted = redact_text(message)

        assert TOKEN_SECRET not in redacted
        assert DB_SECRET not in redacted
        assert QUERY_SECRET not in redacted

    def test_endpoint_path_credentials_are_redacted_while_host_survives(self) -> None:
        message = (
            f"primary blew up calling https://primary.example/v2/{PATH_SEGMENT_SECRET}"
            f" and fallback blew up calling https://fallback.example/v2/{PATH_FALLBACK_SECRET}"
            " (connection reset)"
        )

        redacted = redact_text(message)

        assert PATH_SEGMENT_SECRET not in redacted
        assert PATH_FALLBACK_SECRET not in redacted
        assert "primary.example" in redacted
        assert "fallback.example" in redacted
        assert "(connection reset)" in redacted

    def test_userinfo_query_fragment_and_malformed_nested_shapes_are_redacted(self) -> None:
        message = (
            f"poll failed for https://operator:{USERINFO_URL_SECRET}@rpc.example?apikey={QUERY_URL_SECRET}"
            f" (see https://meta.example#{FRAGMENT_URL_SECRET})"
            f" nested wss://https://operator:{MALFORMED_NESTED_SECRET}@meta.example"
        )

        redacted = redact_text(message)

        assert USERINFO_URL_SECRET not in redacted
        assert QUERY_URL_SECRET not in redacted
        assert FRAGMENT_URL_SECRET not in redacted
        assert MALFORMED_NESTED_SECRET not in redacted
        assert "rpc.example" in redacted
        assert "meta.example" in redacted

    def test_parenthesized_fragment_url_keeps_its_closing_delimiter(self) -> None:
        message = f"(see https://meta.example#{TRAILING_PAREN_FRAGMENT_SECRET}) trailing"

        redacted = redact_text(message)

        assert TRAILING_PAREN_FRAGMENT_SECRET not in redacted
        assert redacted.endswith(") trailing")
        assert "meta.example" in redacted

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


class TestUpstreamSinkRedaction:
    """Round-10: reachable profiler/detector sinks must sanitize before logging.

    Every case drives a real product component and injects the failure through a
    narrow repository fake (a failing Redis command, a raising ``eth`` method, a
    per-market metadata failure, or a credential-bearing RPC endpoint), then
    asserts every captured log line is secret-free while trade IDs and the
    non-secret diagnostic context survive.
    """

    @pytest.mark.asyncio
    async def test_analyzer_token_balance_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from web3.exceptions import Web3Exception

        from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
        from polymarket_insider_tracker.profiler.chain import PolygonClient
        from tests.fakes import FakeAsyncWeb3, FakeEth

        secret = "upstream-token-balance-secret-r10"

        class LeakyTokenEth(FakeEth):
            def contract(self, address: str, abi: object) -> object:
                _ = (address, abi)
                raise Web3Exception(f"balanceOf failed for https://rpc.internal?k={secret}")

        client = PolygonClient("https://polygon-rpc.com", retry_delay_seconds=0.0)
        client._w3 = FakeAsyncWeb3(LeakyTokenEth())
        analyzer = WalletAnalyzer(client)

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            profile = await analyzer.analyze("0x" + "ab" * 20, force_refresh=True)

        assert profile.usdc_balance == 0
        assert secret not in caplog.text
        assert "***" in caplog.text
        assert "Failed to get USDC balance" in caplog.text

    @pytest.mark.asyncio
    async def test_analyzer_profile_cache_failures_are_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
        from polymarket_insider_tracker.profiler.chain import PolygonClient
        from tests.fakes import FakeAsyncWeb3, FakeEth

        read_secret = "upstream-cache-read-secret-r10"
        write_secret = "upstream-cache-write-secret-r10"

        class LeakyGetRedis(FakeAsyncRedis):  # type: ignore[misc]
            async def get(self, *_args: object, **_kwargs: object) -> object:
                raise ConnectionError(
                    f"redis get blew up for https://cache.internal?k={read_secret}"
                )

        class LeakySetRedis(FakeAsyncRedis):  # type: ignore[misc]
            async def set(self, *_args: object, **_kwargs: object) -> object:
                raise ConnectionError(
                    f"redis set blew up for https://cache.internal?k={write_secret}"
                )

        read_client = PolygonClient("https://polygon-rpc.com", retry_delay_seconds=0.0)
        read_client._w3 = FakeAsyncWeb3(FakeEth())
        read_analyzer = WalletAnalyzer(read_client, redis=LeakyGetRedis())
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            profile = await read_analyzer.analyze("0x" + "ab" * 20)
        assert profile is not None
        assert read_secret not in caplog.text
        assert "Failed to get cached profile" in caplog.text

        write_client = PolygonClient(
            "https://polygon-rpc.com", redis=FakeAsyncRedis(), retry_delay_seconds=0.0
        )
        write_client._w3 = FakeAsyncWeb3(FakeEth())
        write_analyzer = WalletAnalyzer(write_client, redis=LeakySetRedis())
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            profile = await write_analyzer.analyze("0x" + "ab" * 20, force_refresh=True)
        assert profile is not None
        assert write_secret not in caplog.text
        assert "***" in caplog.text
        assert "Failed to cache profile" in caplog.text

    @pytest.mark.asyncio
    async def test_chain_cache_failures_are_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.profiler.chain import PolygonClient
        from tests.fakes import FakeAsyncWeb3, FakeEth

        get_secret = "upstream-chain-get-secret-r10"
        set_secret = "upstream-chain-set-secret-r10"

        class LeakyGetRedis(FakeAsyncRedis):  # type: ignore[misc]
            async def get(self, *_args: object, **_kwargs: object) -> object:
                raise ConnectionError(
                    f"redis get blew up for https://cache.internal?k={get_secret}"
                )

        class LeakySetRedis(FakeAsyncRedis):  # type: ignore[misc]
            async def set(self, *_args: object, **_kwargs: object) -> object:
                raise ConnectionError(
                    f"redis set blew up for https://cache.internal?k={set_secret}"
                )

        get_client = PolygonClient("https://polygon-rpc.com", redis=LeakyGetRedis())
        get_client._w3 = FakeAsyncWeb3(FakeEth())
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            assert await get_client.get_transaction_count("0x" + "ab" * 20) == 42
        assert get_secret not in caplog.text
        assert "Cache get failed" in caplog.text

        set_client = PolygonClient("https://polygon-rpc.com", redis=LeakySetRedis())
        set_client._w3 = FakeAsyncWeb3(FakeEth())
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            assert await set_client.get_transaction_count("0x" + "ab" * 20) == 42
        assert set_secret not in caplog.text
        assert "***" in caplog.text
        assert "Cache set failed" in caplog.text

    @pytest.mark.asyncio
    async def test_chain_retry_warnings_are_redacted_and_retries_survive(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from web3.exceptions import Web3Exception

        from polymarket_insider_tracker.profiler.chain import PolygonClient
        from tests.fakes import FakeAsyncWeb3, FakeEth

        secret = "upstream-retry-secret-r10"

        class LeakyRetryEth(FakeEth):
            async def get_transaction_count(self, address: str) -> int:
                _ = address
                raise Web3Exception(f"node refused https://rpc.internal?k={secret}")

        client = PolygonClient("https://polygon-rpc.com", max_retries=2, retry_delay_seconds=0.0)
        client._w3 = FakeAsyncWeb3(LeakyRetryEth())
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with pytest.raises(Exception, match="RPC call get_transaction_count failed"):
                await client.get_transaction_count("0x" + "ab" * 20)

        assert secret not in caplog.text
        assert caplog.text.count("failed (attempt") == 2
        assert "***" in caplog.text

    @pytest.mark.asyncio
    async def test_size_metadata_failure_is_redacted_with_fallback_intact(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
        from tests.fakes.metadata import FakeMetadataSync

        secret = "upstream-metadata-secret-r10"
        trade = _detector_trade()
        sync = FakeMetadataSync()
        sync.failures[trade.market_id] = RuntimeError(
            f"meta blew up for https://meta.internal?k={secret}"
        )
        detector = SizeAnomalyDetector(sync)

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            signal = await detector.analyze(trade)

        assert secret not in caplog.text
        assert "***" in caplog.text
        assert "Failed to get metadata" in caplog.text
        assert trade.market_id in caplog.text
        assert signal is not None

    @pytest.mark.asyncio
    async def test_detector_batch_handlers_are_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from web3.exceptions import Web3Exception

        from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
        from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
        from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
        from polymarket_insider_tracker.profiler.chain import PolygonClient
        from tests.fakes import FakeAsyncWeb3, FakeEth
        from tests.fakes.metadata import FakeMetadataSync

        fresh_secret = "upstream-fresh-batch-secret-r10"
        size_secret = "upstream-size-batch-secret-r10"

        class LeakyTokenEth(FakeEth):
            def contract(self, address: str, abi: object) -> object:
                _ = (address, abi)
                raise Web3Exception(f"balanceOf failed for https://rpc.internal?k={fresh_secret}")

        client = PolygonClient("https://polygon-rpc.com", retry_delay_seconds=0.0)
        client._w3 = FakeAsyncWeb3(LeakyTokenEth())
        fresh_detector = FreshWalletDetector(WalletAnalyzer(client))
        trade = _detector_trade()
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            signals = await fresh_detector.analyze_batch([trade])
        assert signals == []
        assert fresh_secret not in caplog.text
        assert trade.wallet_address[:10] in caplog.text
        assert "Failed to get USDC balance" in caplog.text

        failing_sync = FakeMetadataSync()
        failing_sync.failures[trade.market_id] = RuntimeError(
            f"meta blew up for https://meta.internal?k={size_secret}"
        )
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            signals = await SizeAnomalyDetector(failing_sync).analyze_batch([trade])
        assert len(signals) == 1
        assert size_secret not in caplog.text
        assert "***" in caplog.text


class TestDetectorFailureRedaction:
    """Detector exceptions ride through one boundary before logs, errors, and health."""

    @staticmethod
    def _fresh_exception_texts() -> list[str]:
        return [
            (
                "fresh wallet rpc blew up for "
                f"https://operator:{FRESH_PASSWORD_SECRET}@rpc.example"
                f"?apikey={FRESH_QUERY_SECRET} (connection reset)"
            ),
            (
                "fresh wallet path-credential failure "
                f"https://primary.example/v2/{PATH_SEGMENT_SECRET} (connection reset)"
            ),
        ]

    @staticmethod
    def _size_exception_texts() -> list[str]:
        return [
            (
                "size metadata blew up for "
                f"https://{SIZE_TOKEN_SECRET}@meta.example?token={SIZE_QUERY_SECRET} "
                f"(see https://meta.example#{SIZE_FRAGMENT_SECRET}) (connection reset)"
            ),
            (
                "size metadata fallback path-credential failure "
                f"https://fallback.example/v2/{PATH_FALLBACK_SECRET} (connection reset)"
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
            PATH_SEGMENT_SECRET,
            PATH_FALLBACK_SECRET,
        ]

    def _assert_redacted(self, message: str, *, keep: str) -> None:
        for secret in self._secrets():
            assert secret not in message
        assert keep in message
        assert "***" in message

    def _assert_context_preserved(
        self, *, error: str, trade: TradeEvent, keep: str, logs: str
    ) -> None:
        self._assert_redacted(error, keep=keep)
        assert trade.trade_id in error
        assert "connection reset" in error
        assert trade.trade_id in logs
        assert keep in logs

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
            trade = _detector_trade()
            with caplog.at_level(logging.WARNING):
                caplog.clear()
                _, error = await pipeline._detect_fresh_wallet(trade)

            assert error is not None
            self._assert_context_preserved(
                error=error,
                trade=trade,
                keep="fresh wallet detection failed",
                logs=caplog.text,
            )
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
            trade = _detector_trade()
            with caplog.at_level(logging.WARNING):
                caplog.clear()
                _, error = await pipeline._detect_size_anomaly(trade)

            assert error is not None
            self._assert_context_preserved(
                error=error,
                trade=trade,
                keep="size anomaly detection failed",
                logs=caplog.text,
            )
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
