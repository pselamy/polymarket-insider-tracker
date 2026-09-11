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
from polymarket_insider_tracker.ingestor.models import (
    Market,
    MarketMetadata,
    TradeEvent,
)
from polymarket_insider_tracker.ingestor.trade_rows import (
    OutcomeResolution,
    TradeObservation,
)
from polymarket_insider_tracker.ingestor.websocket import TradeStreamHandler
from polymarket_insider_tracker.redaction import (
    redact_exception_message,
    redact_text,
    redact_url,
)

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
TRADE_PATH_SECRET = "trades-path-secret-r12"
TRADE_QUERY_SECRET = "trades-query-secret-r12"
METADATA_CACHE_SECRET = "metadata-cache-secret-r12"
METADATA_FETCH_SECRET = "metadata-fetch-secret-r12"
METADATA_STARTUP_SECRET = "metadata-startup-secret-r12"
PUBLISHER_SECRET = "publisher-deserialize-secret-r12"
CLOB_SECRET = "clob-retry-secret-r12"
DISPATCHER_SECRET = "dispatcher-claim-secret-r12"
WEBSOCKET_SECRET = "websocket-connect-secret-r12"
MAIN_SECRET = "main-startup-secret-r12"
POLLER_CALLBACK_SECRET = "poller-callback-secret-r12"
POLLER_REPAIR_SECRET = "poller-repair-secret-r12"
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

    def test_bare_query_token_is_masked_fail_closed(self) -> None:
        secret = "bare-query-token-secret-r12"
        redacted = redact_url(f"https://host?{secret}")

        assert secret not in redacted
        assert redacted == "https://host?***"

    def test_valueless_query_pairs_are_masked_fail_closed(self) -> None:
        secret = "valueless-pair-secret-r12"
        redacted = redact_url(f"https://host?a=1&{secret}&b=2")

        assert secret not in redacted
        assert "a=***" in redacted
        assert "b=***" in redacted


class TestAmbiguousUrlShapes:
    """Round-12 (Fable N3/N5): ``@``-in-path and nested-scheme stay fail-closed."""

    @pytest.mark.parametrize(
        ("url", "must_keep"),
        [
            ("https://rpc.example/v2/KEY@prod", "rpc.example"),
            ("https://h/a@b@c", "https://h/"),
            ("https://[2001:db8::1]:8443/v2/K@prod?a=1#f", "[2001:db8::1]:8443"),
            ("https://user:pw@h:8443/v2/K@prod?k=v#f", "h:8443"),
            ("https://h/v2/KEY@prod?k=v", "https://h/"),
            ("https://h/v2/KEY@prod#f", "https://h/"),
            ("https://h/a@b@c?k=v#f", "https://h/"),
            ("https://h/v2/KEY@prod,", "https://h/"),
            ("https://h/v2/KEY@prod.", "https://h/"),
        ],
    )
    def test_at_in_path_shapes_never_emit_the_path(self, url: str, must_keep: str) -> None:
        redacted = redact_url(url)

        assert "KEY@prod" not in redacted
        assert "a@b@c" not in redacted
        assert "K@prod" not in redacted
        assert must_keep in redacted
        assert "***" in redacted

    @pytest.mark.parametrize(
        "url",
        [
            "wss://https://user:PW@meta.example/v2/KEY",
            "wss://https://user:PW@meta.example/v2/KEY?k=V#f",
            "wss://https://user:PW@meta.example/v2/KEY,",
            "wss://https://user:nested-user-secret-r12@meta.example/v2/nested-path-secret-r12",
        ],
    )
    def test_nested_scheme_shapes_mask_userinfo_and_path(self, url: str) -> None:
        redacted = redact_url(url)

        assert "PW" not in redacted.replace("***", "") or "user:PW@" not in redacted
        assert "user:PW@" not in redacted
        assert "/v2/KEY" not in redacted
        assert "nested-user-secret-r12" not in redacted
        assert "nested-path-secret-r12" not in redacted
        # Nested/ambiguous shapes fail closed: no inner host, path, query, or
        # fragment is re-emitted; only the safe placeholder survives.
        assert "meta.example" not in redacted
        assert "?" not in redacted
        assert "#" not in redacted
        assert redact_url(redacted) == redacted
        assert redact_text(redacted) == redacted
        assert "***" in redacted

    def test_fable_exact_probes_in_text(self) -> None:
        message = (
            "x https://rpc.example/v2/KEY@prod y " "(wss://https://user:PW@meta.example/v2/KEY) z"
        )

        redacted = redact_text(message)

        assert "KEY@prod" not in redacted
        assert "/v2/KEY" not in redacted
        assert "user:PW@" not in redacted
        assert "rpc.example" in redacted
        # The nested shape fails closed without re-emitting its inner host.
        assert "meta.example" not in redacted
        assert redacted.endswith(") z")

    def test_fragment_trailing_punctuation_does_not_reemit_secret(self) -> None:
        secret = "fragment-tail-secret-r12"
        for trail in (".", ",", ")", "]", "!"):
            redacted = redact_text(f"(see https://meta.example#{secret}{trail}) end")

            assert secret not in redacted
            assert "meta.example" in redacted
            round_tripped = redact_url(redacted.split("(see ")[1].split(") end")[0].rstrip(".,)!]"))
            assert round_tripped == redact_text(round_tripped)
            assert secret not in round_tripped
            assert "meta.example" in round_tripped

    @pytest.mark.parametrize(
        "url",
        [
            "https://proxy.example/https://inner.example/v2/NESTED_PATH_SECRET_R15@prod",
            "wss://https://h/v2/PATH_SECRET_R14",
            "https://[::1/v2/PATH_SECRET_R14",
            "https://user:pw@h/p@th#FRAG_SECRET_R15",
            "https://host?%26=%3D",
            "https://host?%3F=%23",
            "https://h/v2/KEY@prod,",
            "https://h/v2/KEY@prod.",
            "https://user:pw@[2001:db8::1]:8443/v2/K",
            "https://[2001:db8::1]:8443/v2/K",
            "https://host:PORTSHAPEDTOKEN_R16/v2/x",
            "https://host:99999/v2/x",
            "redis://host:PORTSHAPEDTOKEN_R16/0",
            "https://user:pw@host:PORT_EXEMPT_SECRET_R18/v2/x",
            "https://key@host:PORT_EXEMPT_SECRET_R18/",
            "https://[::1]:PORT_EXEMPT_SECRET_R18/p",
            "https://user@[::1]:PORT_EXEMPT_SECRET_R18/p",
        ],
    )
    def test_governed_corpus_is_fail_closed_and_idempotent(self, url: str) -> None:
        once = redact_url(url)
        twice = redact_url(once)
        composed = redact_text(once)

        for token in (
            "PATH_SECRET_R14",
            "PATH_SECRET_R15",
            "FRAG_SECRET_R15",
            "NESTED_PATH_SECRET_R15",
            "PORTSHAPEDTOKEN_R16",
            "PORT_EXEMPT_SECRET_R18",
            "99999",
            "KEY@prod",
            "#FRAG",
        ):
            assert token not in once
        assert twice == once
        assert composed == once

    @pytest.mark.asyncio
    async def test_runtime_url_untouched_only_diagnostics_change(self) -> None:
        import httpx

        from polymarket_insider_tracker.ingestor.trades_source import TradesSourceClient

        secret_url = "https://proxy.example/v2/at-path-secret-r12/trades"
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(200, json=[])

        client = TradesSourceClient(
            httpx.AsyncClient(transport=httpx.MockTransport(handler)),
            url=secret_url,
            coverage="all",
        )
        page = await client.fetch_primary(boundary_time=None, horizon_seconds=600)

        assert page.http_status == 200
        assert seen and "at-path-secret-r12" in seen[0]


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
        # The ordinary fragment URL keeps its host readable while its secret
        # is masked; the malformed nested shape fails closed without its
        # inner host or userinfo.
        assert "https://meta.example#***" in redacted
        assert "operator:explicit-userinfo" not in redacted
        assert "nested-path-secret-r10" not in redacted

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
        pipeline.__dict__["_db_manager"] = LeakyDatabaseManager()

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

        read_redis = FakeAsyncRedis()

        async def leaky_read_get(name: object) -> object:
            _ = name
            raise ConnectionError(f"redis get blew up for https://cache.internal?k={read_secret}")

        read_redis.__dict__["get"] = leaky_read_get

        write_redis = FakeAsyncRedis()

        async def leaky_write_set(name: object, value: object = None, ex: object = None) -> object:
            _ = (name, value, ex)
            raise ConnectionError(f"redis set blew up for https://cache.internal?k={write_secret}")

        write_redis.__dict__["set"] = leaky_write_set

        read_client = PolygonClient("https://polygon-rpc.com", retry_delay_seconds=0.0)
        read_client._w3 = FakeAsyncWeb3(FakeEth())
        read_analyzer = WalletAnalyzer(read_client, redis=read_redis)
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
        write_analyzer = WalletAnalyzer(write_client, redis=write_redis)
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

        get_redis = FakeAsyncRedis()

        async def leaky_chain_get(name: object) -> object:
            _ = name
            raise ConnectionError(f"redis get blew up for https://cache.internal?k={get_secret}")

        get_redis.__dict__["get"] = leaky_chain_get

        set_redis = FakeAsyncRedis()

        async def leaky_chain_set(name: object, value: object = None, ex: object = None) -> object:
            _ = (name, value, ex)
            raise ConnectionError(f"redis set blew up for https://cache.internal?k={set_secret}")

        set_redis.__dict__["set"] = leaky_chain_set

        get_client = PolygonClient("https://polygon-rpc.com", redis=get_redis)
        get_client._w3 = FakeAsyncWeb3(FakeEth())
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            assert await get_client.get_transaction_count("0x" + "ab" * 20) == 42
        assert get_secret not in caplog.text
        assert "Cache get failed" in caplog.text

        set_client = PolygonClient("https://polygon-rpc.com", redis=set_redis)
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
            with pytest.raises(Exception, match="RPC call get_transaction_count failed") as excinfo:
                await client.get_transaction_count("0x" + "ab" * 20)

        assert secret not in caplog.text
        assert caplog.text.count("failed (attempt") == 2
        assert "***" in caplog.text
        # Round-18: the raised message itself is composed redacted, not only
        # the log sites that happen to render it.
        assert secret not in str(excinfo.value)
        assert "***" in str(excinfo.value)

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


class TestTradesBoundaryRedaction:
    """Round-12 N1: a path-credential trades URL never survives poller logs or errors.

    The real ``Settings`` value flows into the real ``TradesSourceClient`` error
    construction and the real poller ``_degrade``/``_fail`` logging path; only
    the HTTP transport is a narrow explicit fake (``FakeTradesServer`` faults).
    """

    @pytest.mark.asyncio
    async def test_terminal_path_credential_failure_is_redacted_everywhere(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
        from tests.fakes import (
            FakeClock,
            FakeEth,
            FakeTradesServer,
            make_test_settings,
            terminal,
            trade_row,
            wire_pipeline,
        )

        secret_url = f"https://proxy.example/v2/{TRADE_PATH_SECRET}/trades"
        settings = make_test_settings(trades_url=secret_url)
        assert settings.polymarket.trades_url == secret_url
        server = FakeTradesServer(clock=FakeClock(1_788_983_720.0))
        pipeline = await wire_pipeline(
            settings, redis=FakeAsyncRedis(), eth=FakeEth(), trades=server
        )
        poller = pipeline._trade_poller
        assert poller is not None
        server.publish(trade_row(timestamp=1_788_983_719, transaction=1))
        await poller.run_cycle()
        assert poller.status.state is not None
        server.fail_next(terminal(403))

        with caplog.at_level(logging.ERROR):
            caplog.clear()
            await poller.run_cycle()
            clock_advance = poller.status

        assert clock_advance.state is IngestionState.FAILED
        last_error = clock_advance.last_error or ""
        assert TRADE_PATH_SECRET not in last_error
        assert TRADE_PATH_SECRET not in caplog.text
        assert "HTTP 403" in last_error
        assert "proxy.example" in last_error
        assert "***path***" in last_error

    @pytest.mark.asyncio
    async def test_transient_path_credential_failure_degrades_without_leak(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
        from tests.fakes import (
            FakeClock,
            FakeEth,
            FakeTradesServer,
            make_test_settings,
            server_error,
            trade_row,
            wire_pipeline,
        )

        secret_url = f"https://proxy.example/v2/{TRADE_PATH_SECRET}/trades"
        settings = make_test_settings(trades_url=secret_url)
        clock = FakeClock(1_788_983_720.0)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            settings, redis=FakeAsyncRedis(), eth=FakeEth(), trades=server, poll_clock=clock
        )
        poller = pipeline._trade_poller
        assert poller is not None
        server.publish(trade_row(timestamp=1_788_983_719, transaction=1))
        await poller.run_cycle()
        server.fail_next(server_error(503), times=5)

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            await poller.run_cycle()

        degraded = poller.status
        assert degraded.state is IngestionState.DEGRADED
        assert degraded.consecutive_failures == 1
        assert degraded.counts["retries"] == 4
        last_error = degraded.last_error or ""
        assert TRADE_PATH_SECRET not in last_error
        assert TRADE_PATH_SECRET not in caplog.text
        assert "HTTP 503" in last_error
        assert "proxy.example" in last_error

    def test_source_error_construction_masks_path_credential(self) -> None:
        from polymarket_insider_tracker.ingestor.trades_source import redacted_url

        url = f"https://proxy.example/v2/{TRADE_PATH_SECRET}/trades?k={TRADE_QUERY_SECRET}"

        redacted = redacted_url(url)

        assert TRADE_PATH_SECRET not in redacted
        assert TRADE_QUERY_SECRET not in redacted
        assert "proxy.example" in redacted
        assert redacted == "https://proxy.example/***path***?k=***"

    @pytest.mark.asyncio
    async def test_nested_path_credential_failure_is_redacted_everywhere(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Round-15 GPT-6 finding 2: the nested configured URL through real paths."""
        import httpx

        from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
        from polymarket_insider_tracker.ingestor.trades_source import TradesSourceClient
        from polymarket_insider_tracker.redaction import redact_text as central_redact
        from tests.fakes import (
            FakeClock,
            FakeEth,
            FakeTradesServer,
            make_test_settings,
            non_list_body,
            server_error,
            terminal,
            trade_row,
            wire_pipeline,
        )

        nested_url = "https://proxy.example/https://inner.example/v2/NESTED_PATH_SECRET_R15@prod"
        settings = make_test_settings(trades_url=nested_url)
        assert settings.polymarket.trades_url == nested_url

        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(403, text="forbidden")

        source = TradesSourceClient(
            httpx.AsyncClient(transport=httpx.MockTransport(handler)),
            url=nested_url,
            coverage="all",
            clock=FakeClock(1_788_983_720.0),
            sleeper=FakeClock(1_788_983_720.0).sleep,
            random_source=lambda: 0.0,
        )
        with pytest.raises(Exception, match="HTTP 403"):
            await source.fetch_primary(boundary_time=None, horizon_seconds=600)
        assert "NESTED_PATH_SECRET_R15" not in central_redact(nested_url)
        assert seen and "NESTED_PATH_SECRET_R15" in seen[0]

        clock = FakeClock(1_788_983_720.0)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            settings, redis=FakeAsyncRedis(), eth=FakeEth(), trades=server, poll_clock=clock
        )
        poller = pipeline._trade_poller
        assert poller is not None
        server.publish(trade_row(timestamp=1_788_983_719, transaction=1))
        await poller.run_cycle()

        for fault, level in (
            (terminal(403), logging.ERROR),
            (server_error(503), logging.WARNING),
            (non_list_body(), logging.ERROR),
        ):
            server.fail_next(fault, times=5)
            with caplog.at_level(level):
                caplog.clear()
                await poller.run_cycle()
            last_error = poller.status.last_error or ""
            assert "NESTED_PATH_SECRET_R15" not in last_error
            assert "NESTED_PATH_SECRET_R15" not in caplog.text
            assert "inner.example" not in last_error
            assert "proxy.example" in last_error
        assert poller.status.state in (IngestionState.DEGRADED, IngestionState.FAILED)


class TestInvalidPortShapeRedaction:
    """Round-16 N-R16-1: a ``host:token`` typo never survives config output, logs, or status.

    The real product chain is exercised: ``redact_url`` label, the poller
    ``redact_error``/``_degrade`` log-and-status path over a real ``TradePoller``
    wired with ``FakeAsyncRedis``, and real ``Settings`` validation rejection.
    """

    PORTSHAPED_SECRET = "PORTSHAPEDTOKEN_R16_PROBE"

    def test_invalid_port_label_is_fail_closed_and_idempotent(self) -> None:
        url = f"https://host:{self.PORTSHAPED_SECRET}/v2/x"

        once = redact_url(url)

        assert self.PORTSHAPED_SECRET not in once
        assert "host" in once
        assert "***" in once
        assert redact_url(once) == once
        assert redact_text(once) == once

    def test_malformed_and_encoded_values_are_fail_closed(self) -> None:
        label = redact_text("https://host:%50%4F%52%54%54%4F%4B%45%4E/v2/x")

        assert label == "https://host/***path***"
        assert redact_url(label) == label
        assert redact_text(label) == label

    def test_settings_reject_port_shaped_credential_without_echo(self) -> None:
        from pydantic import ValidationError

        from polymarket_insider_tracker.config import PolymarketSettings

        with pytest.raises(ValidationError) as exc_info:
            PolymarketSettings(POLYMARKET_TRADES_URL=f"https://host:{self.PORTSHAPED_SECRET}/v2/x")

        assert self.PORTSHAPED_SECRET not in str(exc_info.value)
        assert "port" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_invalid_port_error_text_cannot_leak_through_poller(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.trade_poller import (
            IngestionState,
            redact_error,
        )
        from tests.fakes import (
            FakeClock,
            FakeEth,
            FakeTradesServer,
            make_test_settings,
            trade_row,
            wire_pipeline,
        )

        invalid_port_text = f"Invalid port: '{self.PORTSHAPED_SECRET}'"
        assert self.PORTSHAPED_SECRET not in redact_error(invalid_port_text)
        assert self.PORTSHAPED_SECRET not in redact_text(redact_error(invalid_port_text))

        # Round-18: the upstream token is repr-rendered, so it may switch
        # quote style or carry whitespace; the mask cannot depend on a
        # single-quoted, whitespace-free token shape.
        for probe in (
            f'Invalid port: "quoted\'{self.PORTSHAPED_SECRET}"',
            f"Invalid port: 'spaced {self.PORTSHAPED_SECRET}'",
        ):
            masked = redact_error(probe)
            assert self.PORTSHAPED_SECRET not in masked
            assert "invalid request port: '***'" in masked

        settings = make_test_settings(trades_url="https://trades.invalid/trades")
        clock = FakeClock(1_788_983_720.0)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            settings, redis=FakeAsyncRedis(), eth=FakeEth(), trades=server, poll_clock=clock
        )
        poller = pipeline._trade_poller
        assert poller is not None
        server.publish(trade_row(timestamp=1_788_983_719, transaction=1))
        await poller.run_cycle()

        poller.__dict__["_client"].__dict__["_url"] = f"https://host:{self.PORTSHAPED_SECRET}/v2/x"
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            await poller.run_cycle()

        last_error = poller.status.last_error or ""
        assert self.PORTSHAPED_SECRET not in last_error
        assert self.PORTSHAPED_SECRET not in caplog.text
        assert poller.status.state is IngestionState.DEGRADED


class TestHandlerBoundaryRedaction:
    """Round-18: the traceback scrub guards the console handler, not one logger.

    ``run_pipeline`` attached ``_RedactingLogFilter`` to the ``__main__``
    logger only; logger-level filters never run for records propagated from
    other loggers, so a sibling module's ``exc_info`` record reached the
    console handler with its raw exception chain. ``configure_logging`` now
    installs the filter on the handler itself, covering every logger that
    propagates to it with one shared fail-closed boundary.
    """

    SIBLING_SECRET = "SIBLING_LOGGER_SECRET_R18"

    def test_sibling_logger_exc_info_is_scrubbed_at_the_console_handler(self) -> None:
        import io
        import logging as stdlib_logging

        from polymarket_insider_tracker.__main__ import _RedactingLogFilter, configure_logging

        configure_logging("INFO")
        console = next(
            handler
            for handler in stdlib_logging.getLogger().handlers
            if isinstance(handler, stdlib_logging.StreamHandler)
        )
        assert any(isinstance(f, _RedactingLogFilter) for f in console.filters)

        capture = io.StringIO()
        previous = console.setStream(capture)
        try:
            sibling = stdlib_logging.getLogger("polymarket_insider_tracker.ingestor.health")
            try:
                raise RuntimeError(
                    f"connect failed for postgresql://tracker:{self.SIBLING_SECRET}@db:5432/x"
                )
            except RuntimeError:
                sibling.error(
                    "health check blew up: %s",
                    f"see https://key@host:{self.SIBLING_SECRET}/v2/k",
                    exc_info=True,
                )
            sibling.info("Health HTTP server started on port %d", 19118)
        finally:
            if previous is not None:
                console.setStream(previous)

        output = capture.getvalue()
        assert self.SIBLING_SECRET not in output
        assert "health check blew up" in output
        assert "RuntimeError" in output
        # Numeric arguments survive the boundary so %d formatting still renders.
        assert "started on port 19118" in output


class TestPortExemptionRemoval:
    """Round-18: userinfo/bracket netlocs are no longer exempt from the port probe.

    Under the round-16 exemption, a credential-shaped token in the port
    position re-emitted verbatim whenever the netloc also carried userinfo or
    an IPv6 bracket (``https://key@host:TOKEN`` masked to ``***@host:TOKEN``).
    Every such shape now fails closed while valid ports stay readable.
    """

    PORT_SECRET = "PORT_EXEMPT_SECRET_R18"

    @pytest.mark.parametrize(
        "url",
        [
            "https://user:pw@host:PORT_EXEMPT_SECRET_R18/x",
            "https://key@host:PORT_EXEMPT_SECRET_R18/",
            "https://[::1]:PORT_EXEMPT_SECRET_R18/path",
            "https://user@[::1]:PORT_EXEMPT_SECRET_R18/p",
            "https://user:pw@host:PORT_EXEMPT_SECRET_R18/a@b",
            "redis://user@host:PORT_EXEMPT_SECRET_R18/0",
            "https://user:pw@host:99999/x",
        ],
    )
    def test_port_position_token_never_survives_any_netloc_shape(self, url: str) -> None:
        once = redact_url(url)

        assert self.PORT_SECRET not in once
        assert "99999" not in once
        assert ":pw@" not in once
        assert redact_url(once) == once
        assert redact_text(once) == once
        assert "***" in once

    def test_port_position_token_never_survives_inside_text(self) -> None:
        message = (
            f"dial failed for https://key@host:{self.PORT_SECRET}/ then "
            f"wss://user:pw@[::1]:{self.PORT_SECRET}/ws (refused)"
        )

        redacted = redact_text(message)

        assert self.PORT_SECRET not in redacted
        assert "dial failed for" in redacted
        assert "(refused)" in redacted

    @pytest.mark.parametrize(
        ("url", "must_keep"),
        [
            ("https://user:pw@host:8443/x", "host:8443"),
            ("https://user@[2001:db8::1]:8443/x", "[2001:db8::1]:8443"),
            ("redis://:pw@cache:6379/0", "cache:6379"),
        ],
    )
    def test_valid_ports_stay_readable_through_the_netloc_mask(
        self, url: str, must_keep: str
    ) -> None:
        redacted = redact_url(url)

        assert must_keep in redacted
        assert ":pw@" not in redacted


class TestNonStringExceptionArgRedaction:
    """Round-16 N-R16-2: container/bytes exception args cannot bypass the traceback filter.

    The real ``_RedactingLogFilter`` plus the production formatter render the
    boundary-injected chain; dict/list/bytes payloads become the placeholder
    while string messages keep their redacted URL structure.
    """

    NONSTR_SECRET = "NONSTRING_ARG_SECRET_R16"

    def _rendered(self, exc: BaseException) -> str:
        import logging as stdlib_logging

        from polymarket_insider_tracker.__main__ import _RedactingLogFilter

        record = stdlib_logging.LogRecord(
            name="probe",
            level=stdlib_logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Pipeline failed: %s",
            args=("boom",),
            exc_info=(type(exc), exc, None),
        )
        assert _RedactingLogFilter().filter(record) is True
        return stdlib_logging.Formatter("%(message)s %(exc_text)s").format(record)

    def test_container_and_bytes_args_become_the_placeholder(self) -> None:
        exc = RuntimeError(
            {"url": f"https://x.internal?k={self.NONSTR_SECRET}"},
            [f"https://y.internal/v2/{self.NONSTR_SECRET}"],
            f"prefix-{self.NONSTR_SECRET}-suffix".encode(),
        )
        exc.add_note(f"note https://n.internal?k={self.NONSTR_SECRET}")
        exc.__dict__.setdefault("__notes__", list(exc.__notes__)).append(
            {"nested": self.NONSTR_SECRET}
        )

        rendered = self._rendered(exc)

        assert self.NONSTR_SECRET not in rendered
        assert rendered.count("***") >= 4
        assert "RuntimeError" in rendered

    def test_full_exception_graph_stays_redacted(self) -> None:
        inner = ValueError(f"inner https://i.internal/v2/{self.NONSTR_SECRET}")
        middle = RuntimeError({"mid": self.NONSTR_SECRET})
        middle.__cause__ = inner
        outer = RuntimeError(f"outer https://o.internal?k={self.NONSTR_SECRET}")
        outer.__cause__ = middle
        outer.__context__ = TypeError((f"https://c.internal/{self.NONSTR_SECRET}",))
        outer.__dict__.setdefault("__notes__", []).append(b"bytes-note")

        rendered = self._rendered(outer)

        assert self.NONSTR_SECRET not in rendered
        assert "ValueError" in rendered
        assert "RuntimeError" in rendered


class TestExceptionMessageRedaction:
    """Round-18 N-R18-2: rendering an exception message cannot leak non-string args.

    ``str(exc)`` interpolates raw arguments before any log filter or
    text-level redaction can see them, so every message-line and status sink
    composes through ``redact_exception_message`` instead.
    """

    MSG_SECRET = "nonstring-message-secret-r18"

    def test_bytes_argument_never_renders(self) -> None:
        exc = RuntimeError(f"prefix-{self.MSG_SECRET}-suffix".encode())

        rendered = redact_exception_message(exc)

        assert self.MSG_SECRET not in rendered
        assert rendered == "***"

    def test_container_arguments_never_render_but_safe_context_survives(self) -> None:
        exc = RuntimeError(
            {"url": f"https://x.internal?k={self.MSG_SECRET}"},
            "context text",
            [f"https://y.internal/v2/{self.MSG_SECRET}"],
        )

        rendered = redact_exception_message(exc)

        assert self.MSG_SECRET not in rendered
        assert "***" in rendered
        assert "context text" in rendered

    def test_string_message_keeps_its_redacted_text(self) -> None:
        exc = RuntimeError(f"connect failed for https://key@host/{self.MSG_SECRET}")

        rendered = redact_exception_message(exc)

        assert self.MSG_SECRET not in rendered
        assert "connect failed for" in rendered
        assert rendered == redact_text(str(exc))

    def test_safe_scalars_render_through_custom_str(self) -> None:
        exc = OSError(2, "No such file or directory")

        rendered = redact_exception_message(exc)

        assert rendered == redact_text(str(exc))
        assert "No such file or directory" in rendered

    def test_no_argument_exception_renders_empty(self) -> None:
        assert redact_exception_message(RuntimeError()) == ""

    def test_rendering_is_idempotent_under_text_redaction(self) -> None:
        exc = RuntimeError(f"prefix-{self.MSG_SECRET}-suffix".encode(), "context")

        rendered = redact_exception_message(exc)

        assert redact_text(rendered) == rendered


class TestAdjacentSinkRedaction:
    """Round-12 N2/N4: every production-reachable raw-exception sink sanitizes.

    Each case drives a real component with a narrow explicit fake whose failure
    embeds a synthetic credential-bearing URL, then asserts every captured log
    line and stored ``last_error`` is secret-free while identifiers and the
    non-secret diagnostic context survive.
    """

    @pytest.mark.asyncio
    async def test_metadata_sync_cache_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
        from tests.fakes import FakeGammaClient
        from tests.ingestor.test_metadata_sync import FakeClobClient

        real = FakeAsyncRedis()

        async def leaky_setex(
            name: bytes | str,
            ttl: object,
            value: object,
        ) -> bool:
            _ = (name, ttl, value)
            raise ConnectionError(
                f"redis setex blew up for https://cache.internal?k={METADATA_CACHE_SECRET}"
            )

        real.__dict__["setex"] = leaky_setex
        sync = MarketMetadataSync(
            redis=real,
            clob_client=FakeClobClient([]),
            gamma_client=FakeGammaClient(),
        )
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            cached, _ = await sync._cache_markets_batch(
                _sync_probe_markets(), await sync._fetch_gamma_stats()
            )
            assert cached == 0
            # The batch path swallows the per-market failure into a log line;
            # the direct path raises the same error. Exercise both real paths.
            with pytest.raises(ConnectionError):
                await sync._cache_market(_sync_probe_metadata())

        assert METADATA_CACHE_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Failed to cache market sync-probe-cond" in caplog.text
        assert sync.stats.last_error is None

    @pytest.mark.asyncio
    async def test_metadata_sync_start_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.metadata_sync import (
            MarketMetadataSync,
            MetadataSyncError,
        )
        from tests.fakes import FakeGammaClient
        from tests.ingestor.test_metadata_sync import FakeClobClient

        failing = FakeClobClient(
            raise_error=RuntimeError(
                f"CLOB blew up for https://meta.internal?k={METADATA_STARTUP_SECRET}"
            )
        )
        sync = MarketMetadataSync(
            redis=FakeAsyncRedis(),
            clob_client=failing,
            gamma_client=FakeGammaClient(),
        )
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            with pytest.raises(MetadataSyncError, match="initial sync failed"):
                await sync.start()

        assert METADATA_STARTUP_SECRET not in caplog.text
        assert METADATA_STARTUP_SECRET not in str(sync.stats.last_error or "")
        assert "Initial sync failed" in caplog.text
        assert sync.stats.last_error is not None
        assert "***" in sync.stats.last_error

    @pytest.mark.asyncio
    async def test_metadata_get_market_fetch_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
        from tests.fakes import FakeGammaClient
        from tests.ingestor.test_metadata_sync import FakeClobClient

        failing = FakeClobClient(
            raise_error=RuntimeError(
                f"fetch blew up for https://meta.internal?k={METADATA_FETCH_SECRET}"
            )
        )
        sync = MarketMetadataSync(
            redis=FakeAsyncRedis(),
            clob_client=failing,
            gamma_client=FakeGammaClient(),
        )

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            assert await sync.get_market("cond123") is None

        assert METADATA_FETCH_SECRET not in caplog.text
        assert "cond123" in caplog.text
        assert "Failed to fetch market" in caplog.text

    def test_publisher_deserialize_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from polymarket_insider_tracker.ingestor.publisher import _parse_stream_entry

        secret = f"https://x.internal?k={PUBLISHER_SECRET}"
        bad_payload: dict[bytes | str, bytes | str] = {
            "market_id": "mkt-publisher-probe",
            "trade_id": "trade-publisher-probe",
            "wallet_address": "0x" + "a" * 40,
            "side": "BUY",
            "outcome": "Yes",
            # The outcome index echoes its raw value into the int() error text,
            # so the credential-bearing URL reaches the real production log sink.
            "outcome_index": secret,
            "price": "0.5",
            "size": "1",
            "timestamp": datetime.now(UTC).isoformat(),
        }
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            entry = _parse_stream_entry(
                "1-1",
                bad_payload,
                context="replay",
                skip_empty=False,
            )

        assert entry is None
        assert PUBLISHER_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Failed to deserialize replay 1-1" in caplog.text

    def test_clob_retry_warning_is_redacted_and_raises_retry_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import polymarket_insider_tracker.ingestor.clob_client as clob_module

        attempts = 0

        def always_fails() -> str:
            nonlocal attempts
            attempts += 1
            raise RuntimeError(f"node refused https://rpc.internal?k={CLOB_SECRET}")

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with pytest.raises(clob_module.RetryError):
                clob_module._execute_with_retry(always_fails, (), {}, 1, 0.0, (RuntimeError,))

        assert attempts == 2
        assert CLOB_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Retrying in" in caplog.text

    def test_clob_client_error_masks_credential(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import polymarket_insider_tracker.ingestor.clob_client as clob_module
        from tests.fakes.clob import FakeBaseClobClient

        fake = FakeBaseClobClient()
        monkeypatch.setattr(clob_module, "BaseClobClient", lambda _host: fake)
        fake.market_error = RuntimeError(f"node refused https://rpc.internal?k={CLOB_SECRET}")
        client = clob_module.ClobClient()
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            with pytest.raises(clob_module.RetryError, match="get_market"):
                client.get_market("cond1")

        assert CLOB_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Failed to fetch market cond1" in caplog.text

    @pytest.mark.asyncio
    async def test_dispatcher_claim_failures_are_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from fakeredis import FakeAsyncRedis as FakeClaimRedis

        from polymarket_insider_tracker.alerter.dispatcher import AlertDispatcher
        from polymarket_insider_tracker.alerter.history import AlertHistory

        failing_redis = FakeClaimRedis()
        history = AlertHistory(redis=failing_redis)

        async def leaky_read(_key: str) -> object:
            raise RuntimeError(f"redis blew up for https://cache.internal?k={DISPATCHER_SECRET}")

        failing_redis.__dict__["exists"] = leaky_read

        async def leaky_write(_key: str, *_args: object, **_kwargs: object) -> object:
            raise RuntimeError(f"redis blew up for https://cache.internal?k={DISPATCHER_SECRET}")

        failing_redis.__dict__["set"] = leaky_write
        dispatcher = AlertDispatcher(channels=[], history=history)
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            assert await dispatcher._check_channel_suppression("discord", "w", "m") is None
            claim, _ = await dispatcher._claim_channel("discord", "w", "m")
            assert claim == "unverified"

        failing_redis.__dict__["set"] = leaky_write
        await dispatcher._release_claim("discord", "w", "m", "token")
        await dispatcher._record_channel_outcome("discord", "delivered", "w", "m", "token")

        assert DISPATCHER_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Deduplication check unavailable" in caplog.text
        assert "Delivery claim unavailable" in caplog.text

    @pytest.mark.asyncio
    async def test_dispatcher_release_claim_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Round-18 N-R18-4: the release-failure warning redacts a raising release path.

        ``release_channel_claim`` runs a WATCH/get/delete transaction, so the
        failure must be injected at the Redis ``pipeline`` boundary — the
        ``exists``/``set`` overrides of the claim test never reach it.
        """
        from fakeredis import FakeAsyncRedis as FakeClaimRedis

        from polymarket_insider_tracker.alerter.dispatcher import AlertDispatcher
        from polymarket_insider_tracker.alerter.history import AlertHistory

        failing_redis = FakeClaimRedis()
        history = AlertHistory(redis=failing_redis)

        def leaky_pipeline(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError(
                f"redis pipeline blew up for https://cache.internal?k={DISPATCHER_SECRET}"
            )

        failing_redis.__dict__["pipeline"] = leaky_pipeline
        dispatcher = AlertDispatcher(channels=[], history=history)
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            await dispatcher._release_claim("discord", "w", "m", "token")

        assert DISPATCHER_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Failed to release delivery claim" in caplog.text

    @pytest.mark.asyncio
    async def test_websocket_connect_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import warnings

        import polymarket_insider_tracker.ingestor.websocket as websocket_module

        async def on_trade(event: TradeEvent) -> None:
            _ = event

        async def exploding_connect(
            host: str, ping_interval: int, ping_timeout: int
        ) -> websocket_module.ClientConnection:
            _ = (host, ping_interval, ping_timeout)
            raise RuntimeError(f"dial blew up for https://meta.internal?k={WEBSOCKET_SECRET}")

        monkeypatch.setattr(websocket_module, "ws_connect", exploding_connect)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            handler = TradeStreamHandler(on_trade)
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            with pytest.raises(websocket_module.ConnectionError, match="Failed to connect"):
                await handler._connect()

        assert WEBSOCKET_SECRET not in caplog.text
        assert "***" in caplog.text
        assert "Failed to connect" in caplog.text
        assert WEBSOCKET_SECRET not in (handler.stats.last_error or "")
        assert "***" in (handler.stats.last_error or "")

    @pytest.mark.asyncio
    async def test_run_pipeline_startup_failure_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging as stdlib_logging

        from polymarket_insider_tracker.__main__ import run_pipeline
        from polymarket_insider_tracker.pipeline import Pipeline
        from polymarket_insider_tracker.redaction import redact_text
        from tests.fakes import make_test_settings

        settings = make_test_settings()

        class ExplodingPipeline(Pipeline):
            async def start(self) -> None:
                raise RuntimeError(
                    "could not connect to " f"postgresql://tracker:{MAIN_SECRET}@db:5432/x"
                ) from ValueError(f"inner https://rpc.internal/v2/{MAIN_SECRET}")

        rendered: list[str] = []

        class CapturingHandler(stdlib_logging.Handler):
            def emit(self, record: stdlib_logging.LogRecord) -> None:
                rendered.append(self.format(record))

        handler = CapturingHandler()
        handler.setFormatter(stdlib_logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        target = stdlib_logging.getLogger("polymarket_insider_tracker.__main__")
        target.addHandler(handler)
        try:
            with caplog.at_level(logging.ERROR):
                caplog.clear()
                code = await run_pipeline(settings, False, pipeline_factory=ExplodingPipeline)
        finally:
            target.removeHandler(handler)

        assert code == 1
        # The production logging formatter/output path renders the message,
        # the attached exception, and the cause chain: all must be sanitized.
        output = "\n".join(rendered) + "\n" + caplog.text
        assert MAIN_SECRET not in output
        assert "Pipeline failed" in output
        assert "***" in output
        # Provenance: the formatter really rendered the chained exception.
        assert "ValueError" in output
        assert "RuntimeError" in output or "could not connect" in output
        assert redact_text(f"https://rpc.internal/v2/{MAIN_SECRET}") in output or "***" in output

    @pytest.mark.asyncio
    async def test_run_pipeline_nonstring_failure_never_renders_on_the_message_line(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Round-18 N-R18-2: a bytes/container payload cannot render at the message line.

        ``str(e)`` before the filter re-emitted non-string arguments verbatim
        while the traceback clone was already masked; the message line now
        composes through the fail-closed exception renderer.
        """
        import logging as stdlib_logging

        from polymarket_insider_tracker.__main__ import run_pipeline
        from polymarket_insider_tracker.pipeline import Pipeline
        from tests.fakes import make_test_settings

        secret = "nonstring-mainline-secret-r18"
        settings = make_test_settings()

        class NonStringExplodingPipeline(Pipeline):
            async def start(self) -> None:
                raise RuntimeError(
                    f"prefix-{secret}-suffix".encode(),
                    {"url": f"https://x.internal?k={secret}"},
                )

        rendered: list[str] = []

        class CapturingHandler(stdlib_logging.Handler):
            def emit(self, record: stdlib_logging.LogRecord) -> None:
                rendered.append(self.format(record))

        handler = CapturingHandler()
        handler.setFormatter(stdlib_logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        target = stdlib_logging.getLogger("polymarket_insider_tracker.__main__")
        target.addHandler(handler)
        try:
            with caplog.at_level(logging.ERROR):
                caplog.clear()
                code = await run_pipeline(
                    settings, False, pipeline_factory=NonStringExplodingPipeline
                )
        finally:
            target.removeHandler(handler)

        output = "\n".join(rendered) + "\n" + caplog.text
        assert code == 1
        assert secret not in output
        assert "Pipeline failed" in output
        assert "***" in output
        assert "RuntimeError" in output

    @pytest.mark.asyncio
    async def test_metadata_sync_nonstring_failure_status_is_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Round-18 N-R18-2: a bytes payload cannot reach ``stats.last_error`` or logs."""
        from polymarket_insider_tracker.ingestor.metadata_sync import (
            MarketMetadataSync,
            MetadataSyncError,
        )
        from tests.fakes import FakeGammaClient
        from tests.ingestor.test_metadata_sync import FakeClobClient

        secret = "nonstring-status-secret-r18"
        failing = FakeClobClient(raise_error=RuntimeError(f"boom-{secret}-payload".encode()))
        sync = MarketMetadataSync(
            redis=FakeAsyncRedis(),
            clob_client=failing,
            gamma_client=FakeGammaClient(),
        )
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            with pytest.raises(MetadataSyncError, match="initial sync failed"):
                await sync.start()

        assert secret not in caplog.text
        assert secret not in str(sync.stats.last_error or "")
        assert sync.stats.last_error is not None
        assert "***" in sync.stats.last_error

    @pytest.mark.asyncio
    async def test_poller_callback_and_repair_failures_are_redacted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from tests.fakes import (
            FakeClock,
            FakeEth,
            FakeTradesServer,
            make_test_settings,
            trade_row,
            wire_pipeline,
        )

        settings = make_test_settings(trades_url="https://trades.invalid/trades")
        clock = FakeClock(1_788_983_720.0)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            settings, redis=FakeAsyncRedis(), eth=FakeEth(), trades=server, poll_clock=clock
        )
        poller = pipeline._trade_poller
        assert poller is not None
        server.publish(trade_row(timestamp=1_788_983_719, transaction=1))
        await poller.run_cycle()

        async def leaky_callback(event: TradeEvent) -> None:
            _ = event
            raise RuntimeError(
                f"downstream blew up for https://x.internal?k={POLLER_CALLBACK_SECRET}"
            )

        poller.__dict__["_on_trade"] = leaky_callback
        server.publish(trade_row(timestamp=1_788_983_719, transaction=2))
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            await poller.run_cycle()

        assert POLLER_CALLBACK_SECRET not in caplog.text
        assert "***" in caplog.text
        assert poller._tallies.counts["callback_errors"] >= 1

        class LeakyLookup:
            async def get_cached_market(self, condition_id: str) -> MarketMetadata | None:
                _ = condition_id
                raise RuntimeError(
                    f"lookup blew up for https://x.internal?k={POLLER_REPAIR_SECRET}"
                )

        poller.__dict__["_metadata"] = LeakyLookup()
        repair_observation = _eligible_observation()
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            repaired = await poller._repair(repair_observation)

        assert POLLER_REPAIR_SECRET not in caplog.text
        assert "metadata lookup failed" in caplog.text
        assert repaired.outcome_resolution is repair_observation.outcome_resolution


def _eligible_observation() -> TradeObservation:
    """A real observation whose outcome is unknown so ``_repair`` hits metadata."""
    return TradeObservation(
        event=_detector_trade(),
        identity="repair-probe-identity",
        provider_timestamp=1_788_983_720,
        outcome_resolution=OutcomeResolution.UNKNOWN,
    )


def _sync_probe_markets() -> list[Market]:
    """One real market so the batch cache path exercises the raising ``setex``."""
    from datetime import UTC as _UTC
    from datetime import datetime as _datetime
    from decimal import Decimal as _Decimal

    from polymarket_insider_tracker.ingestor.models import Token as _Token

    return [
        Market(
            condition_id="sync-probe-cond",
            question="Will the probe sync?",
            description="redaction probe",
            tokens=(_Token(token_id="t1", outcome="Yes", price=_Decimal("0.5")),),
            end_date=_datetime(2026, 12, 31, tzinfo=_UTC),
            active=True,
        )
    ]


def _sync_probe_metadata() -> MarketMetadata:
    """The cached form of the probe market for the direct ``_cache_market`` path."""
    return MarketMetadata.from_market(_sync_probe_markets()[0])
