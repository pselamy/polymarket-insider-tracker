# Contract: Test Quality and Working Fakes Over Mocks

## Purpose

This contract governs test double design and testing quality for `polymarket-insider-tracker`.
It enforces the repository principle of using lightweight working fakes and real values over
`unittest.mock` interaction mocks, ensuring tests verify observable behavior and state rather than
brittle call sequences.

## 1. Core Principles

1. **State Verification Over Interaction Verification**: Tests assert observable outcomes, return values,
   and persisted state rather than call counts or argument tuples (`mock.assert_called_with`).
2. **Working Fakes Over Mocks**: Reusable collaborators (in-memory Redis, repositories, blockchain/API clients)
   must be concrete, lightweight implementations that maintain internal state and simulate the collaborator's
   contracts faithfully within the test context.
3. **Real Values Over Dummy Stubs**: Use real Pydantic models, dataclasses, and domain objects rather than
   dynamically configured mock structures.
4. **Shared Behavioral Contracts**: Reusable fakes representing external infrastructure (such as Redis) must
   pass the same behavioral contract tests as the real implementation in the services profile.

## 2. Prohibited Anti-Patterns

The following patterns are strictly prohibited in the test suite:

- **No `unittest.mock` imports or aliases**: Neither `unittest.mock`, `mock`, `MagicMock`, `AsyncMock`,
  nor `patch` may be imported or used in test files.
- **No generic mock frameworks**: Custom generic `CallableFake`, `MockWrapper`, or general-purpose mock engines
  are prohibited.
- **No dynamic configurators**: `return_value` or `side_effect` assignment ladders that mimic mock frameworks
  are prohibited.
- **No dynamic attribute trees**: `__getattr__` or dynamic attribute dispatch returning self/fakes is prohibited.
- **No synthetic interaction assertions**: Automated or helper-driven `assert_called` or call-list assertions are
  prohibited.
- **No assertion weakening**: Replacing strong expectations with empty assertions or deleting test scenarios
  is prohibited.
- **No test skipping or threshold increases**: No new `skip`, `xfail`, or relaxed quality thresholds.
- **No broad type escapes**: No `Any` or `object` casts to bypass strict type checking.

## 3. Allowed and Recommended Patterns

- **`pytest.monkeypatch`**: Permitted for setting environment variables, substituting concrete fakes at
  system boundaries, and deliberate failure injection.
- **Real HTTP test transports**: Use real `httpx.HTTPTransport`, `httpx.MockTransport`, or custom `httpx.BaseTransport`
  with real `httpx.Response` and `httpx.Request` objects.
- **In-Memory SQLite**: Use real SQLite-backed database sessions and repositories where already practical.
- **Working Fake Collaborators**:
  - `FakeRedis`: In-memory Redis implementing string (get/set/delete/exists/expire/ttl), hash (hget/hset/hgetall/hdel),
    set (sadd/srem/smembers/sismember), sorted set (zadd/zrangebyscore/zremrangebyscore), and stream
    (xadd/xread/xrange/xack) semantics required by the pipeline, with TTL tracking.
  - `FakePolygonClient`: In-memory blockchain client returning real `Transaction` and `WalletInfo` records.
  - `FakeClobClient`: In-memory Polymarket CLOB client returning real `Market` and `Orderbook` models.
  - `FakeGammaClient`: In-memory Gamma API client returning real `GammaMarketStats`.
  - `FakeDispatcher`: In-memory alert dispatcher recording dispatched `Alert` records for state inspection.
  - `FakeHistory`: In-memory alert history recording delivery records for state inspection.

## 4. Shared Contract Verification

Reusable collaborator fakes (such as `FakeRedis`) must be verified through shared contract suites:
- A parameterised test suite executes identical operations against both `FakeRedis` and a real `redis.asyncio.Redis`
  instance (when available under the `services` profile).
- Parity covers key storage, key expiration, hash manipulations, sorted set ranges, and stream append/read semantics.

## 5. Enforcement and Regressions

1. **AST-Based Static Regression**: An automated test inspects all Python files under `tests/` and asserts that
   none import from `unittest.mock` or reference `unittest.mock` symbols.
2. **Standard Gate Enforcement**: All test and fake implementations must satisfy:
   - Black formatting
   - Ruff linting
   - Strict mypy and Pyright type checking
   - Default-confidence Vulture dead-code analysis (no unused fake methods)
   - Fail-closed Complexipy cognitive complexity <= 5 on all functions and modules
