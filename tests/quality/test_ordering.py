"""Reproducible ordering witnesses used by the vendored quality package."""

import pytest
from quality.test_order import pytest_collection_modifyitems, shuffled

from quality.test_ordering import priority


class _Case(pytest.Item):
    """A real collection item that is never run, so the hook is exercised on its true type."""

    def runtest(self) -> None:
        raise NotImplementedError("collection witnesses are never executed")


def _cases(session: pytest.Session, count: int) -> list[pytest.Item]:
    return [_Case.from_parent(session, name=f"case{index}") for index in range(count)]


def test_collection_witness_refuses_to_run(request: pytest.FixtureRequest) -> None:
    with pytest.raises(NotImplementedError):
        _cases(request.session, 1)[0].runtest()


def test_shuffled_is_deterministic_and_preserves_input() -> None:
    items = list(range(10))
    assert shuffled(items, 7) == shuffled(items, 7)
    assert sorted(shuffled(items, 7)) == items
    assert items == list(range(10))


def test_collection_modifyitems_requires_a_seed(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    monkeypatch.delenv("RLCI_TEST_ORDER_SEED", raising=False)
    with pytest.raises(KeyError):
        pytest_collection_modifyitems(_cases(request.session, 3))


def test_collection_modifyitems_reorders_with_the_requested_seed(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    monkeypatch.setenv("RLCI_TEST_ORDER_SEED", "41")
    items = _cases(request.session, 20)
    original = items.copy()
    pytest_collection_modifyitems(items)
    assert sorted(items, key=lambda item: item.name) == sorted(original, key=lambda item: item.name)
    assert items == shuffled(original, 41)


def test_priority_orders_native_receipts_last() -> None:
    fast = priority("tests/test_cli.py::test_x")
    receipt = priority("tests/test_pytest_integrity.py::test_y")
    assert fast[0] is False
    assert receipt[0] is True
    assert priority("tests/test_cli.py::test_x") < priority("tests/zzz.py::test_x")
