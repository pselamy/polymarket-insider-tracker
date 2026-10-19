"""Reproducible ordering witnesses used by the vendored quality package."""

import pytest
from quality.test_order import pytest_collection_modifyitems, shuffled

from quality.test_ordering import priority


def test_shuffled_is_deterministic_and_preserves_input() -> None:
    items = list(range(10))
    assert shuffled(items, 7) == shuffled(items, 7)
    assert sorted(shuffled(items, 7)) == items
    assert items == list(range(10))


def test_collection_modifyitems_requires_a_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RLCI_TEST_ORDER_SEED", raising=False)
    with pytest.raises(KeyError):
        pytest_collection_modifyitems([1, 2, 3])


def test_collection_modifyitems_reorders_with_the_requested_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RLCI_TEST_ORDER_SEED", "41")
    items = list(range(20))
    pytest_collection_modifyitems(items)
    assert sorted(items) == list(range(20))
    assert items == shuffled(list(range(20)), 41)


def test_priority_orders_native_receipts_last() -> None:
    fast = priority("tests/test_cli.py::test_x")
    receipt = priority("tests/test_pytest_integrity.py::test_y")
    assert fast[0] is False
    assert receipt[0] is True
    assert priority("tests/test_cli.py::test_x") < priority("tests/zzz.py::test_x")
