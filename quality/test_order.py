"""Shuffle the full pytest collection with an explicit reproducible seed."""

import os
from random import Random
from typing import TypeVar

import pytest

T = TypeVar("T")


def shuffled(items: list[T], seed: int) -> list[T]:
    """Return a deterministic permutation without changing the input list."""
    result = items.copy()
    Random(seed).shuffle(result)
    return result


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Fail if a requested stability run lacks a usable seed."""
    seed = int(os.environ["RLCI_TEST_ORDER_SEED"])
    items[:] = shuffled(items, seed)
