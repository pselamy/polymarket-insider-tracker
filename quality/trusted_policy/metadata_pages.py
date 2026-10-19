"""Bounded complete native GitHub inventories; no PR-supplied pagination input."""

from collections.abc import Callable
from itertools import count as page_numbers
from typing import TypeGuard

from quality.trusted_policy.review_policy import PolicyFailure, identifier, record

PAGE_SIZE = 100
MAX_PAGES = 1000


ReadAPI = Callable[[str], object]


def is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def sequence(value: object) -> list[object]:
    if not is_list(value):
        raise PolicyFailure("native policy array required")
    return value


def items(value: object) -> list[object]:
    if not is_list(value) or len(value) > PAGE_SIZE:
        raise PolicyFailure("native page must be an array of at most100 entries")
    return value


def page_route(endpoint: str, page: int) -> str:
    if "?" in endpoint or "#" in endpoint:
        raise PolicyFailure("collector endpoint must not contain query or fragment")
    return f"{endpoint}?per_page={PAGE_SIZE}&page={page}"


def array_inventory(api: ReadAPI, endpoint: str) -> list[object]:
    result: list[object] = []
    pages = page_numbers(1)
    while True:
        page = next(pages)
        if page > MAX_PAGES:
            raise PolicyFailure("native inventory exceeded complete collection budget")
        current = items(api(page_route(endpoint, page)))
        if not current:
            return result
        result.extend(current)


def count(value: object) -> int:
    if value == 0 and type(value) is int:
        return 0
    return identifier(value)


def object_page(value: object, key: str) -> tuple[int, list[object]]:
    response = record(value)
    return count(response["total_count"]), items(response[key])


def stable_count(previous: object, current: int) -> int:
    if previous is not None and previous != current:
        raise PolicyFailure("native inventory changed during pagination")
    return current


def object_inventory(api: ReadAPI, endpoint: str, key: str) -> tuple[int, list[object]]:
    result: list[object] = []
    total: int | None = None
    pages = page_numbers(1)
    while True:
        page = next(pages)
        if page > MAX_PAGES:
            raise PolicyFailure("native inventory exceeded complete collection budget")
        current, values = object_page(api(page_route(endpoint, page)), key)
        total = stable_count(total, current)
        if not values:
            return complete_object_inventory(total, result)
        result.extend(values)


def complete_object_inventory(total: int, result: list[object]) -> tuple[int, list[object]]:
    if len(result) != total:
        raise PolicyFailure("native count differs from complete collected inventory")
    return total, result
