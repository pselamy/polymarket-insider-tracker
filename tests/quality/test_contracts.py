"""Enrollment, deadline and portability contracts for the vendored package."""

import json
from pathlib import Path

import pytest
from quality.deadline import read_deadline
from quality.portable_paths import verify_component, verify_path_names, verify_python_name

from quality import enrollment, pipeline

REFERENCE_HOOKS = {
    "verify_sources",
    "verify_tracked",
    "prepare_tests",
    "verify_tests",
    "verify_security",
    "mutate",
}
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("value", [0.5, 1, 3600])
def test_native_deadline_values(tmp_path: Path, value: float) -> None:
    path = tmp_path / "deadline.json"
    path.write_text(json.dumps(value))
    assert read_deadline(path) == value


@pytest.mark.parametrize(
    "value", [None, True, False, "3600", [], {}, 0, -1, float("nan"), float("inf")]
)
def test_invalid_deadline_is_rejected(tmp_path: Path, value: object) -> None:
    path = tmp_path / "deadline.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="timeout must be"):
        read_deadline(path)


def test_rejects_empty_or_duplicate_tracked_inventory() -> None:
    with pytest.raises(ValueError, match="empty or duplicated"):
        verify_path_names([])
    with pytest.raises(ValueError, match="empty or duplicated"):
        verify_path_names(["a.py", "a.py"])


def test_rejects_case_colliding_paths() -> None:
    with pytest.raises(ValueError, match="collide"):
        verify_path_names(["Package/module.py", "package/module.py"])


def test_rejects_non_snake_case_module() -> None:
    with pytest.raises(ValueError, match="not snake_case"):
        verify_python_name(Path("package/BadName.py"))


@pytest.mark.parametrize(
    "component,match",
    [
        ("", "dot or empty"),
        ("..", "dot or empty"),
        ("a:b", "forbidden character"),
        ("trailing.", "trailing dot or space"),
        ("CON", "device name"),
    ],
)
def test_rejects_nonportable_components(component: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        verify_component(component)


def test_enrollment_partitions_every_reference_hook() -> None:
    enrolled = set(enrollment.ENROLLED_HOOKS)
    unenrolled = set(enrollment.UNENROLLED_HOOKS)
    assert enrolled.isdisjoint(unenrolled)
    assert enrolled | unenrolled == REFERENCE_HOOKS
    assert set(enrollment.UNENROLLED_REASONS) == unenrolled
    assert all(callable(hook) for hook in enrollment.ENROLLED_HOOKS.values())
    assert all(callable(hook) for hook in enrollment.UNENROLLED_HOOKS.values())


def test_checks_registry_is_nonempty_and_well_formed() -> None:
    commands = pipeline.checks(REPOSITORY_ROOT)
    assert commands
    assert all(command and all(argument for argument in command) for command in commands)
