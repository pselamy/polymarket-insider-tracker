"""Runtime policy YAML and the pinned native adapters keep rejecting unsafe shapes."""

from pathlib import Path

import pytest
from quality.mutation_workspace import cleanup_unproven
from quality.native_api import native_callable
from quality.owned_commands import SupervisionUnproven
from quality.runtime_data import AnchorFreeLoader, read_yaml


def _write(tmp_path: Path, source: str) -> Path:
    path = tmp_path / "policy.yml"
    path.write_text(source, encoding="utf-8")
    return path


def test_plain_document_is_read(tmp_path: Path) -> None:
    assert read_yaml(_write(tmp_path, "a: 1\nb: [x, y]\n")) == {"a": 1, "b": ["x", "y"]}


@pytest.mark.parametrize("source", ["a: &x 1\n", "a: &x 1\nb: *x\n", "a: *x\n"])
def test_anchors_and_aliases_are_rejected(tmp_path: Path, source: str) -> None:
    with pytest.raises(ValueError, match="anchors or aliases"):
        read_yaml(_write(tmp_path, source))


def test_anchor_scan_runs_before_parsing(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="anchors or aliases"):
        read_yaml(_write(tmp_path, "a: ]\nb: &x 1\n"))


def test_loader_hooks_reject_directly() -> None:
    loader = AnchorFreeLoader("a: 1")
    with pytest.raises(ValueError, match="anchors or aliases"):
        loader.fetch_alias()
    with pytest.raises(ValueError, match="anchors or aliases"):
        loader.fetch_anchor()


def test_empty_and_duplicate_documents_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        read_yaml(_write(tmp_path, ""))
    with pytest.raises(ValueError, match="duplicate key"):
        read_yaml(_write(tmp_path, "a: 1\na: 2\n"))


def test_native_callable_returns_upstream_function() -> None:
    assert native_callable("json", "dumps")({"a": 1}) == '{"a": 1}'


def test_native_callable_rejects_missing_or_non_callable() -> None:
    with pytest.raises(TypeError, match="json.nothing is not callable"):
        native_callable("json", "nothing")
    with pytest.raises(TypeError, match="json.decoder is not callable"):
        native_callable("json", "decoder")


def test_cleanup_unproven_searches_nested_groups() -> None:
    unproven = SupervisionUnproven(1, Path("."), "owner")
    nested = BaseExceptionGroup("outer", [ValueError("x"), ExceptionGroup("inner", [unproven])])
    assert cleanup_unproven(nested)
    assert not cleanup_unproven(BaseExceptionGroup("outer", [KeyboardInterrupt()]))
    assert not cleanup_unproven(ValueError("plain"))
