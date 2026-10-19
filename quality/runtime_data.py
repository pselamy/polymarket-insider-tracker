"""Read runtime policy data without duplicate keys, aliases or implicit coercion."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Protocol, cast

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode
from yaml.tokens import AliasToken, AnchorToken, Token

from quality.report_data import record, text

YAML_STRING_TAG = "tag:yaml.org,2002:str"


class YamlAPI(Protocol):
    """Declare documented upstream return types absent from its incomplete stubs."""

    scan: Callable[[str, type[yaml.SafeLoader]], Iterator[Token]]
    compose: Callable[[str, type[yaml.SafeLoader]], Node | None]
    safe_load: Callable[[str], object]


api = cast(YamlAPI, yaml)


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("runtime JSON contains a duplicate key")
        result[name] = value
    return result


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)


def fields(value: object, expected: set[str]) -> dict[str, object]:
    result = record(value)
    if set(result) != expected:
        raise ValueError("runtime policy keys are missing or unknown")
    return result


def mapping_key(node: Node) -> str:
    if not isinstance(node, ScalarNode) or node.tag != YAML_STRING_TAG:
        raise ValueError("runtime YAML mapping keys must be strings")
    return text(node.value)


def mapping_children(node: MappingNode) -> list[Node]:
    names: set[str] = set()
    children: list[Node] = []
    pairs: list[tuple[Node, Node]] = node.value
    for key, value in pairs:
        name = mapping_key(key)
        if name in names:
            raise ValueError("runtime YAML contains a duplicate key")
        names.add(name)
        children.append(value)
    return children


def children(node: Node) -> list[Node]:
    if isinstance(node, MappingNode):
        return mapping_children(node)
    if isinstance(node, SequenceNode):
        values: list[Node] = node.value
        return values
    return []


def verify_node(node: Node) -> None:
    for child in children(node):
        verify_node(child)


def read_yaml(path: Path) -> object:
    source = path.read_text(encoding="utf-8")
    for token in api.scan(source, yaml.SafeLoader):
        if isinstance(token, (AliasToken, AnchorToken)):
            raise ValueError("runtime YAML cannot contain anchors or aliases")
    node = api.compose(source, yaml.SafeLoader)
    if node is None:
        raise ValueError("runtime YAML document must not be empty")
    verify_node(node)
    return api.safe_load(source)
