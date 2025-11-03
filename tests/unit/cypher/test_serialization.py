"""Unit tests for serialization helpers."""

from __future__ import annotations

from typing import Any, Iterable, List

import pytest

from daplug_cypher.cypher import serialization


class FakeNode:
    def __init__(self, identity: int, labels: Iterable[str], properties: dict):
        self.id = identity
        self.labels = list(labels)
        self._properties = dict(properties)

    def __iter__(self):
        return iter(self._properties.items())


class FakeRelationship:
    def __init__(self, start: FakeNode, end: FakeNode, rel_type: str, properties: dict):
        self.start_node = start
        self.end_node = end
        self.type = rel_type
        self._properties = dict(properties)

    @property
    def start(self) -> int:
        return self.start_node.id

    @property
    def end(self) -> int:
        return self.end_node.id

    def __iter__(self):
        return iter(self._properties.items())


class FakeRecord:
    def __init__(self, values: List[Any]):
        self._values = values

    def values(self) -> List[Any]:
        return self._values


class FakePath:
    def __init__(self, nodes, relationships):
        self.nodes = nodes
        self.relationships = relationships


@pytest.fixture(autouse=True)
def patch_classes(monkeypatch) -> None:
    monkeypatch.setattr(serialization, "Node", FakeNode)
    monkeypatch.setattr(serialization, "Relationship", FakeRelationship)
    monkeypatch.setattr(serialization, "Path", FakePath)


def test_serialize_records_nodes_only() -> None:
    node = FakeNode(1, ["Example"], {"name": "a"})
    record = FakeRecord([node])
    result = serialization.serialize_records([record], label="Example")
    assert result["Example"][0]["name"] == "a"


def test_serialize_records_relationships() -> None:
    start = FakeNode(1, ["Start"], {"name": "s"})
    end = FakeNode(2, ["End"], {"name": "e"})
    relationship = FakeRelationship(start, end, "LINKS", {"weight": 1})
    record = FakeRecord([start, end, relationship])

    result = serialization.serialize_records([record], label="Start")
    assert "Start" in result
    payload = result["Start"][0]
    assert payload["name"] == "s"
    assert payload["LINKS"]["name"] == "e"
