"""Unit tests for serialization helpers."""

from __future__ import annotations

from typing import Any, Iterable, List

import pytest

from daplug_cypher.cypher.serialization import serialize_records
import daplug_cypher.cypher.serialization as serialization


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


def test_serialize_records_with_path() -> None:
    start = FakeNode(1, ["Start"], {"name": "s"})
    end = FakeNode(2, ["End"], {"name": "e"})
    relationship = FakeRelationship(start, end, "LINKS", {"weight": 1})
    path = FakePath([start, end], [relationship])
    record = FakeRecord([path])

    result = serialization.serialize_records([record], label="Start")
    assert result["Start"][0]["LINKS"]["name"] == "e"


def test_serialize_records_deduplicates_relationships() -> None:
    start = FakeNode(1, ["Start"], {"name": "s"})
    end = FakeNode(2, ["End"], {"name": "e"})
    relationship = FakeRelationship(start, end, "LINKS", {"weight": 1})
    record = FakeRecord([start, end, relationship, relationship])

    result = serialization.serialize_records([record], label="Start")
    assert len(result["Start"]) == 1


def test_serialize_records_returns_raw_when_disabled() -> None:
    assert serialization.serialize_records([[1, 2, 3]], label="Any", serialize=False) == [[1, 2, 3]]


def test_serialize_records_defaults_label_when_missing() -> None:
    node = FakeNode(1, ["Example"], {"value": 1})
    record = FakeRecord([node])
    result = serialization.serialize_records([record], label=None)
    assert result["node"][0]["value"] == 1


def test_connect_nodes_skips_edges_for_unknown_nodes() -> None:
    connections = serialization._connect_nodes({}, [{"start": 1, "end": 2, "type": "LINKS", "properties": {}}])
    assert connections == {}


def test_normalize_properties_handles_search_lists() -> None:
    node = FakeNode(1, ["Example"], {"value": 1})
    result = serialization._normalize_properties([[ [node] ]], label="Example", search=True)
    assert result["Example"][0]["value"] == 1


def test_normalize_numbers_supports_to_native() -> None:
    class Wrapper:
        def __init__(self, value: int) -> None:
            self._value = value

        def to_native(self) -> int:  # pragma: no cover - simple shim
            return self._value

    payload = serialization._normalize_numbers({"wrapped": Wrapper(5), "plain": object()})
    assert payload["wrapped"] == 5
    assert isinstance(payload["plain"], object)
