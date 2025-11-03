"""Tests for common package exports."""

from daplug_cypher.common import BaseAdapter, map_to_schema, merge


def test_common_exports() -> None:
    assert BaseAdapter
    assert callable(map_to_schema)
    assert callable(merge)
