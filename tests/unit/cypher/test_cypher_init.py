"""Tests for cypher helper exports."""

from daplug_cypher.cypher import convert_placeholders, serialize_records


def test_cypher_exports() -> None:
    assert callable(convert_placeholders)
    assert callable(serialize_records)
