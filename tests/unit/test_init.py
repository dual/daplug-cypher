"""Tests for package-level exports."""

from daplug_cypher import CypherAdapter, adapter


def test_adapter_factory_returns_cypher_adapter() -> None:
    instance = adapter(
        bolt={"url": "bolt://noop", "user": "user"},
    )
    assert isinstance(instance, CypherAdapter)
