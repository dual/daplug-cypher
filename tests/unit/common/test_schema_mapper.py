"""Tests for schema mapping utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pytest

from daplug_cypher.common.schema_mapper import map_to_schema


@pytest.fixture()
def sample_schema(tmp_path: Path) -> Path:
    schema_content = (
        "components:\n"
        "  schemas:\n"
        "    Node:\n"
        "      type: object\n"
        "      properties:\n"
        "        simple:\n"
        "          type: string\n"
        "        nested:\n"
        "          type: object\n"
        "          properties:\n"
        "            value:\n"
        "              type: number\n"
        "        items:\n"
        "          type: array\n"
        "          items:\n"
        "            properties:\n"
        "              name:\n"
        "                type: string\n"
    )
    path = tmp_path / "schema.yml"
    path.write_text(schema_content)
    return path


def test_map_to_schema(sample_schema: Path) -> None:
    payload: Dict[str, object] = {
        "simple": "hello",
        "nested": {"value": 3},
        "items": [{"name": "a"}, {"name": "b"}],
        "extra": "ignored",
    }
    mapped = map_to_schema(payload, str(sample_schema), "Node")
    assert mapped["simple"] == "hello"
    assert mapped["nested"]["value"] == 3
    assert mapped["items"][0]["name"] == "a"
    assert "extra" not in mapped
