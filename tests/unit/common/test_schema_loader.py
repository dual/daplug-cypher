"""Tests for schema_loader."""

from pathlib import Path

from daplug_cypher.common import schema_loader


def test_load_schema(tmp_path: Path) -> None:
    schema_content = (
        "components:\n"
        "  schemas:\n"
        "    Node:\n"
        "      type: object\n"
        "      properties:\n"
        "        name:\n"
        "          type: string\n"
    )
    schema_path = tmp_path / "schema.yml"
    schema_path.write_text(schema_content)

    schema = schema_loader.load_schema(str(schema_path), "Node")
    assert schema["properties"]["name"]["type"] == "string"
