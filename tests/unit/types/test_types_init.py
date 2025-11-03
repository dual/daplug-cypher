"""Tests for type exports."""

from daplug_cypher.types import (
    DynamoItem,
    DynamoItems,
    MessageAttributes,
    PrefixConfig,
    SchemaConfig,
)


def test_type_aliases() -> None:
    item: DynamoItem = {"key": "value"}
    items: DynamoItems = [item]
    attributes: MessageAttributes = {"key": {"DataType": "String", "StringValue": "value"}}
    prefix_config: PrefixConfig = {"hash_key": "id", "hash_prefix": "pk#"}
    schema_config: SchemaConfig = {"schema": "Node", "schema_file": "schema.yml"}

    assert items[0] == item
    assert attributes["key"]["DataType"] == "String"
    assert prefix_config["hash_prefix"] == "pk#"
    assert schema_config["schema"] == "Node"
