"""Unit tests for parameter utilities."""

from daplug_cypher.cypher.parameters import convert_placeholders


def test_convert_placeholders_converts_numeric_strings() -> None:
    placeholder = {"a": "1", "b": "-2", "c": "NaN"}
    result = convert_placeholders(placeholder)
    assert result["a"] == 1
    assert result["b"] == -2
    assert result["c"] == "NaN"


def test_convert_placeholders_handles_nested_structures() -> None:
    placeholder = {
        "outer": {
            "inner": ["3", {"leaf": "4"}],
        },
        "list": ["5", "value"],
        "mixed": ["0", None, {"deep": "-7"}],
    }
    result = convert_placeholders(placeholder)
    assert result["outer"]["inner"][0] == 3
    assert result["outer"]["inner"][1]["leaf"] == 4
    assert result["list"][0] == 5
    assert result["list"][1] == "value"
    assert result["mixed"][0] == 0
    assert result["mixed"][1] is None
    assert result["mixed"][2]["deep"] == -7
