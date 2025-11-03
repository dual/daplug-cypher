"""Unit tests for dict_merger merge strategies."""

from daplug_cypher.common.dict_merger import merge


def test_merge_upserts_values() -> None:
    original = {"name": "alpha", "count": 1}
    result = merge(original, {"count": 2, "new": "value"})
    assert result["count"] == 2
    assert result["new"] == "value"
    assert original["count"] == 1  # original unchanged


def test_merge_remove_dict_key() -> None:
    original = {"name": "alpha", "remove_me": "x"}
    result = merge(original, {"remove_me": None}, update_dict_operation="remove")
    assert "remove_me" not in result


def test_merge_list_operations() -> None:
    original = {"items": [{"id": 1}]}
    # Add new unique entry
    result = merge(original, {"items": [{"id": 2}]})
    assert {"id": 2} in result["items"]

    # Remove entry
    result = merge(result, {"items": [{"id": 2}]}, update_list_operation="remove")
    assert {"id": 2} not in result["items"]

    # Replace list
    result = merge(result, {"items": [{"id": 3}]}, update_list_operation="replace")
    assert result["items"] == [{"id": 3}]
