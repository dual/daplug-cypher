"""Unit tests for BaseAdapter helpers."""

from __future__ import annotations

from typing import Any, Dict
from unittest import mock

from daplug_cypher.common.base_adapter import BaseAdapter


def _build_adapter(**kwargs: Any) -> BaseAdapter:
    return BaseAdapter(
        identifier="id",
        idempotence_key="version",
        author_identifier="author",
        sns_arn="arn",
        **kwargs,
    )


def test_publish_invokes_publisher_with_formatted_attributes() -> None:
    adapter = _build_adapter(sns_attributes={"custom": "x"})
    payload = {"value": 1}
    with mock.patch("daplug_cypher.common.base_adapter.publisher.publish") as publish_mock:
        adapter.publish("create", payload, sns_attributes={"call": "y"})

    publish_mock.assert_called_once()
    kwargs: Dict[str, Any] = publish_mock.call_args.kwargs
    assert kwargs["arn"] == "arn"
    assert kwargs["data"] == payload
    attributes = kwargs["attributes"]
    assert attributes["operation"]["StringValue"] == "create"
    assert attributes["custom"]["StringValue"] == "x"
    assert attributes["call"]["StringValue"] == "y"


def test_create_format_attributes_omits_none_values() -> None:
    adapter = _build_adapter(sns_default_attributes=False)
    attributes = adapter.create_format_attibutes("update", {"key": None})
    assert "key" not in attributes
