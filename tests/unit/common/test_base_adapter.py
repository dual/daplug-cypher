"""Unit tests for BaseAdapter helpers."""

from __future__ import annotations

from typing import Any, Dict
from unittest import mock

from daplug_cypher.common.base_adapter import BaseAdapter


def _build_adapter(**kwargs: Any) -> BaseAdapter:
    return BaseAdapter(sns_arn="arn", **kwargs)


def test_publish_merges_adapter_and_call_attributes() -> None:
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


def test_publish_call_attributes_override_defaults() -> None:
    adapter = _build_adapter(sns_attributes={"source": "default"})
    with mock.patch("daplug_cypher.common.base_adapter.publisher.publish") as publish_mock:
        adapter.publish("update", {"value": 2}, sns_attributes={"source": "call"})

    attributes = publish_mock.call_args.kwargs["attributes"]
    assert attributes["source"]["StringValue"] == "call"


def test_create_format_attributes_skip_none_and_infer_types() -> None:
    adapter = _build_adapter()
    attributes = adapter.create_format_attributes("delete", {"keep": "x", "skip": None, "count": 3})

    assert "skip" not in attributes
    assert attributes["operation"]["StringValue"] == "delete"
    assert attributes["keep"]["DataType"] == "String"
    assert attributes["count"]["DataType"] == "Number"
