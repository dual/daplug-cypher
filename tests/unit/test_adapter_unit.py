"""Unit tests for CypherAdapter logic using mocks to avoid live connections."""

from __future__ import annotations

from typing import Any, Dict
from unittest import mock

import importlib
import pytest

from daplug_cypher import adapter as build_adapter

adapter_module = importlib.import_module("daplug_cypher.adapter")


def _adapter_kwargs(**overrides: Any) -> Dict[str, Any]:
    base = {
        "bolt": {
            "url": "bolt://unit-test",
            "user": "neo",
            "password": "pass",
        },
    }
    base.update(overrides)
    return base


def test_open_uses_bolt_config() -> None:
    session_mock = mock.Mock()
    driver_instance = mock.Mock()
    driver_instance.session.return_value = session_mock

    with mock.patch.object(adapter_module.GraphDatabase, "driver", return_value=driver_instance) as driver_mock:
        adapter = build_adapter(**_adapter_kwargs())
        adapter.open()

    driver_mock.assert_called_once_with("bolt://unit-test", auth=("neo", "pass"))
    driver_instance.session.assert_called_once()


def test_close_shuts_down_session_and_driver() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    session_mock = mock.Mock()
    driver_mock = mock.Mock()
    adapter._session = session_mock  # type: ignore[attr-defined]
    adapter._driver = driver_mock  # type: ignore[attr-defined]

    adapter.close()

    session_mock.close.assert_called_once()
    driver_mock.close.assert_called_once()
    assert adapter._session is None  # type: ignore[attr-defined]
    assert adapter._driver is None  # type: ignore[attr-defined]


def test_create_runs_write_and_publishes() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    session_mock = mock.Mock()
    adapter._session = session_mock  # type: ignore[attr-defined]
    adapter._driver = mock.Mock()  # type: ignore[attr-defined]
    payload = {"test_id": "abc", "version": 1}
    adapter.publish = mock.Mock()  # type: ignore[assignment]

    def _execute_write(callback):
        tx_mock = mock.Mock()
        callback(tx_mock)
        tx_mock.run.assert_called_once_with(
            "CREATE (n:UnitNode) SET n = $placeholder RETURN n",
            placeholder=payload,
        )
        return None

    session_mock.execute_write.side_effect = _execute_write

    result = adapter.create(data=payload, node="UnitNode", sns_attributes={"call": "create"})

    session_mock.execute_write.assert_called_once()
    adapter.publish.assert_called_once()  # type: ignore[attr-defined]
    publish_args, publish_kwargs = adapter.publish.call_args  # type: ignore[attr-defined]
    assert publish_args == ("create", payload)
    assert publish_kwargs["data"] == payload
    assert publish_kwargs["node"] == "UnitNode"
    assert publish_kwargs["sns_attributes"]["call"] == "create"
    assert result == payload


def test_query_requires_placeholders() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    with pytest.raises(ValueError):
        adapter.query(query="MATCH (n:UnitNode) RETURN n")


def test_query_runs_with_cleaned_placeholders() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    session_mock = mock.Mock()
    adapter._session = session_mock  # type: ignore[attr-defined]
    adapter._driver = mock.Mock()  # type: ignore[attr-defined]
    session_mock.run.return_value = ["row"]

    with mock.patch.object(adapter_module, "convert_placeholders", return_value={"id": 1}) as convert_mock:
        result = adapter.query(
            query="MATCH (n:UnitNode) WHERE n.test_id = $id RETURN n",
            placeholder={"id": "1"},
        )

    convert_mock.assert_called_once_with({"id": "1"})
    session_mock.run.assert_called_once_with(
        "MATCH (n:UnitNode) WHERE n.test_id = $id RETURN n",
        {"id": 1},
    )
    assert result == ["row"]


def test_delete_returns_empty_when_no_matching_record() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    adapter._CypherAdapter__get_before_delete = mock.Mock(return_value={})  # type: ignore[attr-defined]
    adapter._CypherAdapter__perform_delete = mock.Mock()  # type: ignore[attr-defined]
    adapter.publish = mock.Mock()  # type: ignore[assignment]

    result = adapter.delete(
        delete_identifier="missing",
        node="UnitNode",
        identifier="test_id",
        sns_attributes={"call": "delete"},
    )

    assert result == {}
    adapter._CypherAdapter__perform_delete.assert_not_called()  # type: ignore[attr-defined]
    adapter.publish.assert_not_called()  # type: ignore[attr-defined]


def test_update_uses_idempotence_key_and_publishes() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    session_mock = mock.Mock()
    adapter._session = session_mock  # type: ignore[attr-defined]
    adapter._driver = mock.Mock()  # type: ignore[attr-defined]
    tx_mock = mock.Mock()
    tx_mock.run.return_value = [mock.Mock()]

    def _execute_write(callback):
        return callback(tx_mock)

    session_mock.execute_write.side_effect = _execute_write
    adapter._CypherAdapter__match = mock.Mock(return_value=[mock.Mock()])  # type: ignore[attr-defined]
    adapter._CypherAdapter__first_node = mock.Mock(  # type: ignore[attr-defined]
        return_value={"test_id": "abc", "updated_at": 1, "status": "alpha"}
    )
    normalized_payload = {"test_id": "abc", "updated_at": 2, "status": "beta"}
    adapter._CypherAdapter__merge_payload = mock.Mock(return_value=normalized_payload)  # type: ignore[attr-defined]
    adapter._CypherAdapter__map_with_schema = mock.Mock(side_effect=lambda data: data)  # type: ignore[attr-defined]
    adapter.publish = mock.Mock()  # type: ignore[assignment]

    result = adapter.update(
        data={"status": "beta"},
        query="MATCH (n:UnitNode) WHERE n.test_id = $test_id RETURN n",
        placeholder={"test_id": "abc"},
        original_idempotence_value=1,
        node="UnitNode",
        identifier="test_id",
        idempotence_key="updated_at",
        sns_attributes={"call": "update"},
    )

    expected_query = (
        "MATCH (n:UnitNode) WHERE n.test_id = $id AND n.updated_at = $version SET n = $placeholder RETURN n"
    )
    tx_mock.run.assert_called_once_with(expected_query, id=normalized_payload["test_id"], version=1, placeholder=normalized_payload)
    adapter.publish.assert_called_once()  # type: ignore[attr-defined]
    publish_args, publish_kwargs = adapter.publish.call_args  # type: ignore[attr-defined]
    assert publish_args == ("update", normalized_payload)
    assert publish_kwargs["sns_attributes"]["call"] == "update"
    assert result == normalized_payload


def test_delete_publishes_with_sns_attributes() -> None:
    adapter = build_adapter(**_adapter_kwargs(auto_connect=False))
    adapter._CypherAdapter__get_before_delete = mock.Mock(return_value={"test_id": "abc"})  # type: ignore[attr-defined]
    adapter._CypherAdapter__perform_delete = mock.Mock()  # type: ignore[attr-defined]
    adapter.publish = mock.Mock()  # type: ignore[assignment]

    result = adapter.delete(
        delete_identifier="abc",
        node="UnitNode",
        identifier="test_id",
        sns_attributes={"call": "delete"},
    )

    adapter._CypherAdapter__perform_delete.assert_called_once_with(  # type: ignore[attr-defined]
        "UnitNode", "test_id", "abc", None
    )
    adapter.publish.assert_called_once()  # type: ignore[attr-defined]
    publish_args, publish_kwargs = adapter.publish.call_args  # type: ignore[attr-defined]
    assert publish_args == ("delete", {"test_id": "abc"})
    assert publish_kwargs["sns_attributes"]["call"] == "delete"
    assert result == {"test_id": "abc"}
