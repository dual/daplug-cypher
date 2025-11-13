"""Unit tests for CypherAdapter interactions using stubbed support utilities."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict
from unittest import mock

import pytest

from daplug_cypher.adapter import CypherAdapter


class FakeResult(list):
    def consume(self) -> None:  # pragma: no cover - behaviour shim
        return None


class FakeTx:
    def __init__(self, result: Any = None) -> None:
        self.result = result if result is not None else []
        self.runs: list[tuple[str, Dict[str, Any]]] = []

    def run(self, query: str, **params: Any) -> FakeResult:
        self.runs.append((query, params))
        data = self.result if isinstance(self.result, list) else [self.result]
        return FakeResult(data)


def _build_adapter() -> CypherAdapter:
    return CypherAdapter(auto_connect=False, bolt={"url": "bolt://unit", "user": "neo"})


def _stub_support(**overrides: Any) -> SimpleNamespace:
    stub = SimpleNamespace(
        map_with_schema=mock.Mock(side_effect=lambda data: dict(data)),
        default_create_query=mock.Mock(return_value="CREATE (n:Unit) SET n = $placeholder RETURN n"),
        execute_write=mock.Mock(),
        extract_publish_options=mock.Mock(return_value={}),
        extract_read_before_delete_options=mock.Mock(return_value={}),
        publish_with_operation=mock.Mock(),
        match=mock.Mock(return_value={}),
        clean_placeholders=mock.Mock(return_value={}),
        run_read=mock.Mock(return_value=["row"]),
        extract_merge_options=mock.Mock(return_value={}),
        merge_payload=mock.Mock(),
        default_update_query=mock.Mock(return_value="MATCH (n:Unit) RETURN n"),
        map_with_schema_update=mock.Mock(),
        get_before_delete=mock.Mock(return_value={}),
        perform_delete=mock.Mock(),
        run_write=mock.Mock(return_value=["result"]),
        first_node=mock.Mock(),
    )
    for key, value in overrides.items():
        setattr(stub, key, value)
    return stub


def test_create_runs_write_and_publishes() -> None:
    adapter = _build_adapter()
    stub = _stub_support()
    tx = FakeTx()
    stub.execute_write.side_effect = lambda callback: callback(tx)
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.create(data={"x": 1}, node="Unit")

    assert result == {"x": 1}
    stub.map_with_schema.assert_called_once_with({"x": 1})
    assert tx.runs[0][0].startswith("CREATE")
    stub.publish_with_operation.assert_called_once_with("create", {"x": 1}, **{})


def test_read_passes_options_to_support() -> None:
    adapter = _build_adapter()
    stub = _stub_support(match=mock.Mock(return_value={"Unit": []}))
    adapter.support = stub  # type: ignore[assignment]

    adapter.read(query="MATCH () RETURN 1", node="Unit", placeholder={"id": 1}, serialize=False, search=True)

    stub.match.assert_called_once_with(
        "MATCH () RETURN 1",
        {"id": 1},
        node_label="Unit",
        serialize=False,
        search=True,
    )


def test_query_runs_with_clean_placeholders() -> None:
    adapter = _build_adapter()
    stub = _stub_support(
        clean_placeholders=mock.Mock(return_value={"id": 2}),
        run_read=mock.Mock(return_value=["row"]),
    )
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.query(query="MATCH (n) WHERE n.id = $id RETURN n", placeholder={"id": "2"})

    assert result == ["row"]
    stub.clean_placeholders.assert_called_once_with({"id": "2"})
    stub.run_read.assert_called_once_with("MATCH (n) WHERE n.id = $id RETURN n", {"id": 2})


def test_update_executes_full_flow() -> None:
    adapter = _build_adapter()
    merged = {"test_id": "abc", "version": 2, "status": "beta"}
    stub = _stub_support(
        match=mock.Mock(return_value=[object()]),
        first_node=mock.Mock(return_value={"test_id": "abc", "version": 1, "status": "alpha"}),
        merge_payload=mock.Mock(return_value=merged),
        map_with_schema=mock.Mock(side_effect=lambda data: data),
        default_update_query=mock.Mock(return_value="MATCH (n:Unit) RETURN n"),
        clean_placeholders=mock.Mock(return_value={"id": "abc", "version": 1, "placeholder": merged}),
    )
    tx = FakeTx(["ok"])
    stub.execute_write.side_effect = lambda callback: callback(tx)
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.update(
        data={"status": "beta", "version": 2},
        node="Unit",
        identifier="test_id",
        idempotence_key="version",
        original_idempotence_value=1,
        query="MATCH (n:Unit) RETURN n",
    )

    assert result["status"] == "beta"
    stub.publish_with_operation.assert_called_once_with("update", merged, **{})


def test_delete_short_circuits_when_no_record() -> None:
    adapter = _build_adapter()
    stub = _stub_support(get_before_delete=mock.Mock(return_value={}))
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.delete(delete_identifier="abc", node="Unit", identifier="test_id")

    assert result == {}
    stub.perform_delete.assert_not_called()


def test_delete_executes_flow_and_publishes() -> None:
    adapter = _build_adapter()
    stub = _stub_support(get_before_delete=mock.Mock(return_value={"id": "abc"}))
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.delete(delete_identifier="abc", node="Unit", identifier="test_id", delete_query="QUERY")

    assert result == {"id": "abc"}
    stub.perform_delete.assert_called_once_with("Unit", "test_id", "abc", "QUERY")
    stub.publish_with_operation.assert_called_once_with("delete", {"id": "abc"}, **{})


def test_create_relationship_requires_edge() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.create_relationship(query="MATCH (n) RETURN n", placeholder={})


def test_delete_relationship_requires_delete_clause() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.delete_relationship(query="MATCH (n)-[r]->(m) RETURN r", placeholder={})


def test_create_relationship_executes_support_flow() -> None:
    adapter = _build_adapter()
    stub = _stub_support(
        clean_placeholders=mock.Mock(return_value={"a": 1}),
        run_write=mock.Mock(return_value=["r"]),
    )
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.create_relationship(
        query="MATCH (a)-[r:REL]->(b) RETURN r",
        placeholder={"a": "1"},
    )

    assert result == ["r"]
    stub.publish_with_operation.assert_called_once_with("create", ["r"], **{})


def test_delete_relationship_executes_support_flow() -> None:
    adapter = _build_adapter()
    stub = _stub_support(
        clean_placeholders=mock.Mock(return_value={"a": 1}),
        run_write=mock.Mock(return_value=["r"]),
    )
    adapter.support = stub  # type: ignore[assignment]

    result = adapter.delete_relationship(
        query="MATCH (a)-[r:REL]->(b) DETACH DELETE r",
        placeholder={"a": "1"},
    )

    assert result == ["r"]
    stub.publish_with_operation.assert_called_once_with("delete", ["r"], **{})


def test_open_initializes_driver(monkeypatch) -> None:
    session_mock = mock.Mock()
    driver_mock = mock.Mock()
    driver_mock.session.return_value = session_mock
    driver_ctor = mock.Mock(return_value=driver_mock)
    import importlib

    adapter_module = importlib.import_module("daplug_cypher.adapter")

    monkeypatch.setattr(adapter_module.GraphDatabase, "driver", driver_ctor)
    adapter = CypherAdapter(auto_connect=False, bolt={"url": "bolt://unit", "user": "neo", "password": "pass"})

    adapter.open()

    driver_ctor.assert_called_once_with("bolt://unit", auth=("neo", "pass"), **adapter.driver_config)
    assert adapter._session == session_mock


def test_close_shuts_down_resources() -> None:
    adapter = _build_adapter()
    session_mock = mock.Mock()
    driver_mock = mock.Mock()
    adapter._session = session_mock
    adapter._driver = driver_mock

    adapter.close()

    session_mock.close.assert_called_once()
    driver_mock.close.assert_called_once()
    assert adapter._session is None
    assert adapter._driver is None


def test_create_requires_node_label() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.create(data={"x": 1})


def test_create_requires_payload() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.create(node="Unit")


def test_read_requires_query() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.read(node="Unit")


def test_query_requires_placeholder_markers() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.query(query="MATCH (n) RETURN n")


def test_update_requires_node_label() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.update(
            data={},
            identifier="id",
            idempotence_key="version",
            original_idempotence_value=1,
            query="MATCH () RETURN 1",
        )


def test_update_requires_identifier_and_idempotence_key() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.update(
            data={},
            node="Unit",
            idempotence_key="version",
            original_idempotence_value=1,
            query="MATCH () RETURN 1",
        )


def test_update_requires_original_version() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.update(
            data={},
            node="Unit",
            identifier="id",
            idempotence_key="version",
            query="MATCH () RETURN 1",
        )


def test_update_requires_query_text() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.update(
            data={},
            node="Unit",
            identifier="id",
            idempotence_key="version",
            original_idempotence_value=1,
        )


def test_update_raises_when_no_records_found() -> None:
    adapter = _build_adapter()
    stub = _stub_support(match=mock.Mock(return_value=[]))
    adapter.support = stub  # type: ignore[assignment]
    with pytest.raises(ValueError):
        adapter.update(
            data={},
            node="Unit",
            identifier="id",
            idempotence_key="version",
            original_idempotence_value=1,
            query="MATCH () RETURN 1",
        )


def test_update_raises_when_first_node_missing() -> None:
    adapter = _build_adapter()
    stub = _stub_support(
        match=mock.Mock(return_value=[object()]),
        first_node=mock.Mock(return_value=None),
    )
    adapter.support = stub  # type: ignore[assignment]
    with pytest.raises(ValueError):
        adapter.update(
            data={},
            node="Unit",
            identifier="id",
            idempotence_key="version",
            original_idempotence_value=1,
            query="MATCH () RETURN 1",
        )


def test_update_raises_when_no_rows_updated() -> None:
    adapter = _build_adapter()
    stub = _stub_support(
        match=mock.Mock(return_value=[object()]),
        first_node=mock.Mock(return_value={"id": "1"}),
        merge_payload=mock.Mock(return_value={"id": "1"}),
        map_with_schema=mock.Mock(return_value={"id": "1"}),
        clean_placeholders=mock.Mock(return_value={"id": "1"}),
        execute_write=mock.Mock(return_value=[]),
    )
    adapter.support = stub  # type: ignore[assignment]
    with pytest.raises(ValueError):
        adapter.update(
            data={"id": "1"},
            node="Unit",
            identifier="id",
            idempotence_key="version",
            original_idempotence_value=1,
            query="MATCH () RETURN 1",
        )


def test_delete_requires_identifier() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.delete(delete_identifier="abc", node="Unit")


def test_delete_requires_delete_identifier() -> None:
    adapter = _build_adapter()
    with pytest.raises(ValueError):
        adapter.delete(node="Unit", identifier="id")
