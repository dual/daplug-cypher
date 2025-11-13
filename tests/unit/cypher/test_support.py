"""Unit tests for SupportUtilities behaviour."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from daplug_cypher.cypher.support import SupportUtilities


class DummyAdapter:
    def __init__(self) -> None:
        self.schema_file = None
        self.schema_name = None
        self.neptune = None
        self.bolt = {"url": "bolt://dummy", "user": "neo"}
        self._session = None
        self.published: list[tuple[Any, dict[str, Any]]] = []
        self.auto_open_calls = 0
        self.auto_close_calls = 0

    def publish(self, payload: Any, **kwargs: Any) -> None:  # pragma: no cover - trivial shim
        self.published.append((payload, kwargs))

    def _auto_open(self) -> None:
        self.auto_open_calls += 1

    def _auto_close(self) -> None:
        self.auto_close_calls += 1


class FakeSession:
    def __init__(self, *, run_results: list[Any] | None = None) -> None:
        self.run_results = list(run_results or [])
        self.run_calls: list[tuple[str, Any]] = []
        self.write_calls: list[tuple[str, Any]] = []

    def run(self, query: str, params: Any = None) -> Any:  # pragma: no cover - helper
        self.run_calls.append((query, params))
        return self.run_results.pop(0) if self.run_results else []

    def execute_write(self, callback):  # pragma: no cover - helper
        tx = mock.Mock()
        return callback(tx)


def test_extract_publish_options_merges_attributes() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    options = support.extract_publish_options(
        {
            "sns_attributes": {"origin": "api"},
            "fifo_group_id": "group",
            "fifo_duplication_id": "dedupe",
        }
    )
    assert options["sns_attributes"]["origin"] == "api"
    assert options["fifo_group_id"] == "group"
    assert options["fifo_duplication_id"] == "dedupe"


def test_publish_with_operation_invokes_adapter_publish() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    support.publish_with_operation("create", {"id": 1}, sns_attributes={"source": "test"})
    payload, kwargs = adapter.published[0]
    assert payload == {"id": 1}
    assert kwargs["sns_attributes"]["operation"] == "create"
    assert kwargs["sns_attributes"]["source"] == "test"


def test_extract_merge_options_honours_values() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    opts = support.extract_merge_options(
        {
            "update_list_operation": "replace",
            "update_dict_operation": "remove",
        }
    )
    assert opts["update_list_operation"] == "replace"
    assert opts["update_dict_operation"] == "remove"


def test_clean_placeholders_returns_empty_dict() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    assert support.clean_placeholders(None) == {}


def test_resolve_bolt_config_prefers_neptune() -> None:
    adapter = DummyAdapter()
    adapter.neptune = {"url": "bolt://neptune", "user": "svc"}
    support = SupportUtilities(adapter)
    assert support.resolve_bolt_config()["url"] == "bolt://neptune"


def test_execute_write_raises_without_session() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    with pytest.raises(ValueError):
        support.execute_write(lambda tx: tx)


def test_run_read_and_match_invoke_session(monkeypatch) -> None:
    adapter = DummyAdapter()
    session = FakeSession(run_results=[["record"]])
    adapter._session = session
    support = SupportUtilities(adapter)
    result = support.match("MATCH () RETURN 1", {"id": "1"}, node_label="Unit", serialize=False, search=False)
    assert result == ["record"]
    assert session.run_calls[0][0] == "MATCH () RETURN 1"


def test_match_serializes_when_requested(monkeypatch) -> None:
    adapter = DummyAdapter()
    session = FakeSession(run_results=[["raw"]])
    adapter._session = session
    support = SupportUtilities(adapter)
    with mock.patch("daplug_cypher.cypher.support.serialize_records", return_value={"Unit": []}) as serialize_mock:
        payload = support.match("MATCH () RETURN 1", None, node_label="Unit", serialize=True, search=False)
    assert payload == {"Unit": []}
    serialize_mock.assert_called_once()


def test_get_before_delete_returns_first_entry() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    support.match = mock.Mock(return_value={"Unit": [{"id": 1}]})  # type: ignore[assignment]
    result = support.get_before_delete("Unit", "id", 1)
    assert result == {"id": 1}


def test_perform_delete_runs_write(monkeypatch) -> None:
    adapter = DummyAdapter()
    session = FakeSession()
    adapter._session = session
    support = SupportUtilities(adapter)
    support.run_write = mock.Mock(return_value=None)  # type: ignore[assignment]
    support.perform_delete("Unit", "id", 1, None)
    support.run_write.assert_called_once()


def test_first_node_uses_is_node(monkeypatch) -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    sentinel = object()
    support.is_node = lambda value: value is sentinel  # type: ignore[assignment]
    record = mock.Mock()
    record.values.return_value = [sentinel]
    assert support.first_node(record) is sentinel


def test_map_with_schema_with_explicit_schema(tmp_path: Path) -> None:
    schema = """
components:
  schemas:
    Model:
      type: object
      properties:
        allowed:
          type: string
        nested:
          type: object
          properties:
            value:
              type: integer
"""
    path = tmp_path / "schema.yml"
    path.write_text(schema)
    adapter = DummyAdapter()
    adapter.schema_file = str(path)
    adapter.schema_name = "Model"
    support = SupportUtilities(adapter)
    data = {"allowed": "yes", "nested": {"value": 3}, "ignored": True}
    mapped = support.map_with_schema(data)
    assert mapped == {"allowed": "yes", "nested": {"value": 3}}


def test_execute_write_uses_session_interface() -> None:
    adapter = DummyAdapter()
    session = mock.Mock()
    adapter._session = session
    support = SupportUtilities(adapter)
    callback = mock.Mock()
    support.execute_write(callback)
    session.execute_write.assert_called_once_with(callback)


def test_run_read_with_session() -> None:
    adapter = DummyAdapter()
    session = mock.Mock()
    adapter._session = session
    support = SupportUtilities(adapter)
    support.run_read("MATCH", {"id": 1})
    session.run.assert_called_once_with("MATCH", {"id": 1})


def test_run_write_with_session() -> None:
    adapter = DummyAdapter()
    session = mock.Mock()
    adapter._session = session
    support = SupportUtilities(adapter)
    support.run_write("DELETE", {"id": 1})
    session.run.assert_called_once_with("DELETE", {"id": 1})


def test_get_before_delete_returns_first_list_entry() -> None:
    adapter = DummyAdapter()
    support = SupportUtilities(adapter)
    support.match = mock.Mock(return_value=[["first"], ["second"]])  # type: ignore[assignment]
    result = support.get_before_delete("Unit", "id", 1)
    assert result == ["first"]


def test_perform_delete_opens_and_closes_connection() -> None:
    adapter = DummyAdapter()
    session = mock.Mock()
    adapter._session = session
    support = SupportUtilities(adapter)
    support.run_write = mock.Mock(return_value=None)  # type: ignore[assignment]
    support.perform_delete("Unit", "id", 1, None)
    assert adapter.auto_open_calls == 1
    assert adapter.auto_close_calls == 1
