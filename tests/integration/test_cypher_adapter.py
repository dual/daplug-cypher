"""Integration tests validating CypherAdapter against Neo4j and Neptune flows."""

from __future__ import annotations

import uuid
from typing import Any, Dict

import pytest
from neo4j import GraphDatabase

from daplug_cypher import adapter as build_adapter

NEO4J_LABEL = "Neo4jIntegrationNode"
NEPTUNE_LABEL = "NeptuneIntegrationNode"


def _graph_driver(settings: Dict[str, Any]):
    auth = settings.get("auth")
    return GraphDatabase.driver(settings["url"], auth=auth) if auth else GraphDatabase.driver(settings["url"])


def _clear_label(settings: Dict[str, Any], label: str) -> None:
    driver = _graph_driver(settings)
    try:
        with driver.session() as session:
            session.run(f"MATCH (n:{label}) DETACH DELETE n")
    finally:
        driver.close()


def _build_payload() -> Dict[str, Any]:
    return {
        "test_id": str(uuid.uuid4()),
        "version": 1,
        "value": "alpha",
    }


def _build_adapter_instance(settings: Dict[str, Any], *, use_neptune: bool) -> Any:
    connection = {
        "url": settings["url"],
        "user": settings["user"],
        "password": settings["password"],
    }
    kwargs: Dict[str, Any] = {
        "bolt": connection,
    }
    if use_neptune:
        kwargs["neptune"] = connection
    return build_adapter(**kwargs)


def _assert_create_and_read(settings: Dict[str, Any], label: str, *, use_neptune: bool) -> None:
    _clear_label(settings, label)
    adapter = _build_adapter_instance(settings, use_neptune=use_neptune)
    try:
        payload = _build_payload()
        created = adapter.create(data=payload, node=label)
        assert created == payload

        read_result = adapter.read(
            query=f"MATCH (n:{label}) WHERE n.test_id = $test_id RETURN n",
            placeholder={"test_id": payload["test_id"]},
            node=label,
        )
        records = read_result.get(label, [])
        assert records and records[0]["value"] == payload["value"]
    finally:
        adapter.close()
        _clear_label(settings, label)


def _assert_update_flow(settings: Dict[str, Any], label: str, *, use_neptune: bool) -> None:
    _clear_label(settings, label)
    adapter = _build_adapter_instance(settings, use_neptune=use_neptune)
    try:
        payload = _build_payload()
        adapter.create(data=payload, node=label)

        updated = adapter.update(
            data={"version": 2, "value": "beta"},
            query=f"MATCH (n:{label}) WHERE n.test_id = $test_id RETURN n",
            placeholder={"test_id": payload["test_id"]},
            original_idempotence_value=payload["version"],
            node=label,
            identifier="test_id",
            idempotence_key="version",
        )
        assert updated["version"] == 2
        assert updated["value"] == "beta"

        reread = adapter.read(
            query=f"MATCH (n:{label}) WHERE n.test_id = $test_id RETURN n",
            placeholder={"test_id": payload["test_id"]},
            node=label,
        )
        records = reread.get(label, [])
        assert records and records[0]["value"] == "beta"
    finally:
        adapter.close()
        _clear_label(settings, label)


def _assert_delete_flow(settings: Dict[str, Any], label: str, *, use_neptune: bool) -> None:
    _clear_label(settings, label)
    adapter = _build_adapter_instance(settings, use_neptune=use_neptune)
    try:
        payload = _build_payload()
        adapter.create(data=payload, node=label)

        removed = adapter.delete(delete_identifier=payload["test_id"], node=label, identifier="test_id")
        assert removed["test_id"] == payload["test_id"]

        reread = adapter.read(
            query=f"MATCH (n:{label}) WHERE n.test_id = $test_id RETURN n",
            placeholder={"test_id": payload["test_id"]},
            node=label,
        )
        assert reread.get(label, []) == []
    finally:
        adapter.close()
        _clear_label(settings, label)


def _assert_relationship_flow(settings: Dict[str, Any], label: str, *, use_neptune: bool) -> None:
    _clear_label(settings, label)
    adapter = _build_adapter_instance(settings, use_neptune=use_neptune)
    try:
        a_payload = _build_payload()
        b_payload = _build_payload()
        adapter.create(data=a_payload, node=label)
        adapter.create(data=b_payload, node=label)

        create_result = adapter.create_relationship(
            query=(
                f"MATCH (a:{label}), (b:{label}) "
                "WHERE a.test_id = $source AND b.test_id = $target "
                "CREATE (a)-[:ASSOCIATED_WITH]->(b) RETURN a,b"
            ),
            placeholder={"source": a_payload["test_id"], "target": b_payload["test_id"]},
        )
        assert create_result

        adapter.delete_relationship(
            query=(
                f"MATCH (a:{label})-[r:ASSOCIATED_WITH]->(b:{label}) "
                "WHERE a.test_id = $source AND b.test_id = $target "
                "DETACH DELETE r"
            ),
            placeholder={"source": a_payload["test_id"], "target": b_payload["test_id"]},
        )

        check = adapter.query(
            query=(
                f"MATCH (a:{label})-[r:ASSOCIATED_WITH]->(b:{label}) "
                "WHERE a.test_id = $source AND b.test_id = $target "
                "RETURN r"
            ),
            placeholder={"source": a_payload["test_id"], "target": b_payload["test_id"]},
        )
        assert list(check) == []
    finally:
        adapter.close()
        _clear_label(settings, label)


def _assert_query_validation(settings: Dict[str, Any], label: str, *, use_neptune: bool) -> None:
    _clear_label(settings, label)
    adapter = _build_adapter_instance(settings, use_neptune=use_neptune)
    try:
        with pytest.raises(ValueError):
            adapter.query(query=f"MATCH (n:{label}) RETURN n")
    finally:
        adapter.close()
        _clear_label(settings, label)


@pytest.mark.neo4j
def test_neo4j_create_and_read(neo4j_connection_settings: Dict[str, Any]) -> None:
    _assert_create_and_read(neo4j_connection_settings, NEO4J_LABEL, use_neptune=False)


@pytest.mark.neo4j
def test_neo4j_update_flow(neo4j_connection_settings: Dict[str, Any]) -> None:
    _assert_update_flow(neo4j_connection_settings, NEO4J_LABEL, use_neptune=False)


@pytest.mark.neo4j
def test_neo4j_delete_flow(neo4j_connection_settings: Dict[str, Any]) -> None:
    _assert_delete_flow(neo4j_connection_settings, NEO4J_LABEL, use_neptune=False)


@pytest.mark.neo4j
def test_neo4j_relationship_flow(neo4j_connection_settings: Dict[str, Any]) -> None:
    _assert_relationship_flow(neo4j_connection_settings, NEO4J_LABEL, use_neptune=False)


@pytest.mark.neo4j
def test_neo4j_query_validation(neo4j_connection_settings: Dict[str, Any]) -> None:
    _assert_query_validation(neo4j_connection_settings, NEO4J_LABEL, use_neptune=False)


@pytest.mark.neptune
def test_neptune_create_and_read(neptune_connection_settings: Dict[str, Any]) -> None:
    _assert_create_and_read(neptune_connection_settings, NEPTUNE_LABEL, use_neptune=True)


@pytest.mark.neptune
def test_neptune_update_flow(neptune_connection_settings: Dict[str, Any]) -> None:
    _assert_update_flow(neptune_connection_settings, NEPTUNE_LABEL, use_neptune=True)


@pytest.mark.neptune
def test_neptune_delete_flow(neptune_connection_settings: Dict[str, Any]) -> None:
    _assert_delete_flow(neptune_connection_settings, NEPTUNE_LABEL, use_neptune=True)


@pytest.mark.neptune
def test_neptune_relationship_flow(neptune_connection_settings: Dict[str, Any]) -> None:
    _assert_relationship_flow(neptune_connection_settings, NEPTUNE_LABEL, use_neptune=True)


@pytest.mark.neptune
def test_neptune_query_validation(neptune_connection_settings: Dict[str, Any]) -> None:
    _assert_query_validation(neptune_connection_settings, NEPTUNE_LABEL, use_neptune=True)
