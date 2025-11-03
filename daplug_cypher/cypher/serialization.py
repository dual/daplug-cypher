"""Helpers for transforming Cypher records into JSON-compatible data."""

from typing import Any, Dict, Iterable, List, Optional, cast

from neo4j.graph import Node, Path, Relationship


def serialize_records(records: Iterable[Any], *, label: Optional[str], serialize: bool = True, search: bool = False) -> Any:
    """Serialize Neo4j result records into JSON-compatible structures."""
    if not serialize:
        return list(records)

    normalized_fields = [_normalize_record(record) for record in records]
    relationships = _group_relationships(normalized_fields)
    if relationships:
        return _consolidate_relationships(relationships)
    if label is None:
        label = "node"
    return _normalize_properties(normalized_fields, label, search)


def _normalize_record(record: Any) -> List[Any]:
    values: List[Any] = []
    if hasattr(record, "values"):
        for value in record.values():
            if isinstance(value, Path):
                values.extend(list(value.nodes))
                values.extend(list(value.relationships))
            else:
                values.append(value)
    return values


def _group_relationships(records: List[List[Any]]) -> List[Dict[str, Any]]:
    relationships: List[Dict[str, Any]] = []
    for fields in records:
        edges = _group_edges(fields)
        if edges:
            nodes = _group_nodes(fields)
            connections = _connect_nodes(nodes, edges)
            relationship = _consolidate_nodes(connections)
            if relationship:
                relationships.append(relationship)
    return relationships


def _group_edges(fields: List[Any]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    for field in fields:
        if isinstance(field, Relationship):
            start_node = cast(Node, field.start_node)
            end_node = cast(Node, field.end_node)
            edges.append(
                {
                    "start": start_node.id,
                    "end": end_node.id,
                    "type": field.type,
                    "properties": _normalize_numbers(dict(field)),
                }
            )
    return edges


def _group_nodes(fields: List[Any]) -> Dict[Any, Dict[str, Any]]:
    nodes: Dict[Any, Dict[str, Any]] = {}
    for field in fields:
        if isinstance(field, Node):
            node = cast(Node, field)
            nodes[node.id] = {
                "labels": list(node.labels),
                "properties": _normalize_numbers(dict(node)),
            }
    return nodes


def _connect_nodes(nodes: Dict[Any, Dict[str, Any]], edges: List[Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
    connections: Dict[Any, Dict[str, Any]] = {}
    for edge in edges:
        start = edge["start"]
        end = edge["end"]
        if start not in nodes or end not in nodes:
            continue
        if start not in connections:
            connections[start] = dict(nodes[start])
        connections[start].setdefault(edge["type"], {})
        connections[start][edge["type"]] = nodes[end]["properties"]
    return connections


def _consolidate_nodes(connections: Dict[Any, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    consolidated: Dict[str, List[Dict[str, Any]]] = {}
    for connection in connections.values():
        labels = connection.get("labels", [])
        properties = dict(connection.get("properties", {}))
        label_key = "-".join(labels) if labels else "node"
        consolidated.setdefault(label_key, [])
        node_payload = {**properties}
        for key, value in connection.items():
            if key in ("labels", "properties"):
                continue
            node_payload[key] = value
        if _unique_node(node_payload, consolidated[label_key]):
            consolidated[label_key].append(node_payload)
    return consolidated


def _consolidate_relationships(relationships: List[Dict[str, List[Dict[str, Any]]]]) -> Dict[str, List[Dict[str, Any]]]:
    consolidated: Dict[str, List[Dict[str, Any]]] = {}
    for relationship in relationships:
        for label, nodes in relationship.items():
            consolidated.setdefault(label, [])
            for node in nodes:
                if _unique_node(node, consolidated[label]):
                    consolidated[label].append(node)
    return consolidated


def _unique_node(node: Dict[str, Any], node_group: List[Dict[str, Any]]) -> bool:
    return node not in node_group


def _normalize_properties(records: List[List[Any]], label: str, search: bool) -> Dict[str, List[Dict[str, Any]]]:
    normalized: Dict[str, List[Dict[str, Any]]] = {label: []}
    for fields in records:
        iterable = fields[0] if search and fields else fields
        if isinstance(iterable, list):
            targets = iterable
        else:
            targets = fields
        for field in targets:
            if isinstance(field, Node):
                properties = _normalize_numbers(dict(field))
                normalized[label].append(properties)
    return normalized


def _normalize_numbers(properties: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}
    for key, value in properties.items():
        if isinstance(value, (int, float, str)):
            normalized[key] = value
        elif hasattr(value, "to_native"):
            normalized[key] = value.to_native()
        else:
            normalized[key] = value
    return normalized
