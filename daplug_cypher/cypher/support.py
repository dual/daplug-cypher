from __future__ import annotations

from typing import Any, Dict, Optional, cast, Mapping
from typing_extensions import Unpack

from daplug_core.base_adapter import BaseAdapter
from daplug_core.dict_merger import merge
from daplug_core.schema_mapper import map_to_schema

from daplug_cypher.cypher.parameters import convert_placeholders
from daplug_cypher.cypher.serialization import serialize_records
from daplug_cypher.types.options import (
    AdapterSerializationOptions,
    MergeOptions,
    PublishOptions,
    ReadBeforeDeleteOptions,
    SerializeRecordsOptions,
)


class SupportUtilities:

    def __init__(self, adapter: 'CypherAdapter') -> None:  # type: ignore
        self.adapter = adapter

    def map_with_schema(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if self.adapter.schema_file and self.adapter.schema_name:
            return map_to_schema(data, self.adapter.schema_file, self.adapter.schema_name)
        return dict(data)

    def extract_publish_options(self, source: Mapping[str, Any]) -> PublishOptions:
        options: PublishOptions = {}
        sns_attributes = source.get("sns_attributes")
        if isinstance(sns_attributes, dict):
            options["sns_attributes"] = cast(Dict[str, Any], sns_attributes)
        fifo_group = source.get("fifo_group_id")
        if fifo_group is not None:
            options["fifo_group_id"] = fifo_group
        fifo_dup = source.get("fifo_duplication_id")
        if fifo_dup is not None:
            options["fifo_duplication_id"] = fifo_dup
        return options

    def extract_merge_options(self, source: Mapping[str, Any]) -> MergeOptions:
        options: MergeOptions = {}
        list_op = source.get("update_list_operation")
        if list_op is not None:
            options["update_list_operation"] = list_op
        dict_op = source.get("update_dict_operation")
        if dict_op is not None:
            options["update_dict_operation"] = dict_op
        return options

    def extract_read_before_delete_options(self, source: Mapping[str, Any]) -> ReadBeforeDeleteOptions:
        options: ReadBeforeDeleteOptions = {}
        read_query = source.get("read_query")
        if isinstance(read_query, str):
            options["read_query"] = read_query
        return options

    def publish_with_operation(self, operation: str, payload: Any, **options: Unpack[PublishOptions]) -> None:
        adapter: BaseAdapter = self.adapter
        attributes = dict(options.get("sns_attributes") or {})
        attributes["operation"] = operation
        publish_kwargs: PublishOptions = {"sns_attributes": attributes}
        if options.get("fifo_group_id") is not None:
            publish_kwargs["fifo_group_id"] = options.get("fifo_group_id")
        if options.get("fifo_duplication_id") is not None:
            publish_kwargs["fifo_duplication_id"] = options.get("fifo_duplication_id")
        adapter.publish(payload, **publish_kwargs)

    def merge_payload(self, original: Dict[str, Any], incoming: Dict[str, Any], **options: Unpack[MergeOptions]) -> Dict[str, Any]:
        return merge(original, incoming, **options)

    def serialize_records(self, records: Any, **options: Unpack[AdapterSerializationOptions]) -> Any:
        serialize_options: SerializeRecordsOptions = {
            "label": options.get("node_label"),
            "serialize": options.get("serialize", True),
            "search": options.get("search", False),
        }
        return serialize_records(records, **serialize_options)

    def clean_placeholders(self, placeholder: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if placeholder is None:
            return {}
        return convert_placeholders(placeholder)

    def resolve_bolt_config(self) -> Dict[str, Any]:
        return self.adapter.neptune or self.adapter.bolt

    def execute_write(self, callback) -> Any:
        if not self.adapter._session:  # pylint: disable=protected-access
            raise ValueError("session has not been opened")
        session = self.adapter._session  # pylint: disable=protected-access
        if hasattr(session, "execute_write"):
            return session.execute_write(callback)
        if hasattr(session, "write_transaction"):
            return session.write_transaction(callback)
        raise RuntimeError("Unsupported neo4j session: missing write helpers")

    def run_read(self, query: str, parameters: Dict[str, Any]) -> Any:
        if not self.adapter._session:  # pylint: disable=protected-access
            raise ValueError("session has not been opened")
        return self.adapter._session.run(query, parameters)  # pylint: disable=protected-access

    def run_write(self, query: str, parameters: Dict[str, Any]) -> Any:
        if not self.adapter._session:  # pylint: disable=protected-access
            raise ValueError("session has not been opened")
        return self.adapter._session.run(query, parameters)  # pylint: disable=protected-access

    def default_create_query(self, node_label: str) -> str:
        return f"CREATE (n:{node_label}) SET n = $placeholder RETURN n"

    def default_update_query(self, node_label: str, identifier: str, idempotence_key: str) -> str:
        return (
            f"MATCH (n:{node_label}) "
            f"WHERE n.{identifier} = $id AND n.{idempotence_key} = $version "
            f"SET n = $placeholder RETURN n"
        )

    def match(self, query: str, placeholder: Optional[Dict[str, Any]], **options: Unpack[AdapterSerializationOptions]) -> Any:
        node_label = options.get("node_label")
        serialize = options.get("serialize", True)
        search = options.get("search", False)
        self.adapter._auto_open()  # pylint: disable=protected-access
        try:
            parameters = self.clean_placeholders(placeholder)
            result = self.run_read(query, parameters)
            records = list(result)
            if serialize:
                return self.serialize_records(records, node_label=node_label, serialize=True, search=search)
            return records
        finally:
            self.adapter._auto_close()  # pylint: disable=protected-access

    def get_before_delete(self, node_label: str, identifier: str, delete_identifier: Any, **options: Unpack[ReadBeforeDeleteOptions]) -> Dict[str, Any]:
        read_query = options.get("read_query") or f"MATCH (n:{node_label}) WHERE n.{identifier} = $id RETURN n LIMIT 1"
        records = self.match(
            read_query,
            {"id": delete_identifier},
            node_label=node_label,
            serialize=True,
            search=False,
        )
        if isinstance(records, dict):
            nodes = records.get(node_label, [])
            return nodes[0] if nodes else {}
        if isinstance(records, list) and records:
            return records[0]
        return {}

    def perform_delete(self, node_label: str, identifier: str, delete_identifier: Any, delete_query: Optional[str]) -> None:
        delete_query = delete_query or (
            f"MATCH (n:{node_label}) WHERE n.{identifier} = $id WITH n LIMIT 1 DETACH DELETE n"
        )
        parameters = self.clean_placeholders({"id": delete_identifier})
        self.adapter._auto_open()  # pylint: disable=protected-access
        try:
            self.run_write(delete_query, parameters)
        finally:
            self.adapter._auto_close()  # pylint: disable=protected-access

    def first_node(self, record: Any) -> Optional[Any]:
        if hasattr(record, "values"):
            for value in record.values():
                if self.is_node(value):
                    return value
        return None

    def is_node(self, value: Any) -> bool:
        try:
            from neo4j.graph import Node  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise RuntimeError("neo4j package is required for CypherAdapter") from exc
        return isinstance(value, Node)
