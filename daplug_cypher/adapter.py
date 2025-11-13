from __future__ import annotations

from typing import Any, Dict, List, Optional
from typing_extensions import Unpack

from neo4j import GraphDatabase
from neo4j import Driver, Session, Transaction

from daplug_core.base_adapter import BaseAdapter
from daplug_cypher.cypher.support import SupportUtilities
from daplug_cypher.types.options import (
    AdapterConfig,
    CreateParams,
    DeleteParams,
    QueryParams,
    ReadParams,
    RelationshipParams,
    UpdateParams,
)


class CypherAdapter(BaseAdapter):

    def __init__(self, **config: Unpack[AdapterConfig]) -> None:
        super().__init__(**config)
        self.auto_connect: bool = config.get("auto_connect", True)
        self.bolt: Dict[str, Any] = config.get("bolt", {})
        self.neptune: Optional[Dict[str, Any]] = config.get("neptune")
        self.schema_file: Optional[str] = config.get("schema_file")
        self.schema_name: Optional[str] = config.get("schema")
        self.validate_schema: bool = config.get("validate_schema", True)
        self.driver_config: Dict[str, Any] = config.get("driver_config", {})
        self._driver: Optional[Driver] = None
        self._session: Optional[Session] = None
        self.support = SupportUtilities(self)

    def open(self) -> None:
        if self._session:
            return

        bolt_config = self.support.resolve_bolt_config()
        uri = bolt_config.get("url")
        user = bolt_config.get("user")
        password = bolt_config.get("password")

        if not uri or not user:
            raise ValueError("bolt configuration requires 'url' and 'user'")

        auth = (user, password) if password is not None else None
        driver: Driver = GraphDatabase.driver(uri, auth=auth, **self.driver_config)
        session: Session = driver.session()
        self._driver = driver
        self._session = session

    def close(self) -> None:
        if self._session:
            self._session.close()
            self._session = None
        if self._driver:
            self._driver.close()
            self._driver = None

    def _auto_open(self) -> None:
        if self.auto_connect:
            self.open()

    def _auto_close(self) -> None:
        if self.auto_connect:
            self.close()

    def create(self, **params: Unpack[CreateParams]) -> Dict[str, Any]:
        node_label = params.get("node") or params.get("label")
        if not node_label:
            raise ValueError("node label must be provided for create operations")
        if "data" not in params:
            raise ValueError("data payload is required for create operations")
        payload = self.support.map_with_schema(params["data"])
        query = params.get("query") or self.support.default_create_query(node_label)

        def _create(tx: Transaction) -> Any:
            result = tx.run(query, placeholder=payload)
            result.consume()

        self._auto_open()
        try:
            self.support.execute_write(_create)
        finally:
            self._auto_close()

        publish_options = self.support.extract_publish_options(params)
        self.support.publish_with_operation("create", payload, **publish_options)
        return payload

    def read(self, **params: Unpack[ReadParams]) -> Any:
        if "query" not in params:
            raise ValueError("query text is required")
        serialize = params.get("serialize", True)
        search = params.get("search", False)
        node_label = params.get("node") or params.get("label")
        placeholder = params.get("placeholder")
        records = self.support.match(
            params["query"],
            placeholder,
            node_label=node_label,
            serialize=serialize,
            search=search,
        )
        return records

    def query(self, **params: Unpack[QueryParams]) -> Any:
        if "query" not in params:
            raise ValueError("query text is required")
        query_text = params["query"]
        if "$" not in query_text:
            raise ValueError("SECURITY ERROR: parameter placeholders ($) are required")

        parameters = self.support.clean_placeholders(params.get("placeholder"))

        self._auto_open()
        try:
            result = self.support.run_read(query_text, parameters)
            return list(result)
        finally:
            self._auto_close()

    def update(self, **params: Unpack[UpdateParams]) -> Dict[str, Any]:
        node_label = params.get("node") or params.get("label")
        if not node_label:
            raise ValueError("node label must be provided for update operations")
        identifier = params.get("identifier")
        idempotence_key = params.get("idempotence_key")
        if not identifier or not idempotence_key:
            raise ValueError("identifier and idempotence_key must be provided for updates")
        if "original_idempotence_value" not in params:
            raise ValueError("original_idempotence_value is required for optimistic updates")
        original_version = params["original_idempotence_value"]

        query_text = params.get("query")
        if not query_text:
            raise ValueError("query text is required for update operations")
        placeholder = params.get("placeholder")

        original_records: List[Any] = self.support.match(
            query_text,
            placeholder,
            node_label=node_label,
            serialize=False,
            search=params.get("search", False),
        )
        if not original_records:
            raise ValueError("ATOMIC ERROR: No records found; record may have been deleted")

        original_node = self.support.first_node(original_records[0])
        if original_node is None:
            raise ValueError("ATOMIC ERROR: Unable to read existing node properties")

        original_properties = dict(original_node)
        merge_options = self.support.extract_merge_options(params)
        merged = self.support.merge_payload(original_properties, params["data"], **merge_options)
        normalized = self.support.map_with_schema(merged)

        update_query = params.get("update_query") or self.support.default_update_query(
            node_label, identifier, idempotence_key)
        update_params = {
            "id": normalized[identifier],
            "version": original_version,
            "placeholder": normalized,
        }
        update_params = self.support.clean_placeholders(update_params)

        def _run_update(tx: Transaction) -> List[Any]:
            result = tx.run(update_query, **update_params)
            return list(result)

        self._auto_open()
        try:
            records = self.support.execute_write(_run_update)
            if not records:
                raise ValueError("ATOMIC ERROR: No records updated; version may have changed")
        finally:
            self._auto_close()

        publish_options = self.support.extract_publish_options(params)
        self.support.publish_with_operation("update", normalized, **publish_options)
        return normalized

    def delete(self, **params: Unpack[DeleteParams]) -> Dict[str, Any]:
        node_label = params.get("node") or params.get("label")
        if not node_label:
            raise ValueError("node label must be provided for delete operations")
        identifier = params.get("identifier")
        if not identifier:
            raise ValueError("identifier must be provided for delete operations")
        if "delete_identifier" not in params:
            raise ValueError("delete_identifier is required")
        delete_identifier = params["delete_identifier"]

        read_options = self.support.extract_read_before_delete_options(params)
        read_result = self.support.get_before_delete(node_label, identifier, delete_identifier, **read_options)
        if not read_result:
            return {}

        delete_query = params.get("delete_query")
        self.support.perform_delete(node_label, identifier, delete_identifier, delete_query)
        publish_options = self.support.extract_publish_options(params)
        self.support.publish_with_operation("delete", read_result, **publish_options)
        return read_result

    def create_relationship(self, **params: Unpack[RelationshipParams]) -> Any:
        query_text = params.get("query")
        if not query_text:
            raise ValueError("query is required to create relationships")
        if "-" not in query_text or "[" not in query_text:
            raise ValueError("INTEGRITY ERROR: relationship queries must include edges")

        parameters = self.support.clean_placeholders(params.get("placeholder") or {})
        self._auto_open()
        try:
            result = self.support.run_write(query_text, parameters)
            result_list = list(result)
            publish_options = self.support.extract_publish_options(params)
            self.support.publish_with_operation("create", result_list, **publish_options)
            return result_list
        finally:
            self._auto_close()

    def delete_relationship(self, **params: Unpack[RelationshipParams]) -> Any:
        query_text = params.get("query")
        if not query_text:
            raise ValueError("query is required to delete relationships")
        upper_query = query_text.upper()
        if "DELETE" not in upper_query and "DETACH" not in upper_query:
            raise ValueError("INTEGRITY ERROR: delete relationship queries must delete edges")

        parameters = self.support.clean_placeholders(params.get("placeholder") or {})
        self._auto_open()
        try:
            result = self.support.run_write(query_text, parameters)
            result_list = list(result)
            publish_options = self.support.extract_publish_options(params)
            self.support.publish_with_operation("delete", result_list, **publish_options)
            return result_list
        finally:
            self._auto_close()
