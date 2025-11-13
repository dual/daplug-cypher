from __future__ import annotations

from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase
from neo4j import Driver, Session, Transaction

from daplug_core.schema_mapper import map_to_schema
from daplug_core.dict_merger import merge
from daplug_core.base_adapter import BaseAdapter
from daplug_cypher.cypher.parameters import convert_placeholders
from daplug_cypher.cypher.serialization import serialize_records


class CypherAdapter(BaseAdapter):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.auto_connect: bool = kwargs.get("auto_connect", True)
        self.bolt: Dict[str, Any] = kwargs.get("bolt", {})
        self.neptune: Optional[Dict[str, Any]] = kwargs.get("neptune")
        self.schema_file: Optional[str] = kwargs.get("schema_file")
        self.schema_name: Optional[str] = kwargs.get("schema")
        self.validate_schema: bool = kwargs.get("validate_schema", True)
        self.driver_config: Dict[str, Any] = kwargs.get("driver_config", {})

        self._driver: Optional[Driver] = None
        self._session: Optional[Session] = None

    # ---- Connection lifecycle -------------------------------------------------
    def open(self) -> None:
        if self._session:
            return

        bolt_config = self.__resolve_bolt_config()
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

    def __auto_open(self) -> None:
        if self.auto_connect:
            self.open()

    def __auto_close(self) -> None:
        if self.auto_connect:
            self.close()

    # ---- CRUD surface ---------------------------------------------------------
    def create(self, **kwargs: Any) -> Dict[str, Any]:
        node_label = kwargs.get("node") or kwargs.get("label")
        if not node_label:
            raise ValueError("node label must be provided for create operations")
        payload = self.__map_with_schema(kwargs["data"])
        query = kwargs.get("query") or self.__default_create_query(node_label)

        def _create(tx: Transaction) -> Any:
            result = tx.run(query, placeholder=payload)
            result.consume()

        self.__auto_open()
        try:
            self.__execute_write(_create)
        finally:
            self.__auto_close()

        self.__publish_with_operation("create", payload, **kwargs)
        return payload

    def read(self, **kwargs: Any) -> Any:
        params = dict(kwargs)
        serialize = params.pop("serialize", True)
        search = params.pop("search", False)
        node_label = params.get("node") or params.get("label")
        query = params["query"]
        placeholder = params.get("placeholder")
        records = self.__match(query, placeholder, node_label=node_label, serialize=serialize, search=search)
        return records

    def query(self, **kwargs: Any) -> Any:
        if "query" not in kwargs:
            raise ValueError("query text is required")
        query_text = kwargs["query"]
        if "$" not in query_text:
            raise ValueError("SECURITY ERROR: parameter placeholders ($) are required")

        parameters = self.__clean_placeholders(kwargs.get("placeholder") or {})

        self.__auto_open()
        try:
            result = self.__run_read(query_text, parameters)
            return list(result)
        finally:
            self.__auto_close()

    def update(self, **kwargs: Any) -> Dict[str, Any]:
        node_label = kwargs.get("node") or kwargs.get("label")
        if not node_label:
            raise ValueError("node label must be provided for update operations")
        identifier = kwargs.get("identifier")
        idempotence_key = kwargs.get("idempotence_key")
        if not identifier or not idempotence_key:
            raise ValueError("identifier and idempotence_key must be provided for updates")
        original_version = kwargs.get("original_idempotence_value")
        if original_version is None:
            raise ValueError("original_idempotence_value is required for optimistic updates")

        query_text = kwargs.get("query")
        if not query_text:
            raise ValueError("query text is required for update operations")
        placeholder = kwargs.get("placeholder")

        original_records: List[Any] = self.__match(
            query_text,
            placeholder,
            node_label=node_label,
            serialize=False,
            search=kwargs.get("search", False),
        )
        if not original_records:
            raise ValueError("ATOMIC ERROR: No records found; record may have been deleted")

        original_node = self.__first_node(original_records[0])
        if original_node is None:
            raise ValueError("ATOMIC ERROR: Unable to read existing node properties")

        original_properties = dict(original_node)
        merged = self.__merge_payload(original_properties, kwargs["data"], **kwargs)
        normalized = self.__map_with_schema(merged)

        update_query = kwargs.get("update_query") or self.__default_update_query(
            node_label, identifier, idempotence_key)
        update_params = {
            "id": normalized[identifier],
            "version": original_version,
            "placeholder": normalized,
        }
        update_params = self.__clean_placeholders(update_params)

        def _run_update(tx: Transaction) -> List[Any]:
            result = tx.run(update_query, **update_params)
            return list(result)

        self.__auto_open()
        try:
            records = self.__execute_write(_run_update)
            if not records:
                raise ValueError("ATOMIC ERROR: No records updated; version may have changed")
        finally:
            self.__auto_close()

        self.__publish_with_operation("update", normalized, **kwargs)
        return normalized

    def delete(self, **kwargs: Any) -> Dict[str, Any]:
        node_label = kwargs.get("node") or kwargs.get("label")
        if not node_label:
            raise ValueError("node label must be provided for delete operations")
        identifier = kwargs.get("identifier")
        if not identifier:
            raise ValueError("identifier must be provided for delete operations")
        delete_identifier = kwargs.get("delete_identifier")
        if delete_identifier is None:
            raise ValueError("delete_identifier is required")

        helper_kwargs = dict(kwargs)
        helper_kwargs.pop("delete_identifier", None)
        helper_kwargs.pop("node", None)
        helper_kwargs.pop("label", None)
        helper_kwargs.pop("identifier", None)
        read_result = self.__get_before_delete(node_label, identifier, delete_identifier, **helper_kwargs)
        if not read_result:
            return {}

        delete_query = helper_kwargs.get("delete_query")
        self.__perform_delete(node_label, identifier, delete_identifier, delete_query)
        self.__publish_with_operation("delete", read_result, **kwargs)
        return read_result

    def create_relationship(self, **kwargs: Any) -> Any:
        query_text = kwargs.get("query")
        if not query_text:
            raise ValueError("query is required to create relationships")
        if "-" not in query_text or "[" not in query_text:
            raise ValueError("INTEGRITY ERROR: relationship queries must include edges")

        parameters = self.__clean_placeholders(kwargs.get("placeholder") or {})
        self.__auto_open()
        try:
            result = self.__run_write(query_text, parameters)
            result_list = list(result)
            self.__publish_with_operation("create", result_list, **kwargs)
            return result_list
        finally:
            self.__auto_close()

    def delete_relationship(self, **kwargs: Any) -> Any:
        query_text = kwargs.get("query")
        if not query_text:
            raise ValueError("query is required to delete relationships")
        upper_query = query_text.upper()
        if "DELETE" not in upper_query and "DETACH" not in upper_query:
            raise ValueError("INTEGRITY ERROR: delete relationship queries must delete edges")

        parameters = self.__clean_placeholders(kwargs.get("placeholder") or {})
        self.__auto_open()
        try:
            result = self.__run_write(query_text, parameters)
            result_list = list(result)
            self.__publish_with_operation("delete", result_list, **kwargs)
            return result_list
        finally:
            self.__auto_close()

    # ---- Support utilities ----------------------------------------------------
    def __map_with_schema(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if self.schema_file and self.schema_name:
            return map_to_schema(data, self.schema_file, self.schema_name)
        return dict(data)

    def __publish_with_operation(self, operation: str, payload: Any, **kwargs: Any) -> None:
        publish_kwargs = dict(kwargs)
        merged_attributes = dict(publish_kwargs.get("sns_attributes", {}))
        merged_attributes["operation"] = operation
        publish_kwargs["sns_attributes"] = merged_attributes
        self.publish(payload, **publish_kwargs)

    def __merge_payload(self, original: Dict[str, Any], incoming: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        return merge(original, incoming, **kwargs)

    def __serialize(self, records: Any, *, node_label: Optional[str], serialize: bool = True, search: bool = False,) -> Any:
        return serialize_records(records, label=node_label, serialize=serialize, search=search)

    def __clean_placeholders(self, placeholder: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if placeholder is None:
            return {}
        return convert_placeholders(placeholder)

    def __resolve_bolt_config(self) -> Dict[str, Any]:
        return self.neptune or self.bolt

    def __execute_write(self, callback) -> Any:
        if not self._session:
            raise ValueError("session has not been opened")
        session = self._session
        if hasattr(session, "execute_write"):
            return session.execute_write(callback)
        if hasattr(session, "write_transaction"):
            return session.write_transaction(callback)
        raise RuntimeError("Unsupported neo4j session: missing write helpers")

    def __run_read(self, query: str, parameters: Dict[str, Any]) -> Any:
        if not self._session:
            raise ValueError("session has not been opened")
        return self._session.run(query, parameters)

    def __run_write(self, query: str, parameters: Dict[str, Any]) -> Any:
        if not self._session:
            raise ValueError("session has not been opened")
        return self._session.run(query, parameters)

    def __default_create_query(self, node_label: str) -> str:
        return f"CREATE (n:{node_label}) SET n = $placeholder RETURN n"

    def __default_update_query(self, node_label: str, identifier: str, idempotence_key: str) -> str:
        return (
            f"MATCH (n:{node_label}) "
            f"WHERE n.{identifier} = $id AND n.{idempotence_key} = $version "
            f"SET n = $placeholder RETURN n"
        )

    def __match(self, query: str, placeholder: Optional[Dict[str, Any]], *, node_label: Optional[str], serialize: bool, search: bool,) -> Any:
        self.__auto_open()
        try:
            parameters = self.__clean_placeholders(placeholder)
            result = self.__run_read(query, parameters)
            records = list(result)
            if serialize:
                return self.__serialize(records, node_label=node_label, serialize=True, search=search)
            return records
        finally:
            self.__auto_close()

    def __get_before_delete(self, node_label: str, identifier: str, delete_identifier: Any, **kwargs: Any) -> Dict[str, Any]:
        read_query = kwargs.get("read_query") or f"MATCH (n:{node_label}) WHERE n.{identifier} = $id RETURN n LIMIT 1"
        records = self.__match(
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

    def __perform_delete(self, node_label: str, identifier: str, delete_identifier: Any, delete_query: Optional[str]) -> None:
        delete_query = delete_query or (
            f"MATCH (n:{node_label}) WHERE n.{identifier} = $id WITH n LIMIT 1 DETACH DELETE n"
        )
        parameters = self.__clean_placeholders({"id": delete_identifier})
        self.__auto_open()
        try:
            self.__run_write(delete_query, parameters)
        finally:
            self.__auto_close()

    def __first_node(self, record: Any) -> Optional[Any]:
        if hasattr(record, "values"):
            for value in record.values():
                if self.__is_node(value):
                    return value
        return None

    @staticmethod
    def __is_node(value: Any) -> bool:
        try:
            from neo4j.graph import Node  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise RuntimeError("neo4j package is required for CypherAdapter") from exc
        return isinstance(value, Node)
