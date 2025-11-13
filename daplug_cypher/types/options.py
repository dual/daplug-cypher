from __future__ import annotations

from typing import Any, Dict, Optional, TypedDict
from typing import Literal


class AdapterSerializationOptions(TypedDict, total=False):
    node_label: Optional[str]
    serialize: bool
    search: bool


class SerializeRecordsOptions(TypedDict, total=False):
    label: Optional[str]
    serialize: bool
    search: bool


class PublishOptions(TypedDict, total=False):
    sns_attributes: Dict[str, Any]
    fifo_group_id: Optional[str]
    fifo_duplication_id: Optional[str]


class MergeOptions(TypedDict, total=False):
    update_list_operation: Literal["add", "remove", "replace"]
    update_dict_operation: Literal["upsert", "remove"]


class ReadBeforeDeleteOptions(TypedDict, total=False):
    read_query: str


class AdapterConfig(TypedDict, total=False):
    auto_connect: bool
    bolt: Dict[str, Any]
    neptune: Dict[str, Any]
    schema_file: str
    schema: str
    validate_schema: bool
    driver_config: Dict[str, Any]
    sns_arn: str
    sns_endpoint: str
    sns_attributes: Dict[str, Any]


class _CreateParamsRequired(TypedDict):
    data: Dict[str, Any]


class CreateParams(_CreateParamsRequired, total=False):
    node: str
    label: str
    query: str
    sns_attributes: Dict[str, Any]
    fifo_group_id: Optional[str]
    fifo_duplication_id: Optional[str]


class _ReadParamsRequired(TypedDict):
    query: str


class ReadParams(_ReadParamsRequired, total=False):
    placeholder: Dict[str, Any]
    node: str
    label: str
    serialize: bool
    search: bool


class _QueryParamsRequired(TypedDict):
    query: str


class QueryParams(_QueryParamsRequired, total=False):
    placeholder: Dict[str, Any]


class _UpdateParamsRequired(TypedDict):
    data: Dict[str, Any]
    query: str
    identifier: str
    idempotence_key: str
    original_idempotence_value: Any


class UpdateParams(_UpdateParamsRequired, total=False):
    node: str
    label: str
    placeholder: Dict[str, Any]
    search: bool
    update_query: str
    sns_attributes: Dict[str, Any]
    fifo_group_id: Optional[str]
    fifo_duplication_id: Optional[str]
    update_list_operation: Literal["add", "remove", "replace"]
    update_dict_operation: Literal["upsert", "remove"]


class _DeleteParamsRequired(TypedDict):
    delete_identifier: Any
    identifier: str


class DeleteParams(_DeleteParamsRequired, total=False):
    node: str
    label: str
    read_query: str
    delete_query: str
    sns_attributes: Dict[str, Any]
    fifo_group_id: Optional[str]
    fifo_duplication_id: Optional[str]


class _RelationshipParamsRequired(TypedDict):
    query: str


class RelationshipParams(_RelationshipParamsRequired, total=False):
    placeholder: Dict[str, Any]
    sns_attributes: Dict[str, Any]
    fifo_group_id: Optional[str]
    fifo_duplication_id: Optional[str]
