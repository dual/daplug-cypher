"""Utilities for preparing Cypher parameter dictionaries."""

from typing import Any, Dict


def convert_placeholders(placeholder: Dict[str, Any]) -> Dict[str, Any]:
    """Convert numeric strings and nested structures for Cypher parameters."""
    return {
        key: _convert_value(value)
        for key, value in placeholder.items()
    }


def _convert_value(value: Any) -> Any:
    if isinstance(value, dict):
        return convert_placeholders(value)
    if isinstance(value, list):
        return [_convert_value(item) for item in value]
    if isinstance(value, str) and _is_numeric(value):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _is_numeric(value: str) -> bool:
    if not value:
        return False
    if value.startswith("-"):
        return value[1:].isdigit()
    return value.isdigit()
