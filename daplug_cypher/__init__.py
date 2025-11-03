"""Public interface for the daplug_cypher package."""

from typing import Any

from .adapter import CypherAdapter


def adapter(**kwargs: Any) -> CypherAdapter:
    """Factory helper returning a configured CypherAdapter instance."""
    return CypherAdapter(**kwargs)


__all__ = ["adapter", "CypherAdapter"]
