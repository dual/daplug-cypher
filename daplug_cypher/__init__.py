from typing import Any
from .adapter import CypherAdapter


def adapter(**kwargs: Any) -> CypherAdapter:
    return CypherAdapter(**kwargs)

__all__ = ["adapter", "CypherAdapter"]
