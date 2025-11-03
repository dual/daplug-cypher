"""Shared pytest fixtures for integration tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pytest
from neo4j import GraphDatabase

# Ensure project root is importable when running pytest directly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_DEFAULT_SETTINGS = {
    "url": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "password",
}


def _build_settings(prefix: str, fallback: Dict[str, str]) -> Dict[str, Any]:
    url = os.getenv(f"{prefix}_BOLT_URL", fallback["url"])
    user = os.getenv(f"{prefix}_USER", fallback["user"])
    password = os.getenv(f"{prefix}_PASSWORD", fallback["password"])
    auth: Optional[Tuple[str, Optional[str]]] = None
    if user:
        auth = (user, password)
    return {
        "url": url,
        "user": user,
        "password": password,
        "auth": auth,
    }


def _ensure_connection(settings: Dict[str, Any], label: str) -> None:
    auth = settings.get("auth")
    driver = GraphDatabase.driver(settings["url"], auth=auth) if auth else GraphDatabase.driver(settings["url"])
    try:
        with driver.session() as session:
            session.run("RETURN 1").consume()
    except Exception as exc:  # pragma: no cover - defensive guard
        pytest.skip(f"{label} endpoint unavailable at {settings['url']}: {exc}")
    finally:
        driver.close()


@pytest.fixture(scope="session")
def neo4j_connection_settings() -> Dict[str, Any]:
    settings = _build_settings("NEO4J", _DEFAULT_SETTINGS)
    _ensure_connection(settings, "Neo4j")
    return settings


@pytest.fixture(scope="session")
def neptune_connection_settings(neo4j_connection_settings: Dict[str, Any]) -> Dict[str, Any]:
    fallback = {
        "url": neo4j_connection_settings["url"],
        "user": neo4j_connection_settings["user"],
        "password": neo4j_connection_settings["password"],
    }
    settings = _build_settings("NEPTUNE", fallback)
    _ensure_connection(settings, "Neptune")
    return settings
