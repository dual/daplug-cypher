"""Tests for logger helper."""

from __future__ import annotations

from unittest import mock

from daplug_cypher.common import logger


def test_log_outputs_when_not_unittest(monkeypatch) -> None:
    monkeypatch.delenv("RUN_MODE", raising=False)
    with mock.patch("builtins.print") as print_mock:
        logger.log(level="INFO", log={"msg": "value"})
    print_mock.assert_called_once()


def test_log_suppressed_in_unittest_mode(monkeypatch) -> None:
    monkeypatch.setenv("RUN_MODE", "unittest")
    with mock.patch("builtins.print") as print_mock:
        logger.log(level="INFO", log={"msg": "value"})
    print_mock.assert_not_called()
