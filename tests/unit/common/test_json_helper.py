"""Tests for JSON helper utilities."""

from daplug_cypher.common import json_helper


def test_try_decode_json_success() -> None:
    result = json_helper.try_decode_json('{"key": 1}')
    assert result == {"key": 1}


def test_try_decode_json_failure_returns_original() -> None:
    value = object()
    assert json_helper.try_decode_json(value) is value


def test_try_encode_json_failure_returns_original() -> None:
    # objects without default serializer raise TypeError
    value = json_helper.try_encode_json(set([1, 2]))
    assert value == {1, 2}
