"""Tests for SNS publisher helper."""

from __future__ import annotations

from unittest import mock

import boto3

from daplug_cypher.common import publisher


@mock.patch.object(boto3, "client")
def test_publish_sends_message(client_mock: mock.Mock) -> None:
    sns_mock = mock.Mock()
    client_mock.return_value = sns_mock

    publisher.publish(arn="arn", data={"key": "value"}, region="us-east-1")

    client_mock.assert_called_once_with("sns", region_name="us-east-1", endpoint_url=None)
    sns_mock.publish.assert_called_once()


def test_publish_noop_without_required_fields() -> None:
    with mock.patch.object(boto3, "client") as client_mock:
        publisher.publish(data={})
    client_mock.assert_not_called()
