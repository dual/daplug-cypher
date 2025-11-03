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


@mock.patch.object(boto3, "client")
def test_publish_includes_fifo_metadata(client_mock: mock.Mock) -> None:
    sns_mock = mock.Mock()
    client_mock.return_value = sns_mock

    publisher.publish(
        arn="arn",
        data={"k": "v"},
        fifo_group_id="group-1",
        fifo_duplication_id="dup-1",
        attributes={"custom": {"DataType": "String", "StringValue": "value"}},
    )

    args, kwargs = sns_mock.publish.call_args
    assert kwargs["MessageGroupId"] == "group-1"
    assert kwargs["MessageDeduplicationId"] == "dup-1"
    assert kwargs["MessageAttributes"]["custom"]["StringValue"] == "value"


@mock.patch.object(boto3, "client")
def test_publish_logs_on_exception(client_mock: mock.Mock) -> None:
    sns_mock = mock.Mock()
    sns_mock.publish.side_effect = Exception("boom")
    client_mock.return_value = sns_mock

    with mock.patch("daplug_cypher.common.logger.log") as log_mock:
        publisher.publish(arn="arn", data={"k": "v"})

    log_mock.assert_called_once()
