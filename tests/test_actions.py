import pytest
from unittest.mock import patch, MagicMock
from chaoskafka.actions import (
    delete_kafka_topic,
    rebalance_consumer_group,
    delete_consumer_group)
from chaoslib.exceptions import FailedActivity


@patch('chaoskafka.actions.AdminClient')
def test_delete_kafka_topic(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin_instance.delete_topics.return_value = {
        "test_topic": mock_future
        }
    mock_admin_client.return_value = mock_admin_instance

    delete_kafka_topic(bootstrap_servers="localhost:9092", topic="test_topic")

    mock_admin_client.assert_called_with(
        {'bootstrap.servers': 'localhost:9092'}
        )

    mock_admin_instance.delete_topics.assert_called_once_with(
        ['test_topic'], operation_timeout=30)


@patch('chaoskafka.actions.AdminClient')
def test_delete_kafka_topic_failed(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.side_effect = Exception("Deletion failed")
    mock_admin_instance.delete_topics.return_value = {
        "test_topic": mock_future
        }
    mock_admin_client.return_value = mock_admin_instance

    with pytest.raises(FailedActivity) as exc_info:
        delete_kafka_topic(
            bootstrap_servers="localhost:9092",
            topic="test_topic"
        )

    assert "Failed to delete topic test_topic: Deletion failed" in str(
        exc_info.value
        )


def test_rebalance_consumer_group_topic_None():
    with pytest.raises(FailedActivity) as exc_info:
        rebalance_consumer_group(
            bootstrap_servers="localhost:9092",
            topic=None,
            group_id="test_group"
        )

    assert "The topic to subscribe is None" in str(exc_info.value)


@patch('chaoskafka.actions.sleep', return_value=None)
@patch('chaoskafka.actions.Consumer')
def test_rebalance_consumer_group(mock_consumer, mock_sleep):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    rebalance_consumer_group(
        bootstrap_servers="localhost:9092",
        topic="test_topic",
        group_id="test_group"
    )
    mock_consumer.assert_called_with(
        {'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
        )

    mock_consumer.return_value.subscribe.assert_called_with(['test_topic'])


@patch('chaoskafka.actions.Consumer')
def test_rebalance_consumer_group_failed(mock_consumer):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    mock_consumer.side_effect = Exception("Failed instance consumer group")
    expected_error_message = (
        "Failed to rebalance consumer group test_group: "
        "Failed instance consumer group"
    )

    with pytest.raises(FailedActivity) as exc_info:
        rebalance_consumer_group(
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            group_id="test_group"
        )

    assert expected_error_message in str(exc_info.value)


@patch('chaoskafka.actions.AdminClient')
def test_delete_consumer_group(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin_instance.delete_consumer_groups.return_value = {
        "test_group": mock_future
        }
    mock_admin_client.return_value = mock_admin_instance

    delete_consumer_group(
        bootstrap_servers="localhost:9092",
        group_id="test_group"
    )

    mock_admin_client.assert_called_with(
        {'bootstrap.servers': 'localhost:9092'}
        )

    mock_admin_instance.delete_consumer_groups.assert_called_once_with(
        ['test_group'], request_timeout=10
        )


@patch('chaoskafka.actions.AdminClient')
def test_delete_consumer_group_failed(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.side_effect = Exception("Deletion failed")
    mock_admin_instance.delete_consumer_groups.return_value = {
        "test_group": mock_future
        }
    mock_admin_client.return_value = mock_admin_instance
    expected_error_message = (
        "Failed to delete consumer group test_group: Deletion failed"
    )

    with pytest.raises(FailedActivity) as exc_info:
        delete_consumer_group(
            bootstrap_servers="localhost:9092",
            group_id="test_group"
        )

    assert expected_error_message in str(
        exc_info.value
        )

