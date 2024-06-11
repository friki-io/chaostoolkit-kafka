import pytest
from unittest.mock import patch, MagicMock, ANY

import json
import threading

from confluent_kafka import KafkaError

from chaoskafka.actions import (
    delete_kafka_topic,
    rebalance_consumer_group,
    delete_consumer_group,
    produce_messages,
    consume_messages,
    __consume_messages,
)

from chaoslib.exceptions import FailedActivity


@patch("chaoskafka.actions.AdminClient")
def test_delete_kafka_topic(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin_instance.delete_topics.return_value = {"test_topic": mock_future}
    mock_admin_client.return_value = mock_admin_instance

    delete_kafka_topic(bootstrap_servers="localhost:9092", topic="test_topic")

    mock_admin_client.assert_called_with(
        {"bootstrap.servers": "localhost:9092"}
    )

    mock_admin_instance.delete_topics.assert_called_once_with(
        ["test_topic"], operation_timeout=30
    )


@patch("chaoskafka.actions.AdminClient")
def test_delete_kafka_topic_failed(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.side_effect = Exception("Deletion failed")
    mock_admin_instance.delete_topics.return_value = {"test_topic": mock_future}
    mock_admin_client.return_value = mock_admin_instance

    with pytest.raises(FailedActivity) as exc_info:
        delete_kafka_topic(
            bootstrap_servers="localhost:9092", topic="test_topic"
        )

    assert "Failed to delete topic test_topic: Deletion failed" in str(
        exc_info.value
    )


def test_rebalance_consumer_group_topic_None():
    with pytest.raises(FailedActivity) as exc_info:
        rebalance_consumer_group(
            bootstrap_servers="localhost:9092",
            topic=None,
            group_id="test_group",
        )

    assert "The topic to subscribe is None" in str(exc_info.value)


@patch("chaoskafka.actions.sleep", return_value=None)
@patch("chaoskafka.actions.Consumer")
def test_rebalance_consumer_group(mock_consumer, mock_sleep):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance
    rebalance_consumer_group(
        bootstrap_servers="localhost:9092",
        topic="test_topic",
        group_id="test_group",
    )
    mock_consumer.assert_called_with(
        {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
    )

    mock_consumer.return_value.subscribe.assert_called_with(["test_topic"])


@patch("chaoskafka.actions.Consumer")
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
            group_id="test_group",
        )

    assert expected_error_message in str(exc_info.value)


@patch("chaoskafka.actions.AdminClient")
def test_delete_consumer_group(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin_instance.delete_consumer_groups.return_value = {
        "test_group": mock_future
    }
    mock_admin_client.return_value = mock_admin_instance

    delete_consumer_group(
        bootstrap_servers="localhost:9092", group_id="test_group"
    )

    mock_admin_client.assert_called_with(
        {"bootstrap.servers": "localhost:9092"}
    )

    mock_admin_instance.delete_consumer_groups.assert_called_once_with(
        ["test_group"], request_timeout=10
    )


@patch("chaoskafka.actions.AdminClient")
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
            bootstrap_servers="localhost:9092", group_id="test_group"
        )

    assert expected_error_message in str(exc_info.value)


@patch("chaoskafka.actions.Producer")
def test_produce_messages_success(mock_producer):
    mock_producer_instance = MagicMock()
    mock_producer.return_value = mock_producer_instance

    bootstrap_servers = "localhost:9092"
    client_id = "chaostoolkit"
    topic = "test_topic"
    messages = ["message1"]
    partition = 0

    produce_messages(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        topic=topic,
        messages=messages,
        partition=partition,
    )

    mock_producer_instance.produce.assert_called_with(
        topic, value=b"message1", partition=partition, on_delivery=ANY
    )

    mock_producer_instance.flush.assert_called_once()


def test_produce_messages_topic_None():
    bootstrap_servers = "localhost:9092"
    client_id = "chaostoolkit"
    messages = ["message1"]
    partition = 0

    with pytest.raises(FailedActivity) as exc_info:
        produce_messages(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            messages=messages,
            partition=partition,
        )

    assert "The topic to produce a message is None" in str(exc_info.value)


@patch("chaoskafka.actions.Producer")
def test_produce_messages_failed(mock_producer):
    mock_producer_instance = MagicMock()
    mock_producer.return_value = mock_producer_instance
    mock_producer_instance.produce.side_effect = Exception("Error")

    expected_error_message = (
        "Failed to produce message in topic test_topic : Error"
    )

    bootstrap_servers = "localhost:9092"
    client_id = "chaostoolkit"
    topic = "test_topic"
    messages = ["message1"]
    partition = 0

    with pytest.raises(FailedActivity) as exc_info:
        produce_messages(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            topic=topic,
            messages=messages,
            partition=partition,
        )

    assert expected_error_message in str(exc_info.value)


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test_consume_messages_success(mock_consumer, mock_describe_kafka_topic):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    # Mock the topic description
    mock_describe_kafka_topic.return_value = json.dumps({"partitions": 3})

    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = "test_topic"
    partition = 0
    offset = 0
    num_messages = 2

    def mock_poll(timeout):
        msg = MagicMock()
        msg.partition.return_value = partition
        msg.offset.return_value = offset
        msg.value.return_value = b"test_message"
        msg.topic.return_value = topic
        msg.error.return_value = None
        return msg

    mock_consumer_instance.poll.side_effect = [
        mock_poll(1.0),
        mock_poll(1.0),
        None,
    ]

    result = consume_messages(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic,
        partition=partition,
        offset=offset,
        num_messages=num_messages,
    )

    assert len(result) == num_messages
    assert result[0]["value"] == "test_message"


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test_consume_messages_topic_none(mock_consumer, mock_describe_kafka_topic):
    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = None
    partition = 0
    offset = 0
    num_messages = 2

    with pytest.raises(FailedActivity) as exc_info:
        consume_messages(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic=topic,
            partition=partition,
            offset=offset,
            num_messages=num_messages,
        )
    assert "The topic to consume a message is None" in str(exc_info.value)


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test_consume_messages_partition_not_exist(
    mock_consumer, mock_describe_kafka_topic
):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    mock_describe_kafka_topic.return_value = json.dumps({"partitions": 2})

    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = "test_topic"
    partition = 3
    offset = 0
    num_messages = 2

    with pytest.raises(FailedActivity) as exc_info:
        consume_messages(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic=topic,
            partition=partition,
            offset=offset,
            num_messages=num_messages,
        )

    expected_error_message = "Partition 3 does not exist in topic test_topic"

    assert expected_error_message in str(exc_info.value)


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test_consume_messages_error(mock_consumer, mock_describe_kafka_topic):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = "test_topic"
    partition = 0
    offset = 0
    num_messages = 1

    def mock_poll(timeout):
        msg = MagicMock()
        msg.error.return_value = True
        return msg

    mock_consumer_instance.poll.side_effect = [mock_poll(1.0), None]

    with pytest.raises(FailedActivity):
        consume_messages(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic=topic,
            partition=partition,
            offset=offset,
            num_messages=num_messages,
        )


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test___consume_messages_success(mock_consumer, mock_describe_kafka_topic):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = "test_topic"
    partition = 0
    offset = 0
    num_messages = 2
    client_id = "0_asign_client"
    stop_event = threading.Event()
    messages = []

    def mock_poll(timeout):
        msg = MagicMock()
        msg.partition.return_value = partition
        msg.offset.return_value = offset
        msg.value.return_value = b"test_message"
        msg.topic.return_value = topic
        msg.error.return_value = None
        return msg

    mock_consumer_instance.poll.side_effect = [
        mock_poll(1.0),
        mock_poll(1.0),
        None,
    ]

    __consume_messages(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic,
        partition=partition,
        offset=offset,
        num_messages=num_messages,
        client_id=client_id,
        stop_event=stop_event,
        messages=messages,
    )

    assert len(messages) == num_messages
    assert messages[0]["value"] == "test_message"

    mock_consumer_instance.close.assert_called_once()
    mock_consumer_instance.commit.assert_called()


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test_consume_messages_none_message(
    mock_consumer, mock_describe_kafka_topic
):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    mock_describe_kafka_topic.return_value = json.dumps({"partitions": 3})

    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = "test_topic"
    partition = 0
    offset = 0
    num_messages = 2

    mock_consumer_instance.poll.side_effect = [None, None]

    result = consume_messages(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic,
        partition=partition,
        offset=offset,
        num_messages=num_messages,
    )

    assert len(result) == 0


@patch("chaoskafka.actions.describe_kafka_topic")
@patch("chaoskafka.actions.Consumer")
def test_consume_messages_error_message(
    mock_consumer, mock_describe_kafka_topic
):
    mock_consumer_instance = MagicMock()
    mock_consumer.return_value = mock_consumer_instance

    # Mock the topic description
    mock_describe_kafka_topic.return_value = json.dumps({"partitions": 3})

    bootstrap_servers = "localhost:9092"
    group_id = "chaostoolkit"
    topic = "test_topic"
    partition = 0
    offset = 0
    num_messages = 2

    def mock_poll(timeout):
        msg = MagicMock()
        error = MagicMock()
        error.code.return_value = KafkaError._PARTITION_EOF + 1
        msg.error.return_value = error
        return msg

    mock_consumer_instance.poll.side_effect = [mock_poll(1.0)]

    consume_messages(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic,
        partition=partition,
        offset=offset,
        num_messages=num_messages,
    )
    # TODO: Exceptions propagation
