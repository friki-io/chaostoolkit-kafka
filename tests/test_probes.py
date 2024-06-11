import pytest
from unittest.mock import patch, MagicMock
from chaoslib.exceptions import FailedActivity
from chaoskafka.probes import (
    describe_kafka_topic,
    all_replicas_in_sync,
    cluster_doesnt_have_under_replicated_partitions,
    check_consumer_lag_under_threshold,
    topic_has_no_offline_partitions,
)
import json
from confluent_kafka import KafkaException


@patch("chaoskafka.probes.AdminClient")
def test_nonexistent_topic(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_admin_instance.list_topics.return_value = MagicMock(topics={})

    with pytest.raises(FailedActivity) as excinfo:
        describe_kafka_topic(
            bootstrap_servers="localhost:9092", topic="nonexistent_topic"
        )

    assert "Topic 'nonexistent_topic' does not exist" in str(excinfo.value)


@patch("chaoskafka.probes.AdminClient")
def test_describe_kafka_topic_success(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_cluster_metadata = MagicMock()
    mock_topic_metadata = MagicMock()

    mock_cluster_metadata.topics = {"test_topic": mock_topic_metadata}
    mock_topic_metadata.partitions = {
        0: MagicMock(leader=1, replicas=[1, 2], isrs=[1, 2])
    }
    mock_admin_instance.list_topics.return_value = mock_cluster_metadata

    result = describe_kafka_topic(
        bootstrap_servers="localhost:9092", topic="test_topic"
    )

    expected_output = {
        "topic": "test_topic",
        "partitions": 1,
        "partitions_info": {
            "0": {"leader": 1, "replicas": [1, 2], "isr": [1, 2]}
        },
    }
    print(f"the result is {result}")
    assert json.loads(result) == expected_output


@patch("chaoskafka.probes.AdminClient")
def test_describe_kafka_topic_exception(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_admin_instance.list_topics.side_effect = KafkaException("Kafka error")

    with pytest.raises(FailedActivity) as excinfo:
        describe_kafka_topic(
            bootstrap_servers="localhost:9092", topic="test_topic"
        )

    assert "Failed to describe topic: Kafka error" in str(excinfo.value)


@patch("chaoskafka.probes.AdminClient")
def test_all_replicas_in_sync_success(mock_admin_client):
    # Setup the mock objects
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_cluster_metadata = MagicMock()
    mock_topic_metadata = MagicMock()

    # Configure the mock to simulate an existing topic
    mock_cluster_metadata.topics = {"test_topic": mock_topic_metadata}
    mock_topic_metadata.partitions = {
        0: MagicMock(leader=1, replicas=[1, 2], isrs=[1, 2]),
        1: MagicMock(Leader=2, replicas=[2, 1], isrs=[2, 1]),
    }
    mock_admin_instance.list_topics.return_value = mock_cluster_metadata

    result = all_replicas_in_sync(
        bootstrap_servers="localhost:9092", topic="test_topic"
    )

    assert result


@patch("chaoskafka.probes.AdminClient")
def test_all_replicas_in_sync_failed(mock_admin_client):
    # Setup the mock objects
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_cluster_metadata = MagicMock()
    mock_topic_metadata = MagicMock()

    # Configure the mock to simulate an existing topic
    mock_cluster_metadata.topics = {"test_topic": mock_topic_metadata}
    mock_topic_metadata.partitions = {
        0: MagicMock(leader=1, replicas=[1, 2], isrs=[1, 2]),
        1: MagicMock(Leader=2, replicas=[2, 1], isrs=[2]),
    }
    mock_admin_instance.list_topics.return_value = mock_cluster_metadata

    result = all_replicas_in_sync(
        bootstrap_servers="localhost:9092", topic="test_topic"
    )

    assert not result


@patch("chaoskafka.probes.AdminClient")
def test_all_replicas_in_sync_exception(mock_admin_client):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_admin_instance.list_topics.side_effect = KafkaException("Kafka error")

    with pytest.raises(FailedActivity) as excinfo:
        all_replicas_in_sync(
            bootstrap_servers="localhost:9092", topic="test_topic"
        )

    expected_error_output = (
        "Some problem checking if all replicas " "are in sync: Kafka error"
    )
    assert expected_error_output in str(excinfo.value)


@patch("chaoskafka.probes.AdminClient")
def test_topic_has_no_offline_partitions_success(mock_admin_client):
    # Setup the mock objects
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_cluster_metadata = MagicMock()
    mock_topic_metadata = MagicMock()

    mock_cluster_metadata.topics = {"test_topic": mock_topic_metadata}
    mock_topic_metadata.partitions = {
        0: MagicMock(leader=1, isrs=[1, 2]),
        1: MagicMock(leader=2, isrs=[2, 1]),
    }
    mock_admin_instance.list_topics.return_value = mock_cluster_metadata

    result = topic_has_no_offline_partitions(
        bootstrap_servers="localhost:9092", topic="test_topic"
    )

    assert result


@patch("chaoskafka.probes.AdminClient")
def test_topic_has_no_offline_partitions_exception(mock_admin_client):
    # Setup the mock objects
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance

    # Configure the mock to raise a KafkaException
    mock_admin_instance.list_topics.side_effect = KafkaException("Kafka error")

    with pytest.raises(FailedActivity) as excinfo:
        topic_has_no_offline_partitions(
            bootstrap_servers="localhost:9092", topic="test_topic"
        )

    expected_error_output = (
        "Some problem checking if the topic has "
        "no offline partitions: Kafka error"
    )
    assert expected_error_output in str(excinfo.value)


@patch("chaoskafka.probes.AdminClient")
def test_cluster_doesnt_have_under_replicated_partitions_success(
    mock_admin_client,
):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance

    mock_partition = MagicMock(isrs=[1, 2, 3], replicas=[1, 2, 3])
    mock_topic = MagicMock(partitions={0: mock_partition})
    mock_admin_instance.list_topics.return_value = MagicMock(
        topics={"topic_1": mock_topic, "topic_2": mock_topic}
    )
    assert (
        cluster_doesnt_have_under_replicated_partitions(
            bootstrap_servers="localhost:9092"
        )
        is True
    )


@patch("chaoskafka.probes.AdminClient")
def test_cluster_doesnt_have_under_replicated_partitions_failed(
    mock_admin_client,
):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_partition_topic_1 = MagicMock(isrs=[1, 2, 3], replicas=[1, 2, 3])
    mock_partition_topic_2 = MagicMock(isrs=[1, 2], replicas=[1, 2, 3])
    mock_topic_1 = MagicMock(partitions={0: mock_partition_topic_1})
    mock_topic_2 = MagicMock(partitions={0: mock_partition_topic_2})
    mock_admin_instance.list_topics.return_value = MagicMock(
        topics={"topic_1": mock_topic_1, "topic_2": mock_topic_2}
    )

    assert (
        cluster_doesnt_have_under_replicated_partitions(
            bootstrap_servers="localhost:9092"
        )
        is False
    )


@patch("chaoskafka.probes.AdminClient")
def test_cluster_doesnt_have_under_replicated_partitions_exception(
    mock_admin_client,
):
    mock_admin_instance = MagicMock()
    mock_admin_client.return_value = mock_admin_instance
    mock_admin_instance.list_topics.side_effect = KafkaException(
        "Network error"
    )
    with pytest.raises(FailedActivity) as excinfo:
        cluster_doesnt_have_under_replicated_partitions(
            bootstrap_servers="localhost:9092"
        )
    expected_output = (
        "Failed to check if the cluster has under replicated "
        "partitions: Network error"
    )
    assert expected_output in str(excinfo.value)


@patch("chaoskafka.probes.Consumer")
def test_consumer_lag_under_threshold_sucess(mock_consumer_class):
    mock_consumer = mock_consumer_class.return_value
    mock_consumer.list_topics.return_value = MagicMock(
        topics={
            "test_topic": MagicMock(error=None, partitions={0: None, 1: None})
        }
    )
    mock_consumer.committed.return_value = [
        MagicMock(topic="test_topic", partition=0, offset=50),
        MagicMock(topic="test_topic", partition=1, offset=30),
    ]
    mock_consumer.get_watermark_offsets.side_effect = [(0, 100), (0, 60)]

    assert (
        check_consumer_lag_under_threshold(
            bootstrap_servers="localhost:9092",
            group_id="group1",
            topic="test_topic",
            threshold=60,
        )
        is True
    )


@patch("chaoskafka.probes.Consumer")
def test_consumer_lag_under_threshold_failed(mock_consumer_class):
    mock_consumer = mock_consumer_class.return_value
    mock_consumer.list_topics.return_value = MagicMock(
        topics={"test_topic": MagicMock(error=None, partitions={0: None})}
    )
    mock_consumer.committed.return_value = [
        MagicMock(topic="test_topic", partition=0, offset=10)
    ]
    mock_consumer.get_watermark_offsets.return_value = (0, 100)
    assert (
        check_consumer_lag_under_threshold(
            bootstrap_servers="localhost:9092",
            group_id="group1",
            topic="test_topic",
            threshold=89,
            partition=0,
        )
        is False
    )


@patch("chaoskafka.probes.Consumer")
def test_consumer_lag_under_threshold_no_offset_sucess(mock_consumer_class):
    mock_consumer = mock_consumer_class.return_value
    mock_consumer.list_topics.return_value = MagicMock(
        topics={"test_topic": MagicMock(error=None, partitions={0: None})}
    )
    mock_consumer.committed.return_value = [
        MagicMock(topic="test_topic", partition=0, offset=-5)
    ]
    mock_consumer.get_watermark_offsets.return_value = (0, 30)  # lag is 30
    assert (
        check_consumer_lag_under_threshold(
            bootstrap_servers="localhost:9092",
            group_id="group1",
            topic="test_topic",
            threshold=31,
            partition=0,
        )
        is True
    )


@patch("chaoskafka.probes.Consumer")
def test_consumer_lag_under_threshold_no_offset_failed(mock_consumer_class):
    mock_consumer = mock_consumer_class.return_value
    mock_consumer.list_topics.return_value = MagicMock(
        topics={"test_topic": MagicMock(error=None, partitions={0: None})}
    )
    mock_consumer.committed.return_value = [
        MagicMock(topic="test_topic", partition=0, offset=-5)
    ]
    mock_consumer.get_watermark_offsets.return_value = (0, 30)  # lag is 30
    assert (
        check_consumer_lag_under_threshold(
            bootstrap_servers="localhost:9092",
            group_id="group1",
            topic="test_topic",
            threshold=29,
            partition=0,
        )
        is False
    )


@patch("chaoskafka.probes.Consumer")
def test_consumer_lag_under_threshold_exception(mock_consumer_class):
    mock_consumer = mock_consumer_class.return_value
    mock_consumer.list_topics.side_effect = KafkaException("Network error")

    with pytest.raises(FailedActivity) as excinfo:
        check_consumer_lag_under_threshold(
            bootstrap_servers="localhost:9092",
            group_id="group1",
            topic="test_topic",
            threshold=10,
        )
    expected_output = (
        "Failed to calculate the lag of the consumer group: " "Network error"
    )
    assert expected_output in str(excinfo.value)


@patch("chaoskafka.probes.Consumer")
def test_consumer_lag_under_threshold_topic_error(mock_consumer_class):
    mock_consumer = mock_consumer_class.return_value
    mock_consumer.list_topics.return_value = MagicMock(
        topics={"test_topic": MagicMock(error="Error fetching metadata")}
    )

    with pytest.raises(FailedActivity) as exc_info:
        check_consumer_lag_under_threshold(
            bootstrap_servers="localhost:9092",
            group_id="group1",
            topic="test_topic",
            threshold=50,
        )
    expected_output = "Failed to calculate the lag of the consumer group: Error fetching metadata"
    assert expected_output in str(exc_info.value)
