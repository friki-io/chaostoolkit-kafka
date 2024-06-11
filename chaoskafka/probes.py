from typing import Dict, Optional
import json
import logging

from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicPartition, Consumer, KafkaException

from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets

from chaoskafka.utils import check_topic_exist


__all__ = [
    "describe_kafka_topic",
    "all_replicas_in_sync",
    "cluster_doesnt_have_under_replicated_partitions",
    "check_consumer_lag_under_threshold",
    "topic_has_no_offline_partitions",
]

logger = logging.getLogger("chaostoolkit")


def describe_kafka_topic(
    bootstrap_servers: str = None,
    topic: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> Dict:
    """
    Describe a Kafka topic and its partitions.

    This function retrieves metadata for a specified Kafka topic, including
    the number of partitions and detailed information about each partition.
    It first checks if the topic exists in the Kafka cluster.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        topic (str): The name of the Kafka topic to describe.

    Returns:
        Dict: A dictionary containing detailed information about the topic and
        its partitions.

    Raises:
        FailedActivity: If some kafka exception was raised.
    """

    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        cluster_metadata = admin_client.list_topics(timeout=10)

        check_topic_exist(topic, cluster_metadata.topics)

        topic_metadata = cluster_metadata.topics[topic]

        topic_info = {
            "topic": topic,
            "partitions": len(topic_metadata.partitions),
            "partitions_info": {},
        }

        for partition_id, partition in topic_metadata.partitions.items():
            topic_info["partitions_info"][str(partition_id)] = {
                "leader": partition.leader,
                "replicas": list(partition.replicas),
                "isr": list(partition.isrs),
            }

        return json.dumps(topic_info, indent=2)
    except KafkaException as e:
        raise FailedActivity(f"Failed to describe topic: {e}") from e


def all_replicas_in_sync(
    bootstrap_servers: str = None,
    topic: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> bool:
    """
    Check if all replicas for each partition of a Kafka topic are in sync with\
    the leader.

    This function verifies the health of a specified Kafka topic by ensuring
    that all replicas for each partition are in sync with the leader. If all
    replicas are in sync, the topic is considered healthy.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        topic (str): The name of the Kafka topic to check.

    Returns:
        bool: True if all replicas for each partition are in sync with the
        leader, False otherwise.

    Raises:
        FailedActivity: If some kafka exception was raised.
    """

    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        cluster_metadata = admin_client.list_topics(timeout=10)

        check_topic_exist(topic, cluster_metadata.topics)

        topic_metadata = cluster_metadata.topics[topic]

        return all(
            partition.isrs == partition.replicas
            for _, partition in topic_metadata.partitions.items()
        )
    except KafkaException as e:
        raise FailedActivity(
            "Some problem checking if all replicas are in sync: " f"{e}"
        ) from e


def topic_has_no_offline_partitions(
    bootstrap_servers: str = None,
    topic: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> bool:
    """
    Check if a Kafka topic has no offline partitions.

    This function verifies the health of a specified Kafka topic by checking
    if all its partitions have an active leader and at least one in-sync
    replica (ISR). If some partition is offline,the topic is not considered
    healthy.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        topic (str): The name of the Kafka topic to check.

    Returns:
        bool: True if all partitions are online and don't have offline
        replicas, False otherwise.

    Raises:
        FailedActivity: If some kafka exception was raised.
    """
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        cluster_metadata = admin_client.list_topics(timeout=10)

        check_topic_exist(topic, cluster_metadata.topics)

        topic_metadata = cluster_metadata.topics[topic]

        # Check if all partitions have a leader and at least one ISR
        return all(
            partition.leader != -1 and partition.isrs
            for _, partition in topic_metadata.partitions.items()
        )
    except KafkaException as e:
        raise FailedActivity(
            "Some problem checking if the topic has no offline partitions: "
            f"{e}"
        ) from e


def cluster_doesnt_have_under_replicated_partitions(
    bootstrap_servers: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> bool:
    """
    Check if the Kafka cluster has under-replicated partitions.

    This function verifies the health of the entire Kafka cluster by checking
    if any partition is under-replicated. A partition is considered
    under-replicated if the number of in-sync replicas (ISRs) is less
    than the total number of replicas.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.

    Returns:
        bool: True if no partition in the cluster is under-replicated,
        False otherwise.

    Raises:
        KafkaException: If there is an underlying Kafka-related error.
    """

    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        cluster_metadata = admin_client.list_topics(timeout=10)

        return all(
            all(
                len(partition.isrs) == len(partition.replicas)
                for _, partition in topic.partitions.items()
            )
            for topic in cluster_metadata.topics.values()
        )
    except KafkaException as e:
        raise FailedActivity(
            "Failed to check if the cluster has under replicated partitions: "
            f"{e}"
        ) from e


def check_consumer_lag_under_threshold(
    bootstrap_servers: str = None,
    group_id: str = None,
    topic: str = None,
    threshold: int = 0,
    partition: Optional[int] = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> bool:
    """
    Check if the consumer lag is under a certain threshold for a
    specific partition or all partitions.

    This function checks the consumer lag for a specified Kafka topic and
    consumer group, and verifies if the lag is under a defined threshold.
    It can check the lag for either a specific partition or all partitions.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        group_id (str): The consumer group ID.
        topic (str): The name of the Kafka topic.
        threshold (int): The maximum allowable lag threshold.
        partition (Optional[int]): The specific partition to check. If None,
        checks all partitions.

    Returns:
        bool: True if the consumer lag is under the threshold for the
        specified partition or all partitions, False otherwise.

    Raises:
        FailedActivity: If there is an issue accessing the cluster metadata.
    """

    try:
        consumer = Consumer(
            {"bootstrap.servers": bootstrap_servers, "group.id": group_id}
        )
        metadata = consumer.list_topics(topic, timeout=10)
        if metadata.topics[topic].error is not None:
            raise KafkaException(metadata.topics[topic].error)
        partitions = [
            TopicPartition(topic, p) for p in metadata.topics[topic].partitions
        ]
        committed = consumer.committed(partitions, timeout=10)
        lags = []
        for p in committed:
            (lo, hi) = consumer.get_watermark_offsets(
                p, timeout=10, cached=False
            )

            lag = "%d" % (hi - lo) if p.offset < 0 else "%d" % (hi - p.offset)
            lags.append(int(lag))
        consumer.close()
        logger.debug(f"Consumer group {group_id} lags: {lags}")
        logger.debug(f"partition: {partition}")
        return (
            all(threshold > lag_value for lag_value in lags)
            if partition is None
            else threshold > lags[partition]
        )
    except KafkaException as e:
        raise FailedActivity(
            "Failed to calculate the lag of the consumer group: " f"{e}"
        ) from e
