from typing import Dict

from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets

from confluent_kafka.admin import AdminClient
from confluent_kafka import (
    TopicPartition,
    Consumer,
    KafkaException)
from chaoskafka.utils import check_topic_exist

import json
import logging

__all__ = [
    "describe_kafka_topic",
    "all_replicas_in_sync",
    "cluster_doesnt_have_under_replicated_partitions",
    "check_consumer_lag_under_threshold"
    ]

logger = logging.getLogger("chaostoolkit")


def describe_kafka_topic(
    bootstrap_servers: str = None, topic: str = None,
    configuration: Configuration = None, secrets: Secrets = None
) -> Dict:
    """
    The next function is to:

    * Describe a Kafka topic and its partitions.
    Check first if the topic exist and then describe the topic.

    """
    try:
        admin_client = AdminClient(
            {'bootstrap.servers': bootstrap_servers}
        )

        cluster_metadata = admin_client.list_topics(timeout=10)

        check_topic_exist(topic, cluster_metadata.topics)

        topic_metadata = cluster_metadata.topics[topic]

        topic_info = {
            "Topic": topic,
            "Partitions": len(topic_metadata.partitions),
            "partitions_info": {}
        }

        for partition_id, partition in topic_metadata.partitions.items():
            topic_info["partitions_info"][str(partition_id)] = {
                "Leader": partition.leader,
                "Replicas": list(partition.replicas),
                "Isr": list(partition.isrs)
            }

        return json.dumps(topic_info, indent=2)
    except KafkaException as e:
        raise FailedActivity(f"Failed to describe topic: {e}") from e


def all_replicas_in_sync(
    bootstrap_servers: str = None, topic: str = None,
    configuration: Configuration = None, secrets: Secrets = None
) -> bool:

    """
    The next function is to:
    * this is a probe that checks if all replicas for each partition are
    in sync with the leader. If they are, then the topic is healthy.
    """
    try:
        admin_client = AdminClient(
            {'bootstrap.servers': bootstrap_servers}
        )

        cluster_metadata = admin_client.list_topics(timeout=10)

        check_topic_exist(topic, cluster_metadata.topics)

        topic_metadata = cluster_metadata.topics[topic]

        return all(
            partition.isrs == partition.replicas
            for _, partition in topic_metadata.partitions.items()
        )
    except KafkaException as e:
        raise FailedActivity(
            "Some problem checking if all replicas are in sync: "
            f"{e}"
        ) from e


def cluster_doesnt_have_under_replicated_partitions(
    bootstrap_servers: str = None,
    configuration: Configuration = None, secrets: Secrets = None
) -> bool:

    """
    The next function is to:
    * Check if the cluster has under replicated partitions.
    """

    try:
        admin_client = AdminClient(
            {'bootstrap.servers': bootstrap_servers}
        )

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
    bootstrap_servers: str = None, group_id: str = None,
    topic: str = None, threshold: int = 0, partition: int = None,
    configuration: Configuration = None, secrets: Secrets = None
) -> bool:

    """
    The next function is to:

    * Check if the consumer lag is under a certain threshold in any partition
        or some specific partition.
    """
    try:
        consumer = Consumer(
            {'bootstrap.servers': bootstrap_servers, 'group.id': group_id}
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
            (lo, hi) = consumer.get_watermark_offsets(p, timeout=10,
                                                      cached=False)

            if p.offset < 0:
                print("entro aqui")
                lag = "%d" % (hi - lo)
            else:
                lag = "%d" % (hi - p.offset)
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
            "Failed to calculate the lag of the consumer group: "
            f"{e}"
        ) from e
