from typing import Dict

from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets

from confluent_kafka.admin import KafkaException, AdminClient, OffsetSpec
from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions

import json


__all__ = [
    "describe_kafka_topic",
    "all_replicas_in_sync",
    "cluster_doesnt_have_under_replicated_partitions",
    "check_consumer_lag_under_threshold"
    ]


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

        if topic not in cluster_metadata.topics:
            raise FailedActivity(f"Topic '{topic}' does not exist")

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

      The reason why this lag is calculated using AdminClient and not Consumer is
      beacuse if you join as a consumer group with the same name that the consumer
      that you want to calculate the consumer lag you will rebalance all the consumer
      group

      Consumer_lag = latest_offset_topic - consumer_offset
      this function check if the consumer offset is under certain threshold and return
      True if the lag is under the threshold.
    """
    try:
        admin_client = AdminClient(
            {'bootstrap.servers': bootstrap_servers}
        )

        topic_metadata = admin_client.list_topics(timeout=10)
        partitions = topic_metadata.topics[topic].partitions.keys()

        topic_partitions_offsets = {
            TopicPartition(topic, partition): OffsetSpec._latest
            for partition in partitions
        }
        topic_partitions_consumer_offsets = [
            TopicPartition(topic, p) for p in partitions
        ]
        consumer_group_topic_partition = [
            ConsumerGroupTopicPartitions(group_id, topic_partitions_consumer_offsets)
        ]
        topic_latest_offsets = admin_client.list_offsets(topic_partitions_offsets)
        consumer_topic_offsets = admin_client.list_consumer_group_offsets(
            consumer_group_topic_partition
        )

        offset_topic_data = {}
        for res, f in topic_latest_offsets.items():
            t = f.result()
            offset_topic_data[str(res.partition)] = t.offset

        offset_consumer_data = {}
        for res, f in consumer_topic_offsets.items():
            t = f.result()
            t_partitions = t.topic_partitions
            for p in t_partitions:
                offset_consumer_data[str(p.partition)] = p.offset

        lags = {
            f"partition_{i[0]}":
                offset_topic_data[i[0]] - offset_consumer_data[i[0]]
            for i in offset_topic_data.items()
        }

        if partition is None:
            return all(threshold > lag_value for lag_value in lags.values())
        else:
            return threshold > lags[f"partition_{partition}"]

    except KafkaException as e:
        raise FailedActivity(
            "Failed to calculate the lag of the consumer group: "
            f"{e}"
        ) from e
