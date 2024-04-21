from typing import Dict

from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets

from confluent_kafka.admin import KafkaException, AdminClient
import json


__all__ = [
    "describe_kafka_topic",
    "all_replicas_in_sync",
    "cluster_doesnt_have_under_replicated_partitions"
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
