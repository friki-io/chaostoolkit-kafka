from typing import List, Optional
from time import sleep

import logging

from confluent_kafka.admin import AdminClient
from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition,
    KafkaError,
    KafkaException)

from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets

from chaoskafka.utils import delivery_callback


__all__ = ["delete_kafka_topic",
           "rebalance_consumer_group",
           "delete_consumer_group",
           "produce_messages",
           "consume_messages"]

logger = logging.getLogger("chaostoolkit")


def delete_kafka_topic(
    bootstrap_servers: str = None,
    topic: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None
) -> bool:
    """
    The next function is to:
    * Delete a kafka topics.
    """
    admin_client = AdminClient(
        {'bootstrap.servers': bootstrap_servers}
    )

    fs = admin_client.delete_topics([topic], operation_timeout=30)

    for topic, f in fs.items():
        try:
            f.result()
            logger.debug(f"Topic {topic} deleted")
        except Exception as e:
            raise FailedActivity(f"Failed to delete topic {topic}: {e}") from e
    return True


def rebalance_consumer_group(
    bootstrap_servers: str = None,
    topic: str = None,
    group_id: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None
) -> bool:
    """
    The next function is to:
    * Rebalance a consumer group.
    """

    if topic is None:
        raise FailedActivity("The topic to subscribe is None")
    try:
        consumer = Consumer(
            {'bootstrap.servers': bootstrap_servers, 'group.id': group_id}
        )
        consumer.subscribe([topic])
        sleep(3)
        consumer.close()

        return True
    except Exception as e:
        raise FailedActivity(
            f"Failed to rebalance consumer group {group_id}: {e}"
        ) from e


def delete_consumer_group(
    bootstrap_servers: str = None, group_id: str = None,
    configuration: Configuration = None, secrets: Secrets = None
) -> bool:
    """
    The next function is to:
    * Delete a consumer group.
    """

    try:
        admin_client = AdminClient(
            {'bootstrap.servers': bootstrap_servers}
        )

        groups = admin_client.delete_consumer_groups(
            [group_id], request_timeout=10
        )
        for group, future in groups.items():
            future.result()  # The result itself is None
            logger.debug(f"Deleted group with id {group_id} successfully")

        return True
    except Exception as e:
        raise FailedActivity(
            f"Failed to delete consumer group {group_id}: {e}"
        ) from e


def produce_messages(
    bootstrap_servers: str = None,
    client_id: str = "chaostoolkit",
    topic: str = None,
    messages: List[str] = [],
    partition: int = 0,
    configuration: Configuration = None,
    secrets: Secrets = None
) -> List[dict]:
    """
    The next function is to:
    * Produce messages to a kafka topic.
    """

    if topic is None:
        raise FailedActivity("The topic to produce a message is None")
    try:
        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
            }
        )
        messages_outputs = []
        for msg in messages:
            message = bytes(msg, "utf-8")
            producer.produce(
                topic,
                value=message,
                partition=partition,
                on_delivery=delivery_callback(messages_outputs),
            )
            producer.flush()
        return messages_outputs
    except Exception as e:
        raise FailedActivity(
            f"Failed to produce message in topic {topic} : {e}"
        ) from e


def consume_messages(
    bootstrap_servers: str = None,
    group_id: str = "chaostoolkit",
    topic: str = None,
    partition: int = 0,
    offset: int = 0,
    num_messages: int = 1,
    configuration: Configuration = None,
    secrets: Secrets = None
) -> List[dict]:
    """
    The next function is to:
    * Consume messages from a kafka topic.
    """
    logger.debug(f"group_id: {group_id}")
    if topic is None:
        raise FailedActivity("The topic to consume a message is None")
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                'enable.auto.commit': False,
                "client.id": "0_test_asign_client"
            }
        )
        topic_partition = TopicPartition(topic, partition, offset)
        consumer.assign([topic_partition])

        messages = []
        consumed_count = 0

        while num_messages is None or consumed_count < num_messages:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())

                logger.debug(f"End of partition reached {msg.topic()}/"
                             f"{msg.partition()}")
                break
            logger.debug(
                        f"Consumed message from {msg.topic()} partition "
                        f"{msg.partition()} at offset {msg.offset()}:"
                        f" {msg.value().decode('utf-8')}"
                    )
            messages.append(msg)
            consumed_count += 1
            consumer.commit(msg)
            sleep(0.5)
        consumer.close()
        return msg.offset()
    except Exception as e:
        raise FailedActivity(
            f"Failed to consume message in topic {topic} : {e}"
        ) from e