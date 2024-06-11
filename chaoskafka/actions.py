from typing import List
from time import sleep
import threading
import json

import logging

from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets

from chaoskafka.utils import delivery_callback
from chaoskafka.probes import describe_kafka_topic


__all__ = [
    "delete_kafka_topic",
    "rebalance_consumer_group",
    "delete_consumer_group",
    "produce_messages",
    "consume_messages",
]

logger = logging.getLogger("chaostoolkit")


def delete_kafka_topic(
    bootstrap_servers: str = None,
    topic: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> bool:
    """
    Delete a Kafka topic.

    This function deletes a specified Kafka topic from the cluster. It uses the
    Kafka AdminClient to issue the delete operation.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        topic (str): The name of the Kafka topic to delete.

    Returns:
        bool: True if the topic was successfully deleted, False otherwise.

    Raises:
        FailedActivity: If there is an issue deleting the topic.
    """

    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

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
    secrets: Secrets = None,
) -> bool:
    """
    Rebalance a Kafka consumer group.

    This function forces a rebalance of a specified Kafka consumer group by
    subscribing to a given topic, waiting for a short period, and then closing
    the consumer. This action triggers the consumer group to rebalance.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        topic (str): The name of the Kafka topic to subscribe to.
        group_id (str): The consumer group ID.

    Returns:
        bool: True if the rebalance was successfully triggered, False
        otherwise.

    Raises:
        FailedActivity: If the topic is None or if there is an issue
        rebalancing the consumer group.
    """

    if topic is None:
        raise FailedActivity("The topic to subscribe is None")
    try:
        consumer = Consumer(
            {"bootstrap.servers": bootstrap_servers, "group.id": group_id}
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
    bootstrap_servers: str = None,
    group_id: str = None,
    configuration: Configuration = None,
    secrets: Secrets = None,
) -> bool:
    """
    Delete a Kafka consumer group.

    This function deletes a specified Kafka consumer group from the cluster.
    It uses the Kafka AdminClient to issue the delete operation.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        group_id (str): The ID of the consumer group to delete.

    Returns:
        bool: True if the consumer group was successfully deleted, False
        otherwise.

    Raises:
        FailedActivity: If there is an issue deleting the consumer group.

    Note: To delete a consumer group, make sure it has no members.
    """

    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

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
    secrets: Secrets = None,
) -> List[dict]:
    """
    Produce messages to a Kafka topic.

    This function sends a list of messages to a specified Kafka topic using the
    Kafka Producer. Each message is sent to a specified partition within the
    topic.

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        client_id (str): The client ID for the producer.
        topic (str): The name of the Kafka topic to produce messages to.
        messages (List[str]): A list of messages to be sent to the topic.
        partition (int): The partition within the topic to send messages to.

    Returns:
        List[Dict]: A list of dictionaries containing the result of each
                    message produced, with either an "error" key or a
                    "message" key.

    Raises:
        FailedActivity: If the topic is None or if there is an issue producing
        the messages.
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
    secrets: Secrets = None,
) -> List[dict]:
    """
    Consume messages from a Kafka topic.

    This function consumes a specified number of messages from a Kafka topic,
    starting from a given offset in a specific partition. It spawns multiple
    threads to handle the consumers group with diferents clients id that can
    supplant the same consumer group. for more information you can check
    https://kafka.apache.org/24/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html

    Args:
        bootstrap_servers (str): A comma-separated list of Kafka bootstrap
        servers.
        group_id (str): The consumer group ID.
        topic (str): The name of the Kafka topic to consume messages from.
        partition (int): The partition within the topic to consume messages
        from.
        offset (int): The starting offset to consume messages from.
        num_messages (int): The number of messages to consume.

    Returns:
        List[dict]: A list of consumed messages.

    Raises:
        FailedActivity: If the topic is None, the partition does not exist,
                        or if there is an issue consuming the messages.
    """

    logger.debug(f"group_id: {group_id}")
    if topic is None:
        raise FailedActivity("The topic to consume a message is None")
    try:
        topic_data = json.loads(describe_kafka_topic(bootstrap_servers, topic))
        partitions = topic_data["partitions"]
        if partition > partitions:
            raise FailedActivity(
                f"Partition {partition} does not exist in topic {topic}"
            )
        logger.debug(f"Topic {topic} has {partitions} partitions")
        client_suffix = "asign_client"
        client_ids = []
        for i in range(partitions):
            client_id = f"{i}_{client_suffix}"
            client_ids.append(client_id)

        stop_event = threading.Event()
        messages = []

        tasks = [
            threading.Thread(
                name=f"consume_{client_id}",
                target=__consume_messages,
                args=(
                    bootstrap_servers,
                    group_id,
                    topic,
                    partition,
                    offset,
                    num_messages,
                    client_id,
                    stop_event,
                    messages,
                ),
            )
            for client_id in client_ids
        ]

        for task in tasks:
            task.start()

        for task in tasks:
            task.join()

        logger.debug(f"Consumed messages: {messages}")
        return messages
    except Exception as e:
        raise FailedActivity(
            f"Failed to consume message in topic {topic} : {e}"
        ) from e


def __consume_messages(
    bootstrap_servers: str = None,
    group_id: str = "chaostoolkit",
    topic: str = None,
    partition: int = 0,
    offset: int = 0,
    num_messages: int = 1,
    client_id: str = None,
    stop_event: threading.Event = None,
    messages: list = [],
) -> List[dict]:
    """
    Consume messages from a Kafka topic. check the consume_messages function.
    """

    try:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "client.id": client_id,
            }
        )

        logger.info(f"Subscribing to topic {topic}")
        consumer.subscribe([topic])
        logger.info(f"Subscribed to topic {topic}")
        consumed_count = 0

        while (
            num_messages is None or consumed_count < num_messages
        ) and not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.debug(
                        f"End of partition reached {msg.topic()}/"
                        f"{msg.partition()}"
                    )
                    raise KafkaException(msg.error())
            elif msg.partition() == partition:
                if msg.offset() >= offset:
                    logger.info(
                        "We are going to consume the message "
                        f"in the offset: {msg.offset()}"
                    )

                    logger.debug(
                        f"Consumed message from {msg.topic()} partition "
                        f"{msg.partition()} at offset {msg.offset()}:"
                        f" {msg.value().decode('utf-8')}"
                    )
                    messages.append(
                        {
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "value": msg.value().decode("utf-8"),
                        }
                    )
                    consumed_count += 1
                    consumer.commit(msg)
        consumer.close()
        if messages is not None:
            stop_event.set()
    except Exception as e:
        raise Exception(e)
