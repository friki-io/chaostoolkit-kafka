
from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets
from time import sleep
import logging

from confluent_kafka.admin import AdminClient, KafkaException
from confluent_kafka import Consumer

__all__ = ["delete_kafka_topic",
           "rebalance_consumer_group",
           "delete_consumer_group"]

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
        raise FailedActivity(
            f"the topic {topic} to subscribe is empty"
        )
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
            try:
                future.result()  # The result itself is None
                logger.debug(f"Deleted group with id {group_id} successfully")
            except KafkaException as e:
                raise f"Error deleting group id {group_id}: {e}" from e
            except Exception:
                raise

        return True
    except Exception as e:
        raise FailedActivity(
            f"Failed to delete consumer group {group_id}: {e}"
        ) from e
