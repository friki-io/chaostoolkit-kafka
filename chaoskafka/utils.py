from chaoslib.exceptions import FailedActivity


def check_topic_exist(topic, custer_metadata_topics):
    if topic not in custer_metadata_topics:
        raise FailedActivity(f"Topic '{topic}' does not exist")


def delivery_callback(output: list = []):
    def handler(e, msg):
        if e:
            raise FailedActivity(f"Producer error: {e}")
        else:
            output.append(
                dict(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
            )

    return handler
