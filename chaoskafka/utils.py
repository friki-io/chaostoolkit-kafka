from chaoslib.exceptions import FailedActivity


def check_topic_exist(topic, custer_metadata_topics):
    if topic not in custer_metadata_topics:
        raise FailedActivity(f"Topic '{topic}' does not exist")

