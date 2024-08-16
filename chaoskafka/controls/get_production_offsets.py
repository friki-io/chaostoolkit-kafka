from chaoslib.types import Configuration, Activity, Run, Secrets

import logging

logger = logging.getLogger("chaostoolkit")


def after_activity_control(
    context: Activity,
    state: Run,
    configuration: Configuration = None,
    secrets: Secrets = None,
    **kwargs,
):
    # sourcery skip: remove-unnecessary-cast
    """
    This control is to use with the `produce_messages` action, the idea is
    to check all the produced messages, identify the earliest and the number
    of messages to to consume in the rollback section.

    E.g. Output of produce_messages action:
    '[{'topic': 'orders', 'partition': 1, 'offset': 14},
    {'topic': 'orders', 'partition': 1, 'offset': 15},
    {'topic': 'orders', 'partition': 1, 'offset': 16},
    {'topic': 'orders', 'partition': 1, 'offset': 17},
    {'topic': 'orders', 'partition': 1, 'offset': 18}]
    """

    offsets = [m["offset"] for m in state["output"]]
    partition = state["output"][0]["partition"]
    earliest = min(offsets)
    num_messages = len(offsets)
    configuration["partition"] = partition
    configuration["earliest"] = int(earliest)
    configuration["num_messages"] = int(num_messages)
    logger.debug(
        f"Earliest offset: {earliest}, " f"Number of messages: {num_messages}"
    )
