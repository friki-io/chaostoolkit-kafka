import logging
from importlib.metadata import version, PackageNotFoundError
from typing import List

from chaoslib.discovery.discover import (
    discover_probes,
    discover_actions,
    initialize_discovery_result,
)
from chaoslib.types import (
    DiscoveredActivities,
    Discovery,
)

logger = logging.getLogger("chaostoolkit")
try:
    __version__ = version("chaostoolkit-kafka")
except PackageNotFoundError:
    __version__ = "unknown"


def discover(discover_system: bool = True) -> Discovery:
    """
    Discover Kafka capabilities from this extension.
    """
    logger.info("Discovering capabilities from chaostoolkit-kafka")

    discovery = initialize_discovery_result(
        "chaostoolkit-kafka", __version__, "kafka"
    )
    discovery["activities"].extend(load_exported_activities())

    return discovery


###############################################################################
# Private functions
###############################################################################
def load_exported_activities() -> List[DiscoveredActivities]:
    """
    Extract metadata from actions, probes and tolerances
    exposed by this extension.
    """
    activities = []  # type: ignore

    activities.extend(discover_probes("chaoskafka.probes"))
    activities.extend(discover_actions("chaoskafka.actions"))

    return activities
