import logging
from unittest.mock import patch, ANY
from importlib.metadata import PackageNotFoundError


from chaoskafka import discover, load_exported_activities

logger = logging.getLogger("chaostoolkit")


@patch("chaoskafka.initialize_discovery_result")
@patch("chaoskafka.discover_probes")
@patch("chaoskafka.discover_actions")
def test_discover(
    mock_discover_actions,
    mock_discover_probes,
    mock_initialize_discovery_result,
):
    # Mock the return values
    mock_initialize_discovery_result.return_value = {"activities": []}
    mock_discover_probes.return_value = ["probe_activity"]
    mock_discover_actions.return_value = ["action_activity"]

    discovery = discover()

    mock_initialize_discovery_result.assert_called_once_with(
        "chaostoolkit-kafka", ANY, "kafka"
    )

    mock_discover_probes.assert_called_once_with("chaoskafka.probes")
    mock_discover_actions.assert_called_once_with("chaoskafka.actions")

    assert discovery["activities"] == ["probe_activity", "action_activity"]


@patch("chaoskafka.discover_probes")
@patch("chaoskafka.discover_actions")
def test_load_exported_activities(mock_discover_actions, mock_discover_probes):
    mock_discover_probes.return_value = ["probe_activity"]
    mock_discover_actions.return_value = ["action_activity"]

    activities = load_exported_activities()

    mock_discover_probes.assert_called_once_with("chaoskafka.probes")
    mock_discover_actions.assert_called_once_with("chaoskafka.actions")

    assert activities == ["probe_activity", "action_activity"]


@patch("importlib.metadata.version", side_effect=PackageNotFoundError)
def test_version_package_not_found(mock_metadata_version):
    import importlib
    import chaoskafka

    importlib.reload(chaoskafka)

    assert chaoskafka.__version__ == "unknown"
