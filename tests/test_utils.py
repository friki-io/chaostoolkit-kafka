import pytest
from unittest.mock import MagicMock

from chaoskafka.utils import delivery_callback

from chaoslib.exceptions import FailedActivity


def test_delivery_callback_success():
    output = []
    callback = delivery_callback(output)

    msg = MagicMock()
    msg.topic.return_value = "test_topic"
    msg.partition.return_value = 0
    msg.offset.return_value = 42

    callback(None, msg)

    assert len(output) == 1
    assert output[0]["topic"] == "test_topic"
    assert output[0]["partition"] == 0
    assert output[0]["offset"] == 42


def test_delivery_callback_error():
    output = []
    callback = delivery_callback(output)

    error = MagicMock()
    error.__str__.return_value = "Test error"

    with pytest.raises(FailedActivity) as exc_info:
        callback(error, None)

    assert "Producer error: Test error" in str(exc_info.value)
    assert not output
