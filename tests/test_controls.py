from chaoskafka.controls.get_production_offsets import after_activity_control


def test_after_activity_control():
    state = {
        "output": [
            {"topic": "orders", "partition": 1, "offset": 14},
            {"topic": "orders", "partition": 1, "offset": 15},
            {"topic": "orders", "partition": 1, "offset": 16},
            {"topic": "orders", "partition": 1, "offset": 17},
            {"topic": "orders", "partition": 1, "offset": 18},
        ]
    }

    configuration = {}

    after_activity_control(
        context=None, state=state, configuration=configuration, secrets=None
    )

    assert configuration["partition"] == 1
    assert configuration["earliest"] == 14
    assert configuration["num_messages"] == 5
