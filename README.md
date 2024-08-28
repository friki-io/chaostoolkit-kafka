# Chaos Toolkit Extensions for Kafka

[![Version](https://img.shields.io/pypi/v/chaostoolkit-kafka.svg)](https://img.shields.io/pypi/v/chaostoolkit-kafka.svg)
[![License](https://img.shields.io/pypi/l/chaostoolkit-kafka.svg)](https://img.shields.io/pypi/l/chaostoolkit-kafka.svg)

[![Build](https://github.com/jitapichab/chaostoolkit-kafka/actions/workflows/build.yaml/badge.svg)](https://github.com/jitapichab/chaostoolkit-kafka/actions/workflows/build.yaml)
[![Python versions](https://img.shields.io/pypi/pyversions/chaostoolkit-kafka.svg)](https://www.python.org/)

This project contains Chaos Toolkit activities to create kafka chaos experiments.

## Install

This package requires Python 3.8+

To be used from your experiment, this package must be installed in the Python
environment where [chaostoolkit][] already lives.

[chaostoolkit]: https://github.com/chaostoolkit/chaostoolkit

```
$ pip install chaostoolkit-kafka
```

## Usage

A typical experiment using this extension would look like this:

```json
{
  "title": "Kafka Consumer Lag and Corruption Chaos Experiment",
  "description": "This experiment simulates a Kafka chaos scenario by producing a corrupt message and monitoring whether the consumer group experiences lag exceeding a specified threshold.",
  "configuration": {
    "bootstrap_servers": {
      "type": "env",
      "key": "BOOTSTRAP_SERVERS"
    },
    "topic": "poke-orders",
    "group_id": "poke-order-consumer"
  },
  "steady-state-hypothesis": {
    "title": "Check if the consumer group has lag under threshold",
    "probes": [
      {
        "name": "Consumer group has lag under the threshold",
        "type": "probe",
        "tolerance": true,
        "provider": {
          "type": "python",
          "module": "chaoskafka.probes",
          "func": "check_consumer_lag_under_threshold",
          "arguments": {
            "bootstrap_servers": "${bootstrap_servers}",
            "group_id": "${group_id}",
            "topic": "${topic}",
            "threshold": 15,
            "partition": 1
          }
        }
      }
    ]
  },
  "method": [
    {
      "type": "action",
      "name": "Produce corrupt Kafka message",
      "provider": {
        "type": "python",
        "module": "chaoskafka.actions",
        "func": "produce_messages",
        "arguments": {
          "bootstrap_servers": "${bootstrap_servers}",
          "topic": "${topic}",
          "partition": 1,
          "messages": ["corrupted_message"]
        }
      },
      "pauses": {
        "after": 120
      },
      "controls": [
        {
          "name": "calculate offsets and num_messages",
          "provider": {
            "type": "python",
            "module": "chaoskafka.controls.get_production_offsets"
          }
        }
      ]
    }
  ],
  "rollbacks": [
    {
      "type": "action",
      "name": "Manually Consume Unprocessable Kafka Message",
      "provider": {
        "type": "python",
        "module": "chaoskafka.actions",
        "func": "consume_messages",
        "arguments": {
          "bootstrap_servers": "${bootstrap_servers}",
          "topic": "${topic}",
          "group_id": "${group_id}",
          "partition": 1,
          "offset": "${earliest}",
          "num_messages": "${num_messages}"
        }
      }
    }
  ]
}

```

That's it!

Please explore the code to see existing probes and actions.

## Test

To run the tests for the project execute the following:

```
$ pdm run test
```

### Formatting and Linting

We use [`ruff`][ruff] to both lint and format this repositories code.

[ruff]: https://github.com/astral-sh/ruff

Before raising a Pull Request, we recommend you run formatting against your
code with:

```console
$ pdm run format
```

This will automatically format any code that doesn't adhere to the formatting
standards.

As some things are not picked up by the formatting, we also recommend you run:

```console
$ pdm run lint
```

To ensure that any unused import statements/strings that are too long, etc.
are also picked up.

## Contribute

If you wish to contribute more functions to this package, you are more than
welcome to do so. Please, fork this project, make your changes following the
usual [PEP 8][pep8] code style, sprinkling with tests and submit a PR for
review.

[pep8]: https://peps.python.org/pep-0008/