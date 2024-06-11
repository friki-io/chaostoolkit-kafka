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
  "title": "Reboot MSK broker and check the health of a target topic!!",
  "description": "Experiment to ensure that topics should not have offline partitions after a restart",
  "configuration": {
    "aws_profile_name": "aws-profile-msk",
    "boostrap_servers": "msk_bootstrap_servers:9092",
    "aws_region": "aws_region",
    "cluster_arn": "arn_msk_broker",
    "broker_ids": [
      "1"
    ],
    "recovery_time": 120,
    "topic": "kafka-test-offline"
  },
  "steady-state-hypothesis": {
    "title": "After Rebooting the Kafka broker, the topic shouldn't have offline partitions",
    "probes": [
      {
        "name": "Check that Kafka topic doesn't have offline partitions!!",
        "type": "probe",
        "tolerance": true,
        "provider": {
          "type": "python",
          "module": "chaoskafka.probes",
          "func": "topic_has_no_offline_partitions",
          "arguments": {
            "bootstrap_servers": "${boostrap_servers}",
            "topic": "${topic}"
          }
        }
      }
    ]
  },
  "method": [
    {
      "type": "action",
      "name": "Reboot MSK broker",
      "provider": {
        "type": "python",
        "module": "chaosaws.msk.actions",
        "func": "reboot_msk_broker",
        "arguments": {
          "cluster_arn": "${cluster_arn}",
          "broker_ids": "${broker_ids}"
        }
      },
      "pauses": {
        "after": "${recovery_time}"
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