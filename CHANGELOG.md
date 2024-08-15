# Changelog

## [Unreleased][]

[Unreleased]: https://github.com/friki-io/chaostoolkit-kafka

[0.1.1]: https://github.com/friki-io/chaostoolkit-kafka/tree/0.1.1

## [0.1.1][]

[Unreleased]: https://github.com/friki-io/chaostoolkit-kafka

### Fix

- release github workflows permissions

## [0.1.0][]

[0.1.0]: https://github.com/friki-io/chaostoolkit-kafka/tree/0.1.0

### Added 

- **Actions**:
  - `delete_kafka_topic`
  - `rebalance_consumer_group`
  - `delete_consumer_group`
  - `produce_messages`
  - `consume_messages`

- **Probes**:
  - `describe_kafka_topic`
  - `all_replicas_in_sync`
  - `cluster_doesnt_have_under_replicated_partitions`
  - `check_consumer_lag_under_threshold`
  - `topic_has_no_offline_partitions`

- **Tests**
  - Coverage 100%