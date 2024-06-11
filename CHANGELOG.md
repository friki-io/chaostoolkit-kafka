# Changelog

## [Unreleased][]

[Unreleased]: https://github.com/jitapichab/chaostoolkit-kafka

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